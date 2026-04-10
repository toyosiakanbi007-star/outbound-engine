// src/jobs/prequal_worker/batch.rs
//
// PREQUAL_BATCH Job Handler.
//
// PURPOSE:
//   Executor job that processes a batch of company_candidates through the
//   Azure Function with mode="prequal" (which runs Phase0 + Prequal internally).
//
// FLOW (per company):
//   1. Acquire per-candidate lock (atomic status check)
//   2. Load company info (name, domain) from companies table
//   3. Load client_context (from client_configs + ICP)
//   4. Build Azure Function request: { mode: "prequal", client_id, company_id, company_name, domain, client_context }
//   5. POST to Azure Function
//   6. Parse response — extract qualifies/score from nested `prequal` object
//   7. Update candidate status → qualified/disqualified
//   8. On failure: increment attempts, store error, continue with next company
//
// RESPONSE STRUCTURE (from Azure Function):
//   The Azure Function returns a nested JSON:
//   {
//     "prequal": {
//       "qualifies": bool,
//       "score": float,
//       "staleness_adjusted_score": float,
//       "gates_passed": [...],
//       "gates_failed": [...],
//       ...
//     },
//     "company_snapshot": { ... },
//     "offer_fit": { "icp_fit": "qualify", "tags": [...], ... },
//     "pain_hypotheses": [ ... ],
//     "_prequal_reasons": { "summary": "...", "tags": [...], ... },  // if LLM enabled
//     "_meta": { ... }
//   }
//
//   `qualifies` and `score` live inside the `prequal` sub-object — NOT at top level.
//   The Azure Function's progressive_writer already writes the full prequal result
//   (including _prequal_reasons, offer_fit_tags, full_result) to the company_prequal
//   table. This Rust handler only needs to update company_candidates status.
//
// SAFETY:
//   - Each company is processed independently — one failure doesn't stop the batch.
//   - Atomic status check prevents double-processing.
//   - Already-done candidates are skipped (unless force=true).

use reqwest::Client as HttpClient;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::db;
use super::models::{BatchOutcome, CandidateCompanyInfo, CompanyPrequalResult, PrequalBatchPayload};

// ============================================================================
// Config
// ============================================================================

fn get_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

lazy_static::lazy_static! {
    static ref AZURE_FUNCTION_URL: String = get_env("AZURE_FUNCTION_URL", "");
    static ref AZURE_FUNCTION_KEY: String = get_env("AZURE_FUNCTION_KEY", "");
}

// ============================================================================
// Main Handler
// ============================================================================

/// Handle a PREQUAL_BATCH job.
///
/// Processes each candidate in the batch independently.
/// Returns a summary of the batch execution.
pub async fn handle_prequal_batch(
    pool: &PgPool,
    http_client: &HttpClient,
    payload: &PrequalBatchPayload,
    worker_id: &str,
) -> Result<BatchOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let client_id = payload.client_id;
    let batch_id = payload.batch_id;

    info!(
        "Worker {}: starting PREQUAL_BATCH {} for client {} ({} candidates, force={})",
        worker_id, batch_id, client_id, payload.candidate_ids.len(), payload.force
    );

    // Load config for retry limits
    let config = db::load_prequal_config(pool, client_id).await?;

    // Load client_context once for the entire batch (same client)
    let client_context = db::load_client_context(pool, client_id).await?;

    if client_context.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        warn!(
            "Worker {}: empty client_context for client {} — prequal may produce poor results",
            worker_id, client_id
        );
    }

    // Load candidate + company info
    let candidates = db::load_candidate_company_info(pool, &payload.candidate_ids).await?;

    if candidates.is_empty() {
        warn!(
            "Worker {}: no candidates found for batch {} (may have been deleted)",
            worker_id, batch_id
        );
        return Ok(BatchOutcome {
            batch_id,
            ..Default::default()
        });
    }

    let mut outcome = BatchOutcome {
        batch_id,
        total: candidates.len() as i32,
        ..Default::default()
    };

    // Process each candidate independently
    for candidate in &candidates {
        let result = process_single_candidate(
            pool,
            http_client,
            client_id,
            candidate,
            &client_context,
            &config,
            payload.force,
            worker_id,
        )
        .await;

        match result {
            Ok(pr) => {
                outcome.processed += 1;
                if pr.success {
                    outcome.succeeded += 1;
                    if pr.qualifies == Some(true) {
                        outcome.qualified += 1;
                    } else {
                        outcome.disqualified += 1;
                    }
                } else {
                    outcome.failed += 1;
                }
            }
            Err(_) => {
                // process_single_candidate should never return Err
                // (it handles errors internally), but just in case:
                outcome.failed += 1;
            }
        }
    }

    outcome.elapsed_seconds = start.elapsed().as_secs();

    info!(
        "Worker {}: PREQUAL_BATCH {} complete — total={}, processed={}, skipped={}, \
         succeeded={}, failed={}, qualified={}, disqualified={}, elapsed={}s",
        worker_id, batch_id,
        outcome.total, outcome.processed, outcome.skipped,
        outcome.succeeded, outcome.failed,
        outcome.qualified, outcome.disqualified,
        outcome.elapsed_seconds
    );

    Ok(outcome)
}

// ============================================================================
// Per-Company Processing
// ============================================================================

/// Process a single candidate through the Azure Function prequal pipeline.
///
/// Never returns Err — failures are captured in CompanyPrequalResult
/// and the candidate's DB status is updated accordingly.
async fn process_single_candidate(
    pool: &PgPool,
    http_client: &HttpClient,
    client_id: Uuid,
    candidate: &CandidateCompanyInfo,
    client_context: &JsonValue,
    config: &super::models::PrequalConfig,
    force: bool,
    worker_id: &str,
) -> Result<CompanyPrequalResult, ()> {
    let cid = candidate.candidate_id;
    let company_id = candidate.company_id;

    // 1. Acquire lock (atomic status check)
    match db::mark_candidate_in_progress(pool, cid, force).await {
        Ok(true) => {
            debug!(
                "Worker {}: locked candidate {} (company={}, name={})",
                worker_id, cid, company_id, candidate.company_name
            );
        }
        Ok(false) => {
            debug!(
                "Worker {}: skipping candidate {} — already processed or locked",
                worker_id, cid
            );
            return Ok(CompanyPrequalResult {
                candidate_id: cid,
                company_id,
                success: false,
                qualifies: None,
                score: None,
                error: Some("already processed or locked".into()),
            });
        }
        Err(e) => {
            warn!(
                "Worker {}: failed to lock candidate {}: {}",
                worker_id, cid, e
            );
            return Ok(CompanyPrequalResult {
                candidate_id: cid,
                company_id,
                success: false,
                qualifies: None,
                score: None,
                error: Some(format!("lock failed: {}", e)),
            });
        }
    }

    // 2. Build request
    let domain = candidate.domain.clone().unwrap_or_default();
    let request_body = build_prequal_request(
        client_id,
        company_id,
        &candidate.company_name,
        &domain,
        client_context,
    );

    // 3. Call Azure Function
    let call_result = call_azure_prequal(
        http_client,
        &request_body,
        config.azure_function_timeout_secs,
    )
    .await;

    match call_result {
        Ok(response) => {
            // 4. Extract qualifies/score from nested prequal object.
            //
            //    Azure Function returns:
            //    {
            //      "prequal": {
            //        "qualifies": bool,
            //        "score": float,
            //        "staleness_adjusted_score": float,
            //        "final_score": float,
            //        ...
            //      },
            //      ...
            //    }
            //
            //    The progressive_writer inside the Azure Function has already
            //    written the full result (including _prequal_reasons and
            //    offer_fit_tags) to the company_prequal table. We only need
            //    to extract qualifies/score for the candidate status update.

            let prequal_obj = response.get("prequal");

            let qualifies = prequal_obj
                .and_then(|p| p.get("qualifies"))
                .and_then(|v| v.as_bool())
                // Fallback: check top-level (backward compat with older AF versions)
                .or_else(|| response.get("qualifies").and_then(|v| v.as_bool()))
                .unwrap_or(false);

            let score = prequal_obj
                .and_then(|p| p.get("staleness_adjusted_score"))
                .and_then(|v| v.as_f64())
                // Fallback: try final_score, then score at prequal level
                .or_else(|| prequal_obj.and_then(|p| p.get("final_score")).and_then(|v| v.as_f64()))
                .or_else(|| prequal_obj.and_then(|p| p.get("score")).and_then(|v| v.as_f64()))
                // Fallback: top-level score (backward compat)
                .or_else(|| response.get("score").and_then(|v| v.as_f64()));

            let has_reasons = response.get("_prequal_reasons").is_some();

            info!(
                "Worker {}: prequal complete for {} ({}) — qualifies={}, score={:.3}, has_reasons={}",
                worker_id,
                candidate.company_name,
                company_id,
                qualifies,
                score.unwrap_or(0.0),
                has_reasons,
            );

            // 5. Update candidate status (qualified/disqualified)
            if let Err(e) = db::update_candidate_success(pool, cid, qualifies, None).await {
                error!(
                    "Worker {}: failed to update candidate {} after success: {}",
                    worker_id, cid, e
                );
            }

            Ok(CompanyPrequalResult {
                candidate_id: cid,
                company_id,
                success: true,
                qualifies: Some(qualifies),
                score,
                error: None,
            })
        }
        Err(err_msg) => {
            warn!(
                "Worker {}: prequal failed for {} ({}): {}",
                worker_id, candidate.company_name, company_id, err_msg
            );

            // 6. Update candidate with failure
            if let Err(e) = db::update_candidate_failure(
                pool,
                cid,
                &err_msg,
                config.max_attempts_per_company,
            )
            .await
            {
                error!(
                    "Worker {}: failed to update candidate {} after failure: {}",
                    worker_id, cid, e
                );
            }

            Ok(CompanyPrequalResult {
                candidate_id: cid,
                company_id,
                success: false,
                qualifies: None,
                score: None,
                error: Some(err_msg),
            })
        }
    }
}

// ============================================================================
// Azure Function Client
// ============================================================================

/// Build the request body for mode="prequal".
fn build_prequal_request(
    client_id: Uuid,
    company_id: Uuid,
    company_name: &str,
    domain: &str,
    client_context: &JsonValue,
) -> JsonValue {
    json!({
        "client_id": client_id,
        "company_id": company_id,
        "company_name": company_name,
        "domain": domain,
        "mode": "prequal",
        "debug": false,
        "client_context": client_context,
    })
}

/// Call the Azure Function with mode="prequal".
///
/// Returns the parsed JSON response on success, or an error message string.
async fn call_azure_prequal(
    http_client: &HttpClient,
    request_body: &JsonValue,
    timeout_secs: u64,
) -> Result<JsonValue, String> {
    if AZURE_FUNCTION_URL.is_empty() {
        return Err("AZURE_FUNCTION_URL not configured".to_string());
    }

    let url = format!(
        "{}/api/news-fetch",
        AZURE_FUNCTION_URL.trim_end_matches('/')
    );

    let mut req_builder = http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .json(request_body);

    // Add function key if configured
    if !AZURE_FUNCTION_KEY.is_empty() {
        req_builder = req_builder.header("x-functions-key", AZURE_FUNCTION_KEY.as_str());
    }

    let company_name = request_body
        .get("company_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    debug!("Calling Azure Function prequal for {}: {}", company_name, url);

    let response = req_builder
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    let status = response.status();

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Azure Function returned {}: {}", status, truncate(&body, 500)));
    }

    let data: JsonValue = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response JSON: {}", e))?;

    Ok(data)
}

/// Truncate a string with "..." suffix if it exceeds max_len.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_prequal_request() {
        let client_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let company_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let context = json!({"brand_name": "TestBrand", "niche": "CLM"});

        let req = build_prequal_request(
            client_id,
            company_id,
            "Acme Corp",
            "acme.com",
            &context,
        );

        assert_eq!(req["mode"], "prequal");
        assert_eq!(req["company_name"], "Acme Corp");
        assert_eq!(req["domain"], "acme.com");
        assert_eq!(req["client_context"]["brand_name"], "TestBrand");
        assert_eq!(req["debug"], false);
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 5), "hello...");
    }

    /// Verify we correctly extract qualifies/score from the nested prequal object
    /// (the actual Azure Function response structure).
    #[test]
    fn test_extract_qualifies_from_nested_prequal() {
        let response = json!({
            "company_id": "abc-123",
            "prequal": {
                "qualifies": false,
                "score": 0.383,
                "final_score": 0.383,
                "raw_score": 1.0,
                "staleness_adjusted_score": 0.383,
                "gates_passed": ["evidence", "sources", "why_now"],
                "gates_failed": ["score<0.5", "recency"],
            },
            "offer_fit": {
                "icp_fit": "qualify",
                "tags": ["icp_match", "industry_fit"],
            },
            "_prequal_reasons": {
                "qualifies": false,
                "summary": "Disqualified due to stale evidence.",
                "reasons": ["Score below threshold"],
                "tags": ["recency", "low_score"],
                "confidence": 0.9,
            },
        });

        let prequal_obj = response.get("prequal");

        // qualifies from prequal sub-object
        let qualifies = prequal_obj
            .and_then(|p| p.get("qualifies"))
            .and_then(|v| v.as_bool())
            .or_else(|| response.get("qualifies").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        assert_eq!(qualifies, false);

        // score from staleness_adjusted_score
        let score = prequal_obj
            .and_then(|p| p.get("staleness_adjusted_score"))
            .and_then(|v| v.as_f64())
            .or_else(|| prequal_obj.and_then(|p| p.get("final_score")).and_then(|v| v.as_f64()))
            .or_else(|| prequal_obj.and_then(|p| p.get("score")).and_then(|v| v.as_f64()))
            .or_else(|| response.get("score").and_then(|v| v.as_f64()));
        assert!((score.unwrap() - 0.383).abs() < 0.001);

        // has_reasons
        assert!(response.get("_prequal_reasons").is_some());
    }

    /// Verify backward compat: top-level qualifies/score still works for older AF versions.
    #[test]
    fn test_extract_qualifies_backward_compat() {
        let response = json!({
            "qualifies": true,
            "score": 0.75,
        });

        let prequal_obj = response.get("prequal"); // None

        let qualifies = prequal_obj
            .and_then(|p| p.get("qualifies"))
            .and_then(|v| v.as_bool())
            .or_else(|| response.get("qualifies").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        assert_eq!(qualifies, true);

        let score = prequal_obj
            .and_then(|p| p.get("staleness_adjusted_score"))
            .and_then(|v| v.as_f64())
            .or_else(|| prequal_obj.and_then(|p| p.get("final_score")).and_then(|v| v.as_f64()))
            .or_else(|| prequal_obj.and_then(|p| p.get("score")).and_then(|v| v.as_f64()))
            .or_else(|| response.get("score").and_then(|v| v.as_f64()));
        assert!((score.unwrap() - 0.75).abs() < 0.001);
    }

    /// Edge case: prequal object exists but qualifies is missing → default false.
    #[test]
    fn test_extract_qualifies_missing_field() {
        let response = json!({
            "prequal": {
                "score": 0.6,
            },
        });

        let prequal_obj = response.get("prequal");

        let qualifies = prequal_obj
            .and_then(|p| p.get("qualifies"))
            .and_then(|v| v.as_bool())
            .or_else(|| response.get("qualifies").and_then(|v| v.as_bool()))
            .unwrap_or(false);
        assert_eq!(qualifies, false);
    }
}
