// src/jobs/prequal_worker/batch.rs
//
// PREQUAL_BATCH Job Handler.
//
// FLOW (per company):
//   1. Acquire per-candidate lock (atomic status check)
//   2. Build Azure Function request
//   3. Fire HTTP POST to Azure Function
//   4. If HTTP succeeds → extract qualifies/score from response
//   5. If HTTP fails/timeouts → poll DB for results (progressive_writer writes directly)
//   6. Update candidate status based on what we find
//
// KEY INSIGHT: The Azure Function's progressive_writer writes results directly
// to the DB (company_prequal, v3_hypotheses, etc.) BEFORE the HTTP response
// comes back. So if we get a 504/timeout, the data may already be there.
// We poll the DB as the source of truth, not the HTTP response.

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

/// How long to poll the DB after an ambiguous HTTP result (seconds)
const DB_POLL_TIMEOUT_SECS: u64 = 360; // 6 minutes

/// How often to check the DB during polling (seconds)
const DB_POLL_INTERVAL_SECS: u64 = 15;

// ============================================================================
// Azure Function call result — 3-way outcome
// ============================================================================

enum AzureCallOutcome {
    /// HTTP 200 with parsed JSON response
    Success(JsonValue),
    /// Definite failure — don't bother polling (400, config error, parse error)
    DefiniteFailure(String),
    /// Ambiguous — HTTP failed but progressive_writer may have written results
    /// (504, timeout, connection reset, 502, 503). Poll DB to check.
    Ambiguous(String),
}

// ============================================================================
// Main Handler
// ============================================================================

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

    let config = db::load_prequal_config(pool, client_id).await?;
    let client_context = db::load_client_context(pool, client_id).await?;

    if client_context.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        warn!(
            "Worker {}: empty client_context for client {} — prequal may produce poor results",
            worker_id, client_id
        );
    }

    let candidates = db::load_candidate_company_info(pool, &payload.candidate_ids).await?;

    if candidates.is_empty() {
        warn!("Worker {}: no candidates found for batch {}", worker_id, batch_id);
        return Ok(BatchOutcome { batch_id, ..Default::default() });
    }

    let mut outcome = BatchOutcome {
        batch_id,
        total: candidates.len() as i32,
        ..Default::default()
    };

    for candidate in &candidates {
        let result = process_single_candidate(
            pool, http_client, client_id, candidate, &client_context,
            &config, payload.force, worker_id,
        ).await;

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
            Err(_) => { outcome.failed += 1; }
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

    // 1. Acquire lock
    match db::mark_candidate_in_progress(pool, cid, force).await {
        Ok(true) => {
            debug!("Worker {}: locked candidate {} (company={})", worker_id, cid, company_id);
        }
        Ok(false) => {
            debug!("Worker {}: skipping candidate {} — already processed or locked", worker_id, cid);
            return Ok(CompanyPrequalResult {
                candidate_id: cid, company_id, success: false, qualifies: None, score: None,
                error: Some("already processed or locked".into()),
            });
        }
        Err(e) => {
            warn!("Worker {}: failed to lock candidate {}: {}", worker_id, cid, e);
            return Ok(CompanyPrequalResult {
                candidate_id: cid, company_id, success: false, qualifies: None, score: None,
                error: Some(format!("lock failed: {}", e)),
            });
        }
    }

    // 2. Record the timestamp before calling Azure Function
    //    We'll use this to find results written by progressive_writer
    let before_call = chrono::Utc::now();

    // 3. Build request
    let domain = candidate.domain.clone().unwrap_or_default();
    let request_body = build_prequal_request(
        client_id, company_id, &candidate.company_name, &domain, client_context,
    );

    // 4. Fire HTTP call to Azure Function
    let call_outcome = call_azure_prequal(
        http_client, &request_body, config.azure_function_timeout_secs,
    ).await;

    // 5. Process outcome
    match call_outcome {
        AzureCallOutcome::Success(response) => {
            // HTTP 200 — extract from response directly
            let (qualifies, score) = extract_qualifies_score(&response);
            info!(
                "Worker {}: prequal complete for {} ({}) — qualifies={}, score={:.3} [from HTTP response]",
                worker_id, candidate.company_name, company_id, qualifies, score.unwrap_or(0.0),
            );
            if let Err(e) = db::update_candidate_success(pool, cid, qualifies, None).await {
                error!("Worker {}: failed to update candidate {}: {}", worker_id, cid, e);
            }
            Ok(CompanyPrequalResult {
                candidate_id: cid, company_id, success: true,
                qualifies: Some(qualifies), score, error: None,
            })
        }

        AzureCallOutcome::DefiniteFailure(reason) => {
            // Real error (400, config missing, etc.) — no point polling
            warn!(
                "Worker {}: prequal DEFINITE FAILURE for {} ({}): {}",
                worker_id, candidate.company_name, company_id, reason
            );
            if let Err(e) = db::update_candidate_failure(
                pool, cid, &reason, config.max_attempts_per_company,
            ).await {
                error!("Worker {}: failed to update candidate {}: {}", worker_id, cid, e);
            }
            Ok(CompanyPrequalResult {
                candidate_id: cid, company_id, success: false,
                qualifies: None, score: None, error: Some(reason),
            })
        }

        AzureCallOutcome::Ambiguous(reason) => {
            // Timeout / 504 / 502 / connection reset — poll DB for results
            info!(
                "Worker {}: HTTP ambiguous for {} ({}): {} — polling DB for progressive_writer results...",
                worker_id, candidate.company_name, company_id, reason
            );

            match poll_db_for_results(pool, company_id, &before_call, worker_id).await {
                Some((qualifies, score)) => {
                    info!(
                        "Worker {}: prequal complete for {} ({}) — qualifies={}, score={:.3} [from DB poll after: {}]",
                        worker_id, candidate.company_name, company_id, qualifies, score.unwrap_or(0.0), reason,
                    );
                    if let Err(e) = db::update_candidate_success(pool, cid, qualifies, None).await {
                        error!("Worker {}: failed to update candidate {}: {}", worker_id, cid, e);
                    }
                    Ok(CompanyPrequalResult {
                        candidate_id: cid, company_id, success: true,
                        qualifies: Some(qualifies), score, error: None,
                    })
                }
                None => {
                    let full_reason = format!(
                        "HTTP: {} | DB poll: no results found after {}s of polling",
                        reason, DB_POLL_TIMEOUT_SECS
                    );
                    warn!(
                        "Worker {}: prequal FAILED for {} ({}) — {}",
                        worker_id, candidate.company_name, company_id, full_reason
                    );
                    if let Err(e) = db::update_candidate_failure(
                        pool, cid, &full_reason, config.max_attempts_per_company,
                    ).await {
                        error!("Worker {}: failed to update candidate {}: {}", worker_id, cid, e);
                    }
                    Ok(CompanyPrequalResult {
                        candidate_id: cid, company_id, success: false,
                        qualifies: None, score: None, error: Some(full_reason),
                    })
                }
            }
        }
    }
}

// ============================================================================
// DB Polling — check if progressive_writer wrote results
// ============================================================================

/// Poll company_prequal table for results that appeared after `after_time`.
/// Returns Some((qualifies, score)) if found, None if timed out.
async fn poll_db_for_results(
    pool: &PgPool,
    company_id: Uuid,
    after_time: &chrono::DateTime<chrono::Utc>,
    worker_id: &str,
) -> Option<(bool, Option<f64>)> {
    let poll_start = Instant::now();
    let mut attempts = 0u32;

    loop {
        attempts += 1;

        // Check company_prequal for a row created after our HTTP call
        let result = sqlx::query_as::<_, (bool, f64)>(
            r#"SELECT qualifies, score
               FROM company_prequal
               WHERE company_id = $1 AND created_at >= $2
               ORDER BY created_at DESC LIMIT 1"#
        )
        .bind(company_id)
        .bind(after_time)
        .fetch_optional(pool)
        .await;

        match result {
            Ok(Some((qualifies, score))) => {
                info!(
                    "Worker {}: DB poll found results for company {} after {} attempts ({}s) — qualifies={}, score={:.3}",
                    worker_id, company_id, attempts, poll_start.elapsed().as_secs(), qualifies, score,
                );
                return Some((qualifies, Some(score)));
            }
            Ok(None) => {
                // No results yet — keep polling
            }
            Err(e) => {
                warn!("Worker {}: DB poll query error for company {}: {}", worker_id, company_id, e);
                // Don't abort — query might work next time
            }
        }

        // Check timeout
        if poll_start.elapsed().as_secs() >= DB_POLL_TIMEOUT_SECS {
            info!(
                "Worker {}: DB poll timed out for company {} after {} attempts ({}s)",
                worker_id, company_id, attempts, poll_start.elapsed().as_secs(),
            );
            return None;
        }

        // Wait before next poll
        tokio::time::sleep(std::time::Duration::from_secs(DB_POLL_INTERVAL_SECS)).await;
    }
}

// ============================================================================
// Extract qualifies/score from Azure Function response
// ============================================================================

fn extract_qualifies_score(response: &JsonValue) -> (bool, Option<f64>) {
    let prequal_obj = response.get("prequal");

    let qualifies = prequal_obj
        .and_then(|p| p.get("qualifies"))
        .and_then(|v| v.as_bool())
        .or_else(|| response.get("qualifies").and_then(|v| v.as_bool()))
        .unwrap_or(false);

    let score = prequal_obj
        .and_then(|p| p.get("staleness_adjusted_score"))
        .and_then(|v| v.as_f64())
        .or_else(|| prequal_obj.and_then(|p| p.get("final_score")).and_then(|v| v.as_f64()))
        .or_else(|| prequal_obj.and_then(|p| p.get("score")).and_then(|v| v.as_f64()))
        .or_else(|| response.get("score").and_then(|v| v.as_f64()));

    (qualifies, score)
}

// ============================================================================
// Azure Function Client
// ============================================================================

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
/// Returns a 3-way outcome:
///   - Success: HTTP 200 with parsed response
///   - DefiniteFailure: real error, don't bother polling DB
///   - Ambiguous: timeout/502/503/504/connection error — poll DB
async fn call_azure_prequal(
    http_client: &HttpClient,
    request_body: &JsonValue,
    timeout_secs: u64,
) -> AzureCallOutcome {
    if AZURE_FUNCTION_URL.is_empty() {
        return AzureCallOutcome::DefiniteFailure("AZURE_FUNCTION_URL not configured".into());
    }

    let url = format!("{}/api/news-fetch", AZURE_FUNCTION_URL.trim_end_matches('/'));

    let company_name = request_body
        .get("company_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    debug!("Calling Azure Function prequal for {}: {}", company_name, url);

    let mut req_builder = http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .json(request_body);

    if !AZURE_FUNCTION_KEY.is_empty() {
        req_builder = req_builder.header("x-functions-key", AZURE_FUNCTION_KEY.as_str());
    }

    // Send request
    let response = match req_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            let reason = if e.is_timeout() {
                format!("HTTP timeout after {}s — Azure Function did not respond in time", timeout_secs)
            } else if e.is_connect() {
                format!("Connection error: {} — Azure Function may be cold-starting or down", e)
            } else if e.is_request() {
                format!("Request error: {}", e)
            } else {
                format!("HTTP error: {}", e)
            };

            // Timeouts and connection errors are ambiguous — function may still be running
            return if e.is_timeout() || e.is_connect() {
                AzureCallOutcome::Ambiguous(reason)
            } else {
                AzureCallOutcome::DefiniteFailure(reason)
            };
        }
    };

    let status = response.status();
    let status_u16 = status.as_u16();

    // 200 OK — success
    if status.is_success() {
        match response.json::<JsonValue>().await {
            Ok(data) => return AzureCallOutcome::Success(data),
            Err(e) => {
                // Got 200 but couldn't parse body — ambiguous (function ran, body was truncated?)
                return AzureCallOutcome::Ambiguous(
                    format!("HTTP 200 but failed to parse response body: {} — function likely completed", e)
                );
            }
        }
    }

    // Read error body
    let body = response.text().await.unwrap_or_else(|_| "(could not read body)".into());

    // Classify by status code
    match status_u16 {
        // Gateway errors — function is probably still running
        502 => AzureCallOutcome::Ambiguous(
            format!("HTTP 502 Bad Gateway: {} — Azure proxy error, function may still be running", truncate(&body, 200))
        ),
        503 => AzureCallOutcome::Ambiguous(
            format!("HTTP 503 Service Unavailable: {} — function may be scaling or overloaded", truncate(&body, 200))
        ),
        504 => AzureCallOutcome::Ambiguous(
            format!("HTTP 504 Gateway Timeout: {} — Azure gateway timed out but function may still be writing results", truncate(&body, 200))
        ),

        // Client errors — definite failure, no point polling
        400 => AzureCallOutcome::DefiniteFailure(
            format!("HTTP 400 Bad Request: {} — request was malformed", truncate(&body, 300))
        ),
        401 | 403 => AzureCallOutcome::DefiniteFailure(
            format!("HTTP {} Unauthorized/Forbidden: {} — check AZURE_FUNCTION_KEY", status_u16, truncate(&body, 200))
        ),
        404 => AzureCallOutcome::DefiniteFailure(
            format!("HTTP 404 Not Found: {} — check AZURE_FUNCTION_URL", truncate(&body, 200))
        ),
        422 => AzureCallOutcome::DefiniteFailure(
            format!("HTTP 422 Unprocessable: {} — Azure Function rejected the input", truncate(&body, 300))
        ),

        // 500 Internal Server Error — function crashed, but progressive_writer may have
        // already written partial results. Treat as ambiguous.
        500 => AzureCallOutcome::Ambiguous(
            format!("HTTP 500 Internal Server Error: {} — function crashed but may have written partial results", truncate(&body, 300))
        ),

        // 429 Rate limited — ambiguous (retry could work)
        429 => AzureCallOutcome::Ambiguous(
            format!("HTTP 429 Too Many Requests: {} — rate limited, will check DB for results", truncate(&body, 200))
        ),

        // Everything else — definite failure
        _ => AzureCallOutcome::DefiniteFailure(
            format!("HTTP {} {}: {}", status_u16, status.canonical_reason().unwrap_or("Unknown"), truncate(&body, 300))
        ),
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len { s.to_string() } else { format!("{}...", &s[..max_len]) }
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

        let req = build_prequal_request(client_id, company_id, "Acme Corp", "acme.com", &context);

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

    #[test]
    fn test_extract_qualifies_from_nested_prequal() {
        let response = json!({
            "prequal": {
                "qualifies": false,
                "score": 0.383,
                "staleness_adjusted_score": 0.383,
                "gates_passed": ["evidence", "sources", "why_now"],
                "gates_failed": ["score<0.5", "recency"],
            },
        });
        let (qualifies, score) = extract_qualifies_score(&response);
        assert_eq!(qualifies, false);
        assert!((score.unwrap() - 0.383).abs() < 0.001);
    }

    #[test]
    fn test_extract_qualifies_backward_compat() {
        let response = json!({ "qualifies": true, "score": 0.75 });
        let (qualifies, score) = extract_qualifies_score(&response);
        assert_eq!(qualifies, true);
        assert!((score.unwrap() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_extract_qualifies_missing_field() {
        let response = json!({ "prequal": { "score": 0.6 } });
        let (qualifies, _) = extract_qualifies_score(&response);
        assert_eq!(qualifies, false);
    }
}
