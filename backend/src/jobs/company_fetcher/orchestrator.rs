// src/jobs/company_fetcher/orchestrator.rs
//
// Company Fetcher Orchestrator — main entry point for `discover_companies` jobs.
//
// Called by `worker.rs` when it dequeues a job with type `discover_companies`.
// Owns the entire lifecycle of a fetch run.
//
// PROVIDER-AGNOSTIC: Accepts any `OrgSearchProvider` (Apollo or Diffbot).
// The orchestrator builds Apollo-format requests (the common interface),
// and the provider translates them into the appropriate API calls.
//
// FLOW:
//   1. Parse payload → load ICP profile → compute quota plan
//   2. Create fetch run row → seed deduper from existing companies
//   3. Main loop: true round-robin across industries
//      a. For current industry at its current variant:
//         - Paginate provider results until page budget or variant exhausted
//         - Dedup + upsert each page → insert candidates → update counters
//         - On exhaustion: advance variant ladder or mark industry depleted
//      b. Rotate to next industry
//   4. Finalize run (succeeded / depleted / partial / failed)
//   5. Update industry yield metrics
//   6. Enqueue downstream prequal jobs (if configured)
//
// STOP CONDITIONS (checked every page):
//   - Industry quota met → move to next industry
//   - All industries done or depleted → run complete
//   - All variant results exhausted → mark industry depleted
//   - Wall-clock runtime exceeded → finalize as partial
//   - Global unique_upserted >= batch_target → finalize as succeeded
//   - 2+ consecutive high-dupe pages (>80%) → exhaust current variant

use serde_json::json;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::jobs::prequal_worker::db as prequal_db;
use super::apollo_client::ApolloApiError;
use super::providers::OrgSearchProvider;
use super::dedup::{self, BatchDeduper};
use super::models::{
    DiscoverCompaniesPayload, FetchQueryStatus, IcpFetchProfile,
    IndustryProgress, QueryVariant,
};
use super::query_variants::VariantBuilder;
use super::quota_planner;
use super::db;

// ============================================================================
// Constants
// ============================================================================

/// Milliseconds to sleep between consecutive Apollo pages.
/// Keeps us well under rate limits and avoids slamming the API.
const INTER_PAGE_DELAY_MS: u64 = 300;

/// Number of pages to fetch per industry per round-robin turn.
/// Prevents one industry from monopolizing a turn.
const PAGES_PER_TURN: i32 = 5;

/// If this fraction of a page's results are duplicates, count it as a high-dupe page.
const HIGH_DUPE_THRESHOLD: f64 = 0.80;

/// How many consecutive high-dupe pages before we exhaust the current variant.
const MAX_CONSECUTIVE_HIGH_DUPE: i32 = 2;

/// If the first page of a "tight" query returns fewer than this many orgs,
/// fall back to a "loose" query (drop include_keywords_any, keep exclude_keywords).
const LOOSE_FALLBACK_THRESHOLD: usize = 20;

// ============================================================================
// Error Type
// ============================================================================

/// Errors from the orchestrator.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("Database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("Apollo API error: {0}")]
    Apollo(#[from] ApolloApiError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("ICP profile missing for client {0}")]
    MissingIcp(Uuid),

    #[error("ICP profile invalid: {0}")]
    InvalidIcp(String),
}

// ============================================================================
// Run Result
// ============================================================================

/// Summary returned to the worker after a run completes.
#[derive(Debug)]
pub struct RunResult {
    pub run_id: Uuid,
    pub status: String,
    pub raw_fetched: i32,
    pub unique_upserted: i32,
    pub duplicates: i32,
    pub pages_fetched: i32,
    pub api_credits_used: i32,
    pub prequal_jobs_created: i32,
    pub industries_depleted: i32,
    pub elapsed_seconds: u64,
}

// ============================================================================
// Main Entry Point
// ============================================================================

/// Execute a `discover_companies` job.
///
/// # Arguments
/// * `pool` — Database connection pool.
/// * `provider` — Organization search provider (Apollo or Diffbot).
/// * `payload` — Parsed job payload.
/// * `worker_id` — ID of the worker processing this job (for logging).
pub async fn run_company_fetcher(
    pool: &PgPool,
    provider: &dyn OrgSearchProvider,
    payload: &DiscoverCompaniesPayload,
    worker_id: &str,
) -> Result<RunResult, OrchestratorError> {
    let start = Instant::now();
    let client_id = payload.client_id;

    info!(
        "Worker {}: starting company fetch for client {} (batch_target={}, provider={})",
        worker_id, client_id, payload.batch_target, provider.provider_name()
    );

    // ========================================================================
    // 1. Load ICP Profile
    // ========================================================================

    let icp_json = db::load_icp_json(pool, client_id)
        .await?
        .ok_or(OrchestratorError::MissingIcp(client_id))?;

    let icp = IcpFetchProfile::from_icp_json(&icp_json)
        .map_err(OrchestratorError::InvalidIcp)?;

    info!(
        "Loaded ICP: {} industries, {} locations, {} sizes",
        icp.industries.len(), icp.locations.len(), icp.company_sizes.len()
    );

    // ========================================================================
    // 2. Compute Quota Plan
    // ========================================================================

    let yield_metrics = db::load_yield_metrics(pool, client_id).await?;
    let quota_plan = quota_planner::compute_quota_plan(&icp, &yield_metrics, payload);

    // ========================================================================
    // 3. Create (or Resume) Fetch Run
    // ========================================================================

    let run_id = match payload.resume_run_id {
        Some(id) => {
            info!("Resuming existing run {}", id);
            id
        }
        None => {
            db::create_fetch_run(pool, client_id, payload.batch_target, &quota_plan).await?
        }
    };

    info!("Fetch run {} active for client {}", run_id, client_id);

    // ========================================================================
    // 4. Seed Deduper with Known Companies
    // ========================================================================

    let mut deduper = BatchDeduper::with_capacity(payload.batch_target as usize);
    dedup::seed_deduper_from_db(pool, client_id, &mut deduper).await?;
    info!("Deduper seeded with {} known identities", deduper.unique_count());

    // ========================================================================
    // 5. Initialize Per-Industry State
    // ========================================================================

    let variant_builder = VariantBuilder::new(&icp);

    let mut progress_map: HashMap<String, IndustryProgress> = quota_plan
        .iter()
        .map(|(ind, q)| (ind.clone(), IndustryProgress::new(ind.clone(), q.target)))
        .collect();

    // Deterministic industry order for round-robin.
    let mut industry_order: Vec<String> = quota_plan.keys().cloned().collect();
    industry_order.sort(); // alphabetical → deterministic across runs

    // Per-industry page cursors (which Apollo page to fetch next for each variant).
    // Key: "industry::variant" → next page number.
    let mut page_cursors: HashMap<String, i32> = HashMap::new();

    // ========================================================================
    // 6. Global Counters
    // ========================================================================

    let mut global = GlobalCounters::default();

    // ========================================================================
    // 7. Main Fetch Loop (True Round-Robin)
    // ========================================================================

    // Rotating index — each iteration picks the next industry in order.
    let mut rr_index: usize = 0;

    loop {
        // ---- Global stop checks ----

        if start.elapsed().as_secs() >= payload.max_runtime_seconds {
            warn!(
                "Run {} hit max runtime ({}s) — finalizing as partial",
                run_id, payload.max_runtime_seconds
            );
            break;
        }

        if global.upserted >= payload.batch_target {
            info!(
                "Run {} reached batch_target ({}) with {} upserted",
                run_id, payload.batch_target, global.upserted
            );
            break;
        }

        // ---- Pick next active industry (round-robin) ----

        let active = pick_next_active(&industry_order, &progress_map, rr_index);

        let industry = match active {
            Some((ind, next_idx)) => {
                rr_index = next_idx;
                ind
            }
            None => {
                info!("All {} industries done or depleted", industry_order.len());
                break;
            }
        };

        // ---- Fetch a batch of pages for this industry ----

        let turn_result = fetch_industry_turn(
            pool,
            provider,
            &variant_builder,
            &mut deduper,
            payload,
            run_id,
            client_id,
            &industry,
            &mut progress_map,
            &mut page_cursors,
            start,
        )
        .await;

        match turn_result {
            Ok(stats) => {
                // Update global counters
                global.fetched += stats.fetched;
                global.upserted += stats.upserted;
                global.duplicates += stats.duplicates;
                global.pages += stats.pages;
                global.api_credits += stats.pages; // 1 credit per page

                // Flush to DB
                if let Err(e) = db::update_run_counters(
                    pool,
                    run_id,
                    stats.fetched,
                    stats.upserted,
                    stats.duplicates,
                    stats.pages,
                    stats.pages, // api_credits = pages
                )
                .await
                {
                    warn!("Failed to update run counters: {}", e);
                }
            }
            Err(OrchestratorError::Apollo(ref e)) => {
                // Apollo errors are contained per-industry — don't kill the run.
                error!(
                    "Apollo error for industry '{}': {} — marking depleted",
                    industry, e
                );
                if let Some(prog) = progress_map.get_mut(&industry) {
                    prog.depleted = true;
                    prog.depletion_reason = Some(format!("provider_error: {}", e));
                }
            }
            Err(e) => {
                // DB errors are fatal.
                error!("Fatal error during fetch: {}", e);
                let error_json = json!({
                    "message": format!("{}", e),
                    "industry": industry,
                });
                let _ = db::finalize_run_failed(pool, run_id, &error_json).await;
                return Err(e);
            }
        }
    }

    // ========================================================================
    // 8. Finalize Run
    // ========================================================================

    let industry_summary = build_industry_summary(&progress_map);
    let all_depleted = progress_map.values().all(|p| p.depleted);
    let industries_depleted = progress_map.values().filter(|p| p.depleted).count() as i32;
    let timed_out = start.elapsed().as_secs() >= payload.max_runtime_seconds;

    let final_status = if timed_out {
        db::finalize_run_partial(pool, run_id, &industry_summary, "max_runtime_exceeded")
            .await?;
        "partial"
    } else if global.upserted >= payload.batch_target {
        db::finalize_run_succeeded(pool, run_id, &industry_summary).await?;
        "succeeded"
    } else if all_depleted {
        db::finalize_run_depleted(pool, run_id, &industry_summary).await?;
        "depleted"
    } else {
        // Under target but still had active industries — treat as success
        db::finalize_run_succeeded(pool, run_id, &industry_summary).await?;
        "succeeded"
    };

    info!(
        "Run {} finalized: status={}, fetched={}, upserted={}, dupes={}, pages={}, credits={}, elapsed={}s",
        run_id, final_status,
        global.fetched, global.upserted, global.duplicates,
        global.pages, global.api_credits,
        start.elapsed().as_secs()
    );

    // ========================================================================
    // 9. Update Industry Yield Metrics
    // ========================================================================

    for (industry, prog) in &progress_map {
        if prog.fetched > 0 || prog.depleted {
            if let Err(e) = db::upsert_yield_metrics(
                pool, client_id, industry,
                prog.fetched, prog.upserted, prog.duplicates, prog.depleted,
            )
            .await
            {
                warn!("Failed to upsert yield metrics for '{}': {}", industry, e);
            }
        }
    }

     // ========================================================================
    // 10. Autopilot: Enqueue Prequal Dispatch (if new companies were added)
    // ========================================================================

    let prequal_jobs = if global.upserted > 0 {
        // Check if autopilot is enabled for this client
        let prequal_config = prequal_db::load_prequal_config(pool, client_id)
            .await
            .unwrap_or_default();

        if prequal_config.autopilot_enabled {
            // Debounce: don't enqueue if there's already a pending dispatch
            match prequal_db::has_pending_prequal_dispatch(pool, client_id).await {
                Ok(true) => {
                    debug!(
                        "Autopilot: skipping PREQUAL_DISPATCH for client {} — one already pending",
                        client_id
                    );
                    0
                }
                Ok(false) => {
                    match prequal_db::enqueue_prequal_dispatch_job(pool, client_id, "autopilot").await {
                        Ok(job_id) => {
                            info!(
                                "Autopilot: enqueued PREQUAL_DISPATCH {} for client {} ({} new companies)",
                                job_id, client_id, global.upserted
                            );
                            1
                        }
                        Err(e) => {
                            warn!("Autopilot: failed to enqueue PREQUAL_DISPATCH: {}", e);
                            0
                        }
                    }
                }
                Err(e) => {
                    warn!("Autopilot: failed to check pending dispatches: {}", e);
                    0
                }
            }
        } else {
            debug!("Autopilot disabled for client {} — skipping prequal dispatch", client_id);
            0
        }
    } else {
        debug!("No new companies upserted — skipping prequal dispatch");
        0
    };

    // ========================================================================
    // Done
    // ========================================================================

    Ok(RunResult {
        run_id,
        status: final_status.to_string(),
        raw_fetched: global.fetched,
        unique_upserted: global.upserted,
        duplicates: global.duplicates,
        pages_fetched: global.pages,
        api_credits_used: global.api_credits,
        prequal_jobs_created: prequal_jobs,
        industries_depleted,
        elapsed_seconds: start.elapsed().as_secs(),
    })
}

// ============================================================================
// Round-Robin Industry Selection
// ============================================================================

/// Pick the next active (not done/depleted) industry starting from `start_idx`.
///
/// Wraps around the list. Returns the industry name and the next index to use
/// (one past the picked industry), or None if all industries are done.
fn pick_next_active(
    order: &[String],
    progress: &HashMap<String, IndustryProgress>,
    start_idx: usize,
) -> Option<(String, usize)> {
    let n = order.len();
    for offset in 0..n {
        let idx = (start_idx + offset) % n;
        let ind = &order[idx];
        if let Some(prog) = progress.get(ind.as_str()) {
            if !prog.is_done() {
                return Some((ind.clone(), (idx + 1) % n));
            }
        }
    }
    None
}

// ============================================================================
// Per-Industry Turn (Fetch Up To PAGES_PER_TURN Pages)
// ============================================================================

/// Stats from one round-robin turn for one industry.
#[derive(Debug, Default)]
struct TurnStats {
    fetched: i32,
    upserted: i32,
    duplicates: i32,
    pages: i32,
}

/// Fetch up to PAGES_PER_TURN pages for one industry at its current variant.
///
/// If the variant is exhausted within this turn, advances to the next variant
/// (or marks the industry depleted if all variants are exhausted).
///
/// Updates `progress_map` and `page_cursors` in place.
async fn fetch_industry_turn(
    pool: &PgPool,
    provider: &dyn OrgSearchProvider,
    variant_builder: &VariantBuilder,
    deduper: &mut BatchDeduper,
    payload: &DiscoverCompaniesPayload,
    run_id: Uuid,
    client_id: Uuid,
    industry: &str,
    progress_map: &mut HashMap<String, IndustryProgress>,
    page_cursors: &mut HashMap<String, i32>,
    run_start: Instant,
) -> Result<TurnStats, OrchestratorError> {
    let prog = match progress_map.get(industry) {
        Some(p) if p.is_done() => return Ok(TurnStats::default()),
        Some(p) => p.clone(),
        None => return Ok(TurnStats::default()),
    };

    let variant = prog.current_variant;
    let cursor_key = format!("{}::{}", industry, variant.as_str());
    let start_page = *page_cursors.get(&cursor_key).unwrap_or(&1);

    debug!(
        "Turn: industry='{}', variant={}, start_page={}, remaining={}/{}",
        industry, variant.as_str(), start_page,
        prog.remaining(), prog.quota
    );

    // Log the query in DB (logs tight variant; loose is logged in meta if fallback occurs)
    let base_request = variant_builder.build_request(industry, variant, start_page, payload.page_size);
    let query_id = db::create_fetch_query(
        pool, run_id, client_id, industry, variant.as_str(),
        &base_request.to_json_value(), start_page,
    )
    .await?;

    let mut stats = TurnStats::default();
    let mut current_page = start_page;
    let mut consecutive_high_dupe = 0;
    let mut last_response_meta = None;
    let mut query_status = FetchQueryStatus::Succeeded;
    let mut query_error: Option<String> = None;
    let mut variant_exhausted = false;

    // Tight→loose fallback: if first page returns too few results and
    // this industry has include_keywords_any, switch to loose query.
    let mut use_loose = false;

    // Fetch up to PAGES_PER_TURN pages (or until a stop condition)
    while stats.pages < PAGES_PER_TURN {
        // --- Stop conditions ---

        let prog_now = progress_map.get(industry).cloned().unwrap_or(prog.clone());
        if prog_now.upserted + stats.upserted >= prog_now.quota {
            debug!("Industry '{}' reached quota ({})", industry, prog_now.quota);
            break;
        }

        if current_page - start_page >= payload.max_pages_per_query {
            debug!(
                "Industry '{}' variant {} hit max_pages_per_query ({})",
                industry, variant.as_str(), payload.max_pages_per_query
            );
            variant_exhausted = true;
            break;
        }

        if run_start.elapsed().as_secs() >= payload.max_runtime_seconds {
            debug!("Runtime limit hit during industry '{}'", industry);
            break;
        }

        // --- Inter-page delay (avoid hammering the provider) ---

        if stats.pages > 0 {
            sleep(Duration::from_millis(INTER_PAGE_DELAY_MS)).await;
        }

        // --- Fetch one page ---

        let page_request = if use_loose {
            variant_builder.build_loose_request(
                industry, variant, current_page, payload.page_size,
            )
        } else {
            variant_builder.build_request(
                industry, variant, current_page, payload.page_size,
            )
        };

        let (orgs, meta) = match provider.search_page(&page_request).await {
            Ok(result) => result,
            Err(e) => {
                query_error = Some(format!("{}", e));
                query_status = FetchQueryStatus::Failed;
                let _ = db::update_fetch_query(
                    pool, query_id, current_page, stats.pages, stats.fetched,
                    query_status, last_response_meta.as_ref(), query_error.as_deref(),
                )
                .await;
                return Err(OrchestratorError::Apollo(e));
            }
        };

        last_response_meta = Some(json!({
            "total_entries": meta.total_entries,
            "total_pages": meta.total_pages,
            "current_page": meta.page,
        }));

        let orgs_count = orgs.len() as i32;
        stats.fetched += orgs_count;
        stats.pages += 1;

        // --- Tight→loose fallback check on first page ---

        if stats.pages == 1
            && !use_loose
            && (orgs_count as usize) < LOOSE_FALLBACK_THRESHOLD
            && variant_builder.industry_has_include_keywords(industry)
        {
            info!(
                "Industry '{}' tight query returned only {} results (< {}), switching to loose",
                industry, orgs_count, LOOSE_FALLBACK_THRESHOLD
            );
            use_loose = true;
            // Still process this page's results (don't discard them),
            // but subsequent pages will use the loose query.
        }

        // --- Process batch: dedup + upsert ---

        let batch = dedup::process_batch(pool, client_id, &orgs, deduper).await?;

        let new_count = batch.new_count() as i32;
        let dupe_count = batch.dupe_count() as i32;
        stats.upserted += new_count;
        stats.duplicates += dupe_count;

        // --- Write enrichment data (now that company rows exist) ---

        let mut enrichments = provider.drain_pending_enrichments();
        if !enrichments.is_empty() {
            // Stamp match metadata from query context onto each enrichment.
            // This records which industry, keywords, and variant produced the match
            // so we can debug and tune ICP configs without re-running.
            let variant_label = if use_loose { "loose" } else { "tight" };
            let matched_category = page_request
                .organization_industry_tag_ids
                .as_ref()
                .and_then(|v| v.first().cloned())
                .unwrap_or_default();

            let metadata = json!({
                "provider": provider.provider_name(),
                "matched_industry_name": industry,
                "matched_diffbot_category": matched_category,
                "include_keywords_any": page_request.include_keywords_any.as_deref().unwrap_or(&[]),
                "include_keywords_all": page_request.include_keywords_all.as_deref().unwrap_or(&[]),
                "exclude_keywords_any": page_request.exclude_keywords_any.as_deref().unwrap_or(&[]),
                "query_variant": variant_label,
                "query_variant_ladder": variant.as_str(),
            });

            for (_external_id, ref mut data) in enrichments.iter_mut() {
                data.match_metadata = metadata.clone();
            }

            let written = super::enrichment::batch_write_enrichments(
                pool, client_id, &enrichments,
            ).await;
            debug!("  enrichment: wrote {}/{} rows (variant={})", written, enrichments.len(), variant_label);
        }

        // --- Insert candidate rows ---

        for upsert in batch.inserted.iter().chain(batch.updated.iter()) {
            let _ = db::insert_candidate(
                pool, client_id, run_id, upsert.company_id,
                industry, variant.as_str(), !upsert.was_insert,
            )
            .await;
        }

        debug!(
            "  page {}: {} orgs → {} new, {} dupes",
            current_page, orgs_count, new_count, dupe_count
        );

        // --- Check if Apollo results are exhausted ---

        if orgs_count < payload.page_size {
            debug!(
                "Industry '{}' variant {} exhausted at page {} ({} < {})",
                industry, variant.as_str(), current_page,
                orgs_count, payload.page_size
            );
            variant_exhausted = true;
            query_status = FetchQueryStatus::Exhausted;
            break;
        }

        // --- Consecutive high-dupe detection ---

        if orgs_count > 0 && (dupe_count as f64 / orgs_count as f64) > HIGH_DUPE_THRESHOLD {
            consecutive_high_dupe += 1;
            if consecutive_high_dupe >= MAX_CONSECUTIVE_HIGH_DUPE {
                debug!(
                    "Industry '{}' variant {}: {} consecutive high-dupe pages — exhausting",
                    industry, variant.as_str(), consecutive_high_dupe
                );
                variant_exhausted = true;
                query_status = FetchQueryStatus::Exhausted;
                break;
            }
        } else {
            consecutive_high_dupe = 0;
        }

        current_page += 1;
    }

    // --- Save page cursor for next turn ---

    page_cursors.insert(cursor_key, current_page + 1);

    // --- Update the query row ---

    let _ = db::update_fetch_query(
        pool, query_id, current_page, stats.pages, stats.fetched,
        query_status, last_response_meta.as_ref(), query_error.as_deref(),
    )
    .await;

    // --- Update progress for this industry ---

    if let Some(prog) = progress_map.get_mut(industry) {
        prog.fetched += stats.fetched;
        prog.upserted += stats.upserted;
        prog.duplicates += stats.duplicates;

        if variant_exhausted {
            // Try to advance to the next effective variant
            match variant_builder.next_effective_variant(prog.current_variant) {
                Some(next) => {
                    info!(
                        "Industry '{}': advancing {} → {}",
                        industry, prog.current_variant.as_str(), next.as_str()
                    );
                    prog.current_variant = next;
                    prog.variants_used.push(next.as_str().to_string());
                    // Reset page cursor for the new variant
                    let new_key = format!("{}::{}", industry, next.as_str());
                    page_cursors.insert(new_key, 1);
                }
                None => {
                    info!(
                        "Industry '{}': all variants exhausted — marking depleted",
                        industry
                    );
                    prog.depleted = true;
                    prog.depletion_reason = Some("all_variants_exhausted".to_string());
                }
            }
        }
    }

    Ok(stats)
}

// ============================================================================
// Helpers
// ============================================================================

/// Global counters accumulated across all industries and turns.
#[derive(Debug, Default)]
struct GlobalCounters {
    fetched: i32,
    upserted: i32,
    duplicates: i32,
    pages: i32,
    api_credits: i32,
}

/// Build the industry_summary JSONB for the final run row.
fn build_industry_summary(
    progress_map: &HashMap<String, IndustryProgress>,
) -> serde_json::Value {
    let mut summary = serde_json::Map::new();

    for (industry, prog) in progress_map {
        summary.insert(
            industry.clone(),
            json!({
                "quota": prog.quota,
                "fetched": prog.fetched,
                "upserted": prog.upserted,
                "duplicates": prog.duplicates,
                "depleted": prog.depleted,
                "depletion_reason": prog.depletion_reason,
                "variants_used": prog.variants_used,
                "final_variant": prog.current_variant.as_str(),
            }),
        );
    }

    serde_json::Value::Object(summary)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Round-Robin Tests ----

    #[test]
    fn test_pick_next_active_basic() {
        let order = vec!["a".into(), "b".into(), "c".into()];
        let mut progress = HashMap::new();
        progress.insert("a".into(), IndustryProgress::new("a".into(), 100));
        progress.insert("b".into(), IndustryProgress::new("b".into(), 100));
        progress.insert("c".into(), IndustryProgress::new("c".into(), 100));

        // Starting at 0 → picks "a", next index = 1
        let (ind, next) = pick_next_active(&order, &progress, 0).unwrap();
        assert_eq!(ind, "a");
        assert_eq!(next, 1);

        // Starting at 1 → picks "b", next index = 2
        let (ind, next) = pick_next_active(&order, &progress, 1).unwrap();
        assert_eq!(ind, "b");
        assert_eq!(next, 2);

        // Starting at 2 → picks "c", next index = 0 (wraps)
        let (ind, next) = pick_next_active(&order, &progress, 2).unwrap();
        assert_eq!(ind, "c");
        assert_eq!(next, 0);
    }

    #[test]
    fn test_pick_next_active_skips_done() {
        let order = vec!["a".into(), "b".into(), "c".into()];
        let mut progress = HashMap::new();

        // "a" is depleted, "b" is at quota, "c" is active
        let mut a = IndustryProgress::new("a".into(), 100);
        a.depleted = true;
        progress.insert("a".into(), a);

        let mut b = IndustryProgress::new("b".into(), 50);
        b.upserted = 50; // at quota
        progress.insert("b".into(), b);

        progress.insert("c".into(), IndustryProgress::new("c".into(), 100));

        // Starting at 0 → skips "a" (depleted), skips "b" (at quota), picks "c"
        let (ind, next) = pick_next_active(&order, &progress, 0).unwrap();
        assert_eq!(ind, "c");
        assert_eq!(next, 0); // (2+1)%3 = 0
    }

    #[test]
    fn test_pick_next_active_all_done() {
        let order = vec!["a".into(), "b".into()];
        let mut progress = HashMap::new();

        let mut a = IndustryProgress::new("a".into(), 100);
        a.depleted = true;
        progress.insert("a".into(), a);

        let mut b = IndustryProgress::new("b".into(), 50);
        b.upserted = 50;
        progress.insert("b".into(), b);

        assert!(pick_next_active(&order, &progress, 0).is_none());
    }

    #[test]
    fn test_pick_next_active_wraps_around() {
        let order = vec!["a".into(), "b".into(), "c".into()];
        let mut progress = HashMap::new();

        // Only "a" is active
        progress.insert("a".into(), IndustryProgress::new("a".into(), 100));

        let mut b = IndustryProgress::new("b".into(), 50);
        b.depleted = true;
        progress.insert("b".into(), b);

        let mut c = IndustryProgress::new("c".into(), 50);
        c.depleted = true;
        progress.insert("c".into(), c);

        // Starting at 2 → wraps to 0 → picks "a"
        let (ind, _) = pick_next_active(&order, &progress, 2).unwrap();
        assert_eq!(ind, "a");
    }

    // ---- Industry Summary Tests ----

    #[test]
    fn test_build_industry_summary() {
        let mut map = HashMap::new();
        map.insert(
            "fintech".to_string(),
            IndustryProgress {
                industry: "fintech".into(),
                quota: 400,
                fetched: 380,
                upserted: 320,
                duplicates: 60,
                current_variant: QueryVariant::V2BroadenGeo,
                variants_used: vec!["v1_strict".into(), "v2_broaden_geo".into()],
                depleted: false,
                depletion_reason: None,
            },
        );
        map.insert(
            "cybersecurity".to_string(),
            IndustryProgress {
                industry: "cybersecurity".into(),
                quota: 200,
                fetched: 150,
                upserted: 100,
                duplicates: 50,
                current_variant: QueryVariant::V4KeywordAssist,
                variants_used: vec![
                    "v1_strict".into(), "v2_broaden_geo".into(),
                    "v3_broaden_size".into(), "v4_keyword_assist".into(),
                ],
                depleted: true,
                depletion_reason: Some("all_variants_exhausted".into()),
            },
        );

        let summary = build_industry_summary(&map);
        assert!(summary.is_object());

        let ft = &summary["fintech"];
        assert_eq!(ft["quota"], 400);
        assert_eq!(ft["upserted"], 320);
        assert_eq!(ft["depleted"], false);

        let cy = &summary["cybersecurity"];
        assert_eq!(cy["depleted"], true);
        assert_eq!(cy["depletion_reason"], "all_variants_exhausted");
        assert_eq!(cy["final_variant"], "v4_keyword_assist");
    }
}
