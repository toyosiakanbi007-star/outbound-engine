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
//   2. Load persisted fetch cursors → pre-advance exhausted variants
//   3. Create fetch run row → seed deduper from existing companies
//   4. Main loop: true round-robin across industries
//      a. For current industry at its persisted cursor position:
//         - Resume pagination from last saved page (not page 1!)
//         - Dedup + upsert each page → insert candidates → update counters
//         - On exhaustion: advance variant ladder or mark industry depleted
//      b. Save cursor after each turn (crash-safe)
//      c. Rotate to next industry
//   5. Finalize run (succeeded / depleted / partial / failed)
//   6. Update industry yield metrics
//   7. Enqueue downstream prequal jobs (if configured)
//
// CURSOR PERSISTENCE (migration 0028):
//   Each (client_id, industry, variant) has a persistent cursor row in
//   `industry_fetch_cursors` that stores:
//     - next_page: where to resume pagination
//     - exhausted: whether all pages were consumed (skip entirely)
//     - use_loose: whether tight→loose fallback was triggered
//     - last_total_hits: detect dataset changes between runs
//
//   This eliminates ALL wasted API credits on re-fetching known companies:
//     - Diffbot: 25 credits PER ENTITY returned → savings = pages_skipped × page_size × 25
//     - Apollo:  1 credit per page → savings = pages_skipped × 1
//
// CREDIT TRACKING:
//   The orchestrator computes `api_credits` differently per provider:
//     - Apollo:  1 credit per page fetched
//     - Diffbot: 25 credits per entity returned (orgs_count × 25)
//   This is critical for accurate cost reporting and budget monitoring.
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

/// Milliseconds to sleep between consecutive API pages.
const INTER_PAGE_DELAY_MS: u64 = 300;

/// Number of pages to fetch per industry per round-robin turn.
const PAGES_PER_TURN: i32 = 5;

/// If this fraction of a page's results are duplicates, count it as high-dupe.
const HIGH_DUPE_THRESHOLD: f64 = 0.80;

/// Consecutive high-dupe pages before exhausting the current variant.
const MAX_CONSECUTIVE_HIGH_DUPE: i32 = 2;

/// If the first page of a tight query returns fewer than this, switch to loose.
const LOOSE_FALLBACK_THRESHOLD: usize = 20;

/// Fraction of in-memory dupes that count toward industry progress.
/// (From the cross-industry attribution fix in migration 0027.)
const DUPE_PROGRESS_FRACTION: f64 = 0.5;

/// Diffbot charges this many credits per entity returned.
const DIFFBOT_CREDITS_PER_ENTITY: i32 = 25;

// ============================================================================
// In-Memory Cursor State
// ============================================================================

/// Tracks pagination state for one (industry, variant) pair during a run.
/// Initialized from DB cursor at run start, updated after each page,
/// flushed back to DB after each turn and at run end.
#[derive(Debug, Clone)]
struct CursorState {
    /// Next page to fetch from the provider.
    next_page: i32,
    /// Whether to use the loose (no include_keywords_any) query.
    use_loose: bool,
    /// Provider's total_entries for this query (for change detection).
    last_total_hits: Option<i64>,
    /// Entities fetched in THIS run only (delta for DB upsert).
    fetched_this_run: i32,
    /// New (non-dupe) entities found in THIS run only.
    new_this_run: i32,
    /// Whether this variant is exhausted (all pages consumed).
    exhausted: bool,
}

impl CursorState {
    fn fresh() -> Self {
        Self {
            next_page: 1,
            use_loose: false,
            last_total_hits: None,
            fetched_this_run: 0,
            new_this_run: 0,
            exhausted: false,
        }
    }

    /// Create from a persisted DB cursor row.
    fn from_db(cursor: &db::FetchCursor) -> Self {
        Self {
            next_page: cursor.next_page,
            use_loose: cursor.use_loose,
            last_total_hits: cursor.last_total_hits,
            fetched_this_run: 0,
            new_this_run: 0,
            exhausted: cursor.exhausted,
        }
    }
}

// ============================================================================
// Error Type
// ============================================================================

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
    /// Pages saved by cursor persistence (vs starting from page 1).
    pub pages_saved_by_cursors: i32,
    /// Estimated credits saved by cursor persistence.
    pub credits_saved_by_cursors: i64,
}

// ============================================================================
// Credit Calculation
// ============================================================================

/// Compute API credits consumed for one page of results.
///
/// - Apollo:  1 credit per page (flat, regardless of results)
/// - Diffbot: 25 credits per entity returned (per org, not per page)
fn compute_page_credits(provider_name: &str, orgs_returned: i32) -> i32 {
    match provider_name {
        "diffbot" => orgs_returned * DIFFBOT_CREDITS_PER_ENTITY,
        _ => 1, // Apollo and others: 1 credit per page
    }
}

/// Estimate credits that would have been spent on skipped pages.
///
/// For Diffbot: pages_skipped × page_size × 25 (worst case: full pages)
/// For Apollo:  pages_skipped × 1
fn estimate_saved_credits(provider_name: &str, pages_skipped: i32, page_size: i32) -> i64 {
    match provider_name {
        "diffbot" => (pages_skipped as i64) * (page_size as i64) * (DIFFBOT_CREDITS_PER_ENTITY as i64),
        _ => pages_skipped as i64,
    }
}

// ============================================================================
// Main Entry Point
// ============================================================================

pub async fn run_company_fetcher(
    pool: &PgPool,
    provider: &dyn OrgSearchProvider,
    payload: &DiscoverCompaniesPayload,
    worker_id: &str,
) -> Result<RunResult, OrchestratorError> {
    let start = Instant::now();
    let client_id = payload.client_id;
    let provider_name = provider.provider_name();

    info!(
        "Worker {}: starting company fetch for client {} \
         (batch_target={}, provider={}, page_size={}, rescan={})",
        worker_id, client_id, payload.batch_target,
        provider_name, payload.page_size, payload.force_full_rescan
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
    // 3. Load or Reset Persisted Fetch Cursors
    // ========================================================================

    if payload.force_full_rescan {
        let deleted = db::reset_fetch_cursors(pool, client_id).await?;
        if deleted > 0 {
            info!(
                "force_full_rescan: cleared {} persisted cursors for client {}",
                deleted, client_id
            );
        }
    }

    // Prune cursors for industries that were removed from the ICP
    let active_industries = icp.industry_names();
    let pruned = db::prune_stale_cursors(pool, client_id, &active_industries).await
        .unwrap_or(0);
    if pruned > 0 {
        info!("Pruned {} stale cursors (industries removed from ICP)", pruned);
    }

    // Load surviving cursors into memory
    let persisted_cursors = db::load_fetch_cursors(pool, client_id).await?;

    // ========================================================================
    // 4. Create (or Resume) Fetch Run
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
    // 5. Seed Deduper with Known Companies
    // ========================================================================

    let mut deduper = BatchDeduper::with_capacity(payload.batch_target as usize);
    dedup::seed_deduper_from_db(pool, client_id, &mut deduper).await?;
    info!("Deduper seeded with {} known identities", deduper.unique_count());

    // ========================================================================
    // 6. Initialize Per-Industry State + Cursor State
    // ========================================================================

    let variant_builder = VariantBuilder::new(&icp);

    let mut progress_map: HashMap<String, IndustryProgress> = quota_plan
        .iter()
        .map(|(ind, q)| (ind.clone(), IndustryProgress::new(ind.clone(), q.target)))
        .collect();

    // In-memory cursor state map: "industry::variant" → CursorState
    let mut cursor_state: HashMap<String, CursorState> = HashMap::new();

    // Track pages saved across all industries (for reporting)
    let mut total_pages_saved: i32 = 0;

    // ---- Pre-advance exhausted variants from previous runs ----
    //
    // If V1Strict for "fintech" was fully consumed last run, the cursor is
    // marked exhausted=true. We skip it and start at the next non-exhausted
    // variant. If ALL variants are exhausted, the industry starts depleted.

    for (industry, prog) in progress_map.iter_mut() {
        let mut variant = QueryVariant::V1Strict;

        loop {
            let key = db::cursor_key(industry, variant.as_str());

            let cs = if let Some(pc) = persisted_cursors.get(&key) {
                CursorState::from_db(pc)
            } else {
                CursorState::fresh()
            };

            if cs.exhausted {
                debug!(
                    "Cursor: {}::{} exhausted (previous run), skipping",
                    industry, variant.as_str()
                );
                cursor_state.insert(key, cs);

                match variant_builder.next_effective_variant(variant) {
                    Some(next) => variant = next,
                    None => {
                        info!(
                            "Industry '{}': ALL variants exhausted from previous runs → depleted",
                            industry
                        );
                        prog.depleted = true;
                        prog.depletion_reason =
                            Some("all_variants_exhausted_from_cursors".to_string());
                        break;
                    }
                }
            } else {
                // This variant has pages left — resume here
                let pages_saved = cs.next_page.saturating_sub(1);
                if pages_saved > 0 {
                    info!(
                        "Cursor: {}::{} resuming at page {} (skipping {} pages)",
                        industry, variant.as_str(), cs.next_page, pages_saved
                    );
                    total_pages_saved += pages_saved;
                }
                cursor_state.insert(key, cs);
                break;
            }
        }

        prog.current_variant = variant;
        if !prog.depleted {
            prog.variants_used = vec![variant.as_str().to_string()];
        }
    }

    let credits_saved = estimate_saved_credits(
        provider_name, total_pages_saved, payload.page_size,
    );

    if total_pages_saved > 0 {
        info!(
            "Cursor persistence: skipping {} pages (~{} credits saved, provider={})",
            total_pages_saved, credits_saved, provider_name
        );
    }

    // ---- Deterministic but fair industry order (rotate by run_id) ----

    let mut industry_order: Vec<String> = quota_plan.keys().cloned().collect();
    industry_order.sort();
    if !industry_order.is_empty() {
        let rotation = run_id.as_bytes()[0] as usize % industry_order.len();
        industry_order.rotate_left(rotation);
    }

    info!(
        "Industry order for run {}: {:?}",
        run_id,
        industry_order.iter().map(|s| s.as_str()).collect::<Vec<_>>()
    );

    // ========================================================================
    // 7. Global Counters
    // ========================================================================

    let mut global = GlobalCounters::default();

    // ========================================================================
    // 8. Main Fetch Loop (True Round-Robin)
    // ========================================================================

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
            &mut cursor_state,
            &persisted_cursors,
            start,
        )
        .await;

        match turn_result {
            Ok(stats) => {
                global.fetched += stats.fetched;
                global.upserted += stats.upserted;
                global.duplicates += stats.duplicates;
                global.pages += stats.pages;
                global.api_credits += stats.api_credits;

                if let Err(e) = db::update_run_counters(
                    pool, run_id,
                    stats.fetched, stats.upserted, stats.duplicates,
                    stats.pages, stats.api_credits,
                ).await {
                    warn!("Failed to update run counters: {}", e);
                }

                // ---- Persist cursor after each turn (crash-safe) ----
                persist_industry_cursors(
                    pool, client_id, run_id, &industry,
                    &progress_map, &cursor_state,
                ).await;
            }
            Err(OrchestratorError::Apollo(ref e)) => {
                error!(
                    "Provider error for industry '{}': {} — marking depleted",
                    industry, e
                );
                if let Some(prog) = progress_map.get_mut(&industry) {
                    prog.depleted = true;
                    prog.depletion_reason = Some(format!("provider_error: {}", e));
                }
                persist_industry_cursors(
                    pool, client_id, run_id, &industry,
                    &progress_map, &cursor_state,
                ).await;
            }
            Err(e) => {
                // DB errors are fatal — save ALL cursors before dying
                error!("Fatal error during fetch: {}", e);
                persist_all_cursors(
                    pool, client_id, run_id, &cursor_state,
                ).await;

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
    // 9. Final Cursor Persistence (save ALL modified cursors)
    // ========================================================================

    persist_all_cursors(pool, client_id, run_id, &cursor_state).await;

    // ========================================================================
    // 10. Finalize Run
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
        db::finalize_run_succeeded(pool, run_id, &industry_summary).await?;
        "succeeded"
    };

    info!(
        "Run {} finalized: status={}, fetched={}, upserted={}, dupes={}, \
         pages={}, credits={}, pages_saved={}, credits_saved=~{}, elapsed={}s",
        run_id, final_status,
        global.fetched, global.upserted, global.duplicates,
        global.pages, global.api_credits,
        total_pages_saved, credits_saved,
        start.elapsed().as_secs()
    );

    // ========================================================================
    // 11. Update Industry Yield Metrics
    // ========================================================================

    for (industry, prog) in &progress_map {
        if prog.fetched > 0 || prog.depleted {
            if let Err(e) = db::upsert_yield_metrics(
                pool, client_id, industry,
                prog.fetched, prog.upserted, prog.duplicates, prog.depleted,
            ).await {
                warn!("Failed to upsert yield metrics for '{}': {}", industry, e);
            }
        }
    }

    // ========================================================================
    // 12. Autopilot: Enqueue Prequal Dispatch
    // ========================================================================

    let prequal_jobs = if global.upserted > 0 {
        let prequal_config = prequal_db::load_prequal_config(pool, client_id)
            .await
            .unwrap_or_default();

        if prequal_config.autopilot_enabled {
            match prequal_db::has_pending_prequal_dispatch(pool, client_id).await {
                Ok(true) => {
                    debug!("Autopilot: skipping PREQUAL_DISPATCH — one already pending");
                    0
                }
                Ok(false) => {
                    match prequal_db::enqueue_prequal_dispatch_job(pool, client_id, "autopilot").await {
                        Ok(job_id) => {
                            info!(
                                "Autopilot: enqueued PREQUAL_DISPATCH {} ({} new companies)",
                                job_id, global.upserted
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
        pages_saved_by_cursors: total_pages_saved,
        credits_saved_by_cursors: credits_saved,
    })
}

// ============================================================================
// Round-Robin Industry Selection
// ============================================================================

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

#[derive(Debug, Default)]
struct TurnStats {
    fetched: i32,
    upserted: i32,
    duplicates: i32,
    pages: i32,
    /// Provider-specific credit cost for this turn.
    api_credits: i32,
}

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
    cursor_state: &mut HashMap<String, CursorState>,
    persisted_cursors: &HashMap<String, db::FetchCursor>,
    start: Instant,
) -> Result<TurnStats, OrchestratorError> {
    let provider_name = provider.provider_name();

    let prog = match progress_map.get(industry) {
        Some(p) if p.is_done() => return Ok(TurnStats::default()),
        Some(p) => p.clone(),
        None => return Ok(TurnStats::default()),
    };

    let variant = prog.current_variant;
    let cs_key = db::cursor_key(industry, variant.as_str());

    // Load or create cursor state for this (industry, variant)
    let cs = cursor_state
        .entry(cs_key.clone())
        .or_insert_with(CursorState::fresh);

    // If cursor says this variant is exhausted, skip (should have been
    // pre-advanced at run start, but guard against edge cases)
    if cs.exhausted {
        debug!(
            "Turn: {}::{} already exhausted — skipping",
            industry, variant.as_str()
        );
        return Ok(TurnStats::default());
    }

    let start_page = cs.next_page;
    let use_loose_initial = cs.use_loose;

    debug!(
        "Turn: industry='{}', variant={}, page={}, loose={}, remaining={}/{}",
        industry, variant.as_str(), start_page, use_loose_initial,
        prog.remaining(), prog.quota
    );

    // Log the query in DB
    let base_request = variant_builder.build_request(
        industry, variant, start_page, payload.page_size,
    );
    let query_id = db::create_fetch_query(
        pool, run_id, client_id, industry, variant.as_str(),
        &base_request.to_json_value(), start_page,
    ).await?;

    let mut stats = TurnStats::default();
    let mut current_page = start_page;
    let mut consecutive_high_dupe = 0;
    let mut last_response_meta = None;
    let mut query_status = FetchQueryStatus::Succeeded;
    let mut query_error: Option<String> = None;
    let mut variant_exhausted = false;
    let mut use_loose = use_loose_initial;

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

        if start.elapsed().as_secs() >= payload.max_runtime_seconds {
            break;
        }

        // --- Inter-page delay ---

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
                // Save cursor progress even on failure (don't lose pages already done)
                if let Some(cs) = cursor_state.get_mut(&cs_key) {
                    cs.next_page = current_page; // resume AT this page on retry
                }
                let _ = db::update_fetch_query(
                    pool, query_id, current_page, stats.pages, stats.fetched,
                    query_status, last_response_meta.as_ref(), query_error.as_deref(),
                ).await;
                return Err(OrchestratorError::Apollo(e));
            }
        };

        let total_hits = meta.total_entries;

        last_response_meta = Some(json!({
            "total_entries": total_hits,
            "total_pages": meta.total_pages,
            "current_page": meta.page,
        }));

        let orgs_count = orgs.len() as i32;
        stats.fetched += orgs_count;
        stats.pages += 1;

        // --- Provider-specific credit tracking ---
        stats.api_credits += compute_page_credits(provider_name, orgs_count);

        // --- Tight→loose fallback on first page ---

        if current_page == start_page
            && stats.pages == 1
            && !use_loose
            && (orgs_count as usize) < LOOSE_FALLBACK_THRESHOLD
            && variant_builder.industry_has_include_keywords(industry)
        {
            info!(
                "Industry '{}' tight query returned only {} results (< {}), switching to loose",
                industry, orgs_count, LOOSE_FALLBACK_THRESHOLD
            );
            use_loose = true;
            // Process this page's results, subsequent pages use loose query
        }

        // --- Process batch: dedup + upsert ---

        let batch = dedup::process_batch(pool, client_id, &orgs, deduper).await?;

        let new_count = batch.new_count() as i32;
        let dupe_count = batch.dupe_count() as i32;
        let in_memory_dupes = batch.in_memory_dupes;
        stats.upserted += new_count;
        stats.duplicates += dupe_count;

        // Partial credit for in-memory dupes (cross-industry fairness)
        let dupe_progress = ((in_memory_dupes as f64) * DUPE_PROGRESS_FRACTION).round() as i32;
        stats.upserted += dupe_progress;

        // --- Write enrichment data ---

        let mut enrichments = provider.drain_pending_enrichments();
        if !enrichments.is_empty() {
            let variant_label = if use_loose { "loose" } else { "tight" };
            let matched_category = page_request
                .organization_industry_tag_ids
                .as_ref()
                .and_then(|v| v.first().cloned())
                .unwrap_or_default();

            let metadata = json!({
                "provider": provider_name,
                "matched_industry_name": industry,
                "matched_diffbot_category": matched_category,
                "include_keywords_any": page_request.include_keywords_any.as_deref().unwrap_or(&[]),
                "include_keywords_all": page_request.include_keywords_all.as_deref().unwrap_or(&[]),
                "exclude_keywords_any": page_request.exclude_keywords_any.as_deref().unwrap_or(&[]),
                "query_variant": variant_label,
                "query_variant_ladder": variant.as_str(),
            });

            for (_ext_id, ref mut data) in enrichments.iter_mut() {
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
            ).await;
        }

        // Insert candidate rows for in-memory dupes (cross-industry attribution)
        for dupe_company_id in &batch.in_memory_dupe_company_ids {
            let _ = db::insert_candidate(
                pool, client_id, run_id, *dupe_company_id,
                industry, variant.as_str(), true,
            ).await;
        }

        debug!(
            "  page {}: {} orgs → {} new, {} dupes, {} in-mem dupes (credits={})",
            current_page, orgs_count, new_count, dupe_count, in_memory_dupes,
            compute_page_credits(provider_name, orgs_count)
        );

        // --- Update in-memory cursor ---

        if let Some(cs) = cursor_state.get_mut(&cs_key) {
            cs.next_page = current_page + 1;
            cs.use_loose = use_loose;
            cs.last_total_hits = Some(total_hits);
            cs.fetched_this_run += orgs_count;
            cs.new_this_run += new_count;
        }

        // --- Check if results are exhausted ---

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

    // --- Update the query row ---

    let _ = db::update_fetch_query(
        pool, query_id, current_page, stats.pages, stats.fetched,
        query_status, last_response_meta.as_ref(), query_error.as_deref(),
    ).await;

    // --- Mark cursor exhausted if variant is done ---

    if variant_exhausted {
        if let Some(cs) = cursor_state.get_mut(&cs_key) {
            cs.exhausted = true;
        }
    }

    // --- Update progress for this industry ---

    if let Some(prog) = progress_map.get_mut(industry) {
        prog.fetched += stats.fetched;
        prog.upserted += stats.upserted;
        prog.duplicates += stats.duplicates;

        if variant_exhausted {
            match variant_builder.next_effective_variant(prog.current_variant) {
                Some(next) => {
                    info!(
                        "Industry '{}': advancing {} → {}",
                        industry, prog.current_variant.as_str(), next.as_str()
                    );
                    prog.current_variant = next;
                    prog.variants_used.push(next.as_str().to_string());

                    // Initialize cursor state for the new variant
                    let next_key = db::cursor_key(industry, next.as_str());
                    if !cursor_state.contains_key(&next_key) {
                        // Check if there's a persisted cursor from a previous run
                        let cs = if let Some(pc) = persisted_cursors.get(&next_key) {
                            CursorState::from_db(pc)
                        } else {
                            CursorState::fresh()
                        };
                        cursor_state.insert(next_key, cs);
                    }
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
// Cursor Persistence Helpers
// ============================================================================

/// Save cursor(s) for a single industry's active variant.
async fn persist_industry_cursors(
    pool: &PgPool,
    client_id: Uuid,
    run_id: Uuid,
    industry: &str,
    progress_map: &HashMap<String, IndustryProgress>,
    cursor_state: &HashMap<String, CursorState>,
) {
    let Some(prog) = progress_map.get(industry) else { return };

    // Save cursor for current variant
    let key = db::cursor_key(industry, prog.current_variant.as_str());
    save_one_cursor(pool, client_id, run_id, &key, cursor_state).await;

    // Also save any exhausted variants we advanced past this turn
    for v_name in &prog.variants_used {
        if v_name.as_str() != prog.current_variant.as_str() {
            let prev_key = db::cursor_key(industry, v_name);
            save_one_cursor(pool, client_id, run_id, &prev_key, cursor_state).await;
        }
    }
}

/// Save ALL cursors that were modified during this run.
async fn persist_all_cursors(
    pool: &PgPool,
    client_id: Uuid,
    run_id: Uuid,
    cursor_state: &HashMap<String, CursorState>,
) {
    for key in cursor_state.keys() {
        save_one_cursor(pool, client_id, run_id, key, cursor_state).await;
    }
    debug!("Persisted all cursor states for client {}", client_id);
}

/// Save a single cursor to the DB.
async fn save_one_cursor(
    pool: &PgPool,
    client_id: Uuid,
    run_id: Uuid,
    key: &str,
    cursor_state: &HashMap<String, CursorState>,
) {
    let Some(cs) = cursor_state.get(key) else { return };

    // Only save if something changed this run
    if cs.fetched_this_run == 0 && cs.new_this_run == 0 && !cs.exhausted {
        return;
    }

    // Parse "industry::variant" from the key
    let parts: Vec<&str> = key.splitn(2, "::").collect();
    if parts.len() != 2 {
        warn!("Invalid cursor key format: {}", key);
        return;
    }

    if let Err(e) = db::save_fetch_cursor(
        pool, client_id,
        parts[0],  // industry
        parts[1],  // variant
        cs.next_page,
        cs.fetched_this_run,
        cs.new_this_run,
        cs.exhausted,
        cs.use_loose,
        cs.last_total_hits,
        run_id,
    ).await {
        warn!("Failed to save cursor {}: {}", key, e);
    }
}

// ============================================================================
// Helpers
// ============================================================================

#[derive(Debug, Default)]
struct GlobalCounters {
    fetched: i32,
    upserted: i32,
    duplicates: i32,
    pages: i32,
    api_credits: i32,
}

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

        let (ind, next) = pick_next_active(&order, &progress, 0).unwrap();
        assert_eq!(ind, "a");
        assert_eq!(next, 1);

        let (ind, next) = pick_next_active(&order, &progress, 1).unwrap();
        assert_eq!(ind, "b");
        assert_eq!(next, 2);

        let (ind, next) = pick_next_active(&order, &progress, 2).unwrap();
        assert_eq!(ind, "c");
        assert_eq!(next, 0);
    }

    #[test]
    fn test_pick_next_active_skips_done() {
        let order = vec!["a".into(), "b".into(), "c".into()];
        let mut progress = HashMap::new();

        let mut a = IndustryProgress::new("a".into(), 100);
        a.depleted = true;
        progress.insert("a".into(), a);

        let mut b = IndustryProgress::new("b".into(), 50);
        b.upserted = 50;
        progress.insert("b".into(), b);

        progress.insert("c".into(), IndustryProgress::new("c".into(), 100));

        let (ind, _) = pick_next_active(&order, &progress, 0).unwrap();
        assert_eq!(ind, "c");
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

        progress.insert("a".into(), IndustryProgress::new("a".into(), 100));
        let mut b = IndustryProgress::new("b".into(), 50);
        b.depleted = true;
        progress.insert("b".into(), b);
        let mut c = IndustryProgress::new("c".into(), 50);
        c.depleted = true;
        progress.insert("c".into(), c);

        let (ind, _) = pick_next_active(&order, &progress, 2).unwrap();
        assert_eq!(ind, "a");
    }

    // ---- Credit Calculation Tests ----

    #[test]
    fn test_compute_page_credits_apollo() {
        assert_eq!(compute_page_credits("apollo", 100), 1);
        assert_eq!(compute_page_credits("apollo", 0), 1);
        assert_eq!(compute_page_credits("apollo", 50), 1);
    }

    #[test]
    fn test_compute_page_credits_diffbot() {
        assert_eq!(compute_page_credits("diffbot", 25), 625);   // 25 × 25
        assert_eq!(compute_page_credits("diffbot", 100), 2500); // 100 × 25
        assert_eq!(compute_page_credits("diffbot", 0), 0);      // empty page = free
        assert_eq!(compute_page_credits("diffbot", 1), 25);     // 1 entity = 25 credits
    }

    #[test]
    fn test_estimate_saved_credits_diffbot() {
        // 10 pages skipped × 25 page_size × 25 credits/entity = 6250
        assert_eq!(estimate_saved_credits("diffbot", 10, 25), 6250);
        // 50 pages skipped × 50 page_size × 25 = 62500
        assert_eq!(estimate_saved_credits("diffbot", 50, 50), 62500);
    }

    #[test]
    fn test_estimate_saved_credits_apollo() {
        // 10 pages skipped × 1 credit/page = 10
        assert_eq!(estimate_saved_credits("apollo", 10, 100), 10);
    }

    // ---- Cursor State Tests ----

    #[test]
    fn test_cursor_state_fresh() {
        let cs = CursorState::fresh();
        assert_eq!(cs.next_page, 1);
        assert!(!cs.use_loose);
        assert!(!cs.exhausted);
        assert_eq!(cs.fetched_this_run, 0);
        assert_eq!(cs.new_this_run, 0);
    }

    #[test]
    fn test_cursor_key_format() {
        assert_eq!(db::cursor_key("fintech", "v1_strict"), "fintech::v1_strict");
        assert_eq!(
            db::cursor_key("Currency And Lending Services", "v2_broaden_geo"),
            "Currency And Lending Services::v2_broaden_geo"
        );
    }

    // ---- Industry Order Tests ----

    #[test]
    fn test_industry_order_rotation_varies() {
        let industries = vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()];

        let run_id_0 = Uuid::from_bytes([0; 16]);
        let run_id_1 = Uuid::from_bytes([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let run_id_2 = Uuid::from_bytes([2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

        let mut order0 = industries.clone();
        order0.rotate_left(run_id_0.as_bytes()[0] as usize % order0.len());
        let mut order1 = industries.clone();
        order1.rotate_left(run_id_1.as_bytes()[0] as usize % order1.len());
        let mut order2 = industries.clone();
        order2.rotate_left(run_id_2.as_bytes()[0] as usize % order2.len());

        assert_eq!(order0[0], "a"); // rotation=0
        assert_eq!(order1[0], "b"); // rotation=1
        assert_eq!(order2[0], "c"); // rotation=2
    }

    // ---- Summary Tests ----

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
    }
}
