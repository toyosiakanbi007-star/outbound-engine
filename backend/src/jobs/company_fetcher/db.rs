// src/jobs/company_fetcher/db.rs
//
// Database access layer for the Company Fetcher subsystem.
//
// All SQL queries live here — the orchestrator and other modules call these
// functions instead of embedding raw SQL.
//
// TABLES ACCESSED:
//   company_fetch_runs       — run lifecycle (create, update status/counters, finalize)
//   company_fetch_queries    — per-query telemetry (create, update)
//   company_candidates       — candidate attribution (insert, update status)
//   industry_yield_metrics   — adaptive quota data (read, upsert via DB function)
//   client_icp_profiles      — ICP config (read)
//   jobs                     — downstream job creation (prequal jobs)

use serde_json::Value as JsonValue;
use sqlx::PgPool;
use tracing::{debug, warn};
use uuid::Uuid;

use super::models::{
    CandidateStatus, CompanyFetchRun,
    FetchQueryStatus, FetchRunStatus, IndustryYieldMetric, QuotaPlan,
};

// ============================================================================
// Company Fetch Runs
// ============================================================================

/// Create a new fetch run row in 'running' state. Returns the run ID.
pub async fn create_fetch_run(
    pool: &PgPool,
    client_id: Uuid,
    batch_target: i32,
    quota_plan: &QuotaPlan,
) -> Result<Uuid, sqlx::Error> {
    let quota_json = serde_json::to_value(quota_plan).unwrap_or_default();

    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO company_fetch_runs (client_id, batch_target, quota_plan, status, started_at)
        VALUES ($1, $2, $3, 'running', NOW())
        RETURNING id
        "#,
    )
    .bind(client_id)
    .bind(batch_target)
    .bind(&quota_json)
    .fetch_one(pool)
    .await?;

    debug!("Created fetch run {} for client {}", id, client_id);
    Ok(id)
}

/// Load an existing run (for resume scenarios).
pub async fn load_fetch_run(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<Option<CompanyFetchRun>, sqlx::Error> {
    sqlx::query_as::<_, CompanyFetchRun>(
        "SELECT * FROM company_fetch_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await
}

/// Atomically increment run counters after processing a page.
///
/// Uses delta arithmetic so concurrent updates are safe.
/// `api_credits_delta` tracks Apollo API credit consumption (typically 1 per page).
pub async fn update_run_counters(
    pool: &PgPool,
    run_id: Uuid,
    raw_fetched_delta: i32,
    unique_upserted_delta: i32,
    duplicates_delta: i32,
    pages_fetched_delta: i32,
    api_credits_delta: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_fetch_runs SET
            raw_fetched      = raw_fetched      + $2,
            unique_upserted  = unique_upserted  + $3,
            duplicates       = duplicates       + $4,
            pages_fetched    = pages_fetched    + $5,
            api_credits_used = api_credits_used + $6,
            updated_at       = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(raw_fetched_delta)
    .bind(unique_upserted_delta)
    .bind(duplicates_delta)
    .bind(pages_fetched_delta)
    .bind(api_credits_delta)
    .execute(pool)
    .await?;

    Ok(())
}

/// Finalize a run as succeeded.
pub async fn finalize_run_succeeded(
    pool: &PgPool,
    run_id: Uuid,
    industry_summary: &JsonValue,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_fetch_runs SET
            status           = 'succeeded',
            industry_summary = $2,
            ended_at         = NOW(),
            updated_at       = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(industry_summary)
    .execute(pool)
    .await?;

    Ok(())
}

/// Finalize a run as depleted (all industry variants exhausted before batch_target).
pub async fn finalize_run_depleted(
    pool: &PgPool,
    run_id: Uuid,
    industry_summary: &JsonValue,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_fetch_runs SET
            status           = 'depleted',
            industry_summary = $2,
            ended_at         = NOW(),
            updated_at       = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(industry_summary)
    .execute(pool)
    .await?;

    Ok(())
}

/// Finalize a run as partial (stopped early: max runtime, too many dupes, etc.).
pub async fn finalize_run_partial(
    pool: &PgPool,
    run_id: Uuid,
    industry_summary: &JsonValue,
    reason: &str,
) -> Result<(), sqlx::Error> {
    let error_json = serde_json::json!({ "reason": reason });

    sqlx::query(
        r#"
        UPDATE company_fetch_runs SET
            status           = 'partial',
            industry_summary = $2,
            error            = $3,
            ended_at         = NOW(),
            updated_at       = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(industry_summary)
    .bind(&error_json)
    .execute(pool)
    .await?;

    Ok(())
}

/// Finalize a run as failed with error details.
pub async fn finalize_run_failed(
    pool: &PgPool,
    run_id: Uuid,
    error: &JsonValue,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_fetch_runs SET
            status     = 'failed',
            error      = $2,
            ended_at   = NOW(),
            updated_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(run_id)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

// ============================================================================
// Company Fetch Queries
// ============================================================================

/// Create a new query row at the start of a variant/industry query.
pub async fn create_fetch_query(
    pool: &PgPool,
    run_id: Uuid,
    client_id: Uuid,
    industry: &str,
    variant: &str,
    apollo_request: &JsonValue,
    page_start: i32,
) -> Result<Uuid, sqlx::Error> {
    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO company_fetch_queries (
            run_id, client_id, industry, variant,
            apollo_request, page_start, status, started_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'running', NOW())
        RETURNING id
        "#,
    )
    .bind(run_id)
    .bind(client_id)
    .bind(industry)
    .bind(variant)
    .bind(apollo_request)
    .bind(page_start)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

/// Update a query row after pages are fetched.
pub async fn update_fetch_query(
    pool: &PgPool,
    query_id: Uuid,
    page_end: i32,
    pages_fetched: i32,
    orgs_returned: i32,
    status: FetchQueryStatus,
    apollo_response_meta: Option<&JsonValue>,
    error: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_fetch_queries SET
            page_end             = $2,
            pages_fetched        = $3,
            orgs_returned        = $4,
            status               = $5,
            apollo_response_meta = COALESCE($6, apollo_response_meta),
            error                = $7,
            ended_at             = NOW()
        WHERE id = $1
        "#,
    )
    .bind(query_id)
    .bind(page_end)
    .bind(pages_fetched)
    .bind(orgs_returned)
    .bind(status.as_str())
    .bind(apollo_response_meta)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

// ============================================================================
// Company Candidates
// ============================================================================

/// Insert a candidate row linking a company to a fetch run.
///
/// Uses ON CONFLICT on the (run_id, company_id) unique index as a safety net
/// for the same company appearing in overlapping queries within one run.
pub async fn insert_candidate(
    pool: &PgPool,
    client_id: Uuid,
    run_id: Uuid,
    company_id: Uuid,
    industry: &str,
    variant: &str,
    was_duplicate: bool,
) -> Result<Uuid, sqlx::Error> {
    let status = if was_duplicate {
        CandidateStatus::Skipped.as_str()
    } else {
        CandidateStatus::New.as_str()
    };

    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO company_candidates (
            client_id, run_id, company_id, industry, variant, source, status, was_duplicate
        )
        VALUES ($1, $2, $3, $4, $5, 'apollo', $6, $7)
        ON CONFLICT (run_id, company_id) DO UPDATE SET
            status = company_candidates.status
        RETURNING id
        "#,
    )
    .bind(client_id)
    .bind(run_id)
    .bind(company_id)
    .bind(industry)
    .bind(variant)
    .bind(status)
    .bind(was_duplicate)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

/// Update a candidate's status (e.g. new → prequal_queued).
pub async fn update_candidate_status(
    pool: &PgPool,
    candidate_id: Uuid,
    status: CandidateStatus,
    prequal_job_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE company_candidates SET
            status         = $2,
            prequal_job_id = COALESCE($3, prequal_job_id)
        WHERE id = $1
        "#,
    )
    .bind(candidate_id)
    .bind(status.as_str())
    .bind(prequal_job_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Aggregate candidate counts for a run (telemetry / post-run reporting).
#[derive(Debug, sqlx::FromRow)]
pub struct CandidateStats {
    pub total: i64,
    pub new_count: i64,
    pub duplicates: i64,
    pub prequal_queued: i64,
}

pub async fn count_candidates_for_run(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<CandidateStats, sqlx::Error> {
    sqlx::query_as::<_, CandidateStats>(
        r#"
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'new' AND NOT was_duplicate) AS new_count,
            COUNT(*) FILTER (WHERE was_duplicate) AS duplicates,
            COUNT(*) FILTER (WHERE status = 'prequal_queued') AS prequal_queued
        FROM company_candidates
        WHERE run_id = $1
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
}

// ============================================================================
// Industry Yield Metrics
// ============================================================================

/// Load all yield metrics for a client (for quota planning).
pub async fn load_yield_metrics(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Vec<IndustryYieldMetric>, sqlx::Error> {
    sqlx::query_as::<_, IndustryYieldMetric>(
        "SELECT * FROM industry_yield_metrics WHERE client_id = $1",
    )
    .bind(client_id)
    .fetch_all(pool)
    .await
}

/// Upsert yield metrics for one industry after a run completes.
/// Calls the `upsert_industry_yield` DB function from migration 0023
/// which atomically increments rolling totals and updates depletion tracking.
pub async fn upsert_yield_metrics(
    pool: &PgPool,
    client_id: Uuid,
    industry: &str,
    fetched: i32,
    upserted: i32,
    duplicates: i32,
    depleted: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT upsert_industry_yield($1, $2, $3, $4, $5, $6)")
        .bind(client_id)
        .bind(industry)
        .bind(fetched)
        .bind(upserted)
        .bind(duplicates)
        .bind(depleted)
        .execute(pool)
        .await?;

    Ok(())
}

// ============================================================================
// Client ICP Profile
// ============================================================================

/// Load the ICP JSON for a client.
/// Returns None if no ICP profile exists.
pub async fn load_icp_json(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Option<JsonValue>, sqlx::Error> {
    sqlx::query_scalar::<_, JsonValue>(
        "SELECT icp_json FROM client_icp_profiles WHERE client_id = $1 LIMIT 1",
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await
}

// ============================================================================
// Downstream Job Creation (Prequal / NewsFetcher)
// ============================================================================

/// Create a prequal (NewsFetcher) job for a newly discovered company.
pub async fn create_prequal_job(
    pool: &PgPool,
    client_id: Uuid,
    company_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let payload = serde_json::json!({
        "client_id": client_id,
        "company_id": company_id,
    });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO jobs (
            client_id, job_type, status, payload, run_at
        )
        VALUES ($1, 'fetch_news', 'pending', $2, NOW())
        RETURNING id
        "#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(pool)
    .await?;

    debug!(
        "Created prequal job {} for company {} (client {})",
        job_id, company_id, client_id
    );
    Ok(job_id)
}

/// Batch create prequal jobs for all new, non-duplicate candidates from a run.
/// Updates each candidate's status to prequal_queued and records the job ID.
///
/// Returns the number of jobs actually created (may be less than total candidates
/// if individual inserts fail).
pub async fn enqueue_prequal_jobs_for_run(
    pool: &PgPool,
    client_id: Uuid,
    run_id: Uuid,
) -> Result<i32, sqlx::Error> {
    let candidates = sqlx::query_as::<_, (Uuid, Uuid)>(
        r#"
        SELECT id, company_id FROM company_candidates
        WHERE run_id = $1 AND status = 'new' AND was_duplicate = FALSE
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    let total = candidates.len() as i32;
    if total == 0 {
        debug!("No new candidates for run {} — no prequal jobs to create", run_id);
        return Ok(0);
    }

    let mut created = 0;

    for (candidate_id, company_id) in &candidates {
        match create_prequal_job(pool, client_id, *company_id).await {
            Ok(job_id) => {
                update_candidate_status(
                    pool,
                    *candidate_id,
                    CandidateStatus::PrequalQueued,
                    Some(job_id),
                )
                .await?;
                created += 1;
            }
            Err(e) => {
                warn!(
                    "Failed to create prequal job for candidate {} (company {}): {}",
                    candidate_id, company_id, e
                );
            }
        }
    }

    debug!(
        "Enqueued {}/{} prequal jobs for run {} (client {})",
        created, total, run_id, client_id
    );
    Ok(created)
}

// ============================================================================
// Company Reads (for orchestrator / other subsystems)
// ============================================================================

/// Minimal company info for building prequal payloads.
#[derive(Debug, sqlx::FromRow)]
pub struct CompanyBasic {
    pub id: Uuid,
    pub name: String,
    pub domain: Option<String>,
}

pub async fn load_company_basic(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<Option<CompanyBasic>, sqlx::Error> {
    sqlx::query_as::<_, CompanyBasic>(
        "SELECT id, name, domain FROM companies WHERE id = $1",
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // DB-dependent tests are integration tests. Here we verify enum string
    // values match what the DB CHECK constraints expect (compile-time safety net).

    #[test]
    fn test_candidate_status_strings_match_db() {
        // CHECK (status IN ('new','prequal_queued','prequal_done',
        //                    'qualified','disqualified','skipped'))
        assert_eq!(CandidateStatus::New.as_str(), "new");
        assert_eq!(CandidateStatus::PrequalQueued.as_str(), "prequal_queued");
        assert_eq!(CandidateStatus::PrequalDone.as_str(), "prequal_done");
        assert_eq!(CandidateStatus::Qualified.as_str(), "qualified");
        assert_eq!(CandidateStatus::Disqualified.as_str(), "disqualified");
        assert_eq!(CandidateStatus::Skipped.as_str(), "skipped");
    }

    #[test]
    fn test_fetch_run_status_strings_match_db() {
        // CHECK (status IN ('pending','running','succeeded','failed','depleted','partial'))
        assert_eq!(FetchRunStatus::Pending.as_str(), "pending");
        assert_eq!(FetchRunStatus::Running.as_str(), "running");
        assert_eq!(FetchRunStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(FetchRunStatus::Failed.as_str(), "failed");
        assert_eq!(FetchRunStatus::Depleted.as_str(), "depleted");
        assert_eq!(FetchRunStatus::Partial.as_str(), "partial");
    }

    #[test]
    fn test_fetch_query_status_strings_match_db() {
        // CHECK (status IN ('running','succeeded','failed','exhausted'))
        assert_eq!(FetchQueryStatus::Running.as_str(), "running");
        assert_eq!(FetchQueryStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(FetchQueryStatus::Failed.as_str(), "failed");
        assert_eq!(FetchQueryStatus::Exhausted.as_str(), "exhausted");
    }
}
