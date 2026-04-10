// src/jobs/prequal_worker/db.rs
//
// Database operations for the Prequal Worker subsystem.
//
// QUERIES:
//   - load_prequal_config:       read prequal_config from client_configs
//   - load_client_context:       read client_context (ICP + config) for Azure Function
//   - select_eligible_candidates: find candidates needing prequal (FOR UPDATE SKIP LOCKED)
//   - claim_candidates:          atomically set status='prequal_queued'
//   - load_candidate_company_info: join candidate + company for batch processing
//   - mark_candidate_in_progress: atomic status transition for worker safety
//   - update_candidate_success:  set qualified/disqualified after prequal
//   - update_candidate_failure:  increment attempts, set error
//   - enqueue_prequal_batch_job: insert PREQUAL_BATCH into jobs table
//   - enqueue_prequal_dispatch_job: insert PREQUAL_DISPATCH into jobs table

use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tracing::{debug, warn};
use uuid::Uuid;

use super::models::{CandidateCompanyInfo, PrequalConfig, DispatchFilters};

// ============================================================================
// Config Loading
// ============================================================================

/// Load prequal config for a client from client_configs.prequal_config.
///
/// Returns default config if column is NULL/empty or client has no active config.
pub async fn load_prequal_config(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<PrequalConfig, sqlx::Error> {
    let row = sqlx::query_scalar::<_, JsonValue>(
        r#"
        SELECT COALESCE(prequal_config, '{}'::jsonb)
        FROM client_configs
        WHERE client_id = $1 AND is_active = TRUE
        LIMIT 1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?;

    let config = match row {
        Some(val) => serde_json::from_value(val).unwrap_or_else(|e| {
            warn!("Invalid prequal_config for client {}: {}. Using defaults.", client_id, e);
            PrequalConfig::default()
        }),
        None => {
            debug!("No active config for client {} — using default prequal config", client_id);
            PrequalConfig::default()
        }
    };

    Ok(config)
}

/// Load client_context for the Azure Function prequal request.
///
/// Merges data from client_configs.config (brand, offer, tone, etc.)
/// and client_icp_profiles.icp_json (targeting, disqualify rules, etc.)
/// into the `client_context` object the Azure Function expects.
pub async fn load_client_context(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<JsonValue, sqlx::Error> {
    // Load the main client config (brand_name, niche, offer, etc.)
    let config_json = sqlx::query_scalar::<_, JsonValue>(
        r#"
        SELECT COALESCE(config, '{}'::jsonb)
        FROM client_configs
        WHERE client_id = $1 AND is_active = TRUE
        LIMIT 1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?
    .unwrap_or(json!({}));

    // Load the ICP profile (target_profile, disqualify_if, must_have_any, etc.)
    let icp_json = sqlx::query_scalar::<_, JsonValue>(
        r#"
        SELECT COALESCE(icp_json, '{}'::jsonb)
        FROM client_icp_profiles
        WHERE client_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?
    .unwrap_or(json!({}));

    // Merge: config takes precedence, ICP fills in targeting fields.
    // The Azure Function expects a single client_context object.
    let mut context = config_json.clone();
    if let Some(obj) = context.as_object_mut() {
        // Copy ICP fields into context if not already present
        if let Some(icp_obj) = icp_json.as_object() {
            for (key, value) in icp_obj {
                if !obj.contains_key(key) {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        // Ensure target_profile exists (from ICP)
        if !obj.contains_key("target_profile") {
            if let Some(tp) = icp_json.get("target_profile") {
                obj.insert("target_profile".to_string(), tp.clone());
            }
        }

        // Map ICP fields to client_context field names the Azure Function expects
        if !obj.contains_key("icp_criteria") {
            let mut icp_criteria = serde_json::Map::new();
            if let Some(v) = icp_json.get("must_have_any") {
                icp_criteria.insert("must_have_any".to_string(), v.clone());
            }
            if let Some(v) = icp_json.get("disqualify_if") {
                icp_criteria.insert("disqualify_if".to_string(), v.clone());
            }
            if !icp_criteria.is_empty() {
                obj.insert("icp_criteria".to_string(), JsonValue::Object(icp_criteria));
            }
        }

        // Map outreach_angles if present in ICP but not config
        if !obj.contains_key("outreach_angles") {
            if let Some(oa) = icp_json.get("outreach_angles") {
                obj.insert("outreach_angles".to_string(), oa.clone());
            }
        }

        // ALSO include the full raw icp_json as a nested key.
        // This ensures both access patterns work:
        //   - client_context["target_profile"]              (flattened, used by phase0/core_v3)
        //   - client_context["icp_json"]["target_profile"]  (nested, full structure)
        obj.insert("icp_json".to_string(), icp_json.clone());
    }

    Ok(context)
}

// ============================================================================
// Candidate Selection (Dispatch)
// ============================================================================

/// Row returned from the eligible-candidate query.
#[derive(Debug, sqlx::FromRow)]
pub struct EligibleCandidate {
    pub id: Uuid,
    pub company_id: Uuid,
    pub industry: Option<String>,
}

/// Select eligible candidates for prequal dispatch.
///
/// Uses FOR UPDATE SKIP LOCKED so concurrent dispatchers don't grab the same rows.
///
/// Eligible = status IN ('new') AND was_duplicate=false AND prequal_attempts < max.
/// If `force=true`, also includes candidates with status='prequal_done'/'qualified'/'disqualified'.
pub async fn select_eligible_candidates(
    pool: &PgPool,
    client_id: Uuid,
    limit: i32,
    max_attempts: i32,
    force: bool,
    filters: &DispatchFilters,
) -> Result<Vec<EligibleCandidate>, sqlx::Error> {
    // Build dynamic WHERE clauses
    // Base: always filter by client + not duplicate
    //
    // We use a single query with conditional logic rather than dynamic SQL
    // to keep things simple and injection-safe.
    let candidates = sqlx::query_as::<_, EligibleCandidate>(
        r#"
        SELECT id, company_id, industry
        FROM company_candidates
        WHERE client_id = $1
          AND was_duplicate = FALSE
          AND prequal_attempts < $2
          AND (
              -- Normal: only 'new' status (includes failed-retryable that were reset to 'new')
              status = 'new'
              -- Force: also grab already-processed candidates
              OR ($3 = TRUE AND status IN ('prequal_done', 'qualified', 'disqualified'))
          )
          -- Optional: filter by run_id
          AND ($4::uuid IS NULL OR run_id = $4)
          -- Optional: filter by industry
          AND ($5::text IS NULL OR industry = $5)
          -- Optional: filter by created_at
          AND ($6::timestamptz IS NULL OR created_at >= $6)
        ORDER BY created_at DESC
        FOR UPDATE SKIP LOCKED
        LIMIT $7
        "#,
    )
    .bind(client_id)                             // $1
    .bind(max_attempts)                          // $2
    .bind(force)                                 // $3
    .bind(filters.run_id)                        // $4
    .bind(filters.industry_name.as_deref())      // $5
    .bind(filters.only_new_since)                // $6
    .bind(limit)                                 // $7
    .fetch_all(pool)
    .await?;

    Ok(candidates)
}

/// Claim candidates by atomically updating status → 'prequal_queued'.
///
/// Call this within the same transaction as select_eligible_candidates
/// (or immediately after, using the FOR UPDATE lock).
pub async fn claim_candidates(
    pool: &PgPool,
    candidate_ids: &[Uuid],
) -> Result<u64, sqlx::Error> {
    if candidate_ids.is_empty() {
        return Ok(0);
    }

    let result = sqlx::query(
        r#"
        UPDATE company_candidates
        SET status = 'prequal_queued',
            prequal_started_at = NULL,
            prequal_completed_at = NULL
        WHERE id = ANY($1)
        "#,
    )
    .bind(candidate_ids)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// ============================================================================
// Candidate Info Loading (Batch)
// ============================================================================

/// Load candidate + company info for batch processing.
pub async fn load_candidate_company_info(
    pool: &PgPool,
    candidate_ids: &[Uuid],
) -> Result<Vec<CandidateCompanyInfo>, sqlx::Error> {
    if candidate_ids.is_empty() {
        return Ok(vec![]);
    }

    let rows = sqlx::query_as::<_, CandidateCompanyRow>(
        r#"
        SELECT
            cc.id           AS candidate_id,
            cc.company_id,
            c.name          AS company_name,
            c.domain,
            cc.industry,
            cc.status,
            cc.prequal_attempts
        FROM company_candidates cc
        JOIN companies c ON c.id = cc.company_id
        WHERE cc.id = ANY($1)
        ORDER BY cc.created_at DESC
        "#,
    )
    .bind(candidate_ids)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| CandidateCompanyInfo {
            candidate_id: r.candidate_id,
            company_id: r.company_id,
            company_name: r.company_name,
            domain: r.domain,
            industry: r.industry,
            status: r.status,
            prequal_attempts: r.prequal_attempts,
        })
        .collect())
}

/// Internal row type for the join query.
#[derive(Debug, sqlx::FromRow)]
struct CandidateCompanyRow {
    candidate_id: Uuid,
    company_id: Uuid,
    company_name: String,
    domain: Option<String>,
    industry: Option<String>,
    status: String,
    prequal_attempts: i32,
}

// ============================================================================
// Candidate Status Updates (Batch Worker)
// ============================================================================

/// Atomically mark a candidate as in_progress.
///
/// Returns true if the transition succeeded (row was in 'prequal_queued' state).
/// Returns false if the row was already being processed or done (skip it).
pub async fn mark_candidate_in_progress(
    pool: &PgPool,
    candidate_id: Uuid,
    force: bool,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE company_candidates
        SET status = 'prequal_queued',
            prequal_started_at = NOW()
        WHERE id = $1
          AND (
              status = 'prequal_queued'
              OR ($2 = TRUE AND status IN ('prequal_done', 'qualified', 'disqualified'))
          )
        "#,
    )
    .bind(candidate_id)
    .bind(force)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Update candidate after successful prequal.
pub async fn update_candidate_success(
    pool: &PgPool,
    candidate_id: Uuid,
    qualifies: bool,
    prequal_job_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    let status = if qualifies { "qualified" } else { "disqualified" };

    sqlx::query(
        r#"
        UPDATE company_candidates
        SET status              = $2,
            prequal_completed_at = NOW(),
            prequal_job_id      = COALESCE($3, prequal_job_id),
            prequal_last_error  = NULL
        WHERE id = $1
        "#,
    )
    .bind(candidate_id)
    .bind(status)
    .bind(prequal_job_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Update candidate after failed prequal attempt.
///
/// Increments prequal_attempts and stores the error.
/// If attempts >= max, leaves status as 'new' for retry (caller can check).
pub async fn update_candidate_failure(
    pool: &PgPool,
    candidate_id: Uuid,
    error_msg: &str,
    max_attempts: i32,
) -> Result<(), sqlx::Error> {
    // Increment attempts and reset to 'new' so it can be retried.
    // If attempts reach max, set a special error prefix so dispatch knows to skip.
    sqlx::query(
        r#"
        UPDATE company_candidates
        SET prequal_attempts   = prequal_attempts + 1,
            prequal_last_error = $2,
            prequal_started_at = NULL,
            status = CASE
                WHEN prequal_attempts + 1 >= $3 THEN 'skipped'
                ELSE 'new'
            END
        WHERE id = $1
        "#,
    )
    .bind(candidate_id)
    .bind(error_msg)
    .bind(max_attempts)
    .execute(pool)
    .await?;

    Ok(())
}

// ============================================================================
// Job Enqueueing
// ============================================================================

/// Enqueue a PREQUAL_BATCH job.
pub async fn enqueue_prequal_batch_job(
    pool: &PgPool,
    client_id: Uuid,
    candidate_ids: &[Uuid],
    batch_id: Uuid,
    source: &str,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "client_id": client_id,
        "candidate_ids": candidate_ids,
        "batch_id": batch_id,
        "force": false,
        "source": source,
    });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO jobs (
            client_id, job_type, status, payload, run_at
        )
        VALUES ($1, 'prequal_batch', 'pending', $2, NOW())
        RETURNING id
        "#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(pool)
    .await?;

    debug!(
        "Enqueued PREQUAL_BATCH job {} (batch_id={}, {} candidates, client={})",
        job_id, batch_id, candidate_ids.len(), client_id
    );

    // Link candidates to this job
    if !candidate_ids.is_empty() {
        sqlx::query(
            r#"
            UPDATE company_candidates
            SET prequal_job_id = $2
            WHERE id = ANY($1)
            "#,
        )
        .bind(candidate_ids)
        .bind(job_id)
        .execute(pool)
        .await?;
    }

    Ok(job_id)
}

/// Enqueue a PREQUAL_DISPATCH job (used by autopilot and scheduled triggers).
pub async fn enqueue_prequal_dispatch_job(
    pool: &PgPool,
    client_id: Uuid,
    source: &str,
) -> Result<Uuid, sqlx::Error> {
    let payload = json!({
        "client_id": client_id,
        "source": source,
    });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO jobs (
            client_id, job_type, status, payload, run_at
        )
        VALUES ($1, 'prequal_dispatch', 'pending', $2, NOW())
        RETURNING id
        "#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(pool)
    .await?;

    debug!(
        "Enqueued PREQUAL_DISPATCH job {} for client {} (source={})",
        job_id, client_id, source
    );

    Ok(job_id)
}

/// Check if there's already a pending/running PREQUAL_DISPATCH for this client.
///
/// Used by autopilot to avoid duplicate dispatch jobs.
pub async fn has_pending_prequal_dispatch(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM jobs
            WHERE client_id = $1
              AND job_type = 'prequal_dispatch'
              AND status IN ('pending', 'running')
        )
        "#,
    )
    .bind(client_id)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

// ============================================================================
// Telemetry / Backlog
// ============================================================================

/// Count candidates by status for a client (backlog view).
#[derive(Debug, sqlx::FromRow)]
pub struct PrequalBacklog {
    pub new_count: i64,
    pub queued_count: i64,
    pub qualified_count: i64,
    pub disqualified_count: i64,
    pub skipped_count: i64,
}

pub async fn count_prequal_backlog(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<PrequalBacklog, sqlx::Error> {
    let backlog = sqlx::query_as::<_, PrequalBacklog>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'new' AND was_duplicate = FALSE)       AS new_count,
            COUNT(*) FILTER (WHERE status = 'prequal_queued')   AS queued_count,
            COUNT(*) FILTER (WHERE status = 'qualified')        AS qualified_count,
            COUNT(*) FILTER (WHERE status = 'disqualified')     AS disqualified_count,
            COUNT(*) FILTER (WHERE status = 'skipped')          AS skipped_count
        FROM company_candidates
        WHERE client_id = $1
        "#,
    )
    .bind(client_id)
    .fetch_one(pool)
    .await?;

    Ok(backlog)
}

// ============================================================================
// Stale Candidate Recovery (Fire-and-Forget Support)
// ============================================================================

/// Recover candidates stuck in 'prequal_queued' due to Azure Function timeouts
/// or crashes that never wrote to v3_analysis_runs.
///
/// Calls the DB function recover_stale_prequal_candidates() which resets
/// stale candidates to 'new' so dispatch can retry them.
///
/// Should be called at the start of each PREQUAL_BATCH or PREQUAL_DISPATCH.
pub async fn recover_stale_candidates(
    pool: &PgPool,
    stale_threshold_minutes: i32,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        WITH stale AS (
            SELECT id
            FROM company_candidates
            WHERE status = 'prequal_queued'
              AND prequal_started_at IS NOT NULL
              AND prequal_started_at < NOW() - make_interval(mins => $1)
              AND prequal_attempts < 4
            FOR UPDATE SKIP LOCKED
        )
        UPDATE company_candidates cc
        SET status             = 'new',
            prequal_last_error = 'stale: reset after timeout',
            prequal_started_at = NULL
        FROM stale
        WHERE cc.id = stale.id
        "#,
    )
    .bind(stale_threshold_minutes)
    .execute(pool)
    .await?;

    let recovered = result.rows_affected();

    if recovered > 0 {
        debug!("Recovered {} stale prequal candidates (threshold={}min)", recovered, stale_threshold_minutes);
    }

    Ok(recovered)
}

// ============================================================================
// Candidate Status Update by Company (for prequal_listener)
// ============================================================================

/// Update company_candidates.status based on company_id + client_id.
///
/// Used by the prequal_listener as a belt-and-suspenders complement to the
/// DB trigger (trg_sync_candidate_status). If the trigger already ran,
/// this is a no-op (the WHERE clause won't match any rows).
pub async fn update_candidate_status_by_company(
    pool: &PgPool,
    company_id: Uuid,
    client_id: Uuid,
    qualifies: bool,
    score: Option<f64>,
) -> Result<u64, sqlx::Error> {
    let new_status = if qualifies { "qualified" } else { "disqualified" };

    let result = sqlx::query(
        r#"
        UPDATE company_candidates
        SET status              = $3,
            prequal_completed_at = NOW(),
            prequal_last_error  = NULL
        WHERE id = (
            SELECT id
            FROM company_candidates
            WHERE company_id = $1
              AND client_id  = $2
              AND status IN ('prequal_queued', 'new')
            ORDER BY created_at DESC
            LIMIT 1
        )
        "#,
    )
    .bind(company_id)
    .bind(client_id)
    .bind(new_status)
    .execute(pool)
    .await?;

    let affected = result.rows_affected();

    if affected > 0 {
        debug!(
            "Updated candidate status for company {} / client {}: {} (score={:.2})",
            company_id, client_id, new_status, score.unwrap_or(0.0)
        );
    }

    Ok(affected)
}
