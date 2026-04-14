// src/routes/manage.rs
//
// Management endpoints:
//   POST /api/clients/:id/flush-companies      — delete all companies + reset cursors
//   POST /api/clients/:id/manual-setup         — manually set config + ICP (no AI)
//   POST /api/queue-status/retry               — requeue failed jobs
//   POST /api/companies/bulk-prequal           — prequal multiple companies
//   POST /api/companies/bulk-aggregate         — aggregate multiple companies
//   POST /api/companies/:id/actions/aggregate  — aggregate single company

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::AppState;

// ============================================================================
// POST /api/clients/:id/flush-companies
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct FlushCompaniesRequest {
    #[serde(default = "default_true")]
    pub reset_cursors: bool,
    #[serde(default = "default_true")]
    pub cancel_jobs: bool,
}

fn default_true() -> bool { true }

pub async fn flush_companies(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<FlushCompaniesRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clients WHERE id = $1)")
        .bind(client_id).fetch_one(&state.db).await
        .map_err(|e| { error!("DB error: {:?}", e); db_error("DB error") })?;
    if !exists { return Err(not_found("Client not found")); }

    let prequal_deleted = sqlx::query("DELETE FROM company_prequal WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)")
        .bind(client_id).execute(&state.db).await.map(|r| r.rows_affected()).unwrap_or(0);
    let _ = sqlx::query("DELETE FROM v3_hypotheses WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM v3_evidence WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM company_news WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM discovered_urls WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM offer_fit_decisions WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM company_snapshots WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let _ = sqlx::query("DELETE FROM aggregate_results WHERE company_id IN (SELECT id FROM companies WHERE client_id=$1)").bind(client_id).execute(&state.db).await;
    let candidates_deleted = sqlx::query("DELETE FROM company_candidates WHERE client_id=$1")
        .bind(client_id).execute(&state.db).await.map(|r| r.rows_affected()).unwrap_or(0);
    let companies_deleted = sqlx::query("DELETE FROM companies WHERE client_id=$1")
        .bind(client_id).execute(&state.db).await.map(|r| r.rows_affected()).unwrap_or(0);

    let mut cursors_reset: u64 = 0;
    if body.reset_cursors {
        cursors_reset = sqlx::query("DELETE FROM company_fetch_runs WHERE client_id=$1")
            .bind(client_id).execute(&state.db).await.map(|r| r.rows_affected()).unwrap_or(0);
    }
    let mut jobs_cancelled: u64 = 0;
    if body.cancel_jobs {
        jobs_cancelled = sqlx::query(
            "UPDATE jobs SET status='failed', last_error='cancelled (flush)', completed_at=NOW() WHERE client_id=$1 AND status IN ('pending','running')"
        ).bind(client_id).execute(&state.db).await.map(|r| r.rows_affected()).unwrap_or(0);
    }

    info!("Flushed client {}: {} companies, {} candidates, {} cursors, {} jobs", client_id, companies_deleted, candidates_deleted, cursors_reset, jobs_cancelled);
    Ok(Json(json!({ "data": { "companies_deleted": companies_deleted, "candidates_deleted": candidates_deleted, "prequal_deleted": prequal_deleted, "cursors_reset": cursors_reset, "jobs_cancelled": jobs_cancelled } })))
}

// ============================================================================
// POST /api/clients/:id/manual-setup — set config + ICP without AI
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ManualSetupRequest {
    pub config: JsonValue,
    #[serde(default)]
    pub prequal_config: Option<JsonValue>,
    #[serde(default)]
    pub icp: Option<JsonValue>,
    /// Activate the client after setup
    #[serde(default = "default_true")]
    pub activate: bool,
}

pub async fn manual_setup(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<ManualSetupRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clients WHERE id = $1)")
        .bind(client_id).fetch_one(&state.db).await
        .map_err(|e| { error!("DB error: {:?}", e); db_error("DB error") })?;
    if !exists { return Err(not_found("Client not found")); }

    // Deactivate existing configs
    let _ = sqlx::query("UPDATE client_configs SET is_active=FALSE WHERE client_id=$1")
        .bind(client_id).execute(&state.db).await;

    // Insert new config
    sqlx::query(
        r#"INSERT INTO client_configs (client_id, config, prequal_config, is_active, version)
           VALUES ($1, $2, $3, TRUE, COALESCE((SELECT MAX(version) FROM client_configs WHERE client_id=$1), 0) + 1)"#
    )
    .bind(client_id)
    .bind(&body.config)
    .bind(&body.prequal_config.clone().unwrap_or(json!({
        "autopilot_enabled": true,
        "batch_size": 10,
        "max_batches_per_dispatch": 5,
        "dispatch_interval_minutes": 30,
        "max_attempts_per_company": 3,
        "azure_function_timeout_secs": 300
    })))
    .execute(&state.db)
    .await
    .map_err(|e| { error!("Failed to save config: {:?}", e); db_error("Failed to save config") })?;

    // Insert ICP if provided
    if let Some(ref icp) = body.icp {
        sqlx::query("INSERT INTO client_icp_profiles (client_id, icp_json) VALUES ($1, $2)")
            .bind(client_id).bind(icp)
            .execute(&state.db).await
            .map_err(|e| { error!("Failed to save ICP: {:?}", e); db_error("Failed to save ICP") })?;
    }

    if body.activate {
        let _ = sqlx::query("UPDATE clients SET is_active=TRUE, updated_at=NOW() WHERE id=$1")
            .bind(client_id).execute(&state.db).await;
    }

    info!("Manual setup for client {} — config saved, ICP={}", client_id, body.icp.is_some());
    Ok(Json(json!({ "data": { "saved": true, "client_id": client_id, "activated": body.activate } })))
}

// ============================================================================
// POST /api/queue-status/retry
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct RetryJobsRequest {
    pub job_ids: Option<Vec<Uuid>>,
    pub job_type: Option<String>,
    pub client_id: Option<Uuid>,
    pub failed_after: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn retry_jobs(
    State(state): State<AppState>,
    Json(body): Json<RetryJobsRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let jobs_to_retry: Vec<(Uuid, String, JsonValue)> = if let Some(ref ids) = body.job_ids {
        let mut rows = Vec::new();
        for id in ids {
            if let Ok(Some(row)) = sqlx::query_as::<_, (Uuid, String, JsonValue)>(
                "SELECT id, job_type, payload FROM jobs WHERE id=$1 AND status='failed'"
            ).bind(id).fetch_optional(&state.db).await { rows.push(row); }
        }
        rows
    } else {
        sqlx::query_as::<_, (Uuid, String, JsonValue)>(
            r#"SELECT id, job_type, payload FROM jobs
               WHERE status='failed' AND ($1::text IS NULL OR job_type=$1)
                 AND ($2::uuid IS NULL OR client_id=$2) AND ($3::timestamptz IS NULL OR completed_at>=$3)"#,
        )
        .bind(body.job_type.as_deref()).bind(body.client_id).bind(body.failed_after)
        .fetch_all(&state.db).await.unwrap_or_default()
    };

    if jobs_to_retry.is_empty() {
        return Ok(Json(json!({ "data": { "jobs_retried": 0, "candidates_reset": 0 } })));
    }

    let mut candidates_reset: u64 = 0;
    for (_job_id, job_type, payload) in &jobs_to_retry {
        if job_type == "prequal_batch" {
            if let Some(candidate_ids) = payload.get("candidate_ids").and_then(|v| v.as_array()) {
                for cid in candidate_ids {
                    if let Some(cid_str) = cid.as_str() {
                        if let Ok(cid_uuid) = Uuid::parse_str(cid_str) {
                            let r = sqlx::query(
                                "UPDATE company_candidates SET status='prequal_queued', prequal_started_at=NULL, prequal_last_error=NULL WHERE id=$1 AND status IN ('prequal_in_progress','prequal_error','qualified','disqualified')"
                            ).bind(cid_uuid).execute(&state.db).await;
                            if let Ok(r) = r { candidates_reset += r.rows_affected(); }
                        }
                    }
                }
            }
        } else if job_type == "prequal_dispatch" {
            if let Some(cid) = payload.get("client_id").and_then(|v| v.as_str()) {
                if let Ok(client_uuid) = Uuid::parse_str(cid) {
                    let r = sqlx::query("UPDATE company_candidates SET status='new', prequal_started_at=NULL, prequal_last_error=NULL WHERE client_id=$1 AND status='prequal_error'")
                        .bind(client_uuid).execute(&state.db).await;
                    if let Ok(r) = r { candidates_reset += r.rows_affected(); }
                }
            }
        }
    }

    let job_ids: Vec<Uuid> = jobs_to_retry.iter().map(|(id, _, _)| *id).collect();
    let retried = sqlx::query(
        "UPDATE jobs SET status='pending', last_error=NULL, assigned_worker=NULL, completed_at=NULL, run_at=NOW(), updated_at=NOW() WHERE id=ANY($1) AND status='failed'"
    ).bind(&job_ids).execute(&state.db).await.map(|r| r.rows_affected())
    .map_err(|e| { error!("Retry error: {:?}", e); db_error("Retry failed") })?;

    info!("Retried {} jobs, reset {} candidates", retried, candidates_reset);
    Ok(Json(json!({ "data": { "jobs_retried": retried, "candidates_reset": candidates_reset } })))
}

// ============================================================================
// POST /api/companies/bulk-prequal — prequal multiple selected companies
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct BulkPrequalRequest {
    pub company_ids: Vec<Uuid>,
    /// Force re-prequal even if already qualified/disqualified
    #[serde(default)]
    pub force: bool,
    /// Batch size (how many per prequal_batch job)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize { 10 }

pub async fn bulk_prequal(
    State(state): State<AppState>,
    Json(body): Json<BulkPrequalRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    if body.company_ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": { "message": "No companies selected" } }))));
    }

    let mut candidate_ids: Vec<Uuid> = Vec::new();
    let mut client_id: Option<Uuid> = None;
    let mut cleared_count: u64 = 0;

    for company_id in &body.company_ids {
        // Get client_id and latest candidate
        let row = sqlx::query_as::<_, (Uuid, Uuid)>(
            r#"SELECT cc.id, cc.client_id FROM company_candidates cc
               WHERE cc.company_id=$1 ORDER BY cc.created_at DESC LIMIT 1"#
        ).bind(company_id).fetch_optional(&state.db).await
        .map_err(|e| { error!("DB error: {:?}", e); db_error("DB error") })?;

        if let Some((cand_id, cid)) = row {
            client_id = Some(cid);

            if body.force {
                // Clear old results
                let _ = sqlx::query("DELETE FROM company_prequal WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM v3_hypotheses WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM v3_evidence WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM company_news WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM discovered_urls WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM offer_fit_decisions WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                let _ = sqlx::query("DELETE FROM company_snapshots WHERE company_id=$1").bind(company_id).execute(&state.db).await;
                cleared_count += 1;
            }

            // Reset candidate
            sqlx::query("UPDATE company_candidates SET status='prequal_queued', prequal_started_at=NULL, prequal_last_error=NULL WHERE id=$1")
                .bind(cand_id).execute(&state.db).await.ok();

            candidate_ids.push(cand_id);
        }
    }

    let client_id = client_id.ok_or_else(|| (StatusCode::BAD_REQUEST, Json(json!({ "error": { "message": "No candidates found" } }))))?;

    // Split into batches and create prequal_batch jobs
    let mut jobs_created = 0;
    for chunk in candidate_ids.chunks(body.batch_size) {
        let batch_id = Uuid::new_v4();
        let payload = json!({
            "client_id": client_id,
            "batch_id": batch_id,
            "candidate_ids": chunk,
            "force": body.force,
        });
        sqlx::query("INSERT INTO jobs (client_id, job_type, payload, status, run_at) VALUES ($1, 'prequal_batch', $2, 'pending', NOW())")
            .bind(client_id).bind(&payload).execute(&state.db).await.ok();
        jobs_created += 1;
    }

    info!("Bulk prequal: {} companies → {} candidates → {} batch jobs (force={})", body.company_ids.len(), candidate_ids.len(), jobs_created, body.force);
    Ok(Json(json!({ "data": { "companies": body.company_ids.len(), "candidates_queued": candidate_ids.len(), "jobs_created": jobs_created, "old_results_cleared": cleared_count } })))
}

// ============================================================================
// POST /api/companies/bulk-aggregate — aggregate multiple companies
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct BulkAggregateRequest {
    pub company_ids: Vec<Uuid>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

pub async fn bulk_aggregate(
    State(state): State<AppState>,
    Json(body): Json<BulkAggregateRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    if body.company_ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, Json(json!({ "error": { "message": "No companies selected" } }))));
    }

    let mut jobs_created = 0;
    for company_id in &body.company_ids {
        // Get client_id and latest run_id from v3_analysis_runs (not company_prequal)
        let row = sqlx::query_as::<_, (Uuid, Uuid)>(
            r#"SELECT a.client_id::uuid, a.run_id FROM v3_analysis_runs a
               WHERE a.company_id=$1 ORDER BY a.created_at DESC LIMIT 1"#
        ).bind(company_id).fetch_optional(&state.db).await.unwrap_or(None);

        if let Some((client_id, run_id)) = row {
            let payload = json!({
                "client_id": client_id,
                "company_id": company_id,
                "run_id": run_id,
            });
            sqlx::query("INSERT INTO jobs (client_id, job_type, payload, status, run_at) VALUES ($1, 'run_aggregate', $2, 'pending', NOW())")
                .bind(client_id).bind(&payload).execute(&state.db).await.ok();
            jobs_created += 1;
        } else {
            warn!("Skipping aggregate for company {} — no prequal result found", company_id);
        }
    }

    info!("Bulk aggregate: {} companies → {} jobs", body.company_ids.len(), jobs_created);
    Ok(Json(json!({ "data": { "companies_requested": body.company_ids.len(), "jobs_created": jobs_created } })))
}

// ============================================================================
// POST /api/companies/:id/actions/aggregate — aggregate single company
// ============================================================================

pub async fn aggregate_company(
    State(state): State<AppState>,
    Path(company_id): Path<Uuid>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    let row = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT client_id::uuid, run_id FROM v3_analysis_runs WHERE company_id=$1 ORDER BY created_at DESC LIMIT 1"
    ).bind(company_id).fetch_optional(&state.db).await
    .map_err(|e| { error!("DB error: {:?}", e); db_error("DB error") })?
    .ok_or_else(|| not_found("No prequal analysis run found — prequal must run first"))?;

    let (client_id, run_id) = row;
    let payload = json!({ "client_id": client_id, "company_id": company_id, "run_id": run_id });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO jobs (client_id, job_type, payload, status, run_at) VALUES ($1, 'run_aggregate', $2, 'pending', NOW()) RETURNING id"
    ).bind(client_id).bind(&payload).fetch_one(&state.db).await
    .map_err(|e| { error!("Failed to enqueue: {:?}", e); db_error("Failed to enqueue") })?;

    info!("Enqueued aggregate job {} for company {}", job_id, company_id);
    Ok((StatusCode::ACCEPTED, Json(json!({ "data": { "job_id": job_id, "company_id": company_id } }))))
}

// ============================================================================
fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}
fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": { "code": "not_found", "message": msg } })))
}
