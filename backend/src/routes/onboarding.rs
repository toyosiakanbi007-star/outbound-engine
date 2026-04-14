// src/routes/onboarding.rs
//
// Onboarding API endpoints:
//   POST   /api/clients/:id/onboarding-runs       — start onboarding
//   GET    /api/onboarding-runs                    — list all runs
//   GET    /api/onboarding-runs/:id                — run detail
//   GET    /api/onboarding-runs/:id/artifacts      — artifacts for run
//   POST   /api/onboarding-runs/:id/activate       — activate drafts into live config
//   POST   /api/onboarding-runs/:id/regenerate     — re-run draft generation

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tracing::{error, info};
use uuid::Uuid;

use crate::AppState;
use crate::jobs::onboarding::models::*;

// ============================================================================
// POST /api/clients/:id/onboarding-runs — start onboarding
// ============================================================================

pub async fn create(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<CreateOnboardingRequest>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    let domain = body.client_domain.trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();

    // Create onboarding run
    let run_id = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO client_onboarding_runs
            (client_id, status, input_name, input_domain, operator_note)
           VALUES ($1, 'pending', $2, $3, $4)
           RETURNING id"#,
    )
    .bind(client_id)
    .bind(&body.client_name)
    .bind(&domain)
    .bind(body.operator_note.as_deref())
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to create onboarding run: {:?}", e);
        db_error("Failed to create onboarding run")
    })?;

    // Enqueue START_CLIENT_ONBOARDING job
    let payload = StartOnboardingPayload { client_id, run_id };

    sqlx::query(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'start_client_onboarding', $2, 'pending', NOW())"#,
    )
    .bind(client_id)
    .bind(json!(payload))
    .execute(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to enqueue onboarding job: {:?}", e);
        db_error("Failed to enqueue onboarding job")
    })?;

    info!("Created onboarding run {} for client {} ({})", run_id, client_id, domain);

    Ok((StatusCode::ACCEPTED, Json(json!({
        "data": {
            "run_id": run_id,
            "status": "pending",
            "message": "Onboarding started — enrichment and crawl will begin shortly"
        }
    }))))
}

// ============================================================================
// GET /api/onboarding-runs — list all runs
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    pub client_id: Option<Uuid>,
    pub status: Option<String>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}
fn default_page() -> i64 { 1 }
fn default_per_page() -> i64 { 20 }

pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListRunsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;

    let runs = sqlx::query_as::<_, OnboardingRunRow>(
        r#"SELECT * FROM client_onboarding_runs
           WHERE ($1::uuid IS NULL OR client_id = $1)
             AND ($2::text IS NULL OR status = $2)
           ORDER BY created_at DESC
           LIMIT $3 OFFSET $4"#,
    )
    .bind(params.client_id)
    .bind(params.status.as_deref())
    .bind(params.per_page)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("Failed to list onboarding runs: {:?}", e); db_error("Query failed") })?;

    let total: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM client_onboarding_runs
           WHERE ($1::uuid IS NULL OR client_id = $1)
             AND ($2::text IS NULL OR status = $2)"#,
    )
    .bind(params.client_id)
    .bind(params.status.as_deref())
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    Ok(Json(json!({
        "data": runs,
        "meta": { "total": total, "page": params.page, "per_page": params.per_page }
    })))
}

// ============================================================================
// GET /api/onboarding-runs/:id — run detail
// ============================================================================

pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("Failed to fetch run: {:?}", e); db_error("Query failed") })?
    .ok_or_else(|| not_found("Onboarding run not found"))?;

    // Also get artifact count by type
    let artifact_summary: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT artifact_type, COUNT(*) as count
           FROM client_onboarding_artifacts
           WHERE run_id = $1
           GROUP BY artifact_type"#,
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let artifacts_map: serde_json::Map<String, JsonValue> = artifact_summary.into_iter()
        .map(|(t, c)| (t, json!(c)))
        .collect();

    Ok(Json(json!({
        "data": {
            "run": run,
            "artifact_counts": artifacts_map,
        }
    })))
}

// ============================================================================
// GET /api/onboarding-runs/:id/artifacts — all artifacts for a run
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ArtifactQuery {
    pub artifact_type: Option<String>,
}

pub async fn artifacts(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(params): Query<ArtifactQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let rows = sqlx::query_as::<_, OnboardingArtifactRow>(
        r#"SELECT * FROM client_onboarding_artifacts
           WHERE run_id = $1
             AND ($2::text IS NULL OR artifact_type = $2)
           ORDER BY created_at ASC"#,
    )
    .bind(id)
    .bind(params.artifact_type.as_deref())
    .fetch_all(&state.db)
    .await
    .map_err(|e| { error!("Failed to fetch artifacts: {:?}", e); db_error("Query failed") })?;

    Ok(Json(json!({ "data": rows })))
}

// ============================================================================
// POST /api/onboarding-runs/:id/activate — push drafts to live config
// ============================================================================

pub async fn activate(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Json(body): Json<ActivateOnboardingRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Load run
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("Activate error: {:?}", e); db_error("Query failed") })?
    .ok_or_else(|| not_found("Run not found"))?;

    if run.status != "review_ready" && run.status != "activated" {
        return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": { "code": "invalid_status", "message": format!("Run is '{}', must be 'review_ready'", run.status) }
        }))));
    }

    let client_id = run.client_id;

    // Use edited versions if provided, otherwise use drafts
    let config = body.edited_config
        .or(run.draft_config)
        .unwrap_or(json!({}));

    let prequal_config = body.edited_prequal_config
        .or(run.draft_prequal_config)
        .unwrap_or(json!({}));

    let icp = body.edited_icp
        .or(run.draft_icp)
        .unwrap_or(json!({}));

    // Deactivate existing configs
    sqlx::query("UPDATE client_configs SET is_active = FALSE WHERE client_id = $1")
        .bind(client_id)
        .execute(&state.db)
        .await
        .ok();

    // Insert new active config
    sqlx::query(
        r#"INSERT INTO client_configs (client_id, config, prequal_config, is_active, version)
           VALUES ($1, $2, $3, TRUE,
                   COALESCE((SELECT MAX(version) FROM client_configs WHERE client_id = $1), 0) + 1)"#,
    )
    .bind(client_id)
    .bind(&config)
    .bind(&prequal_config)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("Failed to insert config: {:?}", e); db_error("Failed to save config") })?;

    // Insert new ICP profile
    sqlx::query(
        r#"INSERT INTO client_icp_profiles (client_id, icp_json)
           VALUES ($1, $2)"#,
    )
    .bind(client_id)
    .bind(&icp)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("Failed to insert ICP: {:?}", e); db_error("Failed to save ICP") })?;

    // Activate client if not already active
    sqlx::query("UPDATE clients SET is_active = TRUE, updated_at = NOW() WHERE id = $1")
        .bind(client_id)
        .execute(&state.db)
        .await
        .ok();

    // Mark run as activated
    sqlx::query(
        "UPDATE client_onboarding_runs SET status = 'activated', activated_at = NOW(), updated_at = NOW() WHERE id = $1"
    )
    .bind(run_id)
    .execute(&state.db)
    .await
    .ok();

    info!("Activated onboarding run {} for client {}", run_id, client_id);

    // Optionally launch discovery
    let mut discovery_job_id: Option<Uuid> = None;
    if body.run_discovery {
        let batch_target = body.discovery_batch_target.unwrap_or(100);
        let disc_payload = json!({
            "client_id": client_id,
            "batch_target": batch_target,
            "page_size": 100,
            "enqueue_prequal_jobs": true,
        });

        let jid = sqlx::query_scalar::<_, Uuid>(
            r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
               VALUES ($1, 'discover_companies', $2, 'pending', NOW())
               RETURNING id"#,
        )
        .bind(client_id)
        .bind(&disc_payload)
        .fetch_one(&state.db)
        .await
        .ok();

        discovery_job_id = jid;
        if let Some(jid) = &discovery_job_id {
            info!("Launched discovery job {} after onboarding activation", jid);
        }
    }

    Ok(Json(json!({
        "data": {
            "status": "activated",
            "client_id": client_id,
            "config_saved": true,
            "icp_saved": true,
            "discovery_job_id": discovery_job_id,
        }
    })))
}

// ============================================================================
// POST /api/onboarding-runs/:id/regenerate — re-run draft generation
// ============================================================================

pub async fn regenerate(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("Regen error: {:?}", e); db_error("Query failed") })?
    .ok_or_else(|| not_found("Run not found"))?;

    if run.knowledge_pack.is_none() {
        return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": { "code": "no_knowledge_pack", "message": "No knowledge pack — enrich step must complete first" }
        }))));
    }

    // Reset status and enqueue generation
    sqlx::query(
        "UPDATE client_onboarding_runs SET status = 'generating', updated_at = NOW() WHERE id = $1"
    )
    .bind(run_id)
    .execute(&state.db)
    .await
    .ok();

    let payload = json!({ "client_id": run.client_id, "run_id": run_id });

    sqlx::query(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'onboarding_generate_drafts', $2, 'pending', NOW())"#,
    )
    .bind(run.client_id)
    .bind(&payload)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("Failed to enqueue regen: {:?}", e); db_error("Failed") })?;

    Ok((StatusCode::ACCEPTED, Json(json!({
        "data": { "status": "generating", "message": "Regeneration started" }
    }))))
}

// ============================================================================
// Helpers
// ============================================================================

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": { "code": "not_found", "message": msg } })))
}
