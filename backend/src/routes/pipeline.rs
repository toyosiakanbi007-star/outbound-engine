// src/routes/pipeline.rs
//
// Full Pipeline trigger: discovery → auto-prequal chain.
//
// Uses a run-scoped `force_full_pipeline` flag in the job payload.
// Does NOT mutate stored prequal_config.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tracing::{error, info};
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct RunPipelineRequest {
    #[serde(default = "default_batch_target")]
    pub batch_target: i32,
    #[serde(default = "default_max_runtime")]
    pub max_runtime_seconds: u64,
}

fn default_batch_target() -> i32 { 500 }
fn default_max_runtime() -> u64 { 1800 }

/// POST /api/clients/:id/pipeline/run — enqueue full pipeline
///
/// Creates a discover_companies job with `force_full_pipeline: true`.
/// The orchestrator reads this flag and enqueues PREQUAL_DISPATCH
/// after discovery regardless of stored autopilot_enabled setting.
pub async fn run(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<RunPipelineRequest>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    // Verify client exists
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clients WHERE id = $1)")
        .bind(client_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to check client: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": { "message": "DB error" } })),
            )
        })?;

    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": { "message": "Client not found" } })),
        ));
    }

    // Enqueue discover_companies with force_full_pipeline flag
    let payload = json!({
        "client_id": client_id,
        "batch_target": body.batch_target,
        "max_runtime_seconds": body.max_runtime_seconds,
        "enqueue_prequal_jobs": true,
        "force_full_pipeline": true,
    });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'discover_companies', $2, 'pending', NOW())
           RETURNING id"#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to enqueue pipeline job: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": { "message": "Failed to enqueue pipeline job" } })),
        )
    })?;

    info!(
        "Enqueued full pipeline (discover_companies + force_full_pipeline) job {} for client {}",
        job_id, client_id
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "data": {
                "discovery_job_id": job_id,
                "force_full_pipeline": true,
                "message": "Full pipeline started: discovery → prequal (run-scoped, config unchanged)"
            }
        })),
    ))
}
