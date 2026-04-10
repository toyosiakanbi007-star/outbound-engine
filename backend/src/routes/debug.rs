// src/routes/debug.rs
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use tracing::error;
use uuid::Uuid;

use crate::{jobs, AppState};

#[derive(Serialize)]
pub struct TestJobResponse {
    pub ok: bool,
    pub job_id: Uuid,
}

/// POST /debug/jobs/test-send
///
/// Creates a dummy SendEmails job for a hard-coded client_id.
/// Later we’ll wire this to a real client.
pub async fn create_test_send_job_handler(
    State(state): State<AppState>,
) -> Result<Json<TestJobResponse>, (StatusCode, String)> {
    // TODO: replace this with a real client_id from DB/config
    let client_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111")
        .expect("hard-coded client_id must be a valid UUID");

    match jobs::service::create_test_send_job(&state.db, client_id).await {
        Ok(job) => Ok(Json(TestJobResponse {
            ok: true,
            job_id: job.id,
        })),
        Err(e) => {
            error!("Failed to create test send job: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create test job".to_string(),
            ))
        }
    }
}
