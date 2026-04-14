// src/jobs/onboarding/finalize.rs
//
// Job 4: ONBOARDING_FINALIZE_DRAFT
//
// Validates that all drafts exist and marks the run as review_ready.
// The operator then reviews in the frontend and clicks "Activate".

use reqwest::Client as HttpClient;
use serde_json::json;
use sqlx::PgPool;
use tracing::{info, warn};

use super::models::*;

pub async fn run_finalize_draft(
    pool: &PgPool,
    _http_client: &HttpClient,
    payload: FinalizeDraftPayload,
    worker_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = payload.run_id;

    info!("Worker {}: finalizing onboarding run {}", worker_id, run_id);

    // Load run
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?
    .ok_or("Onboarding run not found")?;

    // Validate drafts exist
    let has_config = run.draft_config.is_some();
    let has_icp = run.draft_icp.is_some();
    let has_prequal = run.draft_prequal_config.is_some();

    if !has_config || !has_icp {
        warn!(
            "Worker {}: run {} missing drafts (config={}, icp={}, prequal={})",
            worker_id, run_id, has_config, has_icp, has_prequal
        );
        sqlx::query(
            r#"UPDATE client_onboarding_runs
               SET status = 'failed', error = 'Draft generation incomplete', completed_at = NOW(), updated_at = NOW()
               WHERE id = $1"#
        )
        .bind(run_id)
        .execute(pool)
        .await?;

        return Ok(());
    }

    // Mark as review_ready
    sqlx::query(
        r#"UPDATE client_onboarding_runs
           SET status = 'review_ready', completed_at = NOW(), updated_at = NOW()
           WHERE id = $1"#
    )
    .bind(run_id)
    .execute(pool)
    .await?;

    info!("Worker {}: onboarding run {} is now review_ready", worker_id, run_id);

    Ok(())
}
