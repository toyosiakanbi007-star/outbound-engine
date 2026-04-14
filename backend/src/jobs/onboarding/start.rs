// src/jobs/onboarding/start.rs
//
// Job 1: START_CLIENT_ONBOARDING
//
// Creates the onboarding run record and enqueues ENRICH_AND_CRAWL.

use reqwest::Client as HttpClient;
use serde_json::json;
use sqlx::PgPool;
use tracing::{error, info};
use uuid::Uuid;

use super::models::*;

pub async fn run_start_onboarding(
    pool: &PgPool,
    _http_client: &HttpClient,
    payload: StartOnboardingPayload,
    worker_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = payload.run_id;
    let client_id = payload.client_id;

    info!("Worker {}: starting onboarding run {} for client {}", worker_id, run_id, client_id);

    // Load run to get input_name and input_domain
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?
    .ok_or("Onboarding run not found")?;

    // Update status → enriching
    sqlx::query(
        "UPDATE client_onboarding_runs SET status = 'enriching', started_at = NOW(), updated_at = NOW() WHERE id = $1"
    )
    .bind(run_id)
    .execute(pool)
    .await?;

    // Enqueue the next job: ENRICH_AND_CRAWL
    let next_payload = EnrichAndCrawlPayload {
        client_id,
        run_id,
        client_name: run.input_name,
        client_domain: run.input_domain,
    };

    sqlx::query(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'onboarding_enrich_and_crawl', $2, 'pending', NOW())"#,
    )
    .bind(client_id)
    .bind(json!(next_payload))
    .execute(pool)
    .await?;

    info!("Worker {}: onboarding run {} → enrich_and_crawl enqueued", worker_id, run_id);

    Ok(())
}
