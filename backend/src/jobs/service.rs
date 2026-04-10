use crate::db::DbPool;
use crate::jobs::models::{Job, JobStatus, JobType};
use serde_json::json;
use sqlx::Result;
use uuid::Uuid;

/// Create a dummy test job of type `SendEmails` for a given client_id.
pub async fn create_test_send_job(pool: &DbPool, client_id: Uuid) -> Result<Job> {
    let payload = json!({ "example": true });

    let job = sqlx::query_as::<_, Job>(
        r#"
        INSERT INTO jobs (
            client_id,
            campaign_id,
            contact_id,
            job_type,
            status,
            payload,
            assigned_worker
        )
        VALUES ($1, NULL, NULL, $2, $3, $4, NULL)
        RETURNING
            id,
            client_id,
            campaign_id,
            contact_id,
            job_type,
            status,
            payload,
            assigned_worker,
            run_at,
            attempts,
            last_error,
            created_at,
            updated_at,
            completed_at
        "#,
    )
    .bind(client_id)
    .bind(JobType::SendEmails.as_str())
    .bind(JobStatus::Pending.as_str())
    .bind(payload)
    .fetch_one(pool)
    .await?;

    Ok(job)
}
