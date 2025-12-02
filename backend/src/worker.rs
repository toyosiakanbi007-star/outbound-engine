// src/worker.rs

use crate::db::DbPool;
use crate::jobs::models::{Job, JobStatus, JobType};
use sqlx::Error;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Worker entry point.
/// Called when MODE=worker; runs an infinite loop.
pub async fn run_worker(pool: DbPool) {
    let worker_id = std::env::var("WORKER_ID").unwrap_or_else(|_| "worker-1".to_string());
    info!("Worker starting with id={} (MODE=worker)", worker_id);

    loop {
        match fetch_next_job(&pool, &worker_id).await {
            Ok(Some(job)) => {
                if let Err(err) = process_job(&pool, &job, &worker_id).await {
                    error!("Error while processing job {}: {:?}", job.id, err);

                    if let Err(e) =
                        mark_job_failed(&pool, job.id, &worker_id, &format!("{:?}", err)).await
                    {
                        error!("Failed to mark job {} as failed: {:?}", job.id, e);
                    }
                }
            }
            Ok(None) => {
                info!("No jobs available, sleeping…");
                sleep(Duration::from_secs(2)).await;
            }
            Err(err) => {
                error!("Error fetching next job: {:?}", err);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Fetch the next pending job and mark it as running for this worker.
/// Uses a single UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING ... query.
pub async fn fetch_next_job(
    pool: &DbPool,
    worker_id: &str,
) -> Result<Option<Job>, Error> {
    let job = sqlx::query_as::<_, Job>(
        r#"
        UPDATE jobs
        SET status = $2,
            assigned_worker = $1,
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = $3
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
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
    // $1 = assigned_worker
    .bind(worker_id)
    // $2 = new status
    .bind(JobStatus::Running.as_str())
    // $3 = old status filter
    .bind(JobStatus::Pending.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

/// Process a single job: decode type, log, and mark it done (for now).
pub async fn process_job(
    pool: &DbPool,
    job: &Job,
    worker_id: &str,
) -> Result<(), Error> {
    let job_type = match parse_job_type(&job.job_type) {
        Some(t) => t,
        None => {
            warn!(
                "Unknown job_type '{}' for job {}, marking as failed",
                job.job_type, job.id
            );
            mark_job_failed(pool, job.id, worker_id, "unknown job_type").await?;
            return Ok(());
        }
    };

    match job_type {
        JobType::DiscoverProspects => {
            info!(
                "Worker {} handling job {} of type DISCOVER_PROSPECTS",
                worker_id, job.id
            );
        }
        JobType::EnrichLeads => {
            info!(
                "Worker {} handling job {} of type ENRICH_LEADS",
                worker_id, job.id
            );
        }
        JobType::AiPersonalize => {
            info!(
                "Worker {} handling job {} of type AI_PERSONALIZE",
                worker_id, job.id
            );
        }
        JobType::SendEmails => {
            info!(
                "Worker {} handling job {} of type SEND_EMAILS",
                worker_id, job.id
            );
        }
        JobType::ClientAcquisitionOutreach => {
            info!(
                "Worker {} handling job {} of type CLIENT_ACQUISITION_OUTREACH",
                worker_id, job.id
            );
        }
    }

    // TODO: actual business logic for each type.
    // For now we just mark the job as done immediately.
    mark_job_done(pool, job.id).await?;

    Ok(())
}

/// Map job_type string from the DB into the JobType enum.
fn parse_job_type(s: &str) -> Option<JobType> {
    match s {
        "discover_prospects" => Some(JobType::DiscoverProspects),
        "enrich_leads" => Some(JobType::EnrichLeads),
        "ai_personalize" => Some(JobType::AiPersonalize),
        "send_emails" => Some(JobType::SendEmails),
        "client_acquisition_outreach" => Some(JobType::ClientAcquisitionOutreach),
        _ => None,
    }
}

async fn mark_job_done(pool: &DbPool, job_id: Uuid) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = $2,
            updated_at = NOW(),
            completed_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .bind(JobStatus::Done.as_str())
    .execute(pool)
    .await?;

    Ok(())
}

async fn mark_job_failed(
    pool: &DbPool,
    job_id: Uuid,
    worker_id: &str,
    error_message: &str,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = $2,
            last_error = $3,
            updated_at = NOW(),
            completed_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .bind(JobStatus::Failed.as_str())
    .bind(format!("worker {}: {}", worker_id, error_message))
    .execute(pool)
    .await?;

    Ok(())
}
