// src/worker.rs

use crate::db::DbPool;
use crate::jobs::models::{Job, JobStatus, JobType};
use crate::news::client::{DynNewsSourcingClient, NewsFetchRequest};
use crate::news::models::NewsItem;

// V3: Import Phase B handlers
use crate::jobs::{
    handle_analyze_employee_metrics, AnalyzeEmployeeMetricsPayload,
    handle_analyze_funding_events, AnalyzeFundingEventsPayload,
    handle_phase_b_enrich, PhaseBPayload,
};

use reqwest::Client as HttpClient;
use serde::Deserialize;
use sqlx::{self, Error};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Worker entry point.
/// Called when MODE=worker; runs an infinite loop.
pub async fn run_worker(pool: DbPool, news_client: DynNewsSourcingClient) -> anyhow::Result<()> {
    let worker_id = std::env::var("WORKER_ID").unwrap_or_else(|_| "worker-1".to_string());

    // V3: Create HTTP client for Apollo API + Azure Function calls
    let http_client = Arc::new(
        HttpClient::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .expect("Failed to create HTTP client")
    );

    tracing::info!(
        target: "backend::worker",
        "Worker starting with id={} (MODE=worker)",
        worker_id
    );
    info!("Worker starting with id={} (MODE=worker)", worker_id);

    loop {
        match fetch_next_job(&pool, &worker_id).await {
            Ok(Some(job)) => {
                if let Err(err) = process_job(&pool, &job, &worker_id, &news_client, &http_client).await {
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
pub async fn fetch_next_job(pool: &DbPool, worker_id: &str) -> Result<Option<Job>, Error> {
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

/// Process a single job: decode type, dispatch, and mark it done if successful.
pub async fn process_job(
    pool: &DbPool,
    job: &Job,
    worker_id: &str,
    news_client: &DynNewsSourcingClient,
    http_client: &Arc<HttpClient>,  // V3: Added HTTP client
) -> Result<(), Error> {
    // Parse DB job_type string -> JobType enum
    let job_type = match job.job_type_enum() {
        Ok(t) => t,
        Err(err) => {
            warn!(
                "Unknown job_type '{}' for job {} ({}) , marking as failed",
                job.job_type, job.id, err
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
            // TODO: real logic
        }
        JobType::EnrichLeads => {
            info!(
                "Worker {} handling job {} of type ENRICH_LEADS",
                worker_id, job.id
            );
            // TODO: real logic
        }
        JobType::AiPersonalize => {
            info!(
                "Worker {} handling job {} of type AI_PERSONALIZE",
                worker_id, job.id
            );
            // TODO: real logic
        }
        JobType::SendEmails => {
            info!(
                "Worker {} handling job {} of type SEND_EMAILS",
                worker_id, job.id
            );
            // TODO: real logic
        }
        JobType::ClientAcquisitionOutreach => {
            info!(
                "Worker {} handling job {} of type CLIENT_ACQUISITION_OUTREACH",
                worker_id, job.id
            );
            // TODO: real logic
        }
        JobType::FetchNews => {
            info!(
                "Worker {} handling job {} of type FETCH_NEWS",
                worker_id, job.id
            );

            if let Err(e) = handle_fetch_news_job(pool, job, worker_id, news_client).await {
                warn!(
                    "Worker {}: error while handling FETCH_NEWS job {}: {:?}",
                    worker_id, job.id, e
                );
                // Bubble DB errors up so the caller can mark job failed
                return Err(e);
            }
        }
        
        // =====================================================
        // V3: Phase B Job Handlers
        // =====================================================
        
        JobType::PhaseBEnrichApollo => {
            info!(
                "Worker {} handling job {} of type PHASE_B_ENRICH_APOLLO",
                worker_id, job.id
            );
            
            let payload: PhaseBPayload = match serde_json::from_value(job.payload.clone()) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "Worker {}: invalid PHASE_B_ENRICH_APOLLO payload for job {}: {}",
                        worker_id, job.id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("invalid payload: {}", e)).await?;
                    return Ok(());
                }
            };
            
            match handle_phase_b_enrich(pool, http_client.as_ref(), &payload, worker_id).await {
                Ok(result) => {
                    info!(
                        "Worker {}: Phase B complete for company {} (hypotheses={})",
                        worker_id,
                        payload.company_id,
                        result.get("pain_hypotheses")
                            .and_then(|h| h.as_array())
                            .map(|a| a.len())
                            .unwrap_or(0)
                    );
                }
                Err(e) => {
                    error!(
                        "Worker {}: Phase B failed for company {}: {:?}",
                        worker_id, payload.company_id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("{:?}", e)).await?;
                    return Ok(());
                }
            }
        }
        
        JobType::AnalyzeEmployeeMetrics => {
            info!(
                "Worker {} handling job {} of type ANALYZE_EMPLOYEE_METRICS",
                worker_id, job.id
            );
            
            let payload: AnalyzeEmployeeMetricsPayload = match serde_json::from_value(job.payload.clone()) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "Worker {}: invalid ANALYZE_EMPLOYEE_METRICS payload for job {}: {}",
                        worker_id, job.id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("invalid payload: {}", e)).await?;
                    return Ok(());
                }
            };
            
            match handle_analyze_employee_metrics(pool, &payload, worker_id).await {
                Ok(analysis) => {
                    info!(
                        "Worker {}: Employee metrics analysis complete for company {} (growth_90d={:?})",
                        worker_id, payload.company_id, analysis.headcount_growth_rate_90d
                    );
                }
                Err(e) => {
                    error!(
                        "Worker {}: Employee metrics analysis failed for company {}: {:?}",
                        worker_id, payload.company_id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("{:?}", e)).await?;
                    return Ok(());
                }
            }
        }
        
        JobType::AnalyzeFundingEvents => {
            info!(
                "Worker {} handling job {} of type ANALYZE_FUNDING_EVENTS",
                worker_id, job.id
            );
            
            let payload: AnalyzeFundingEventsPayload = match serde_json::from_value(job.payload.clone()) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "Worker {}: invalid ANALYZE_FUNDING_EVENTS payload for job {}: {}",
                        worker_id, job.id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("invalid payload: {}", e)).await?;
                    return Ok(());
                }
            };
            
            match handle_analyze_funding_events(pool, &payload, worker_id).await {
                Ok(analysis) => {
                    info!(
                        "Worker {}: Funding analysis complete for company {} (total={}, stage={})",
                        worker_id, payload.company_id, analysis.total_raised_formatted, analysis.stage
                    );
                }
                Err(e) => {
                    error!(
                        "Worker {}: Funding analysis failed for company {}: {:?}",
                        worker_id, payload.company_id, e
                    );
                    mark_job_failed(pool, job.id, worker_id, &format!("{:?}", e)).await?;
                    return Ok(());
                }
            }
        }
    }

    // After handler runs successfully, mark job as done.
    mark_job_done(pool, job.id).await?;

    Ok(())
}

/// Payload shape for FETCH_NEWS jobs:
///
/// {
///   "client_id": "uuid",
///   "company_id": "uuid",
///   "max_results": 10   // optional
/// }
#[derive(Debug, Clone, Deserialize)]
struct FetchNewsPayload {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub max_results: Option<u32>,
}

/// Minimal subset of company fields we need for the news request.
#[derive(Debug, sqlx::FromRow)]
struct CompanyInfo {
    pub id: Uuid,
    pub client_id: Uuid,
    pub name: String,
    pub domain: String,
    pub industry: Option<String>,
    pub country: Option<String>,
}

/// Handle a FETCH_NEWS job:
/// - parse payload,
/// - load company,
/// - call news service,
/// - insert rows into company_news (with dedupe via UNIQUE index).
async fn handle_fetch_news_job(
    pool: &DbPool,
    job: &Job,
    worker_id: &str,
    news_client: &DynNewsSourcingClient,
) -> Result<(), Error> {
    // 1) Parse payload JSON into FetchNewsPayload
    let payload: FetchNewsPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(err) => {
            warn!(
                "Worker {}: invalid FETCH_NEWS payload for job {}: {}",
                worker_id, job.id, err
            );
            // Treat as handled but bad payload; do not crash the worker.
            return Ok(());
        }
    };

    // 2) Load company from DB
    let company: Option<CompanyInfo> = sqlx::query_as::<_, CompanyInfo>(
        r#"
        SELECT
            id,
            client_id,
            name,
            domain,
            industry,
            country
        FROM companies
        WHERE id = $1 AND client_id = $2
        "#,
    )
    .bind(payload.company_id)
    .bind(payload.client_id)
    .fetch_optional(pool)
    .await?;

    let company = match company {
        Some(c) => c,
        None => {
            warn! {
                "Worker {}: FETCH_NEWS job {} refers to missing company_id={} / client_id={}",
                worker_id, job.id, payload.company_id, payload.client_id
            };
            return Ok(());
        }
    };

    // 3) Build request for the news sourcing service
    let req = NewsFetchRequest {
        client_id: company.client_id,
        company_id: company.id,
        company_name: company.name.clone(),
        domain: company.domain.clone(),
        industry: company.industry.clone(),
        country: company.country.clone(),
        max_results: payload.max_results,
    };

    // 4) Call the news service (Lambda)
    let items_res = news_client.fetch_news_for_company(&req).await;

    let items: Vec<NewsItem> = match items_res {
        Ok(list) => list,
        Err(err) => {
            // Not a DB error, so log and treat job as "handled but failed external call".
            warn!(
                "Worker {}: news service error for job {} / company {}: {}",
                worker_id, job.id, company.id, err
            );
            return Ok(());
        }
    };

    if items.is_empty() {
        info!(
            "Worker {}: FETCH_NEWS job {}: no news items returned for company {}",
            worker_id, job.id, company.id
        );
        return Ok(());
    }

    info!(
        "Worker {}: FETCH_NEWS job {}: inserting {} news items for company {}",
        worker_id,
        job.id,
        items.len(),
        company.id
    );

    // 5) Insert each NewsItem into company_news.
    //
    // Dedupe via unique index; if we hit 23505, log and continue.
    for item in items {
        let insert_res = sqlx::query(
            r#"
            INSERT INTO company_news (
                client_id,
                company_id,
                title,
                published_at,
                location,
                summary,
                url,
                source_type,
                source_name,
                tags,
                confidence
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11
            )
            "#,
        )
        .bind(company.client_id)
        .bind(company.id)
        .bind(&item.title)
        .bind(item.date)          // Option<DateTime<Utc>>
        .bind(item.location)      // Option<String>
        .bind(item.summary)       // Option<String>
        .bind(&item.url)
        .bind(&item.source_type)
        .bind(item.source_name)   // Option<String>
        .bind(item.tags)          // Vec<String> -> text[]
        .bind(item.confidence)    // f32
        .execute(pool)
        .await;

        match insert_res {
            Ok(_) => {
                info!(
                    "Worker {}: inserted news item '{}' for company {}",
                    worker_id, item.title, company.id
                );
            }
            Err(e) => {
                if let sqlx::Error::Database(db_err) = &e {
                    if db_err.code().as_deref() == Some("23505") {
                        // Unique violation => duplicate news; skip.
                        warn!(
                            "Worker {}: duplicate news item '{}' for company {}, skipping",
                            worker_id, item.title, company.id
                        );
                        continue;
                    }
                }

                // Any other DB error is serious: bubble up.
                return Err(e);
            }
        }
    }

    Ok(())
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
