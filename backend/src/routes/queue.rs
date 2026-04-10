// src/routes/queue.rs
//
// Queue status and job listing endpoints.
// Worker info is best-effort (inferred from jobs table until heartbeats table exists).

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::FromRow;
use tracing::error;
use uuid::Uuid;

use crate::AppState;

// ============================================================================
// Models
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
pub struct JobRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub job_type: String,
    pub status: String,
    pub payload: JsonValue,
    pub assigned_worker: Option<String>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, FromRow)]
struct TypeCount {
    job_type: String,
    status: String,
    count: i64,
}

#[derive(Debug, Serialize, FromRow)]
struct InferredWorker {
    assigned_worker: String,
    last_seen: DateTime<Utc>,
    jobs_running: i64,
    jobs_completed_24h: i64,
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub status: Option<String>,
    pub job_type: Option<String>,
    pub client_id: Option<Uuid>,
    pub since: Option<DateTime<Utc>>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}

fn default_page() -> i64 { 1 }
fn default_per_page() -> i64 { 50 }

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/queue-status — aggregate job queue stats
pub async fn status(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Counts by type and status
    let counts = sqlx::query_as::<_, TypeCount>(
        r#"SELECT job_type, status, COUNT(*) AS count
           FROM jobs
           WHERE created_at > NOW() - INTERVAL '24 hours'
              OR status IN ('pending', 'running')
           GROUP BY job_type, status"#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to get queue status: {:?}", e);
        db_error("Failed to get queue status")
    })?;

    // Build by_type map
    let mut by_type: serde_json::Map<String, JsonValue> = serde_json::Map::new();
    let mut total_pending: i64 = 0;
    let mut total_running: i64 = 0;
    let mut total_failed_24h: i64 = 0;
    let mut total_completed_24h: i64 = 0;

    for row in &counts {
        let entry = by_type
            .entry(row.job_type.clone())
            .or_insert_with(|| json!({}));

        let obj = entry.as_object_mut().unwrap();
        obj.insert(row.status.clone(), json!(row.count));

        match row.status.as_str() {
            "pending" => total_pending += row.count,
            "running" => total_running += row.count,
            "failed" => total_failed_24h += row.count,
            "done" => total_completed_24h += row.count,
            _ => {}
        }
    }

    // Oldest pending job age
    let oldest_pending: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT MIN(created_at) FROM jobs WHERE status = 'pending'",
    )
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let oldest_age_secs = oldest_pending
        .map(|t| (Utc::now() - t).num_seconds().max(0))
        .unwrap_or(0);

    // Average completion time (last 24h)
    let avg_duration: Option<f64> = sqlx::query_scalar(
        r#"SELECT AVG(EXTRACT(EPOCH FROM (completed_at - created_at)))
           FROM jobs
           WHERE status = 'done' AND completed_at > NOW() - INTERVAL '24 hours'"#,
    )
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    // Stuck jobs (running for > 30 minutes)
    let stuck: Vec<JobRow> = sqlx::query_as::<_, JobRow>(
        r#"SELECT id, client_id, job_type, status, payload, assigned_worker,
                  attempts, last_error, created_at, updated_at, completed_at
           FROM jobs
           WHERE status = 'running' AND updated_at < NOW() - INTERVAL '30 minutes'
           ORDER BY updated_at ASC
           LIMIT 10"#,
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Ok(Json(json!({
        "data": {
            "total_pending": total_pending,
            "total_running": total_running,
            "total_failed_24h": total_failed_24h,
            "total_completed_24h": total_completed_24h,
            "by_type": by_type,
            "oldest_pending_age_seconds": oldest_age_secs,
            "avg_completion_time_seconds": avg_duration.unwrap_or(0.0),
            "stuck_jobs": stuck,
        }
    })))
}

/// GET /api/queue-status/jobs — paginated job list
pub async fn jobs(
    State(state): State<AppState>,
    Query(params): Query<ListJobsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;

    let rows = sqlx::query_as::<_, JobRow>(
        r#"SELECT id, client_id, job_type, status, payload, assigned_worker,
                  attempts, last_error, created_at, updated_at, completed_at
           FROM jobs
           WHERE ($1::text IS NULL OR status = $1)
             AND ($2::text IS NULL OR job_type = $2)
             AND ($3::uuid IS NULL OR client_id = $3)
             AND ($4::timestamptz IS NULL OR created_at >= $4)
           ORDER BY created_at DESC
           LIMIT $5 OFFSET $6"#,
    )
    .bind(params.status.as_deref())
    .bind(params.job_type.as_deref())
    .bind(params.client_id)
    .bind(params.since)
    .bind(params.per_page)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to list jobs: {:?}", e);
        db_error("Failed to list jobs")
    })?;

    let total: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM jobs
           WHERE ($1::text IS NULL OR status = $1)
             AND ($2::text IS NULL OR job_type = $2)
             AND ($3::uuid IS NULL OR client_id = $3)
             AND ($4::timestamptz IS NULL OR created_at >= $4)"#,
    )
    .bind(params.status.as_deref())
    .bind(params.job_type.as_deref())
    .bind(params.client_id)
    .bind(params.since)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    Ok(Json(json!({
        "data": rows,
        "meta": { "total": total, "page": params.page, "per_page": params.per_page }
    })))
}

/// GET /api/workers — best-effort worker info inferred from jobs table
///
/// Without a worker_heartbeats table, we infer worker activity from
/// the assigned_worker field on recently-active jobs.
pub async fn workers(
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let inferred = sqlx::query_as::<_, InferredWorker>(
        r#"SELECT
             assigned_worker,
             MAX(updated_at) AS last_seen,
             COUNT(*) FILTER (WHERE status = 'running') AS jobs_running,
             COUNT(*) FILTER (WHERE status = 'done' AND completed_at > NOW() - INTERVAL '24 hours') AS jobs_completed_24h
           FROM jobs
           WHERE assigned_worker IS NOT NULL
             AND updated_at > NOW() - INTERVAL '1 hour'
           GROUP BY assigned_worker
           ORDER BY MAX(updated_at) DESC"#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to infer worker status: {:?}", e);
        db_error("Failed to infer worker status")
    })?;

    Ok(Json(json!({
        "data": inferred,
        "meta": {
            "source": "inferred_from_jobs",
            "note": "Worker info is inferred from job activity. Workers idle for >1 hour won't appear."
        }
    })))
}

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}
