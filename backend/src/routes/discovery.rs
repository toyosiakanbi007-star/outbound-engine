// src/routes/discovery.rs
//
// Discovery run endpoints: trigger, list, detail, companies.
//
// Discovery runs are async: POST creates a job, worker picks it up.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::FromRow;
use tracing::{error, info};
use uuid::Uuid;

use crate::AppState;

// ============================================================================
// Models
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
pub struct DiscoveryRunRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub status: String,
    pub batch_target: i32,
    pub quota_plan: JsonValue,
    pub raw_fetched: i32,
    pub unique_upserted: i32,
    pub duplicates: i32,
    pub pages_fetched: i32,
    pub api_credits_used: i32,
    pub industry_summary: Option<JsonValue>,
    pub error: Option<JsonValue>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDiscoveryRequest {
    #[serde(default = "default_batch_target")]
    pub batch_target: i32,
    #[serde(default = "default_page_size")]
    pub page_size: i32,
    #[serde(default = "default_exploration_pct")]
    pub exploration_pct: f64,
    #[serde(default = "default_max_runtime")]
    pub max_runtime_seconds: u64,
    #[serde(default = "default_true")]
    pub enqueue_prequal_jobs: bool,
}

fn default_batch_target() -> i32 { 500 }
fn default_page_size() -> i32 { 100 }
fn default_exploration_pct() -> f64 { 0.20 }
fn default_max_runtime() -> u64 { 1800 }
fn default_true() -> bool { true }

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
fn default_per_page() -> i64 { 50 }

#[derive(Debug, Serialize, FromRow)]
pub struct CandidateRow {
    pub id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub company_domain: Option<String>,
    pub industry: Option<String>,
    pub variant: Option<String>,
    pub status: String,
    pub was_duplicate: bool,
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /api/clients/:id/discovery-runs — enqueue a discover_companies job
pub async fn create(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<CreateDiscoveryRequest>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    // Verify client exists
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clients WHERE id = $1)")
        .bind(client_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to check client: {:?}", e);
            db_error("Failed to check client")
        })?;

    if !exists {
        return Err(not_found("Client not found"));
    }

    // Enqueue discover_companies job
    let payload = json!({
        "client_id": client_id,
        "batch_target": body.batch_target,
        "page_size": body.page_size,
        "exploration_pct": body.exploration_pct,
        "max_runtime_seconds": body.max_runtime_seconds,
        "enqueue_prequal_jobs": body.enqueue_prequal_jobs,
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
        error!("Failed to enqueue discovery job: {:?}", e);
        db_error("Failed to enqueue discovery job")
    })?;

    info!("Enqueued discover_companies job {} for client {}", job_id, client_id);

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "data": {
                "job_id": job_id,
                "status": "pending",
                "message": "Discovery job enqueued"
            }
        })),
    ))
}

/// GET /api/discovery-runs — list discovery runs
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListRunsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;

    let runs = if let Some(client_id) = params.client_id {
        sqlx::query_as::<_, DiscoveryRunRow>(
            r#"SELECT id, client_id, status, batch_target, quota_plan,
                      raw_fetched, unique_upserted, duplicates, pages_fetched,
                      api_credits_used, industry_summary, error,
                      started_at, ended_at, created_at, updated_at
               FROM company_fetch_runs
               WHERE client_id = $1
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
        )
        .bind(client_id)
        .bind(params.per_page)
        .bind(offset)
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, DiscoveryRunRow>(
            r#"SELECT id, client_id, status, batch_target, quota_plan,
                      raw_fetched, unique_upserted, duplicates, pages_fetched,
                      api_credits_used, industry_summary, error,
                      started_at, ended_at, created_at, updated_at
               FROM company_fetch_runs
               ORDER BY created_at DESC
               LIMIT $1 OFFSET $2"#,
        )
        .bind(params.per_page)
        .bind(offset)
        .fetch_all(&state.db)
        .await
    };

    let runs = runs.map_err(|e| {
        error!("Failed to list discovery runs: {:?}", e);
        db_error("Failed to list discovery runs")
    })?;

    let total: i64 = if let Some(client_id) = params.client_id {
        sqlx::query_scalar("SELECT COUNT(*) FROM company_fetch_runs WHERE client_id = $1")
            .bind(client_id)
            .fetch_one(&state.db)
            .await
            .unwrap_or(0)
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM company_fetch_runs")
            .fetch_one(&state.db)
            .await
            .unwrap_or(0)
    };

    Ok(Json(json!({
        "data": runs,
        "meta": {
            "total": total,
            "page": params.page,
            "per_page": params.per_page,
        }
    })))
}

/// GET /api/discovery-runs/:id — run detail
pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let run = sqlx::query_as::<_, DiscoveryRunRow>(
        r#"SELECT id, client_id, status, batch_target, quota_plan,
                  raw_fetched, unique_upserted, duplicates, pages_fetched,
                  api_credits_used, industry_summary, error,
                  started_at, ended_at, created_at, updated_at
           FROM company_fetch_runs WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch discovery run: {:?}", e);
        db_error("Failed to fetch discovery run")
    })?;

    let run = run.ok_or_else(|| not_found("Discovery run not found"))?;

    Ok(Json(json!({ "data": run })))
}

/// GET /api/discovery-runs/:id/companies — paginated candidates from this run
pub async fn companies(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(params): Query<ListRunsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;

    let candidates = sqlx::query_as::<_, CandidateRow>(
        r#"SELECT cc.id, cc.company_id, c.name AS company_name, c.domain AS company_domain,
                  cc.industry, cc.variant, cc.status, cc.was_duplicate, cc.created_at
           FROM company_candidates cc
           JOIN companies c ON c.id = cc.company_id
           WHERE cc.run_id = $1
           ORDER BY cc.created_at DESC
           LIMIT $2 OFFSET $3"#,
    )
    .bind(run_id)
    .bind(params.per_page)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch candidates: {:?}", e);
        db_error("Failed to fetch candidates")
    })?;

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM company_candidates WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    Ok(Json(json!({
        "data": candidates,
        "meta": { "total": total, "page": params.page, "per_page": params.per_page }
    })))
}

// ============================================================================
// Helpers
// ============================================================================

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": { "code": "db_error", "message": msg } })),
    )
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": { "code": "not_found", "message": msg } })),
    )
}
