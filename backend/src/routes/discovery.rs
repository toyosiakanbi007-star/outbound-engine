// src/routes/discovery.rs
//
// Discovery run endpoints: trigger with full config, list, detail, companies.

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

/// Full discovery config — all fields from DiscoverCompaniesPayload.
/// Every field is optional; backend defaults apply if omitted.
#[derive(Debug, Deserialize)]
pub struct CreateDiscoveryRequest {
    /// Total unique companies to target. Default: 2000 (use 50-200 for testing!)
    #[serde(default)]
    pub batch_target: Option<i32>,

    /// Results per page (max 100). Default: 100
    #[serde(default)]
    pub page_size: Option<i32>,

    /// Fraction of quota for exploration vs yield-based. Default: 0.20
    #[serde(default)]
    pub exploration_pct: Option<f64>,

    /// Min companies per industry. Default: 50
    #[serde(default)]
    pub min_per_industry: Option<i32>,

    /// Max companies per industry. Default: 600
    #[serde(default)]
    pub max_per_industry: Option<i32>,

    /// Max pages per query variant. Default: 50
    #[serde(default)]
    pub max_pages_per_query: Option<i32>,

    /// Wall-clock limit in seconds. Default: 3600
    #[serde(default)]
    pub max_runtime_seconds: Option<u64>,

    /// Auto-enqueue prequal after discovery. Default: true
    #[serde(default = "default_true")]
    pub enqueue_prequal_jobs: bool,

    /// Ignore persisted cursors, re-scan from page 1. Default: false
    #[serde(default)]
    pub force_full_rescan: bool,

    /// Force full pipeline (discovery → prequal) regardless of autopilot setting
    #[serde(default)]
    pub force_full_pipeline: bool,
}

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

/// POST /api/clients/:id/discovery-runs — enqueue with full config
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

    // Build payload with all config fields
    // Only include fields that were explicitly set; backend defaults handle the rest
    let mut payload = json!({
        "client_id": client_id,
        "enqueue_prequal_jobs": body.enqueue_prequal_jobs,
        "force_full_rescan": body.force_full_rescan,
    });

    if body.force_full_pipeline {
        payload["force_full_pipeline"] = json!(true);
    }

    // Only include optional fields if set (otherwise backend defaults apply)
    let obj = payload.as_object_mut().unwrap();
    if let Some(v) = body.batch_target { obj.insert("batch_target".into(), json!(v)); }
    if let Some(v) = body.page_size { obj.insert("page_size".into(), json!(v)); }
    if let Some(v) = body.exploration_pct { obj.insert("exploration_pct".into(), json!(v)); }
    if let Some(v) = body.min_per_industry { obj.insert("min_per_industry".into(), json!(v)); }
    if let Some(v) = body.max_per_industry { obj.insert("max_per_industry".into(), json!(v)); }
    if let Some(v) = body.max_pages_per_query { obj.insert("max_pages_per_query".into(), json!(v)); }
    if let Some(v) = body.max_runtime_seconds { obj.insert("max_runtime_seconds".into(), json!(v)); }

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

    info!(
        "Enqueued discover_companies job {} for client {} (batch_target={}, enqueue_prequal={})",
        job_id, client_id,
        body.batch_target.unwrap_or(2000),
        body.enqueue_prequal_jobs,
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "data": {
                "job_id": job_id,
                "status": "pending",
                "config": payload,
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
        "meta": { "total": total, "page": params.page, "per_page": params.per_page }
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
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": { "code": "not_found", "message": msg } })))
}
