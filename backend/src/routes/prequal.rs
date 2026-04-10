// src/routes/prequal.rs
//
// Prequal endpoints: dispatch trigger, list runs, detail, latest per company.

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

#[derive(Debug, Deserialize)]
pub struct DispatchRequest {
    #[serde(default)]
    pub batch_size: Option<i32>,
    #[serde(default)]
    pub max_batches: Option<i32>,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub filters: DispatchFilters,
    #[serde(default = "default_manual")]
    pub source: String,
}

fn default_manual() -> String { "manual".into() }

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DispatchFilters {
    pub only_new_since: Option<DateTime<Utc>>,
    pub industry_name: Option<String>,
    pub run_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct ListPrequalQuery {
    pub client_id: Option<Uuid>,
    pub job_type: Option<String>,
    pub status: Option<String>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}

fn default_page() -> i64 { 1 }
fn default_per_page() -> i64 { 50 }

#[derive(Debug, Serialize, FromRow)]
pub struct PrequalJobRow {
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
pub struct PrequalResultRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub run_id: Uuid,
    pub score: f64,
    pub qualifies: bool,
    pub evidence_count: Option<i32>,
    pub distinct_sources: Option<i32>,
    pub why_now_indicators: Option<Vec<String>>,
    pub gates_passed: Option<Vec<String>>,
    pub gates_failed: Option<Vec<String>>,
    pub prequal_reasons: Option<JsonValue>,
    pub offer_fit_tags: Option<JsonValue>,
    pub full_result: Option<JsonValue>,
    pub created_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /api/clients/:id/prequal-dispatch — manually enqueue PREQUAL_DISPATCH
pub async fn dispatch(
    State(state): State<AppState>,
    Path(client_id): Path<Uuid>,
    Json(body): Json<DispatchRequest>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    // Verify client exists
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM clients WHERE id = $1)")
        .bind(client_id)
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to check client: {:?}", e);
            db_error("DB error")
        })?;

    if !exists {
        return Err(not_found("Client not found"));
    }

    let payload = json!({
        "client_id": client_id,
        "batch_size": body.batch_size,
        "max_batches": body.max_batches,
        "force": body.force,
        "filters": body.filters,
        "source": body.source,
    });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'prequal_dispatch', $2, 'pending', NOW())
           RETURNING id"#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to enqueue prequal dispatch: {:?}", e);
        db_error("Failed to enqueue prequal dispatch")
    })?;

    info!("Enqueued prequal_dispatch job {} for client {}", job_id, client_id);

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "data": {
                "job_id": job_id,
                "status": "pending",
                "message": "Prequal dispatch job enqueued"
            }
        })),
    ))
}

/// GET /api/prequal-runs — list prequal dispatch + batch jobs
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListPrequalQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;

    // Build filter conditions
    let job_type_filter = params.job_type.as_deref().unwrap_or("prequal_%");

    let runs = sqlx::query_as::<_, PrequalJobRow>(
        r#"SELECT id, client_id, job_type, status, payload, assigned_worker,
                  attempts, last_error, created_at, updated_at, completed_at
           FROM jobs
           WHERE job_type LIKE $1
             AND ($2::uuid IS NULL OR client_id = $2)
             AND ($3::text IS NULL OR status = $3)
           ORDER BY created_at DESC
           LIMIT $4 OFFSET $5"#,
    )
    .bind(job_type_filter)
    .bind(params.client_id)
    .bind(params.status.as_deref())
    .bind(params.per_page)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to list prequal runs: {:?}", e);
        db_error("Failed to list prequal runs")
    })?;

    let total: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM jobs
           WHERE job_type LIKE $1
             AND ($2::uuid IS NULL OR client_id = $2)
             AND ($3::text IS NULL OR status = $3)"#,
    )
    .bind(job_type_filter)
    .bind(params.client_id)
    .bind(params.status.as_deref())
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    Ok(Json(json!({
        "data": runs,
        "meta": { "total": total, "page": params.page, "per_page": params.per_page }
    })))
}

/// GET /api/prequal-runs/:id — job detail
pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let run = sqlx::query_as::<_, PrequalJobRow>(
        r#"SELECT id, client_id, job_type, status, payload, assigned_worker,
                  attempts, last_error, created_at, updated_at, completed_at
           FROM jobs WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch prequal run: {:?}", e);
        db_error("Failed to fetch prequal run")
    })?;

    let run = run.ok_or_else(|| not_found("Prequal run not found"))?;

    Ok(Json(json!({ "data": run })))
}

/// GET /api/companies/:id/latest-prequal — latest company_prequal result
pub async fn latest_for_company(
    State(state): State<AppState>,
    Path(company_id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let prequal = sqlx::query_as::<_, PrequalResultRow>(
        r#"SELECT id, client_id, company_id, run_id, score, qualifies,
                  evidence_count, distinct_sources, why_now_indicators,
                  gates_passed, gates_failed, prequal_reasons, offer_fit_tags,
                  full_result, created_at
           FROM company_prequal
           WHERE company_id = $1
           ORDER BY created_at DESC
           LIMIT 1"#,
    )
    .bind(company_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch prequal result: {:?}", e);
        db_error("Failed to fetch prequal result")
    })?;

    match prequal {
        Some(p) => Ok(Json(json!({ "data": p }))),
        None => Err(not_found("No prequal result found for this company")),
    }
}

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": { "code": "not_found", "message": msg } })))
}
