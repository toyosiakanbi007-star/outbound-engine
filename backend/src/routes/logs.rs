// src/routes/logs.rs
//
// Structured log query endpoint.
// Reads from the structured_logs table (populated by the batch tracing subscriber).

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

#[derive(Debug, Serialize, FromRow)]
pub struct LogRow {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub service: String,
    pub module: Option<String>,
    pub run_id: Option<Uuid>,
    pub company_id: Option<Uuid>,
    pub client_id: Option<Uuid>,
    pub job_id: Option<Uuid>,
    pub message: String,
    pub data_json: Option<JsonValue>,
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    pub run_id: Option<Uuid>,
    pub company_id: Option<Uuid>,
    pub client_id: Option<Uuid>,
    pub job_id: Option<Uuid>,
    pub level: Option<String>,
    pub service: Option<String>,
    pub module: Option<String>,
    pub search: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
    #[serde(default = "default_order")]
    pub order: String,
}

fn default_page() -> i64 { 1 }
fn default_per_page() -> i64 { 100 }
fn default_order() -> String { "desc".into() }

/// GET /api/logs — query structured logs with filters
pub async fn query(
    State(state): State<AppState>,
    Query(params): Query<LogsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;
    let per_page = params.per_page.min(500); // hard cap

    // Determine sort order
    let order_clause = if params.order == "asc" { "ASC" } else { "DESC" };

    // Build the query with optional filters
    // Note: we use ($N::type IS NULL OR column = $N) pattern for optional filters
    let query_str = format!(
        r#"SELECT id, timestamp, level, service, module,
                  run_id, company_id, client_id, job_id,
                  message, data_json
           FROM structured_logs
           WHERE ($1::uuid IS NULL OR run_id = $1)
             AND ($2::uuid IS NULL OR company_id = $2)
             AND ($3::uuid IS NULL OR client_id = $3)
             AND ($4::uuid IS NULL OR job_id = $4)
             AND ($5::text IS NULL OR level = $5)
             AND ($6::text IS NULL OR service = $6)
             AND ($7::text IS NULL OR module ILIKE '%' || $7 || '%')
             AND ($8::text IS NULL OR to_tsvector('english', message) @@ plainto_tsquery('english', $8))
             AND ($9::timestamptz IS NULL OR timestamp >= $9)
             AND ($10::timestamptz IS NULL OR timestamp <= $10)
           ORDER BY timestamp {}
           LIMIT $11 OFFSET $12"#,
        order_clause
    );

    let rows = sqlx::query_as::<_, LogRow>(&query_str)
        .bind(params.run_id)
        .bind(params.company_id)
        .bind(params.client_id)
        .bind(params.job_id)
        .bind(params.level.as_deref())
        .bind(params.service.as_deref())
        .bind(params.module.as_deref())
        .bind(params.search.as_deref())
        .bind(params.since)
        .bind(params.until)
        .bind(per_page)
        .bind(offset)
        .fetch_all(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to query logs: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": { "code": "db_error", "message": "Failed to query logs" } })),
            )
        })?;

    // Count total (for pagination) — use a simpler count query
    let total: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM structured_logs
           WHERE ($1::uuid IS NULL OR run_id = $1)
             AND ($2::uuid IS NULL OR company_id = $2)
             AND ($3::uuid IS NULL OR client_id = $3)
             AND ($4::uuid IS NULL OR job_id = $4)
             AND ($5::text IS NULL OR level = $5)
             AND ($6::timestamptz IS NULL OR timestamp >= $6)
             AND ($7::timestamptz IS NULL OR timestamp <= $7)"#,
    )
    .bind(params.run_id)
    .bind(params.company_id)
    .bind(params.client_id)
    .bind(params.job_id)
    .bind(params.level.as_deref())
    .bind(params.since)
    .bind(params.until)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    Ok(Json(json!({
        "data": rows,
        "meta": {
            "total": total,
            "page": params.page,
            "per_page": per_page,
        }
    })))
}
