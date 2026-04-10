// src/routes/clients.rs
//
// Client CRUD + config + ICP endpoints.
//
// Tables: clients, client_configs, client_icp_profiles

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use tracing::{error, info};
use uuid::Uuid;

use crate::AppState;

// ============================================================================
// Models
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
pub struct ClientRow {
    pub id: Uuid,
    pub name: String,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ClientWithStats {
    #[serde(flatten)]
    pub client: ClientRow,
    pub stats: ClientStats,
}

#[derive(Debug, Serialize, Default)]
pub struct ClientStats {
    pub total_companies: i64,
    pub qualified_companies: i64,
    pub disqualified_companies: i64,
    pub pending_prequal: i64,
    pub last_discovery_run: Option<DateTime<Utc>>,
    pub last_prequal_run: Option<DateTime<Utc>>,
    pub autopilot_enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateClientRequest {
    pub name: String,
    #[serde(default)]
    pub config: Option<JsonValue>,
    #[serde(default)]
    pub icp: Option<JsonValue>,
    #[serde(default)]
    pub prequal_config: Option<JsonValue>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateClientRequest {
    pub name: Option<String>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct ClientConfigRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub config: JsonValue,
    pub version: i32,
    pub is_active: bool,
    pub prequal_config: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateConfigRequest {
    #[serde(default)]
    pub config: Option<JsonValue>,
    #[serde(default)]
    pub prequal_config: Option<JsonValue>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct IcpRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub icp_json: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateIcpRequest {
    pub icp_json: JsonValue,
}

#[derive(Debug, Deserialize)]
pub struct ListClientsQuery {
    #[serde(default)]
    pub active_only: Option<bool>,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/clients — list all clients with summary stats
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListClientsQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let clients = if params.active_only.unwrap_or(false) {
        sqlx::query_as::<_, ClientRow>(
            "SELECT id, name, is_active, created_at, updated_at FROM clients WHERE is_active = true ORDER BY name",
        )
        .fetch_all(&state.db)
        .await
    } else {
        sqlx::query_as::<_, ClientRow>(
            "SELECT id, name, is_active, created_at, updated_at FROM clients ORDER BY name",
        )
        .fetch_all(&state.db)
        .await
    };

    let clients = clients.map_err(|e| {
        error!("Failed to list clients: {:?}", e);
        db_error("Failed to list clients")
    })?;

    let mut result = Vec::with_capacity(clients.len());

    for client in clients {
        let stats = load_client_stats(&state.db, client.id).await;
        result.push(ClientWithStats { client, stats });
    }

    Ok(Json(serde_json::json!({ "data": result })))
}

/// POST /api/clients — create a new client
pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateClientRequest>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    if body.name.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": { "code": "validation_error", "message": "name is required" }
            })),
        ));
    }

    // 1. Create client
    let client = sqlx::query_as::<_, ClientRow>(
        "INSERT INTO clients (name) VALUES ($1) RETURNING id, name, is_active, created_at, updated_at",
    )
    .bind(body.name.trim())
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e {
            if db_err.code().as_deref() == Some("23505") {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "error": { "code": "duplicate", "message": "A client with this name already exists" }
                    })),
                );
            }
        }
        error!("Failed to create client: {:?}", e);
        db_error("Failed to create client")
    })?;

    // 2. Create config (if provided, otherwise empty)
    let config = body.config.unwrap_or(serde_json::json!({}));
    let prequal_config = body.prequal_config.unwrap_or(serde_json::json!({}));

    sqlx::query(
        r#"INSERT INTO client_configs (client_id, config, prequal_config)
           VALUES ($1, $2, $3)"#,
    )
    .bind(client.id)
    .bind(&config)
    .bind(&prequal_config)
    .execute(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to create client config: {:?}", e);
        db_error("Failed to create client config")
    })?;

    // 3. Create ICP profile (if provided)
    if let Some(icp) = body.icp {
        sqlx::query(
            "INSERT INTO client_icp_profiles (client_id, icp_json) VALUES ($1, $2)",
        )
        .bind(client.id)
        .bind(&icp)
        .execute(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to create ICP profile: {:?}", e);
            db_error("Failed to create ICP profile")
        })?;
    }

    info!("Created client: {} ({})", client.name, client.id);

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "data": client })),
    ))
}

/// GET /api/clients/:id — client detail
pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let client = sqlx::query_as::<_, ClientRow>(
        "SELECT id, name, is_active, created_at, updated_at FROM clients WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch client: {:?}", e);
        db_error("Failed to fetch client")
    })?;

    let client = client.ok_or_else(|| not_found("Client not found"))?;
    let stats = load_client_stats(&state.db, client.id).await;

    // Load recent runs
    let recent_runs = sqlx::query_as::<_, RecentRun>(
        r#"SELECT id, status, batch_target, unique_upserted, pages_fetched, created_at, ended_at
           FROM company_fetch_runs
           WHERE client_id = $1
           ORDER BY created_at DESC
           LIMIT 5"#,
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    // Load top qualified companies
    let top_companies = sqlx::query_as::<_, TopCompany>(
        r#"SELECT c.id, c.name, c.domain, c.industry,
                  cp.score, cp.qualifies, cp.why_now_indicators
           FROM companies c
           JOIN company_prequal cp ON cp.company_id = c.id AND cp.client_id = c.client_id
           WHERE c.client_id = $1 AND cp.qualifies = true
           ORDER BY cp.score DESC
           LIMIT 10"#,
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    Ok(Json(serde_json::json!({
        "data": {
            "client": client,
            "stats": stats,
            "recent_runs": recent_runs,
            "top_companies": top_companies,
        }
    })))
}

/// PUT /api/clients/:id — update client name/active status
pub async fn update(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateClientRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Build dynamic update
    let client = sqlx::query_as::<_, ClientRow>(
        r#"UPDATE clients
           SET name = COALESCE($2, name),
               is_active = COALESCE($3, is_active),
               updated_at = NOW()
           WHERE id = $1
           RETURNING id, name, is_active, created_at, updated_at"#,
    )
    .bind(id)
    .bind(body.name.as_deref())
    .bind(body.is_active)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to update client: {:?}", e);
        db_error("Failed to update client")
    })?;

    let client = client.ok_or_else(|| not_found("Client not found"))?;

    info!("Updated client: {} ({})", client.name, client.id);

    Ok(Json(serde_json::json!({ "data": client })))
}

/// GET /api/clients/:id/config — get active config + prequal_config
pub async fn get_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let config = sqlx::query_as::<_, ClientConfigRow>(
        r#"SELECT id, client_id, config, version, is_active, prequal_config, created_at, updated_at
           FROM client_configs
           WHERE client_id = $1 AND is_active = true"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch config: {:?}", e);
        db_error("Failed to fetch config")
    })?;

    let config = config.ok_or_else(|| not_found("Config not found for this client"))?;

    Ok(Json(serde_json::json!({ "data": config })))
}

/// PUT /api/clients/:id/config — update config and/or prequal_config
pub async fn update_config(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateConfigRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Check config exists
    let existing = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM client_configs WHERE client_id = $1 AND is_active = true",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to check config: {:?}", e);
        db_error("Failed to check config")
    })?;

    if existing.is_none() {
        return Err(not_found("Config not found for this client"));
    }

    let config = sqlx::query_as::<_, ClientConfigRow>(
        r#"UPDATE client_configs
           SET config = COALESCE($2, config),
               prequal_config = COALESCE($3, prequal_config),
               version = version + 1,
               updated_at = NOW()
           WHERE client_id = $1 AND is_active = true
           RETURNING id, client_id, config, version, is_active, prequal_config, created_at, updated_at"#,
    )
    .bind(id)
    .bind(body.config.as_ref())
    .bind(body.prequal_config.as_ref())
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to update config: {:?}", e);
        db_error("Failed to update config")
    })?;

    info!("Updated config for client {} (version={})", id, config.version);

    Ok(Json(serde_json::json!({ "data": config })))
}

/// GET /api/clients/:id/icp — get ICP profile
pub async fn get_icp(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let icp = sqlx::query_as::<_, IcpRow>(
        r#"SELECT id, client_id, icp_json, created_at, updated_at
           FROM client_icp_profiles
           WHERE client_id = $1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch ICP: {:?}", e);
        db_error("Failed to fetch ICP")
    })?;

    let icp = icp.ok_or_else(|| not_found("ICP profile not found for this client"))?;

    Ok(Json(serde_json::json!({ "data": icp })))
}

/// PUT /api/clients/:id/icp — update ICP profile
pub async fn update_icp(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateIcpRequest>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let icp = sqlx::query_as::<_, IcpRow>(
        r#"INSERT INTO client_icp_profiles (client_id, icp_json)
           VALUES ($1, $2)
           ON CONFLICT (client_id) DO UPDATE
           SET icp_json = $2, updated_at = NOW()
           RETURNING id, client_id, icp_json, created_at, updated_at"#,
    )
    .bind(id)
    .bind(&body.icp_json)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to update ICP: {:?}", e);
        db_error("Failed to update ICP")
    })?;

    info!("Updated ICP profile for client {}", id);

    Ok(Json(serde_json::json!({ "data": icp })))
}

// ============================================================================
// Helpers
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
struct RecentRun {
    id: Uuid,
    status: String,
    batch_target: i32,
    unique_upserted: i32,
    pages_fetched: i32,
    created_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, FromRow)]
struct TopCompany {
    id: Uuid,
    name: String,
    domain: Option<String>,
    industry: Option<String>,
    score: f64,
    qualifies: bool,
    why_now_indicators: Option<Vec<String>>,
}

async fn load_client_stats(db: &sqlx::PgPool, client_id: Uuid) -> ClientStats {
    // Total companies
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM companies WHERE client_id = $1",
    )
    .bind(client_id)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // Qualified
    let qualified: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM company_candidates
           WHERE client_id = $1 AND status = 'qualified'"#,
    )
    .bind(client_id)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // Disqualified
    let disqualified: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM company_candidates
           WHERE client_id = $1 AND status = 'disqualified'"#,
    )
    .bind(client_id)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // Pending prequal
    let pending: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM company_candidates
           WHERE client_id = $1 AND status IN ('new', 'prequal_queued')"#,
    )
    .bind(client_id)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // Last discovery run
    let last_discovery: Option<DateTime<Utc>> = sqlx::query_scalar(
        r#"SELECT created_at FROM company_fetch_runs
           WHERE client_id = $1 ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(client_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    // Last prequal run
    let last_prequal: Option<DateTime<Utc>> = sqlx::query_scalar(
        r#"SELECT created_at FROM jobs
           WHERE client_id = $1 AND job_type = 'prequal_dispatch'
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(client_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    // Autopilot enabled
    let autopilot: bool = sqlx::query_scalar::<_, JsonValue>(
        r#"SELECT COALESCE(prequal_config, '{}'::jsonb)
           FROM client_configs
           WHERE client_id = $1 AND is_active = true"#,
    )
    .bind(client_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None)
    .and_then(|v| v.get("autopilot_enabled").and_then(|x| x.as_bool()))
    .unwrap_or(true); // default true per PrequalConfig

    ClientStats {
        total_companies: total,
        qualified_companies: qualified,
        disqualified_companies: disqualified,
        pending_prequal: pending,
        last_discovery_run: last_discovery,
        last_prequal_run: last_prequal,
        autopilot_enabled: autopilot,
    }
}

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": { "code": "db_error", "message": msg }
        })),
    )
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": { "code": "not_found", "message": msg }
        })),
    )
}
