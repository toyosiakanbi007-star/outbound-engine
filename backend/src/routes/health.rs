// src/routes/health.rs
//
// Health, version, and Azure Function reachability endpoints.

use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use serde_json::json;
use std::time::Instant;
use tracing::warn;

use crate::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub env: String,
    pub db: &'static str,
    pub version: &'static str,
}

/// GET /api/health — server + DB health
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();

    let status_code = if db_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status_code,
        Json(json!({
            "status": if db_ok { "ok" } else { "degraded" },
            "env": format!("{:?}", state.config.env),
            "db": if db_ok { "ok" } else { "down" },
            "version": env!("CARGO_PKG_VERSION"),
        })),
    )
}

/// GET /api/version
pub async fn version() -> Json<serde_json::Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "build": "outbound-engine",
    }))
}

/// GET /api/health/azure-function — shallow reachability check
///
/// Performs a lightweight HTTP GET to the Azure Function URL with a 3s timeout.
/// Any HTTP response (even 401/404) counts as "reachable" — we're checking
/// infrastructure, not auth. Only connection failures count as unreachable.
pub async fn azure_function_health() -> Json<serde_json::Value> {
    let url = std::env::var("AZURE_FUNCTION_URL").unwrap_or_default();

    if url.is_empty() {
        return Json(json!({
            "reachable": false,
            "error": "AZURE_FUNCTION_URL not configured",
            "url": null,
            "latency_ms": null,
            "checked_at": chrono::Utc::now(),
        }));
    }

    // Derive a health URL: try /health first, fall back to base URL
    let health_url = if url.contains("/api/") {
        // e.g. https://xxx.azurewebsites.net/api/news-fetch → https://xxx.azurewebsites.net/api/health
        let base = url.rsplitn(2, "/api/").last().unwrap_or(&url);
        format!("{}/api/health", base)
    } else {
        url.clone()
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap_or_default();

    let start = Instant::now();
    let result = client.get(&health_url).send().await;
    let latency_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(resp) => {
            // Any HTTP response = infrastructure is up
            Json(json!({
                "reachable": true,
                "status_code": resp.status().as_u16(),
                "latency_ms": latency_ms,
                "url": health_url,
                "checked_at": chrono::Utc::now(),
            }))
        }
        Err(e) => {
            warn!("Azure Function health check failed: {:?}", e);
            Json(json!({
                "reachable": false,
                "error": format!("{}", e),
                "latency_ms": latency_ms,
                "url": health_url,
                "checked_at": chrono::Utc::now(),
            }))
        }
    }
}
