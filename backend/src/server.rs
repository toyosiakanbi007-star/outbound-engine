// src/server.rs
//
// Updated run_server() with all control panel API routes.
// 
// USAGE: Replace the existing `run_server` function in main.rs with this,
// and add `mod server;` to main.rs, or inline the function.
//
// CARGO.TOML ADDITIONS NEEDED:
//   tower-http = { version = "0.5", features = ["cors"] }
//   reqwest = { version = "0.12", features = ["json"] }  # (likely already present)

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::routes;
use crate::AppState;

/// Run the HTTP server with all control panel routes.
///
/// Called when MODE=server (default).
pub async fn run_server(
    state: AppState,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        // ============================================================
        // Health & Meta
        // ============================================================
        .route("/api/health", get(routes::health::health))
        .route("/api/version", get(routes::health::version))
        .route(
            "/api/health/azure-function",
            get(routes::health::azure_function_health),
        )
        // ============================================================
        // Clients
        // ============================================================
        .route(
            "/api/clients",
            get(routes::clients::list).post(routes::clients::create),
        )
        .route(
            "/api/clients/:id",
            get(routes::clients::get).put(routes::clients::update).delete(routes::clients::delete),
        )
        .route(
            "/api/clients/:id/config",
            get(routes::clients::get_config).put(routes::clients::update_config),
        )
        .route(
            "/api/clients/:id/icp",
            get(routes::clients::get_icp).put(routes::clients::update_icp),
        )
        // ============================================================
        // Discovery Runs
        // ============================================================
        .route(
            "/api/clients/:id/discovery-runs",
            post(routes::discovery::create),
        )
        .route("/api/discovery-runs", get(routes::discovery::list))
        .route("/api/discovery-runs/:id", get(routes::discovery::get))
        .route(
            "/api/discovery-runs/:id/companies",
            get(routes::discovery::companies),
        )
        // ============================================================
        // Companies
        // ============================================================
        .route("/api/companies", get(routes::companies::list))
        .route(
            "/api/companies/:id",
            get(routes::companies::get).delete(routes::companies::delete),
        )
        .route(
            "/api/companies/:id/evidence",
            get(routes::companies::evidence),
        )
        .route(
            "/api/companies/:id/hypotheses",
            get(routes::companies::hypotheses),
        )
        .route("/api/companies/:id/news", get(routes::companies::news))
        .route(
            "/api/companies/:id/discovered-urls",
            get(routes::companies::discovered_urls),
        )
        .route(
            "/api/companies/:id/candidates",
            get(routes::companies::candidates),
        )
        .route(
            "/api/companies/:id/aggregate",
            get(routes::companies::aggregate),
        )
        .route(
            "/api/companies/:id/raw-combined",
            get(routes::companies::raw_combined),
        )
        .route(
            "/api/companies/:id/actions/rerun-prequal",
            post(routes::companies::rerun_prequal),
        )
        .route(
            "/api/companies/:id/actions/rerun-fetch",
            post(routes::companies::rerun_fetch),
        )
        // ============================================================
        // Prequal
        // ============================================================
        .route(
            "/api/clients/:id/prequal-dispatch",
            post(routes::prequal::dispatch),
        )
        .route("/api/prequal-runs", get(routes::prequal::list))
        .route("/api/prequal-runs/:id", get(routes::prequal::get))
        .route(
            "/api/companies/:id/latest-prequal",
            get(routes::prequal::latest_for_company),
        )
        // ============================================================
        // Pipeline (Full Pipeline Trigger)
        // ============================================================
        .route(
            "/api/clients/:id/pipeline/run",
            post(routes::pipeline::run),
        )
        // ============================================================
        // Client Onboarding AI
        // ============================================================
        .route(
            "/api/clients/:id/onboarding-runs",
            post(routes::onboarding::create),
        )
        .route("/api/onboarding-runs", get(routes::onboarding::list))
        .route("/api/onboarding-runs/:id", get(routes::onboarding::get))
        .route(
            "/api/onboarding-runs/:id/artifacts",
            get(routes::onboarding::artifacts),
        )
        .route(
            "/api/onboarding-runs/:id/activate",
            post(routes::onboarding::activate),
        )
        .route(
            "/api/onboarding-runs/:id/regenerate",
            post(routes::onboarding::regenerate),
        )
        // ============================================================
        // Queue & Workers
        // ============================================================
        .route("/api/queue-status", get(routes::queue::status))
        .route("/api/queue-status/jobs", get(routes::queue::jobs))
        .route("/api/queue-status/cancel", post(routes::queue::cancel))
        .route("/api/queue-status/retry", post(routes::manage::retry_jobs))
        .route("/api/workers", get(routes::queue::workers))
        // ============================================================
        // Client Management
        // ============================================================
        .route(
            "/api/clients/:id/flush-companies",
            post(routes::manage::flush_companies),
        )
        .route(
            "/api/clients/:id/manual-setup",
            post(routes::manage::manual_setup),
        )
        // ============================================================
        // Bulk Company Actions
        // ============================================================
        .route("/api/companies/bulk-prequal", post(routes::manage::bulk_prequal))
        .route("/api/companies/bulk-aggregate", post(routes::manage::bulk_aggregate))
        .route(
            "/api/companies/:id/actions/aggregate",
            post(routes::manage::aggregate_company),
        )
        // ============================================================
        // Structured Logs
        // ============================================================
        .route("/api/logs", get(routes::logs::query))
        // ============================================================
        // Legacy endpoints (keep for backward compat)
        // ============================================================
        .route("/health", get(routes::health::health))
        .route("/version", get(routes::health::version))
        .route(
            "/debug/jobs/test-send",
            post(routes::debug::create_test_send_job_handler),
        )
        .route("/news/fetch", post(routes::news::mock_fetch_news_handler))
        // ============================================================
        // CORS (permissive for local dev; tighten in production)
        // ============================================================
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP server listening on http://{}", addr);
    info!("Control panel API ready at /api/*");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
