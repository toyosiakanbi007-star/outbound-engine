mod config;
mod db;
mod jobs;
mod news;
mod routes;
mod worker;

use crate::news::client::HttpNewsSourcingClient;
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use config::Config;
use db::DbPool;
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct AppState {
    pub db: DbPool,
    pub config: Config,
}

#[tokio::main]
async fn main() -> config::Result<()> {
    // 1. Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // 2. Optional CLI mode for quick DB checks: `cargo run -- --db-check`
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--db-check") {
        run_db_check().await?;
        return Ok(());
    }

    // 3. MODE env var decides server vs worker
    let mode = std::env::var("MODE").unwrap_or_else(|_| "server".to_string());

    // 4. Load config & create DB pool
    let cfg = config::load()?;
    info!("Starting backend in {:?} mode (MODE={})", cfg.env, mode);

    let pool = db::create_pool(&cfg.database_url).await?;
    info!("Connected to Postgres");

    // 5. Branch: server mode or worker mode
    match mode.as_str() {
        "worker" => {
            // Build news client from config
            let news_client = HttpNewsSourcingClient::from_config(
                cfg.news_service_base_url.clone(),
                cfg.news_service_api_key.clone(),
            )
            .unwrap_or_else(|| {
                Arc::new(HttpNewsSourcingClient::new(
                    "http://127.0.0.1:3000".to_string(),
                    None,
                )) as crate::news::client::DynNewsSourcingClient
            });

            worker::run_worker(pool, news_client).await?;
        }
        _ => {
            let state = AppState {
                db: pool,
                config: cfg.clone(),
            };
            run_server(state, cfg.http_port).await?;
        }
    }

    Ok(())
}

// ========== Server mode ==========

async fn run_server(
    state: AppState,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/db-health", get(db_health_handler))
        .route("/version", get(version_handler))
        .route(
            "/debug/jobs/test-send",
            post(routes::debug::create_test_send_job_handler),
        )
        // Mock news endpoint for Checklist 7
        .route(
            "/news/fetch",
            post(routes::news::mock_fetch_news_handler),
        )
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP server listening on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Health: simple JSON with env
async fn health_handler(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "env": format!("{:?}", state.config.env),
    }))
}

// Version: hard-coded for now
async fn version_handler() -> Json<serde_json::Value> {
    Json(json!({ "version": "0.1.0" }))
}

// DB health: run `SELECT 1`
async fn db_health_handler(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "env": format!("{:?}", state.config.env) })),
        ),
        Err(err) => {
            error!("DB health check failed: {:?}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "status": "error", "db": "down" })),
            )
        }
    }
}

// CLI-db-check: `cargo run -- --db-check`
async fn run_db_check() -> config::Result<()> {
    let cfg = config::load()?;
    let pool = db::create_pool(&cfg.database_url).await?;

    info!("Running DB check against {}", cfg.database_url);

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM clients")
        .fetch_one(&pool)
        .await?;

    println!("clients table row count = {}", count);

    Ok(())
}
