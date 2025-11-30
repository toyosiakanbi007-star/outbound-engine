mod config;
mod db;
mod jobs;
mod routes;

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
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    db: DbPool,
    config: Config,
}

#[tokio::main]
async fn main() -> config::Result<()> {
    // 1. Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Allow a CLI mode: `cargo run -- --db-check`
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--db-check") {
        run_db_check().await?;
        return Ok(());
    }

    // 2. Load configuration
    let cfg = config::load()?;
    info!("Starting backend in {:?} mode", cfg.env);

    // 3. Create Postgres connection pool
    let pool = db::create_pool(&cfg.database_url).await?;
    info!("Connected to Postgres");

    // 4. Build application state
    let state = AppState {
        db: pool,
        config: cfg.clone(),
    };

    // 5. Build router with /health and /db-health routes
    let app = Router::new()
        .route("/health", get(db_health))
        .route("/db-health", get(db_health))
        .with_state(state);

    // 6. Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.http_port));
    info!("Listening on http://{}", addr);

    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;

    Ok(())
}

/// Shared DB health handler
async fn db_health(State(state): State<AppState>) -> Json<serde_json::Value> {
    // Simple DB check: SELECT 1
    if let Err(err) = sqlx::query("SELECT 1").execute(&state.db).await {
        error!("DB health check failed: {:?}", err);
        return Json(json!({
            "status": "error",
            "db": "down",
        }));
    }

    Json(json!({
        "status": "ok",
        "env": format!("{:?}", state.config.env),
    }))
}

/// CLI-style DB check: `cargo run -- --db-check`
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
