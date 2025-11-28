mod config;

use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use config::{AppEnv, Config};
use serde_json::json;
    use sqlx::PgPool;
use std::net::SocketAddr;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    db: PgPool,
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

    // 2. Load configuration
    let cfg = config::load()?;
    info!("Starting backend in {:?} mode", cfg.env);

    // 3. Create Postgres connection pool
    let pool = PgPool::connect(&cfg.database_url).await?;
    info!("Connected to Postgres");

    // 4. Build application state
    let state = AppState {
        db: pool,
        config: cfg.clone(),
    };

    // 5. Build router with /health route
    let app = Router::new()
        .route("/health", get(health_check))
        .with_state(state);

    // 6. Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.http_port));
    info!("Listening on http://{}", addr);

    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;

    Ok(())
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
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
