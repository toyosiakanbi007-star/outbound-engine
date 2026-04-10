// src/main.rs

mod config;
mod db;
mod jobs;
mod logging;
mod news;
mod routes;
mod server;
mod worker;

use crate::news::listener::run_news_listener;
use crate::news::client::build_news_client;
use crate::jobs::prequal_listener::run_prequal_listener;
use config::Config;
use db::DbPool;
use tracing::{error, info, warn};
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
            info!("Starting in WORKER mode");
            let news_client = build_news_client(&cfg).await?;
            worker::run_worker(pool, news_client).await?;
        }

        "listener" => {
            info!("Starting in LISTENER mode (real-time news alerts)");
            run_news_listener(pool).await?;
        }

        "worker+listener" => {
            info!("Starting in WORKER+LISTENER mode (parallel)");
            
            // Clone pool for listener
            let listener_pool = pool.clone();
            
            // Build news client for worker
            let news_client = build_news_client(&cfg).await?;
            
            // Spawn listener task
            let listener_handle = tokio::spawn(async move {
                if let Err(e) = run_news_listener(listener_pool).await {
                    error!("News listener error: {:?}", e);
                }
            });
            
            // Spawn worker task  
            let worker_pool = pool.clone();
            let worker_handle = tokio::spawn(async move {
                if let Err(e) = worker::run_worker(worker_pool, news_client).await {
                    error!("Worker error: {:?}", e);
                }
            });
            
            // Wait for either to exit (they should run forever)
            tokio::select! {
                _ = listener_handle => {
                    warn!("News listener exited unexpectedly");
                }
                _ = worker_handle => {
                    warn!("Worker exited unexpectedly");
                }
            }
        }

        // NEW: Prequal listener only (for Phase B triggering)
        "prequal_listener" => {
            info!("Starting in PREQUAL_LISTENER mode (Phase B trigger)");
            run_prequal_listener(pool).await?;
        }

        // NEW: Worker + Prequal listener (recommended for V3)
        "worker+prequal_listener" => {
            info!("Starting in WORKER+PREQUAL_LISTENER mode (parallel)");
            
            let prequal_pool = pool.clone();
            let news_client = build_news_client(&cfg).await?;
            
            // Spawn prequal listener task
            let prequal_handle = tokio::spawn(async move {
                if let Err(e) = run_prequal_listener(prequal_pool).await {
                    error!("Prequal listener error: {:?}", e);
                }
            });
            
            // Spawn worker task
            let worker_pool = pool.clone();
            let worker_handle = tokio::spawn(async move {
                if let Err(e) = worker::run_worker(worker_pool, news_client).await {
                    error!("Worker error: {:?}", e);
                }
            });
            
            tokio::select! {
                _ = prequal_handle => {
                    warn!("Prequal listener exited unexpectedly");
                }
                _ = worker_handle => {
                    warn!("Worker exited unexpectedly");
                }
            }
        }

        // NEW: All three listeners (news + prequal + worker)
        "worker+listener+prequal" | "full" => {
            info!("Starting in FULL mode (worker + news listener + prequal listener)");
            
            let news_listener_pool = pool.clone();
            let prequal_pool = pool.clone();
            let news_client = build_news_client(&cfg).await?;
            
            // Spawn news listener
            let news_handle = tokio::spawn(async move {
                if let Err(e) = run_news_listener(news_listener_pool).await {
                    error!("News listener error: {:?}", e);
                }
            });
            
            // Spawn prequal listener
            let prequal_handle = tokio::spawn(async move {
                if let Err(e) = run_prequal_listener(prequal_pool).await {
                    error!("Prequal listener error: {:?}", e);
                }
            });
            
            // Spawn worker
            let worker_pool = pool.clone();
            let worker_handle = tokio::spawn(async move {
                if let Err(e) = worker::run_worker(worker_pool, news_client).await {
                    error!("Worker error: {:?}", e);
                }
            });
            
            tokio::select! {
                _ = news_handle => { warn!("News listener exited unexpectedly"); }
                _ = prequal_handle => { warn!("Prequal listener exited unexpectedly"); }
                _ = worker_handle => { warn!("Worker exited unexpectedly"); }
            }
        }

        _ => {
            info!("Starting in SERVER mode (control panel API)");
            let state = AppState {
                db: pool,
                config: cfg.clone(),
            };
            server::run_server(state, cfg.http_port).await?;
        }
    }
    Ok(())
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
