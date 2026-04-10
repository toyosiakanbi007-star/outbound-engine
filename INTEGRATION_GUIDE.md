# Integration Guide — Wiring the Control Panel into the Existing Backend

This guide explains how to integrate the new control panel routes and logging
into your existing Rust backend. Follow these steps in order.

---

## 1. Cargo.toml Additions

Add these dependencies (some may already exist):

```toml
[dependencies]
# ... existing deps ...
tower-http = { version = "0.5", features = ["cors"] }
reqwest = { version = "0.12", features = ["json"] }
chrono = { version = "0.4", features = ["serde"] }
```

`reqwest` is likely already present (used by company_fetcher and prequal_worker).
`chrono` with serde is likely already present too.

---

## 2. Run the Migration

```bash
psql $DATABASE_URL -f backend/migrations/0031_create_structured_logs.sql
```

---

## 3. Add New Source Files

Copy these files into `backend/src/`:

```
src/
├── logging/
│   ├── mod.rs          # NEW
│   ├── db_layer.rs     # NEW
│   └── flusher.rs      # NEW
├── routes/
│   ├── mod.rs          # REPLACE with mod_new.rs content
│   ├── health.rs       # NEW
│   ├── clients.rs      # NEW
│   ├── discovery.rs    # NEW
│   ├── pipeline.rs     # NEW
│   ├── prequal.rs      # NEW (routes, not the worker)
│   ├── companies.rs    # NEW
│   ├── queue.rs        # NEW
│   ├── logs.rs         # NEW
│   ├── debug.rs        # KEEP existing
│   └── news.rs         # KEEP existing
├── server.rs           # NEW (updated run_server)
```

---

## 4. Update main.rs

### 4.1 Add module declarations

At the top of `main.rs`, add:

```rust
mod logging;
mod server;
```

### 4.2 Update routes/mod.rs

Replace `src/routes/mod.rs` with the content from `mod_new.rs`:

```rust
pub mod clients;
pub mod companies;
pub mod debug;
pub mod discovery;
pub mod health;
pub mod logs;
pub mod news;
pub mod pipeline;
pub mod prequal;
pub mod queue;
```

### 4.3 Replace run_server call

In `main.rs`, the server mode branch currently calls `run_server(state, cfg.http_port)`.

Replace:
```rust
_ => {
    info!("Starting in SERVER mode");
    let state = AppState {
        db: pool,
        config: cfg.clone(),
    };
    run_server(state, cfg.http_port).await?;
}
```

With:
```rust
_ => {
    info!("Starting in SERVER mode");
    let state = AppState {
        db: pool,
        config: cfg.clone(),
    };
    server::run_server(state, cfg.http_port).await?;
}
```

And remove the old `run_server` function, `health_handler`, `version_handler`,
and `db_health_handler` from `main.rs` (they're now in `routes/health.rs`
and `server.rs`).

### 4.4 (Optional) Add DB logging to worker mode

To capture structured logs from workers too:

```rust
"worker" => {
    info!("Starting in WORKER mode");
    
    // Set up DB logging (optional — workers can also just use console)
    let (log_layer, _flusher_handle) = logging::flusher::setup_db_logging(
        pool.clone(),
        "worker",
    );
    
    // Note: you'd need to restructure tracing init to use layers.
    // For now, workers can rely on console logging.
    // The DB logging is most useful for server mode where the
    // control panel needs to query logs.
    
    let news_client = build_news_client(&cfg).await?;
    worker::run_worker(pool, news_client).await?;
}
```

Full tracing layer setup (for both console + DB):

```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// In main(), before the mode branch:
let pool = db::create_pool(&cfg.database_url).await?;

// Set up layered subscriber
let fmt_layer = tracing_subscriber::fmt::layer()
    .with_env_filter(
        EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info")),
    );

let (db_layer, _flusher_handle) = logging::flusher::setup_db_logging(
    pool.clone(),
    &mode,  // "server", "worker", etc.
);

tracing_subscriber::registry()
    .with(fmt_layer)
    .with(db_layer)
    .init();
```

---

## 5. CORS Configuration

For local development (frontend on :3001, backend on :3000),
the `CorsLayer::permissive()` in `server.rs` allows all origins.

For production, replace with:

```rust
use tower_http::cors::{CorsLayer, AllowOrigin};

.layer(
    CorsLayer::new()
        .allow_origin(AllowOrigin::exact("https://your-domain.com".parse().unwrap()))
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(tower_http::cors::Any)
)
```

Or configure the Next.js frontend to proxy `/api` to the backend (recommended
for production), which eliminates CORS entirely.

---

## 6. force_full_pipeline Support in Orchestrator

The pipeline trigger uses `force_full_pipeline: true` in the job payload.
The orchestrator needs a small change to respect this flag.

In `src/jobs/company_fetcher/orchestrator.rs`, update the autopilot section:

```rust
// Current code (around line 588-626):
let prequal_jobs = if global.upserted > 0 {
    let prequal_config = prequal_db::load_prequal_config(pool, client_id)
        .await
        .unwrap_or_default();

    if prequal_config.autopilot_enabled {
        // ... enqueue logic ...

// Add force_full_pipeline check:
let prequal_jobs = if global.upserted > 0 {
    let prequal_config = prequal_db::load_prequal_config(pool, client_id)
        .await
        .unwrap_or_default();

    // Check run-scoped flag from job payload
    let force_pipeline = payload.force_full_pipeline.unwrap_or(false);

    if prequal_config.autopilot_enabled || force_pipeline {
        // ... existing enqueue logic ...
```

And add the field to `DiscoverCompaniesPayload` in `models.rs`:

```rust
/// Run-scoped flag: force prequal dispatch after discovery,
/// regardless of stored autopilot_enabled setting.
/// Set by the "Run Full Pipeline" frontend action.
#[serde(default)]
pub force_full_pipeline: Option<bool>,
```

---

## 7. Frontend Proxy (Next.js)

In your Next.js `next.config.ts`:

```typescript
const nextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://localhost:3000/api/:path*',
      },
    ];
  },
};

export default nextConfig;
```

This proxies all `/api/*` requests to the Rust backend during development.

---

## 8. Verification

After integration, verify:

```bash
# Start backend in server mode
MODE=server cargo run

# Health check
curl http://localhost:3000/api/health

# List clients
curl http://localhost:3000/api/clients

# Queue status
curl http://localhost:3000/api/queue-status

# Azure Function health
curl http://localhost:3000/api/health/azure-function

# Logs (may be empty initially)
curl http://localhost:3000/api/logs
```

---

## 9. What's NOT Wired Yet

These items are documented in the architecture but deferred:

- **Worker heartbeats table** — `/api/workers` uses inferred data for now
- **Log retention cleanup** — add a cron job or background task later:
  `DELETE FROM structured_logs WHERE timestamp < NOW() - INTERVAL '30 days'`
- **Full-text search optimization** — the GIN index on message is created
  but search performance should be monitored under load
- **Prequal reasons route (Phase 2)** — `GET /api/companies/:id/latest-prequal`
  returns the summary; the `full_result` JSONB can be added as a separate
  endpoint when the Raw JSON tab is built in the frontend
