// ============================================================================
// INTEGRATION GUIDE
// ============================================================================
// This file is NOT a standalone module — it shows the exact code changes
// needed in your existing files to wire up the onboarding subsystem.
// ============================================================================

// ============================================================================
// 1. In backend/src/main.rs — add module declaration
// ============================================================================
// Add this line alongside your other job module declarations:

// mod jobs;  // (already exists)
// Inside jobs/mod.rs, add:
pub mod onboarding;


// ============================================================================
// 2. In backend/src/worker.rs — add job type handlers
// ============================================================================
// Inside your match block where you dispatch by job_type, add these 4 arms:

/*
    // Existing job types...
    "discover_companies" => { ... }
    "prequal_dispatch" => { ... }
    "prequal_batch" => { ... }

    // === NEW: Onboarding jobs ===
    "start_client_onboarding" => {
        let payload: jobs::onboarding::models::StartOnboardingPayload =
            serde_json::from_value(job.payload.clone())?;
        jobs::onboarding::run_start_onboarding(pool, http_client, payload, &worker_id).await?;
    }

    "onboarding_enrich_and_crawl" => {
        let payload: jobs::onboarding::models::EnrichAndCrawlPayload =
            serde_json::from_value(job.payload.clone())?;
        jobs::onboarding::run_enrich_and_crawl(pool, http_client, payload, &worker_id).await?;
    }

    "onboarding_generate_drafts" => {
        let payload: jobs::onboarding::models::GenerateDraftsPayload =
            serde_json::from_value(job.payload.clone())?;
        jobs::onboarding::run_generate_drafts(pool, http_client, payload, &worker_id).await?;
    }

    "onboarding_finalize_draft" => {
        let payload: jobs::onboarding::models::FinalizeDraftPayload =
            serde_json::from_value(job.payload.clone())?;
        jobs::onboarding::run_finalize_draft(pool, http_client, payload, &worker_id).await?;
    }
*/


// ============================================================================
// 3. In backend/src/server.rs — add route registration
// ============================================================================
// Add these routes inside your Router builder:

/*
    // === NEW: Onboarding routes ===
    .route("/api/clients/:id/onboarding-runs", post(routes::onboarding::create))
    .route("/api/onboarding-runs", get(routes::onboarding::list))
    .route("/api/onboarding-runs/:id", get(routes::onboarding::get))
    .route("/api/onboarding-runs/:id/artifacts", get(routes::onboarding::artifacts))
    .route("/api/onboarding-runs/:id/activate", post(routes::onboarding::activate))
    .route("/api/onboarding-runs/:id/regenerate", post(routes::onboarding::regenerate))
*/


// ============================================================================
// 4. In backend/src/routes/mod.rs — add module declaration
// ============================================================================
// Add:
pub mod onboarding;


// ============================================================================
// 5. In backend/Cargo.toml — add dependency (if not already present)
// ============================================================================
// lazy_static = "1"
// urlencoding = "2"
// url = "2"


// ============================================================================
// 6. In backend/.env — add new env vars
// ============================================================================
/*
# Azure OpenAI (for onboarding LLM calls — direct from Rust)
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT=gpt-4o
AZURE_OPENAI_API_VERSION=2024-08-01-preview
*/


// ============================================================================
// 7. Run the migration
// ============================================================================
// psql "$DATABASE_URL" -f migrations/0040_client_onboarding.sql
