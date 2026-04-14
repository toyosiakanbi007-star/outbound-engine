// src/jobs/onboarding/mod.rs
//
// Client Onboarding AI subsystem.
//
// 4-job pipeline:
//   1. START_CLIENT_ONBOARDING     → creates run, enqueues enrich
//   2. ONBOARDING_ENRICH_AND_CRAWL → Diffbot org + website crawl
//   3. ONBOARDING_GENERATE_DRAFTS  → knowledge pack + LLM → draft config/ICP
//   4. ONBOARDING_FINALIZE_DRAFT   → mark review_ready (or auto-activate)

pub mod models;
pub mod llm_client;
pub mod site_crawler;
pub mod start;
pub mod enrich_and_crawl;
pub mod generate_drafts;
pub mod finalize;

pub use start::run_start_onboarding;
pub use enrich_and_crawl::run_enrich_and_crawl;
pub use generate_drafts::run_generate_drafts;
pub use finalize::run_finalize_draft;
