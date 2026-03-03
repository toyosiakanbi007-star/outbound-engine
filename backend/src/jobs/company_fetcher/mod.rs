// src/jobs/company_fetcher/mod.rs
//
// Company Fetcher subsystem.
//
// Discovers companies matching a client's ICP via organization search providers
// (Apollo or Diffbot), upserts them into the `companies` table with robust
// deduplication, writes enrichment data, and enqueues downstream prequal jobs.
//
// MODULES:
// - models:         All data types (payloads, DB rows, request/response, enums)
// - apollo_client:  HTTP client for Apollo Organization Search with pagination + backoff
// - quota_planner:  Computes per-industry quotas (cold-start vs adaptive yield-based)
// - query_variants: Builds progressively broader queries per variant ladder
// - dedup:          In-memory batch dedup + SQL cascading upsert into companies
// - db:             All SQL queries for runs, queries, candidates, yield metrics
// - enrichment:     Upsert enrichment data into company_apollo_enrichment
// - orchestrator:   Main entry point — ties everything together
// - providers:      Provider abstraction (Apollo, Diffbot) + Diffbot KG client/converter

pub mod apollo_client;
pub mod db;
pub mod dedup;
pub mod enrichment;
pub mod models;
pub mod orchestrator;
pub mod providers;
pub mod query_variants;
pub mod quota_planner;

// Re-export commonly used items
pub use models::{
    DiscoverCompaniesPayload,
    FetchRunStatus,
    QueryVariant,
    CandidateStatus,
    FetchQueryStatus,
    IndustryQuota,
    IndustrySpec,
    QuotaPlan,
    IndustryProgress,
    IcpFetchProfile,
    ApolloOrgSearchRequest,
    ApolloOrgSearchResponse,
    ApolloOrganization,
    UpsertResult,
    normalize_domain,
};

pub use apollo_client::{
    ApolloClient,
    ApolloApiError,
    PageFetchMeta,
    PaginationSummary,
    StopReason,
};

pub use quota_planner::compute_quota_plan;
pub use query_variants::VariantBuilder;
pub use dedup::{
    BatchDeduper,
    BatchResult,
    upsert_company,
    process_batch,
    seed_deduper_from_db,
};

pub use orchestrator::{run_company_fetcher, RunResult, OrchestratorError};

// Provider abstraction
pub use providers::{
    OrgSearchProvider,
    OrgProviderKind,
};
pub use providers::diffbot_client::DiffbotClient;
