// src/jobs/onboarding/models.rs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use uuid::Uuid;

// ============================================================================
// Job Payloads
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartOnboardingPayload {
    pub client_id: Uuid,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichAndCrawlPayload {
    pub client_id: Uuid,
    pub run_id: Uuid,
    pub client_name: String,
    pub client_domain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateDraftsPayload {
    pub client_id: Uuid,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeDraftPayload {
    pub client_id: Uuid,
    pub run_id: Uuid,
}

// ============================================================================
// API Request/Response
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateOnboardingRequest {
    pub client_name: String,
    pub client_domain: String,
    #[serde(default)]
    pub operator_note: Option<String>,
    #[serde(default)]
    pub target_country: Option<String>,
    #[serde(default)]
    pub known_competitors: Option<Vec<String>>,
    #[serde(default)]
    pub known_customers: Option<Vec<String>>,
    #[serde(default)]
    pub tone_override: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ActivateOnboardingRequest {
    /// Operator may have edited the drafts before activating
    #[serde(default)]
    pub edited_config: Option<JsonValue>,
    #[serde(default)]
    pub edited_prequal_config: Option<JsonValue>,
    #[serde(default)]
    pub edited_icp: Option<JsonValue>,
    /// Optionally launch discovery after activation
    #[serde(default)]
    pub run_discovery: bool,
    #[serde(default)]
    pub discovery_batch_target: Option<i32>,
}

// ============================================================================
// DB Rows
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
pub struct OnboardingRunRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub status: String,
    pub input_name: String,
    pub input_domain: String,
    pub operator_note: Option<String>,
    pub diffbot_uri: Option<String>,
    pub llm_model: Option<String>,
    pub draft_config: Option<JsonValue>,
    pub draft_prequal_config: Option<JsonValue>,
    pub draft_icp: Option<JsonValue>,
    pub review_notes: Option<JsonValue>,
    pub brand_profile: Option<JsonValue>,
    pub knowledge_pack: Option<JsonValue>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub activated_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct OnboardingArtifactRow {
    pub id: Uuid,
    pub run_id: Uuid,
    pub artifact_type: String,
    pub page_url: Option<String>,
    pub page_type: Option<String>,
    pub content_json: JsonValue,
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// Internal data structures
// ============================================================================

/// Normalized Diffbot org profile — extracted from raw response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiffbotOrgProfile {
    pub name: Option<String>,
    pub homepage: Option<String>,
    pub description: Option<String>,
    pub short_description: Option<String>,
    pub logo: Option<String>,
    pub industries: Vec<String>,
    pub categories: Vec<String>,
    pub employee_count: Option<i64>,
    pub employee_range: Option<String>,
    pub location: Option<String>,
    pub founding_date: Option<String>,
    pub social_links: Vec<SocialLink>,
    pub competitors: Vec<CompetitorInfo>,
    pub similar_companies: Vec<String>,
    pub diffbot_uri: Option<String>,
    pub stock_symbol: Option<String>,
    pub revenue_range: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SocialLink {
    pub platform: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorInfo {
    pub name: String,
    pub domain: Option<String>,
    pub summary: Option<String>,
}

/// Per-page crawl result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawledPage {
    pub url: String,
    pub page_type: String,
    pub title: Option<String>,
    pub raw_text_length: usize,
    pub summary: Option<String>,
    pub product_claims: Vec<String>,
    pub target_buyer_clues: Vec<String>,
    pub industry_clues: Vec<String>,
    pub use_case_clues: Vec<String>,
    pub integration_clues: Vec<String>,
    pub security_compliance_clues: Vec<String>,
    pub gtm_motion_clues: Vec<String>,
    pub recurring_terms: Vec<String>,
    pub fetch_error: Option<String>,
}

/// The merged knowledge pack sent to the LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgePack {
    pub company_name: String,
    pub domain: String,
    pub diffbot_profile: Option<DiffbotOrgProfile>,
    pub crawled_pages: Vec<CrawledPage>,
    pub operator_note: Option<String>,
    pub target_country: Option<String>,
    pub known_competitors: Option<Vec<String>>,
    pub known_customers: Option<Vec<String>>,
    pub tone_override: Option<String>,
}

/// LLM generation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationResult {
    pub draft_config: JsonValue,
    pub draft_prequal_config: JsonValue,
    pub draft_icp: JsonValue,
    pub review_notes: JsonValue,
    pub brand_profile: JsonValue,
    pub llm_model: String,
}
