// src/jobs/company_fetcher/models.rs
//
// Data models for the Company Fetcher subsystem.
//
// OVERVIEW:
// - Job payload for `discover_companies` jobs
// - DB row structs for company_fetch_runs, company_fetch_queries,
//   company_candidates, industry_yield_metrics
// - Apollo Organization Search request/response types
// - Status enums with string conversions for DB storage
// - ICP profile and client config extraction types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::FromRow;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

// ============================================================================
// Job Payload
// ============================================================================

/// Payload for `discover_companies` jobs in the `jobs` table.
///
/// Stored as JSONB in `jobs.payload`. Contains all parameters the
/// orchestrator needs to execute a company fetch run.
///
/// Example:
/// ```json
/// {
///   "client_id": "uuid",
///   "batch_target": 2000,
///   "page_size": 100,
///   "exploration_pct": 0.20,
///   "min_per_industry": 50,
///   "max_per_industry": 600,
///   "max_pages_per_query": 50,
///   "max_runtime_seconds": 3600,
///   "enqueue_prequal_jobs": true
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverCompaniesPayload {
    pub client_id: Uuid,

    /// Total number of unique companies to target for this run.
    #[serde(default = "default_batch_target")]
    pub batch_target: i32,

    /// Apollo results per page (max 100).
    #[serde(default = "default_page_size")]
    pub page_size: i32,

    /// Fraction of quota allocated evenly across all industries
    /// for exploration (0.0–1.0). Remainder allocated by yield.
    #[serde(default = "default_exploration_pct")]
    pub exploration_pct: f64,

    /// Minimum companies to fetch per industry (even if yield is low).
    #[serde(default = "default_min_per_industry")]
    pub min_per_industry: i32,

    /// Maximum companies to fetch per industry (cap even if yield is high).
    #[serde(default = "default_max_per_industry")]
    pub max_per_industry: i32,

    /// Max Apollo pages to fetch per query variant before moving on.
    #[serde(default = "default_max_pages_per_query")]
    pub max_pages_per_query: i32,

    /// Hard wall-clock limit for the entire run (seconds).
    #[serde(default = "default_max_runtime_seconds")]
    pub max_runtime_seconds: u64,

    /// Whether to automatically create prequal (NewsFetcher) jobs
    /// for newly discovered companies.
    #[serde(default = "default_enqueue_prequal")]
    pub enqueue_prequal_jobs: bool,

    /// Optional: override the fetch run ID (for retries/resume).
    pub resume_run_id: Option<Uuid>,

    /// If true, ignore all persisted fetch cursors and re-scan from page 1.
    ///
    /// Use this when:
    ///   - The ICP has changed significantly (new keywords, categories, sizes)
    ///   - You suspect the provider's dataset has shifted substantially
    ///   - You want to catch companies that appeared in earlier positions
    ///     since the last run (e.g. new Diffbot crawl data)
    ///
    /// Default: false (resume from persisted cursors — zero wasted credits).
    #[serde(default)]
    pub force_full_rescan: bool,
}

fn default_batch_target() -> i32 { 2000 }
fn default_page_size() -> i32 { 100 }
fn default_exploration_pct() -> f64 { 0.20 }
fn default_min_per_industry() -> i32 { 50 }
fn default_max_per_industry() -> i32 { 600 }
fn default_max_pages_per_query() -> i32 { 50 }
fn default_max_runtime_seconds() -> u64 { 3600 }
fn default_enqueue_prequal() -> bool { true }

// ============================================================================
// Fetch Run Status
// ============================================================================

/// Status of a company_fetch_runs row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    /// All industry variants exhausted before reaching batch_target.
    Depleted,
    /// Stopped early (max runtime, too many dupes, etc.)
    Partial,
}

impl FetchRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending   => "pending",
            Self::Running   => "running",
            Self::Succeeded => "succeeded",
            Self::Failed    => "failed",
            Self::Depleted  => "depleted",
            Self::Partial   => "partial",
        }
    }
}

impl FromStr for FetchRunStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending"   => Ok(Self::Pending),
            "running"   => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed"    => Ok(Self::Failed),
            "depleted"  => Ok(Self::Depleted),
            "partial"   => Ok(Self::Partial),
            other       => Err(format!("Unknown FetchRunStatus: {}", other)),
        }
    }
}

impl std::fmt::Display for FetchRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Query Variant
// ============================================================================

/// Apollo query variant ladder — determines how filters are relaxed
/// as earlier variants are exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryVariant {
    /// Industry + geo + company_size — strictest.
    V1Strict,
    /// Broader geo (expand locations / add neighboring regions).
    V2BroadenGeo,
    /// Wider employee count ranges.
    V3BroadenSize,
    /// Add keyword_tags to reduce taxonomy noise.
    V4KeywordAssist,
}

impl QueryVariant {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V1Strict        => "v1_strict",
            Self::V2BroadenGeo    => "v2_broaden_geo",
            Self::V3BroadenSize   => "v3_broaden_size",
            Self::V4KeywordAssist => "v4_keyword_assist",
        }
    }

    /// Returns the ladder in order — the orchestrator should try each in
    /// sequence, moving to the next only when the previous is exhausted.
    pub fn ladder() -> &'static [QueryVariant] {
        &[
            Self::V1Strict,
            Self::V2BroadenGeo,
            Self::V3BroadenSize,
            Self::V4KeywordAssist,
        ]
    }
}

impl FromStr for QueryVariant {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "v1_strict"         => Ok(Self::V1Strict),
            "v2_broaden_geo"    => Ok(Self::V2BroadenGeo),
            "v3_broaden_size"   => Ok(Self::V3BroadenSize),
            "v4_keyword_assist" => Ok(Self::V4KeywordAssist),
            other               => Err(format!("Unknown QueryVariant: {}", other)),
        }
    }
}

impl std::fmt::Display for QueryVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Candidate Status
// ============================================================================

/// Pipeline status for a company_candidates row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    New,
    PrequalQueued,
    PrequalDone,
    Qualified,
    Disqualified,
    Skipped,
}

impl CandidateStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::New           => "new",
            Self::PrequalQueued => "prequal_queued",
            Self::PrequalDone   => "prequal_done",
            Self::Qualified     => "qualified",
            Self::Disqualified  => "disqualified",
            Self::Skipped       => "skipped",
        }
    }
}

impl FromStr for CandidateStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "new"             => Ok(Self::New),
            "prequal_queued"  => Ok(Self::PrequalQueued),
            "prequal_done"    => Ok(Self::PrequalDone),
            "qualified"       => Ok(Self::Qualified),
            "disqualified"    => Ok(Self::Disqualified),
            "skipped"         => Ok(Self::Skipped),
            other             => Err(format!("Unknown CandidateStatus: {}", other)),
        }
    }
}

impl std::fmt::Display for CandidateStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Fetch Query Status
// ============================================================================

/// Status of a company_fetch_queries row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchQueryStatus {
    Running,
    Succeeded,
    Failed,
    /// Fewer results returned than expected (end of Apollo result set).
    Exhausted,
}

impl FetchQueryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running   => "running",
            Self::Succeeded => "succeeded",
            Self::Failed    => "failed",
            Self::Exhausted => "exhausted",
        }
    }
}

impl FromStr for FetchQueryStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "running"   => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed"    => Ok(Self::Failed),
            "exhausted" => Ok(Self::Exhausted),
            other       => Err(format!("Unknown FetchQueryStatus: {}", other)),
        }
    }
}

impl std::fmt::Display for FetchQueryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// DB Row Structs
// ============================================================================

/// Row from `company_fetch_runs`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CompanyFetchRun {
    pub id: Uuid,
    pub client_id: Uuid,
    pub batch_target: i32,
    pub quota_plan: JsonValue,
    pub status: String,
    pub raw_fetched: i32,
    pub unique_upserted: i32,
    pub duplicates: i32,
    pub pages_fetched: i32,
    pub api_credits_used: i32,
    pub industry_summary: Option<JsonValue>,
    pub error: Option<JsonValue>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Row from `company_fetch_queries`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CompanyFetchQuery {
    pub id: Uuid,
    pub run_id: Uuid,
    pub client_id: Uuid,
    pub industry: String,
    pub variant: String,
    pub apollo_request: JsonValue,
    pub apollo_response_meta: Option<JsonValue>,
    pub page_start: i32,
    pub page_end: Option<i32>,
    pub pages_fetched: i32,
    pub orgs_returned: i32,
    pub status: String,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Row from `company_candidates`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CompanyCandidate {
    pub id: Uuid,
    pub client_id: Uuid,
    pub run_id: Uuid,
    pub company_id: Uuid,
    pub industry: Option<String>,
    pub variant: Option<String>,
    pub source: String,
    pub status: String,
    pub was_duplicate: bool,
    pub prequal_job_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

/// Row from `industry_yield_metrics`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct IndustryYieldMetric {
    pub id: Uuid,
    pub client_id: Uuid,
    pub industry: String,
    pub total_fetched: i32,
    pub total_upserted: i32,
    pub total_duplicates: i32,
    pub prequal_passed: i32,
    pub prequal_failed: i32,
    pub tier_a_count: i32,
    pub tier_b_count: i32,
    pub tier_c_count: i32,
    pub tier_d_count: i32,
    pub prequal_pass_rate: f64,
    pub tier_ab_rate: f64,
    pub avg_aggregate_score: f64,
    pub last_depleted_at: Option<DateTime<Utc>>,
    pub depletion_count: i32,
    pub run_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================================
// Quota Plan
// ============================================================================

/// Per-industry quota computed by the quota planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndustryQuota {
    /// Target number of unique companies for this industry in this run.
    pub target: i32,
    /// How much was allocated from exploration budget.
    pub exploration_share: i32,
    /// How much was allocated from yield-based budget.
    pub yield_share: i32,
}

/// The complete quota plan for a run.
pub type QuotaPlan = HashMap<String, IndustryQuota>;

// ============================================================================
// Industry Progress (in-memory tracking during a run)
// ============================================================================

/// Tracks progress for one industry during an active fetch run.
/// Held in memory by the orchestrator; periodically flushed to DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndustryProgress {
    pub industry: String,
    pub quota: i32,
    pub fetched: i32,
    pub upserted: i32,
    pub duplicates: i32,
    pub current_variant: QueryVariant,
    pub variants_used: Vec<String>,
    pub depleted: bool,
    pub depletion_reason: Option<String>,
}

impl IndustryProgress {
    pub fn new(industry: String, quota: i32) -> Self {
        Self {
            industry,
            quota,
            fetched: 0,
            upserted: 0,
            duplicates: 0,
            current_variant: QueryVariant::V1Strict,
            variants_used: vec!["v1_strict".to_string()],
            depleted: false,
            depletion_reason: None,
        }
    }

    /// Returns true if this industry has met its quota or is depleted.
    pub fn is_done(&self) -> bool {
        self.upserted >= self.quota || self.depleted
    }

    /// How many more unique companies are needed.
    pub fn remaining(&self) -> i32 {
        (self.quota - self.upserted).max(0)
    }
}

// ============================================================================
// ICP Profile (read from client_icp_profiles.icp_json)
// ============================================================================

/// Parsed subset of `client_icp_profiles.icp_json` that the Company Fetcher
/// needs for building Apollo queries.
///
/// The full ICP JSON may contain much more (personas, messaging, etc.)
/// but we only extract what's needed for discovery.
///
/// Expected icp_json structure (new format with industry objects):
/// ```json
/// {
///   "target_profile": {
///     "industries": [
///       {
///         "name": "Currency And Lending Services",
///         "diffbot_category": "Currency And Lending Services",
///         "include_keywords_any": ["loan", "lending", "credit"],
///         "include_keywords_all": [],
///         "exclude_keywords_any": ["crypto exchange", "forex signals"]
///       }
///     ],
///     "company_sizes": ["51-200"],
///     "locations": ["United States"],
///     "keywords": ["SaaS"]
///   }
/// }
/// ```
///
/// Backward-compatible with old format:
/// ```json
/// { "target_profile": { "industries": ["Software Companies", "Healthcare Companies"] } }
/// ```

// ============================================================================
// Industry Specification (per-industry keyword constraints)
// ============================================================================

/// Per-industry specification with keyword constraints for targeted DQL queries.
///
/// Each industry in the ICP can be either:
/// - A plain string (backward compat) → converted to IndustrySpec with no keyword constraints
/// - A full object with category name + include/exclude keyword lists
///
/// The `diffbot_category` is used in DQL `categories.name:"..."`.
/// Keywords are used to narrow results within that category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndustrySpec {
    /// Human-readable industry name (used as key in quota plan + progress map).
    pub name: String,

    /// Diffbot category name for DQL `categories.name:"..."`.
    pub diffbot_category: String,

    /// At least one of these keywords must appear in description/name (OR semantics).
    /// Empty = no include filter (broader results).
    #[serde(default)]
    pub include_keywords_any: Vec<String>,

    /// ALL of these keywords must appear in description (AND semantics).
    /// Empty = no mandatory keywords.
    #[serde(default)]
    pub include_keywords_all: Vec<String>,

    /// None of these keywords should appear in description/name (NOT semantics).
    /// Empty = no exclude filter.
    #[serde(default)]
    pub exclude_keywords_any: Vec<String>,

    /// Priority weight (0.0–1.0) for quota allocation. Default 0.5.
    #[serde(default = "default_industry_priority")]
    pub priority: f64,
}

fn default_industry_priority() -> f64 { 0.5 }

impl IndustrySpec {
    /// Create from a plain category string (backward compat with old ICP format).
    pub fn from_category_string(category: &str) -> Self {
        Self {
            name: category.to_string(),
            diffbot_category: category.to_string(),
            include_keywords_any: vec![],
            include_keywords_all: vec![],
            exclude_keywords_any: vec![],
            priority: default_industry_priority(),
        }
    }

    /// Whether this spec has include keyword constraints (tight query possible).
    pub fn has_include_constraints(&self) -> bool {
        !self.include_keywords_any.is_empty() || !self.include_keywords_all.is_empty()
    }
}

// ============================================================================
// ICP Fetch Profile
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcpFetchProfile {
    pub industries: Vec<IndustrySpec>,
    pub company_sizes: Vec<String>,
    pub locations: Vec<String>,
    #[serde(default)]
    pub excluded_locations: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    pub revenue_min: Option<i64>,
    pub revenue_max: Option<i64>,

    // V2 expansion: broader geo
    #[serde(default)]
    pub v2_locations: Vec<String>,

    // V3 expansion: broader size ranges
    #[serde(default)]
    pub v3_sizes: Vec<String>,
}

impl IcpFetchProfile {
    /// Extract the fetch-relevant fields from raw `icp_json`.
    ///
    /// Handles both flat format and nested `target_profile` format.
    /// Industries can be either:
    ///   - Array of objects: [{ "name": "...", "diffbot_category": "...", ... }]
    ///   - Array of strings: ["Software Companies", ...] (backward compat)
    pub fn from_icp_json(raw: &JsonValue) -> Result<Self, String> {
        // Try nested format first: { "target_profile": { ... } }
        let target = raw.get("target_profile").unwrap_or(raw);

        // Parse industries: handle both object array and string array
        let industries = parse_industry_specs(target)?;

        if industries.is_empty() {
            return Err("icp_json 'industries' array is empty".to_string());
        }

        let company_sizes = json_string_array(target, "company_sizes")
            .unwrap_or_else(|| vec!["51-200".into(), "201-500".into(), "501-1000".into()]);

        let locations = json_string_array(target, "locations")
            .unwrap_or_else(|| vec!["United States".into()]);

        let excluded_locations = json_string_array(target, "excluded_locations")
            .unwrap_or_default();

        let keywords = json_string_array(target, "keywords")
            .unwrap_or_default();

        let revenue_min = target.get("revenue_min")
            .and_then(|v| v.as_i64());
        let revenue_max = target.get("revenue_max")
            .and_then(|v| v.as_i64());

        // V2/V3 expansion from sibling keys
        let geo_exp = raw.get("geo_expansion").unwrap_or(target);
        let v2_locations = json_string_array(geo_exp, "v2_locations")
            .unwrap_or_default();

        let size_exp = raw.get("size_expansion").unwrap_or(target);
        let v3_sizes = json_string_array(size_exp, "v3_sizes")
            .unwrap_or_default();

        Ok(Self {
            industries,
            company_sizes,
            locations,
            excluded_locations,
            keywords,
            revenue_min,
            revenue_max,
            v2_locations,
            v3_sizes,
        })
    }

    /// Get industry names (for round-robin order, quota plan keys, etc.).
    pub fn industry_names(&self) -> Vec<String> {
        self.industries.iter().map(|s| s.name.clone()).collect()
    }

    /// Build a lookup map: industry name → IndustrySpec reference.
    pub fn industry_spec_map(&self) -> HashMap<String, &IndustrySpec> {
        self.industries
            .iter()
            .map(|s| (s.name.clone(), s))
            .collect()
    }
}

/// Parse industries array from icp_json — supports both object and string formats.
fn parse_industry_specs(target: &JsonValue) -> Result<Vec<IndustrySpec>, String> {
    let arr = target
        .get("industries")
        .and_then(|v| v.as_array())
        .ok_or("icp_json missing 'industries' array")?;

    let mut specs = Vec::with_capacity(arr.len());

    for item in arr {
        if let Some(s) = item.as_str() {
            // Backward compat: plain string → IndustrySpec
            specs.push(IndustrySpec::from_category_string(s));
        } else if item.is_object() {
            // New format: full object
            let name = item.get("name")
                .and_then(|v| v.as_str())
                .ok_or("industry object missing 'name'")?
                .to_string();

            let diffbot_category = item.get("diffbot_category")
                .and_then(|v| v.as_str())
                .ok_or("industry object missing 'diffbot_category'")?
                .to_string();

            let include_keywords_any = item.get("include_keywords_any")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();

            let include_keywords_all = item.get("include_keywords_all")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();

            let exclude_keywords_any = item.get("exclude_keywords_any")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();

            let priority = item.get("priority")
                .and_then(|v| v.as_f64())
                .unwrap_or(default_industry_priority());

            specs.push(IndustrySpec {
                name,
                diffbot_category,
                include_keywords_any,
                include_keywords_all,
                exclude_keywords_any,
                priority,
            });
        } else {
            return Err(format!("industry item must be a string or object, got: {}", item));
        }
    }

    Ok(specs)
}

/// Helper: extract a JSON array of strings from an object key.
fn json_string_array(obj: &JsonValue, key: &str) -> Option<Vec<String>> {
    obj.get(key)?
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
}

// ============================================================================
// Apollo Organization Search — Request
// ============================================================================

/// Request body for Apollo Organization Search API.
///
/// POST https://api.apollo.io/api/v1/mixed_companies/search
///
/// Apollo accepts these as query params or JSON body fields.
/// We use JSON body for cleanliness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApolloOrgSearchRequest {
    /// Page number (1-indexed, max 500).
    pub page: i32,

    /// Results per page (max 100).
    pub per_page: i32,

    /// Filter by industry tag IDs.
    /// Apollo uses internal tag IDs — these can also be industry name strings
    /// depending on the API version. We send what the ICP specifies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_industry_tag_ids: Option<Vec<String>>,

    /// Filter by HQ location (city, state, or country strings).
    /// e.g. ["United States", "California", "New York, NY"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_locations: Option<Vec<String>>,

    /// Locations to exclude from results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_not_locations: Option<Vec<String>>,

    /// Employee count range strings.
    /// e.g. ["51-200", "201-500", "501-1000"]
    /// Valid: "1-10", "11-50", "51-200", "201-500", "501-1000",
    ///        "1001-5000", "5001-10000", "10001+"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_num_employees_ranges: Option<Vec<String>>,

    /// Keyword tags to search in organization data.
    /// Useful for narrowing taxonomy noise (e.g. "SaaS", "platform").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q_organization_keyword_tags: Option<Vec<String>>,

    /// Revenue range filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revenue_range: Option<ApolloRevenueRange>,

    /// Organization name search (rarely used in bulk discovery).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q_organization_name: Option<String>,

    // ---- Per-industry keyword constraints (Diffbot-specific, ignored by Apollo) ----

    /// Include keywords (OR semantics): at least one must appear in description/name.
    /// Set by VariantBuilder from IndustrySpec.include_keywords_any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_keywords_any: Option<Vec<String>>,

    /// Include keywords (AND semantics): ALL must appear in description.
    /// Set by VariantBuilder from IndustrySpec.include_keywords_all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_keywords_all: Option<Vec<String>>,

    /// Exclude keywords (NOT semantics): none should appear in description/name.
    /// Set by VariantBuilder from IndustrySpec.exclude_keywords_any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_keywords_any: Option<Vec<String>>,

    /// The variant label for enrichment metadata ("tight" or "loose").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_variant_label: Option<String>,
}

/// Revenue range sub-object for Apollo requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApolloRevenueRange {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

// ============================================================================
// Apollo Organization Search — Response
// ============================================================================

/// Top-level response from Apollo Organization Search.
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloOrgSearchResponse {
    #[serde(default)]
    pub organizations: Vec<ApolloOrganization>,

    #[serde(default)]
    pub pagination: ApolloPagination,
}

/// Pagination metadata from Apollo.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ApolloPagination {
    /// Current page number.
    #[serde(default)]
    pub page: i32,

    /// Results per page.
    #[serde(default)]
    pub per_page: i32,

    /// Total matching organizations (up to 50,000 display limit).
    #[serde(default)]
    pub total_entries: i64,

    /// Total pages available.
    #[serde(default)]
    pub total_pages: i32,
}

/// A single organization from Apollo search results.
///
/// Only includes fields we actually use for upsert into `companies`.
/// Apollo returns many more fields; we ignore them via `#[serde(default)]`
/// and `Option` to keep deserialization robust.
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloOrganization {
    /// Apollo internal org ID (our `companies.external_id`).
    pub id: String,

    /// Company name.
    #[serde(default)]
    pub name: Option<String>,

    /// Primary website domain (e.g. "acme.com").
    #[serde(default)]
    pub primary_domain: Option<String>,

    /// Full website URL (fallback if primary_domain is missing).
    #[serde(default)]
    pub website_url: Option<String>,

    /// LinkedIn company page URL.
    #[serde(default)]
    pub linkedin_url: Option<String>,

    /// LinkedIn numeric UID.
    #[serde(default)]
    pub linkedin_uid: Option<String>,

    /// Primary industry classification.
    #[serde(default)]
    pub industry: Option<String>,

    /// All industry classifications.
    #[serde(default)]
    pub industries: Vec<String>,

    /// Estimated headcount.
    #[serde(default)]
    pub estimated_num_employees: Option<i32>,

    /// Year founded.
    #[serde(default)]
    pub founded_year: Option<i32>,

    // ---- Location ----

    #[serde(default)]
    pub city: Option<String>,

    #[serde(default)]
    pub state: Option<String>,

    #[serde(default)]
    pub country: Option<String>,

    #[serde(default)]
    pub postal_code: Option<String>,

    #[serde(default)]
    pub raw_address: Option<String>,

    #[serde(default)]
    pub street_address: Option<String>,

    // ---- Metadata ----

    /// Short company description.
    #[serde(default)]
    pub short_description: Option<String>,

    /// SEO description (fallback).
    #[serde(default)]
    pub seo_description: Option<String>,

    /// Keyword tags.
    #[serde(default)]
    pub keywords: Vec<String>,

    /// Company phone number.
    #[serde(default)]
    pub phone: Option<String>,

    /// Logo image URL.
    #[serde(default)]
    pub logo_url: Option<String>,

    // ---- Financials (available on enriched responses) ----

    /// Annual revenue in USD.
    #[serde(default)]
    pub annual_revenue: Option<i64>,

    /// Total funding raised.
    #[serde(default)]
    pub total_funding: Option<i64>,

    /// Display string for total funding (e.g. "$10M").
    #[serde(default)]
    pub total_funding_printed: Option<String>,

    /// Most recent funding round date.
    #[serde(default)]
    pub latest_funding_round_date: Option<String>,

    /// Most recent funding stage.
    #[serde(default)]
    pub latest_funding_stage: Option<String>,

    // ---- Industry tag (for tracking) ----

    /// Apollo industry tag ID.
    #[serde(default)]
    pub industry_tag_id: Option<String>,

    /// Map of industry name → tag ID.
    #[serde(default)]
    pub industry_tag_hash: Option<HashMap<String, String>>,
}

impl ApolloOrganization {
    /// Extract the best available domain, normalized (lowercase, no www.).
    pub fn normalized_domain(&self) -> Option<String> {
        let raw = self.primary_domain.as_deref()
            .or(self.website_url.as_deref());

        raw.map(|d| normalize_domain(d))
    }

    /// Extract a normalized LinkedIn URL.
    pub fn normalized_linkedin(&self) -> Option<String> {
        self.linkedin_url.as_deref().map(|u| {
            u.to_lowercase().trim_end_matches('/').to_string()
        })
    }
}

// ============================================================================
// Company Upsert Result
// ============================================================================

/// Result of a single company upsert attempt.
#[derive(Debug, Clone)]
pub struct UpsertResult {
    /// The company ID (whether newly inserted or existing).
    pub company_id: Uuid,
    /// True if this was a new insert; false if it matched an existing row.
    pub was_insert: bool,
}

// ============================================================================
// Apollo Error Response
// ============================================================================

/// Error body returned by Apollo when a request fails.
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloErrorResponse {
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl std::fmt::Display for ApolloErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = self.message.as_deref()
            .or(self.error.as_deref())
            .unwrap_or("unknown Apollo error");
        f.write_str(msg)
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Normalize a domain string: lowercase, strip protocol, strip www., strip path.
///
/// Examples:
///   "https://www.Acme.com/about" → "acme.com"
///   "Www.EXAMPLE.IO"             → "example.io"
///   "example.com"                → "example.com"
pub fn normalize_domain(raw: &str) -> String {
    let mut d = raw.to_lowercase();

    // Strip protocol
    if let Some(rest) = d.strip_prefix("https://") {
        d = rest.to_string();
    } else if let Some(rest) = d.strip_prefix("http://") {
        d = rest.to_string();
    }

    // Strip www.
    if let Some(rest) = d.strip_prefix("www.") {
        d = rest.to_string();
    }

    // Strip path, query, fragment
    if let Some(idx) = d.find('/') {
        d.truncate(idx);
    }
    if let Some(idx) = d.find('?') {
        d.truncate(idx);
    }
    if let Some(idx) = d.find('#') {
        d.truncate(idx);
    }

    d.trim().to_string()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_domain() {
        assert_eq!(normalize_domain("https://www.Acme.com/about"), "acme.com");
        assert_eq!(normalize_domain("http://WWW.EXAMPLE.IO"), "example.io");
        assert_eq!(normalize_domain("example.com"), "example.com");
        assert_eq!(normalize_domain("www.foo.bar"), "foo.bar");
        assert_eq!(normalize_domain("https://test.co?q=1"), "test.co");
    }

    #[test]
    fn test_icp_fetch_profile_nested() {
        let raw = serde_json::json!({
            "target_profile": {
                "industries": ["healthcare_saas", "fintech"],
                "company_sizes": ["51-200", "201-500"],
                "locations": ["United States"],
                "keywords": ["SaaS"]
            },
            "geo_expansion": {
                "v2_locations": ["Germany", "France"]
            }
        });

        let profile = IcpFetchProfile::from_icp_json(&raw).unwrap();
        assert_eq!(profile.industries.len(), 2);
        assert_eq!(profile.industries[0].name, "healthcare_saas");
        assert_eq!(profile.industries[0].diffbot_category, "healthcare_saas");
        assert!(profile.industries[0].include_keywords_any.is_empty());
        assert_eq!(profile.v2_locations.len(), 2);
        assert_eq!(profile.company_sizes[0], "51-200");
    }

    #[test]
    fn test_icp_fetch_profile_flat() {
        let raw = serde_json::json!({
            "industries": ["cybersecurity"],
            "company_sizes": ["501-1000"],
            "locations": ["Canada"]
        });

        let profile = IcpFetchProfile::from_icp_json(&raw).unwrap();
        assert_eq!(profile.industries[0].name, "cybersecurity");
        assert_eq!(profile.locations[0], "Canada");
    }

    #[test]
    fn test_icp_fetch_profile_missing_industries() {
        let raw = serde_json::json!({
            "target_profile": { "locations": ["US"] }
        });
        assert!(IcpFetchProfile::from_icp_json(&raw).is_err());
    }

    #[test]
    fn test_icp_fetch_profile_object_industries() {
        let raw = serde_json::json!({
            "target_profile": {
                "industries": [
                    {
                        "name": "Lending",
                        "diffbot_category": "Currency And Lending Services",
                        "include_keywords_any": ["loan", "lending", "credit"],
                        "include_keywords_all": [],
                        "exclude_keywords_any": ["crypto exchange"],
                        "priority": 0.9
                    },
                    {
                        "name": "Construction",
                        "diffbot_category": "Infrastructure Construction Companies",
                        "include_keywords_any": ["contract", "build"],
                        "include_keywords_all": ["construction"],
                        "exclude_keywords_any": ["residential"],
                        "priority": 0.8
                    }
                ],
                "company_sizes": ["51-200"],
                "locations": ["United States"]
            }
        });

        let profile = IcpFetchProfile::from_icp_json(&raw).unwrap();
        assert_eq!(profile.industries.len(), 2);

        let lending = &profile.industries[0];
        assert_eq!(lending.name, "Lending");
        assert_eq!(lending.diffbot_category, "Currency And Lending Services");
        assert_eq!(lending.include_keywords_any.len(), 3);
        assert_eq!(lending.include_keywords_any[0], "loan");
        assert_eq!(lending.include_keywords_any[1], "lending");
        assert_eq!(lending.include_keywords_any[2], "credit");
        assert!(lending.include_keywords_all.is_empty());
        assert_eq!(lending.exclude_keywords_any.len(), 1);
        assert_eq!(lending.exclude_keywords_any[0], "crypto exchange");
        assert!((lending.priority - 0.9).abs() < 0.01);

        let construction = &profile.industries[1];
        assert_eq!(construction.name, "Construction");
        assert_eq!(construction.include_keywords_all.len(), 1);
        assert_eq!(construction.include_keywords_all[0], "construction");
        assert!(construction.has_include_constraints());

        // industry_names helper
        let names = profile.industry_names();
        assert_eq!(names, vec!["Lending", "Construction"]);
    }

    #[test]
    fn test_icp_fetch_profile_mixed_format_rejected() {
        // Can't mix strings and objects in same array
        let raw = serde_json::json!({
            "industries": ["Software", {"name": "Lending", "diffbot_category": "Lending"}]
        });

        // Both should parse fine — strings become IndustrySpec::from_category_string
        let profile = IcpFetchProfile::from_icp_json(&raw).unwrap();
        assert_eq!(profile.industries.len(), 2);
        assert_eq!(profile.industries[0].name, "Software");
        assert_eq!(profile.industries[1].name, "Lending");
    }

    #[test]
    fn test_icp_industry_object_missing_name() {
        let raw = serde_json::json!({
            "industries": [{"diffbot_category": "Lending"}]
        });
        assert!(IcpFetchProfile::from_icp_json(&raw).is_err());
    }

    #[test]
    fn test_icp_industry_object_missing_category() {
        let raw = serde_json::json!({
            "industries": [{"name": "Lending"}]
        });
        assert!(IcpFetchProfile::from_icp_json(&raw).is_err());
    }

    #[test]
    fn test_icp_industry_priority_default() {
        let raw = serde_json::json!({
            "industries": [{"name": "X", "diffbot_category": "Y"}]
        });
        let profile = IcpFetchProfile::from_icp_json(&raw).unwrap();
        assert!((profile.industries[0].priority - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_query_variant_ladder() {
        let ladder = QueryVariant::ladder();
        assert_eq!(ladder.len(), 4);
        assert_eq!(ladder[0], QueryVariant::V1Strict);
        assert_eq!(ladder[3], QueryVariant::V4KeywordAssist);
    }

    #[test]
    fn test_apollo_org_normalized_domain() {
        let org = ApolloOrganization {
            id: "abc123".into(),
            name: Some("Test Corp".into()),
            primary_domain: Some("www.TestCorp.com".into()),
            website_url: None,
            linkedin_url: Some("http://www.linkedin.com/company/testcorp/".into()),
            linkedin_uid: None,
            industry: None,
            industries: vec![],
            estimated_num_employees: None,
            founded_year: None,
            city: None,
            state: None,
            country: None,
            postal_code: None,
            raw_address: None,
            street_address: None,
            short_description: None,
            seo_description: None,
            keywords: vec![],
            phone: None,
            logo_url: None,
            annual_revenue: None,
            total_funding: None,
            total_funding_printed: None,
            latest_funding_round_date: None,
            latest_funding_stage: None,
            industry_tag_id: None,
            industry_tag_hash: None,
        };

        assert_eq!(org.normalized_domain().unwrap(), "testcorp.com");
        assert_eq!(
            org.normalized_linkedin().unwrap(),
            "http://www.linkedin.com/company/testcorp"
        );
    }

    #[test]
    fn test_deserialize_payload_defaults() {
        let json = r#"{ "client_id": "550e8400-e29b-41d4-a716-446655440000" }"#;
        let payload: DiscoverCompaniesPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.batch_target, 2000);
        assert_eq!(payload.page_size, 100);
        assert!((payload.exploration_pct - 0.20).abs() < f64::EPSILON);
        assert!(payload.enqueue_prequal_jobs);
        assert!(payload.resume_run_id.is_none());
        assert!(!payload.force_full_rescan);
    }

    #[test]
    fn test_deserialize_payload_force_rescan() {
        let json = r#"{
            "client_id": "550e8400-e29b-41d4-a716-446655440000",
            "force_full_rescan": true,
            "batch_target": 500
        }"#;
        let payload: DiscoverCompaniesPayload = serde_json::from_str(json).unwrap();
        assert!(payload.force_full_rescan);
        assert_eq!(payload.batch_target, 500);
    }
}
