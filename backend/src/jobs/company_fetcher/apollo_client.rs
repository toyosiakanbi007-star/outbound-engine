// src/jobs/company_fetcher/apollo_client.rs
//
// Apollo Organization Search API client.
//
// RESPONSIBILITIES:
// - Build and send POST requests to /api/v1/mixed_companies/search
// - Handle pagination (fetch multiple pages for one query)
// - Exponential backoff on 429 (rate limit) and 5xx errors
// - Parse responses into typed ApolloOrgSearchResponse structs
// - Track API credit usage and provide telemetry
//
// APOLLO API CONSTRAINTS:
// - Max 100 results per page
// - Max 500 pages per query (50,000 record display limit)
// - Rate limits vary by plan (typically 50-100 req/min on free)
// - Auth via x-api-key header OR api_key body param
//
// USAGE:
//   let client = ApolloClient::new(http_client, api_key, base_url);
//   let (orgs, meta) = client.search_page(&request).await?;
//   let all_orgs = client.search_all_pages(&request, max_pages, callback).await?;

use reqwest::Client as HttpClient;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::models::{
    ApolloErrorResponse, ApolloOrgSearchRequest, ApolloOrgSearchResponse,
    ApolloOrganization,
};

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur when calling the Apollo API.
#[derive(Debug, thiserror::Error)]
pub enum ApolloApiError {
    /// HTTP request failed (network error, timeout, etc.)
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    /// Apollo returned a rate-limit response (HTTP 429).
    /// The client should back off and retry.
    #[error("Apollo rate limited (429). Retry after backoff.")]
    RateLimited,

    /// Apollo returned a server error (5xx).
    #[error("Apollo server error: HTTP {status} — {body}")]
    ServerError { status: u16, body: String },

    /// Apollo returned a client error (4xx, not 429).
    #[error("Apollo client error: HTTP {status} — {body}")]
    ClientError { status: u16, body: String },

    /// Response body could not be parsed.
    #[error("Failed to parse Apollo response: {0}")]
    ParseError(String),

    /// API key is missing or empty.
    #[error("Apollo API key not configured")]
    MissingApiKey,

    /// Max retries exhausted after repeated failures.
    #[error("Max retries ({max_retries}) exhausted. Last error: {last_error}")]
    RetriesExhausted {
        max_retries: u32,
        last_error: String,
    },
}

// ============================================================================
// Configuration
// ============================================================================

/// Default Apollo API base URL.
const DEFAULT_BASE_URL: &str = "https://api.apollo.io";

/// Organization Search endpoint path.
const SEARCH_PATH: &str = "/api/v1/mixed_companies/search";

/// Default request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Max retries per single API call (for 429 / 5xx).
const MAX_RETRIES: u32 = 5;

/// Base delay for exponential backoff (seconds).
const BACKOFF_BASE_SECS: u64 = 2;

/// Max delay cap for backoff (seconds).
const BACKOFF_MAX_SECS: u64 = 60;

/// Apollo's max results per page.
pub const APOLLO_MAX_PER_PAGE: i32 = 100;

/// Apollo's max page number (display limit = 50,000 records).
pub const APOLLO_MAX_PAGE: i32 = 500;

// ============================================================================
// Page Fetch Result
// ============================================================================

/// Metadata from a single page fetch (for telemetry).
#[derive(Debug, Clone)]
pub struct PageFetchMeta {
    pub page: i32,
    pub orgs_returned: usize,
    pub total_entries: i64,
    pub total_pages: i32,
    /// Number of retries that were needed for this page.
    pub retries: u32,
    /// Time taken for this page fetch (including retries).
    pub duration_ms: u64,
}

// ============================================================================
// Apollo Client
// ============================================================================

/// HTTP client wrapper for Apollo Organization Search API.
///
/// Handles auth, pagination, rate-limit backoff, and response parsing.
/// Stateless — safe to share across tasks via Arc.
#[derive(Clone)]
pub struct ApolloClient {
    http: HttpClient,
    api_key: String,
    base_url: String,
}

impl ApolloClient {
    /// Create a new Apollo API client.
    ///
    /// # Arguments
    /// * `http` — Shared reqwest HTTP client (with its own timeout/pool settings).
    /// * `api_key` — Apollo API key.
    /// * `base_url` — Optional override (defaults to `https://api.apollo.io`).
    pub fn new(http: HttpClient, api_key: String, base_url: Option<String>) -> Self {
        Self {
            http,
            api_key,
            base_url: base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
        }
    }

    /// Validate that the client is configured properly.
    pub fn validate(&self) -> Result<(), ApolloApiError> {
        if self.api_key.is_empty() {
            return Err(ApolloApiError::MissingApiKey);
        }
        Ok(())
    }

    // ========================================================================
    // Single Page Fetch
    // ========================================================================

    /// Fetch a single page of organization search results.
    ///
    /// Includes retry logic with exponential backoff for 429 and 5xx errors.
    ///
    /// Returns the list of organizations and page metadata.
    pub async fn search_page(
        &self,
        request: &ApolloOrgSearchRequest,
    ) -> Result<(Vec<ApolloOrganization>, PageFetchMeta), ApolloApiError> {
        self.validate()?;

        let url = format!("{}{}", self.base_url, SEARCH_PATH);
        let start = std::time::Instant::now();
        let mut retries: u32 = 0;

        loop {
            let result = self.execute_search(&url, request).await;

            match result {
                Ok(response) => {
                    let meta = PageFetchMeta {
                        page: response.pagination.page,
                        orgs_returned: response.organizations.len(),
                        total_entries: response.pagination.total_entries,
                        total_pages: response.pagination.total_pages,
                        retries,
                        duration_ms: start.elapsed().as_millis() as u64,
                    };

                    debug!(
                        "Apollo page {} returned {} orgs (total_entries={}, total_pages={})",
                        request.page,
                        response.organizations.len(),
                        response.pagination.total_entries,
                        response.pagination.total_pages,
                    );

                    return Ok((response.organizations, meta));
                }
                Err(ApolloApiError::RateLimited) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(ApolloApiError::RetriesExhausted {
                            max_retries: MAX_RETRIES,
                            last_error: "429 rate limited".to_string(),
                        });
                    }
                    let delay = backoff_delay(retries);
                    warn!(
                        "Apollo 429 rate limited on page {}. Retry {}/{} after {:.1}s",
                        request.page, retries, MAX_RETRIES, delay.as_secs_f64()
                    );
                    sleep(delay).await;
                }
                Err(ApolloApiError::ServerError { status, ref body }) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(ApolloApiError::RetriesExhausted {
                            max_retries: MAX_RETRIES,
                            last_error: format!("HTTP {} — {}", status, body),
                        });
                    }
                    let delay = backoff_delay(retries);
                    warn!(
                        "Apollo {} server error on page {}. Retry {}/{} after {:.1}s",
                        status, request.page, retries, MAX_RETRIES, delay.as_secs_f64()
                    );
                    sleep(delay).await;
                }
                Err(ApolloApiError::HttpError(ref e)) if is_transient(e) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(ApolloApiError::RetriesExhausted {
                            max_retries: MAX_RETRIES,
                            last_error: format!("HTTP error: {}", e),
                        });
                    }
                    let delay = backoff_delay(retries);
                    warn!(
                        "Apollo transient error on page {}. Retry {}/{} after {:.1}s: {}",
                        request.page, retries, MAX_RETRIES, delay.as_secs_f64(), e
                    );
                    sleep(delay).await;
                }
                // Non-retryable errors: client errors (4xx except 429), parse errors, etc.
                Err(e) => return Err(e),
            }
        }
    }

    // ========================================================================
    // Multi-Page Fetch
    // ========================================================================

    /// Fetch multiple pages of results for a single query configuration.
    ///
    /// Stops when:
    /// - `max_pages` pages fetched, OR
    /// - Apollo returns fewer orgs than `per_page` (end of results), OR
    /// - Apollo page number exceeds its 500-page limit, OR
    /// - `should_continue` callback returns false (e.g. quota reached)
    ///
    /// The `on_page` callback is invoked after each page with the orgs and meta,
    /// allowing the caller to process/upsert results incrementally.
    /// It returns `true` to continue fetching or `false` to stop.
    pub async fn search_pages<F>(
        &self,
        base_request: &ApolloOrgSearchRequest,
        max_pages: i32,
        mut on_page: F,
    ) -> Result<PaginationSummary, ApolloApiError>
    where
        F: FnMut(Vec<ApolloOrganization>, &PageFetchMeta) -> bool,
    {
        let per_page = base_request.per_page.min(APOLLO_MAX_PER_PAGE);
        let mut current_page = base_request.page;
        let mut summary = PaginationSummary::default();

        loop {
            // Check Apollo's hard limit
            if current_page > APOLLO_MAX_PAGE {
                info!(
                    "Reached Apollo max page limit ({}) — stopping pagination",
                    APOLLO_MAX_PAGE
                );
                summary.stop_reason = StopReason::ApolloPageLimit;
                break;
            }

            // Check caller's max pages
            if summary.pages_fetched >= max_pages {
                info!(
                    "Reached max_pages limit ({}) — stopping pagination",
                    max_pages
                );
                summary.stop_reason = StopReason::MaxPages;
                break;
            }

            // Build request for this page
            let mut page_request = base_request.clone();
            page_request.page = current_page;
            page_request.per_page = per_page;

            // Fetch
            let (orgs, meta) = self.search_page(&page_request).await?;

            // Update summary
            summary.pages_fetched += 1;
            summary.total_orgs_returned += orgs.len() as i32;
            summary.total_entries = meta.total_entries;
            summary.total_available_pages = meta.total_pages;
            summary.total_retries += meta.retries;

            let orgs_count = orgs.len() as i32;

            // Callback: process this page. Returns false → stop.
            let should_continue = on_page(orgs, &meta);

            if !should_continue {
                info!("Callback signaled stop at page {}", current_page);
                summary.stop_reason = StopReason::CallbackStopped;
                break;
            }

            // If fewer results than per_page, we've exhausted this query
            if orgs_count < per_page {
                info!(
                    "Apollo returned {} orgs (< per_page {}): results exhausted at page {}",
                    orgs_count, per_page, current_page
                );
                summary.stop_reason = StopReason::ResultsExhausted;
                break;
            }

            current_page += 1;
        }

        Ok(summary)
    }

    // ========================================================================
    // Internal: Execute a single HTTP request
    // ========================================================================

    async fn execute_search(
        &self,
        url: &str,
        request: &ApolloOrgSearchRequest,
    ) -> Result<ApolloOrgSearchResponse, ApolloApiError> {
        let response = self
            .http
            .post(url)
            .timeout(DEFAULT_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .header("x-api-key", &self.api_key)
            .json(request)
            .send()
            .await?;

        let status = response.status();

        // 429 — Rate limited
        if status.as_u16() == 429 {
            return Err(ApolloApiError::RateLimited);
        }

        // 5xx — Server error (retryable)
        if status.is_server_error() {
            let body = response.text().await.unwrap_or_default();
            return Err(ApolloApiError::ServerError {
                status: status.as_u16(),
                body: truncate_body(&body),
            });
        }

        // 4xx — Client error (not retryable)
        if status.is_client_error() {
            let body = response.text().await.unwrap_or_default();

            // Try to parse Apollo error format
            if let Ok(err) = serde_json::from_str::<ApolloErrorResponse>(&body) {
                return Err(ApolloApiError::ClientError {
                    status: status.as_u16(),
                    body: err.to_string(),
                });
            }

            return Err(ApolloApiError::ClientError {
                status: status.as_u16(),
                body: truncate_body(&body),
            });
        }

        // 2xx — Parse response
        let body_text = response.text().await?;
        let parsed: ApolloOrgSearchResponse = serde_json::from_str(&body_text)
            .map_err(|e| {
                ApolloApiError::ParseError(format!(
                    "JSON parse error: {} — body preview: {}",
                    e,
                    truncate_body(&body_text)
                ))
            })?;

        Ok(parsed)
    }
}

// ============================================================================
// Pagination Summary
// ============================================================================

/// Summary of a multi-page fetch operation.
#[derive(Debug, Clone, Default)]
pub struct PaginationSummary {
    pub pages_fetched: i32,
    pub total_orgs_returned: i32,
    /// Total matching orgs reported by Apollo (may exceed what we fetched).
    pub total_entries: i64,
    /// Total pages available for this query in Apollo.
    pub total_available_pages: i32,
    /// Cumulative retries across all pages.
    pub total_retries: u32,
    /// Why pagination stopped.
    pub stop_reason: StopReason,
}

/// Reason why multi-page fetching stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StopReason {
    #[default]
    Unknown,
    /// All results fetched (last page returned fewer than per_page).
    ResultsExhausted,
    /// Reached the caller's max_pages limit.
    MaxPages,
    /// Hit Apollo's 500-page display limit.
    ApolloPageLimit,
    /// on_page callback returned false.
    CallbackStopped,
}

impl StopReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown           => "unknown",
            Self::ResultsExhausted  => "results_exhausted",
            Self::MaxPages          => "max_pages",
            Self::ApolloPageLimit   => "apollo_page_limit",
            Self::CallbackStopped   => "callback_stopped",
        }
    }
}

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Compute exponential backoff delay with jitter.
///
/// delay = min(BACKOFF_MAX, BACKOFF_BASE * 2^(attempt-1)) + jitter
fn backoff_delay(attempt: u32) -> Duration {
    let base = BACKOFF_BASE_SECS.saturating_mul(1u64 << (attempt - 1).min(6));
    let capped = base.min(BACKOFF_MAX_SECS);

    // Add small jitter (0–1 second) to prevent thundering herd
    let jitter_ms = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis() % 1000) as u64;

    Duration::from_millis(capped * 1000 + jitter_ms)
}

/// Check if a reqwest error is transient (timeout, connection error).
fn is_transient(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

/// Truncate a response body for logging (avoid logging megabytes of HTML).
fn truncate_body(body: &str) -> String {
    if body.len() <= 500 {
        body.to_string()
    } else {
        format!("{}... [truncated, {} bytes total]", &body[..500], body.len())
    }
}

// ============================================================================
// Builder helpers for ApolloOrgSearchRequest
// ============================================================================

impl ApolloOrgSearchRequest {
    /// Create a minimal request for a specific page.
    pub fn new(page: i32, per_page: i32) -> Self {
        Self {
            page,
            per_page: per_page.min(APOLLO_MAX_PER_PAGE),
            organization_industry_tag_ids: None,
            organization_locations: None,
            organization_not_locations: None,
            organization_num_employees_ranges: None,
            q_organization_keyword_tags: None,
            revenue_range: None,
            q_organization_name: None,
            include_keywords_any: None,
            include_keywords_all: None,
            exclude_keywords_any: None,
            query_variant_label: None,
        }
    }

    /// Set industry filter.
    pub fn with_industries(mut self, industries: Vec<String>) -> Self {
        if !industries.is_empty() {
            self.organization_industry_tag_ids = Some(industries);
        }
        self
    }

    /// Set location filter.
    pub fn with_locations(mut self, locations: Vec<String>) -> Self {
        if !locations.is_empty() {
            self.organization_locations = Some(locations);
        }
        self
    }

    /// Set excluded locations.
    pub fn with_excluded_locations(mut self, locations: Vec<String>) -> Self {
        if !locations.is_empty() {
            self.organization_not_locations = Some(locations);
        }
        self
    }

    /// Set employee count ranges.
    pub fn with_employee_ranges(mut self, ranges: Vec<String>) -> Self {
        if !ranges.is_empty() {
            self.organization_num_employees_ranges = Some(ranges);
        }
        self
    }

    /// Set keyword tags.
    pub fn with_keywords(mut self, keywords: Vec<String>) -> Self {
        if !keywords.is_empty() {
            self.q_organization_keyword_tags = Some(keywords);
        }
        self
    }

    /// Set revenue range.
    pub fn with_revenue(mut self, min: Option<i64>, max: Option<i64>) -> Self {
        if min.is_some() || max.is_some() {
            self.revenue_range = Some(super::models::ApolloRevenueRange { min, max });
        }
        self
    }

    /// Set include keywords (OR semantics) from IndustrySpec.
    pub fn with_include_keywords_any(mut self, keywords: Vec<String>) -> Self {
        if !keywords.is_empty() {
            self.include_keywords_any = Some(keywords);
        }
        self
    }

    /// Set include keywords (AND semantics) from IndustrySpec.
    pub fn with_include_keywords_all(mut self, keywords: Vec<String>) -> Self {
        if !keywords.is_empty() {
            self.include_keywords_all = Some(keywords);
        }
        self
    }

    /// Set exclude keywords from IndustrySpec.
    pub fn with_exclude_keywords_any(mut self, keywords: Vec<String>) -> Self {
        if !keywords.is_empty() {
            self.exclude_keywords_any = Some(keywords);
        }
        self
    }

    /// Set the query variant label (for enrichment metadata).
    pub fn with_variant_label(mut self, label: &str) -> Self {
        self.query_variant_label = Some(label.to_string());
        self
    }

    /// Convert to a JSON Value for logging/storage in `company_fetch_queries`.
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(json!({}))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay_increases() {
        let d1 = backoff_delay(1);
        let d2 = backoff_delay(2);
        let d3 = backoff_delay(3);
        // Base: 2s, 4s, 8s (plus jitter)
        assert!(d1.as_secs() >= 2);
        assert!(d2.as_secs() >= 4);
        assert!(d3.as_secs() >= 8);
    }

    #[test]
    fn test_backoff_capped() {
        // Attempt 10: 2^9 * 2 = 1024, but capped at 60
        let d = backoff_delay(10);
        assert!(d.as_secs() <= 61); // 60 + 1s jitter
    }

    #[test]
    fn test_request_builder() {
        let req = ApolloOrgSearchRequest::new(1, 100)
            .with_industries(vec!["fintech".into()])
            .with_locations(vec!["United States".into()])
            .with_employee_ranges(vec!["51-200".into(), "201-500".into()])
            .with_keywords(vec!["SaaS".into()]);

        assert_eq!(req.page, 1);
        assert_eq!(req.per_page, 100);
        assert_eq!(
            req.organization_industry_tag_ids,
            Some(vec!["fintech".into()])
        );
        assert_eq!(
            req.organization_locations,
            Some(vec!["United States".into()])
        );
        assert_eq!(
            req.organization_num_employees_ranges,
            Some(vec!["51-200".into(), "201-500".into()])
        );
        assert_eq!(
            req.q_organization_keyword_tags,
            Some(vec!["SaaS".into()])
        );
    }

    #[test]
    fn test_request_builder_skips_empty() {
        let req = ApolloOrgSearchRequest::new(1, 100)
            .with_industries(vec![])
            .with_locations(vec![])
            .with_keywords(vec![]);

        assert!(req.organization_industry_tag_ids.is_none());
        assert!(req.organization_locations.is_none());
        assert!(req.q_organization_keyword_tags.is_none());
    }

    #[test]
    fn test_request_to_json() {
        let req = ApolloOrgSearchRequest::new(3, 50)
            .with_industries(vec!["healthcare_saas".into()]);

        let json = req.to_json_value();
        assert_eq!(json["page"], 3);
        assert_eq!(json["per_page"], 50);
        assert!(json["organization_industry_tag_ids"].is_array());
        // Skip-serialized None fields should not appear
        assert!(json.get("organization_locations").is_none());
    }

    #[test]
    fn test_per_page_capped() {
        let req = ApolloOrgSearchRequest::new(1, 999);
        assert_eq!(req.per_page, 100);
    }

    #[test]
    fn test_stop_reason_display() {
        assert_eq!(StopReason::ResultsExhausted.as_str(), "results_exhausted");
        assert_eq!(StopReason::CallbackStopped.as_str(), "callback_stopped");
    }

    #[test]
    fn test_truncate_body() {
        let short = "hello";
        assert_eq!(truncate_body(short), "hello");

        let long = "x".repeat(1000);
        let truncated = truncate_body(&long);
        assert!(truncated.contains("truncated"));
        assert!(truncated.contains("1000 bytes"));
    }

    #[test]
    fn test_deserialize_apollo_response() {
        let json = r#"{
            "organizations": [
                {
                    "id": "abc123",
                    "name": "Test Corp",
                    "primary_domain": "testcorp.com",
                    "industry": "technology",
                    "estimated_num_employees": 250,
                    "city": "San Francisco",
                    "state": "California",
                    "country": "United States"
                }
            ],
            "pagination": {
                "page": 1,
                "per_page": 100,
                "total_entries": 4200,
                "total_pages": 42
            }
        }"#;

        let resp: ApolloOrgSearchResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.organizations.len(), 1);
        assert_eq!(resp.organizations[0].id, "abc123");
        assert_eq!(resp.organizations[0].name.as_deref(), Some("Test Corp"));
        assert_eq!(resp.organizations[0].estimated_num_employees, Some(250));
        assert_eq!(resp.pagination.total_entries, 4200);
        assert_eq!(resp.pagination.total_pages, 42);
    }

    #[test]
    fn test_deserialize_apollo_response_minimal() {
        // Apollo sometimes returns sparse org objects
        let json = r#"{
            "organizations": [
                { "id": "xyz789" }
            ],
            "pagination": {}
        }"#;

        let resp: ApolloOrgSearchResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.organizations.len(), 1);
        assert_eq!(resp.organizations[0].id, "xyz789");
        assert!(resp.organizations[0].name.is_none());
        assert!(resp.organizations[0].primary_domain.is_none());
        assert_eq!(resp.pagination.page, 0); // Default
    }

    #[test]
    fn test_deserialize_apollo_error() {
        let json = r#"{ "error": "rate_limit_exceeded", "message": "Too many requests" }"#;
        let err: ApolloErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.to_string(), "Too many requests");
    }
}
