// src/jobs/company_fetcher/providers/diffbot_client.rs
//
// Diffbot Knowledge Graph API client.
//
// RESPONSIBILITIES:
// - Translate Apollo-format search requests into Diffbot DQL queries
// - Execute HTTP calls against the KG DQL endpoint with retry/backoff
// - Parse Diffbot JSON into DiffbotKGResponse structs
// - Convert results to Apollo-compatible organization structs
// - Cache enrichment data for post-dedup writing by the orchestrator
//
// DQL ENDPOINT:
//   GET https://kg.diffbot.com/kg/v3/dql
//     ?type=query
//     &token=<token>
//     &query=<DQL>
//     &from=<offset>
//     &size=<limit>
//
// DQL QUERY BUILDING:
//   Apollo request fields map to Diffbot DQL clauses:
//     organization_locations[]      → or(location.country.name:"us", ...)
//     organization_not_locations[]  → not(or(location.country.name:"cn", ...))
//     organization_industry_tag_ids → categories.name:"Software Companies"
//     organization_num_employees_ranges → nbEmployees>=51 nbEmployees<=200
//     page/per_page                → from=(page-1)*per_page, size=per_page

use async_trait::async_trait;
use reqwest::Client as HttpClient;
use serde_json;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::diffbot_converter::{self, EnrichmentData};
use super::diffbot_models::DiffbotKGResponse;
use super::OrgSearchProvider;
use crate::jobs::company_fetcher::apollo_client::{ApolloApiError, PageFetchMeta};
use crate::jobs::company_fetcher::models::{ApolloOrgSearchRequest, ApolloOrganization};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_DIFFBOT_BASE_URL: &str = "https://kg.diffbot.com/kg/v3/dql";
const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 1000;

// ============================================================================
// Diffbot Client
// ============================================================================

/// HTTP client for the Diffbot Knowledge Graph API.
///
/// Translates Apollo-format requests into DQL queries, fetches results,
/// and converts them back to Apollo-compatible response shapes.
///
/// Enrichment data is cached internally during `search_page()` and must be
/// drained by the caller via `drain_pending_enrichments()` AFTER company
/// rows have been created by the dedup/upsert pipeline.
pub struct DiffbotClient {
    http: HttpClient,
    token: String,
    base_url: String,
    /// Enrichment data accumulated during the last `search_page()` call.
    /// Drained by the orchestrator after `process_batch()` creates company rows.
    pending_enrichments: Mutex<Vec<(String, EnrichmentData)>>,
}

impl DiffbotClient {
    /// Create a new Diffbot KG client.
    pub fn new(
        http: HttpClient,
        token: String,
        base_url: Option<String>,
    ) -> Self {
        Self {
            http,
            token,
            base_url: base_url.unwrap_or_else(|| DEFAULT_DIFFBOT_BASE_URL.to_string()),
            pending_enrichments: Mutex::new(Vec::new()),
        }
    }

    // ========================================================================
    // DQL Query Builder
    // ========================================================================

    /// Build a DQL query string from an Apollo-format request.
    ///
    /// Maps Apollo filters to DQL clauses, including per-industry keyword
    /// constraints using `description:"..."` and `name:"..."` fields.
    ///
    /// DQL template:
    ///   type:Organization
    ///   categories.name:"{diffbot_category}"
    ///   or(location.country.name:"{country1}", location.country.name:"{country2}")
    ///   not(or(location.country.name:"{excl1}", ...))
    ///   nbEmployees>{min} nbEmployees<{max}
    ///   {include_all: description:"must1" description:"must2"}
    ///   ( {include_any: or(description:"kw1", name:"kw1", description:"kw2", ...)} )
    ///   NOT ( {exclude: not(or(description:"bad1", name:"bad1", ...)) )
    ///   not(isDissolved:true)
    fn build_dql_query(request: &ApolloOrgSearchRequest) -> String {
        let mut clauses: Vec<String> = vec!["type:Organization".to_string()];

        // Industry/category filter
        if let Some(ref tags) = request.organization_industry_tag_ids {
            for tag in tags {
                clauses.push(format!("categories.name:\"{}\"", escape_dql(tag)));
            }
        }

        // ---- FIX #1: Location filter — use or() for multiple locations ----
        // Previously each location was a separate AND clause, meaning
        // "United States" AND "Germany" → 0 results (company can't be in both).
        // Now we wrap multiple locations in or() for correct OR semantics.
        if let Some(ref locations) = request.organization_locations {
            if locations.len() == 1 {
                // Single location: simple clause (no or() wrapper needed)
                clauses.push(format!(
                    "location.country.name:\"{}\"",
                    escape_dql(&locations[0].to_lowercase())
                ));
            } else if locations.len() > 1 {
                // Multiple locations: wrap in or() for correct OR semantics
                let loc_parts: Vec<String> = locations
                    .iter()
                    .map(|loc| {
                        format!(
                            "location.country.name:\"{}\"",
                            escape_dql(&loc.to_lowercase())
                        )
                    })
                    .collect();
                clauses.push(format!("or({})", loc_parts.join(", ")));
            }
        }

        // ---- FIX #2: Excluded locations — was completely missing ----
        // organization_not_locations was populated by the VariantBuilder but
        // never consumed by the DQL builder. Now we emit not(or(...)) for them.
        if let Some(ref not_locations) = request.organization_not_locations {
            if !not_locations.is_empty() {
                if not_locations.len() == 1 {
                    clauses.push(format!(
                        "not(location.country.name:\"{}\")",
                        escape_dql(&not_locations[0].to_lowercase())
                    ));
                } else {
                    let not_parts: Vec<String> = not_locations
                        .iter()
                        .map(|loc| {
                            format!(
                                "location.country.name:\"{}\"",
                                escape_dql(&loc.to_lowercase())
                            )
                        })
                        .collect();
                    clauses.push(format!("not(or({}))", not_parts.join(", ")));
                }
            }
        }

        // Employee range filter
        if let Some(ref ranges) = request.organization_num_employees_ranges {
            let (global_min, global_max) = parse_employee_ranges(ranges);
            if let Some(min) = global_min {
                clauses.push(format!("nbEmployees>{}", min));
            }
            if let Some(max) = global_max {
                clauses.push(format!("nbEmployees<{}", max));
            }
        }

        // Revenue range
        if let Some(ref rev) = request.revenue_range {
            if let Some(min) = rev.min {
                clauses.push(format!("revenue.value>={}", min));
            }
            if let Some(max) = rev.max {
                clauses.push(format!("revenue.value<={}", max));
            }
        }

        // --- Per-industry keyword constraints ---

        // include_keywords_all: AND semantics — each keyword is a separate clause
        // (DQL treats concatenated clauses as AND)
        if let Some(ref all_kws) = request.include_keywords_all {
            for kw in all_kws {
                clauses.push(format!("description:\"{}\"", escape_dql(kw)));
            }
        }

        // include_keywords_any: OR semantics — grouped with description + name fallback
        // DQL uses or(clause, clause, ...) function syntax, NOT parenthetical grouping.
        if let Some(ref any_kws) = request.include_keywords_any {
            if !any_kws.is_empty() {
                let or_parts: Vec<String> = any_kws
                    .iter()
                    .flat_map(|kw| {
                        let escaped = escape_dql(kw);
                        vec![
                            format!("description:\"{}\"", escaped),
                            format!("name:\"{}\"", escaped),
                        ]
                    })
                    .collect();
                clauses.push(format!("or({})", or_parts.join(", ")));
            }
        }

        // exclude_keywords_any: NOT OR group — none should appear
        // DQL: not(or(clause, clause, ...))
        if let Some(ref exc_kws) = request.exclude_keywords_any {
            if !exc_kws.is_empty() {
                let or_parts: Vec<String> = exc_kws
                    .iter()
                    .flat_map(|kw| {
                        let escaped = escape_dql(kw);
                        vec![
                            format!("description:\"{}\"", escaped),
                            format!("name:\"{}\"", escaped),
                        ]
                    })
                    .collect();
                clauses.push(format!("not(or({}))", or_parts.join(", ")));
            }
        }

        // Legacy keyword search (old-style ICP `keywords` field)
        if let Some(ref keywords) = request.q_organization_keyword_tags {
            for kw in keywords {
                clauses.push(format!("descriptors:\"{}\"", escape_dql(kw)));
            }
        }

        // Organization name search
        if let Some(ref name) = request.q_organization_name {
            clauses.push(format!("name:\"{}\"", escape_dql(name)));
        }

        // Filter out dissolved companies
        clauses.push("not(isDissolved:true)".to_string());

        clauses.join(" ")
    }

    /// Compute Diffbot `from` offset from Apollo page/per_page.
    fn compute_offset(request: &ApolloOrgSearchRequest) -> i32 {
        let page = request.page.max(1);
        let per_page = request.per_page.max(1);
        (page - 1) * per_page
    }

    // ========================================================================
    // HTTP Execution
    // ========================================================================

    /// Execute a DQL query with retry/backoff.
    async fn execute_query(
        &self,
        dql: &str,
        from: i32,
        size: i32,
    ) -> Result<DiffbotKGResponse, ApolloApiError> {
        let mut retries: u32 = 0;

        loop {
            let result = self
                .http
                .get(&self.base_url)
                .query(&[
                    ("type", "query"),
                    ("token", &self.token),
                    ("query", dql),
                    ("from", &from.to_string()),
                    ("size", &size.to_string()),
                ])
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        let body = response.text().await.map_err(|e| {
                            ApolloApiError::ParseError(format!(
                                "Failed to read Diffbot response body: {}",
                                e
                            ))
                        })?;

                        let parsed: DiffbotKGResponse =
                            serde_json::from_str(&body).map_err(|e| {
                                ApolloApiError::ParseError(format!(
                                    "Failed to parse Diffbot JSON: {} (body: {}...)",
                                    e,
                                    &body[..body.len().min(200)]
                                ))
                            })?;

                        // Check for API-level errors in the response
                        if let Some(ref err) = parsed.error {
                            return Err(ApolloApiError::ClientError {
                                status: parsed.error_code.unwrap_or(400) as u16,
                                body: err.clone(),
                            });
                        }

                        return Ok(parsed);
                    }

                    let status_code = status.as_u16();
                    let body = response.text().await.unwrap_or_default();

                    if status_code == 429 {
                        retries += 1;
                        if retries > MAX_RETRIES {
                            return Err(ApolloApiError::RetriesExhausted {
                                max_retries: MAX_RETRIES,
                                last_error: "Diffbot 429 rate limited".to_string(),
                            });
                        }
                        let delay = backoff_delay(retries);
                        warn!(
                            "Diffbot 429 rate limited. Retry {}/{} after {:.1}s",
                            retries, MAX_RETRIES, delay.as_secs_f64()
                        );
                        sleep(delay).await;
                        continue;
                    }

                    if status_code >= 500 {
                        retries += 1;
                        if retries > MAX_RETRIES {
                            return Err(ApolloApiError::RetriesExhausted {
                                max_retries: MAX_RETRIES,
                                last_error: format!("Diffbot {} — {}", status_code, body),
                            });
                        }
                        let delay = backoff_delay(retries);
                        warn!(
                            "Diffbot {} server error. Retry {}/{} after {:.1}s",
                            status_code, retries, MAX_RETRIES, delay.as_secs_f64()
                        );
                        sleep(delay).await;
                        continue;
                    }

                    // 4xx (not 429)
                    return Err(ApolloApiError::ClientError {
                        status: status_code,
                        body: format!("Diffbot error: {}", body),
                    });
                }
                Err(e) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(ApolloApiError::RetriesExhausted {
                            max_retries: MAX_RETRIES,
                            last_error: format!("Diffbot HTTP error: {}", e),
                        });
                    }
                    let delay = backoff_delay(retries);
                    warn!(
                        "Diffbot HTTP error. Retry {}/{} after {:.1}s: {}",
                        retries, MAX_RETRIES, delay.as_secs_f64(), e
                    );
                    sleep(delay).await;
                }
            }
        }
    }

}

// ============================================================================
// OrgSearchProvider Implementation
// ============================================================================

#[async_trait]
impl OrgSearchProvider for DiffbotClient {
    async fn search_page(
        &self,
        request: &ApolloOrgSearchRequest,
    ) -> Result<(Vec<ApolloOrganization>, PageFetchMeta), ApolloApiError> {
        let start = std::time::Instant::now();

        // Build DQL query
        let dql = Self::build_dql_query(request);
        let from = Self::compute_offset(request);
        let size = request.per_page;

        debug!(
            "Diffbot DQL: query='{}', from={}, size={}",
            dql, from, size
        );

        // Execute
        let response = self.execute_query(&dql, from, size).await?;

        // Extract entities
        let entities: Vec<super::diffbot_models::DiffbotEntity> = response
            .data
            .into_iter()
            .filter_map(|w| w.entity)
            .collect();

        let orgs_returned = entities.len();

        // Convert to Apollo format
        let apollo_orgs: Vec<ApolloOrganization> = entities
            .iter()
            .map(diffbot_converter::to_apollo_organization)
            .collect();

        // Compute pagination metadata
        let total_entries = response.hits;
        let total_pages = if size > 0 {
            ((total_entries as f64) / (size as f64)).ceil() as i32
        } else {
            1
        };

        let meta = PageFetchMeta {
            page: request.page,
            orgs_returned,
            total_entries,
            total_pages,
            retries: 0,
            duration_ms: start.elapsed().as_millis() as u64,
        };

        info!(
            "Diffbot returned {} entities (total_hits={}, page={}/{})",
            orgs_returned, total_entries, request.page, total_pages
        );

        // Cache enrichment data for each entity.
        // The orchestrator will drain this AFTER process_batch() creates company rows.
        {
            // Build match metadata that applies to all results from this query
            let variant_label = request.query_variant_label
                .as_deref()
                .unwrap_or("unknown");

            let industry_name = request.organization_industry_tag_ids
                .as_ref()
                .and_then(|tags| tags.first())
                .cloned()
                .unwrap_or_default();

            let include_any = request.include_keywords_any
                .as_ref()
                .cloned()
                .unwrap_or_default();

            let include_all = request.include_keywords_all
                .as_ref()
                .cloned()
                .unwrap_or_default();

            let exclude_any = request.exclude_keywords_any
                .as_ref()
                .cloned()
                .unwrap_or_default();

            let base_metadata = serde_json::json!({
                "provider": "diffbot",
                "matched_industry_name": industry_name,
                "matched_diffbot_category": industry_name,
                "query_variant": variant_label,
                "dql_query": dql,
                "include_keywords_any": include_any,
                "include_keywords_all": include_all,
                "exclude_keywords_any": exclude_any,
            });

            let enrichments: Vec<(String, EnrichmentData)> = entities
                .iter()
                .map(|entity| {
                    let external_id = entity.entity_id();
                    let mut data = diffbot_converter::build_enrichment_data(entity);
                    data.match_metadata = base_metadata.clone();
                    (external_id, data)
                })
                .collect();

            if let Ok(mut buffer) = self.pending_enrichments.lock() {
                buffer.clear();
                buffer.extend(enrichments);
                debug!("Cached {} enrichment records for post-dedup write", buffer.len());
            } else {
                warn!("Failed to lock enrichment buffer — enrichments will be skipped this page");
            }
        }

        Ok((apollo_orgs, meta))
    }

    /// Drain enrichment data accumulated during the last `search_page()`.
    fn drain_pending_enrichments(&self) -> Vec<(String, EnrichmentData)> {
        match self.pending_enrichments.lock() {
            Ok(mut buffer) => std::mem::take(&mut *buffer),
            Err(e) => {
                warn!("Failed to drain enrichment buffer: {}", e);
                vec![]
            }
        }
    }

    fn provider_name(&self) -> &str {
        "diffbot"
    }

    fn validate(&self) -> Result<(), ApolloApiError> {
        if self.token.is_empty() {
            return Err(ApolloApiError::MissingApiKey);
        }
        Ok(())
    }
}

// ============================================================================
// DQL Helpers
// ============================================================================

/// Escape special characters in DQL values.
fn escape_dql(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Parse Apollo employee range strings like "51-200" into (min, max).
///
/// When multiple ranges are provided, we take the global min and max
/// to form a single Diffbot range (since DQL doesn't support OR ranges natively).
fn parse_employee_ranges(ranges: &[String]) -> (Option<i32>, Option<i32>) {
    let mut global_min: Option<i32> = None;
    let mut global_max: Option<i32> = None;

    for range in ranges {
        let range = range.trim();

        if range.ends_with('+') {
            // e.g. "10001+"
            if let Ok(min) = range.trim_end_matches('+').parse::<i32>() {
                global_min = Some(global_min.map_or(min, |cur: i32| cur.min(min)));
            }
            continue;
        }

        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(min), Ok(max)) = (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
                global_min = Some(global_min.map_or(min, |cur: i32| cur.min(min)));
                global_max = Some(global_max.map_or(max, |cur: i32| cur.max(max)));
            }
        }
    }

    (global_min, global_max)
}

/// Exponential backoff delay.
fn backoff_delay(retry: u32) -> Duration {
    let base = INITIAL_BACKOFF_MS * 2u64.pow(retry.saturating_sub(1));
    // Add jitter: ±25%
    let jitter = (base as f64 * 0.25 * (rand_simple() * 2.0 - 1.0)) as i64;
    let ms = (base as i64 + jitter).max(500) as u64;
    Duration::from_millis(ms)
}

/// Simple deterministic-ish random for jitter (no external crate needed).
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::company_fetcher::models::ApolloRevenueRange;

    fn default_request() -> ApolloOrgSearchRequest {
        ApolloOrgSearchRequest {
            page: 1,
            per_page: 25,
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

    #[test]
    fn test_build_dql_basic() {
        let request = ApolloOrgSearchRequest {
            organization_locations: Some(vec!["United States".to_string()]),
            organization_num_employees_ranges: Some(vec!["51-200".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(dql.contains("type:Organization"));
        assert!(dql.contains("location.country.name:\"united states\""));
        assert!(dql.contains("nbEmployees>"));
        assert!(dql.contains("not(isDissolved:true)"));
        // Single location should NOT be wrapped in or()
        assert!(!dql.contains("or(location.country.name"));
    }

    // ---- FIX #1 TEST: Multiple locations use OR, not AND ----

    #[test]
    fn test_build_dql_multiple_locations_use_or() {
        let request = ApolloOrgSearchRequest {
            organization_locations: Some(vec![
                "United States".to_string(),
                "Germany".to_string(),
                "France".to_string(),
            ]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);

        // Multiple locations MUST be wrapped in or() — NOT separate AND clauses
        assert!(
            dql.contains("or(location.country.name:\"united states\", location.country.name:\"germany\", location.country.name:\"france\")"),
            "Expected or() wrapper for multiple locations, got: {}",
            dql
        );
    }

    #[test]
    fn test_build_dql_single_location_no_or_wrapper() {
        let request = ApolloOrgSearchRequest {
            organization_locations: Some(vec!["Canada".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(dql.contains("location.country.name:\"canada\""));
        assert!(!dql.contains("or(location.country.name"));
    }

    // ---- FIX #2 TEST: Excluded locations ----

    #[test]
    fn test_build_dql_excluded_locations_single() {
        let request = ApolloOrgSearchRequest {
            organization_locations: Some(vec!["United States".to_string()]),
            organization_not_locations: Some(vec!["China".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(
            dql.contains("not(location.country.name:\"china\")"),
            "Expected excluded location, got: {}",
            dql
        );
    }

    #[test]
    fn test_build_dql_excluded_locations_multiple() {
        let request = ApolloOrgSearchRequest {
            organization_not_locations: Some(vec![
                "China".to_string(),
                "Russia".to_string(),
            ]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(
            dql.contains("not(or(location.country.name:\"china\", location.country.name:\"russia\"))"),
            "Expected not(or(...)) for excluded locations, got: {}",
            dql
        );
    }

    // ---- Existing tests (unchanged) ----

    #[test]
    fn test_build_dql_with_industry_and_keywords() {
        let request = ApolloOrgSearchRequest {
            organization_industry_tag_ids: Some(vec!["Software Companies".to_string()]),
            q_organization_keyword_tags: Some(vec!["SaaS".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(dql.contains("categories.name:\"Software Companies\""));
        assert!(dql.contains("descriptors:\"SaaS\""));
    }

    #[test]
    fn test_build_dql_with_revenue() {
        let request = ApolloOrgSearchRequest {
            revenue_range: Some(ApolloRevenueRange {
                min: Some(1_000_000),
                max: Some(50_000_000),
            }),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        assert!(dql.contains("revenue.value>=1000000"));
        assert!(dql.contains("revenue.value<=50000000"));
    }

    #[test]
    fn test_build_dql_with_include_keywords_any() {
        let request = ApolloOrgSearchRequest {
            organization_industry_tag_ids: Some(vec!["Currency And Lending Services".to_string()]),
            include_keywords_any: Some(vec![
                "loan".to_string(),
                "lending".to_string(),
                "credit".to_string(),
            ]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        // Should produce an or() group with description + name fallback
        assert!(dql.contains("description:\"loan\""));
        assert!(dql.contains("name:\"loan\""));
        assert!(dql.contains("or("));
    }

    #[test]
    fn test_build_dql_with_include_keywords_all() {
        let request = ApolloOrgSearchRequest {
            include_keywords_all: Some(vec!["construction".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        // AND semantics: each keyword is a separate top-level clause
        assert!(dql.contains("description:\"construction\""));
        // Should NOT be inside an or() group — it's a standalone AND clause
        assert!(!dql.contains("or(description:\"construction\""));
    }

    #[test]
    fn test_build_dql_with_exclude_keywords() {
        let request = ApolloOrgSearchRequest {
            exclude_keywords_any: Some(vec![
                "residential".to_string(),
                "roofing".to_string(),
            ]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);
        // Should produce a not(or(...)) group
        assert!(dql.contains("not(or("));
        assert!(dql.contains("description:\"residential\""));
        assert!(dql.contains("name:\"roofing\""));
    }

    #[test]
    fn test_build_dql_full_industry_spec() {
        // Simulates a "tight" variant with all keyword constraints
        let request = ApolloOrgSearchRequest {
            organization_industry_tag_ids: Some(vec![
                "Infrastructure Construction Companies".to_string(),
            ]),
            organization_locations: Some(vec!["United States".to_string()]),
            organization_num_employees_ranges: Some(vec!["51-200".to_string()]),
            include_keywords_all: Some(vec!["construction".to_string()]),
            include_keywords_any: Some(vec![
                "contract".to_string(),
                "design".to_string(),
                "build".to_string(),
            ]),
            exclude_keywords_any: Some(vec![
                "residential".to_string(),
                "home renovation".to_string(),
            ]),
            query_variant_label: Some("tight".to_string()),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);

        // Base clauses
        assert!(dql.contains("type:Organization"));
        assert!(dql.contains("categories.name:\"Infrastructure Construction Companies\""));
        assert!(dql.contains("location.country.name:\"united states\""));

        // include_all (AND): top-level clauses
        assert!(dql.contains("description:\"construction\""));

        // include_any (OR): grouped with or() function
        assert!(dql.contains("or(description:\"contract\""));
        assert!(dql.contains("name:\"design\""));

        // exclude (NOT): grouped with not(or())
        assert!(dql.contains("not(or("));
        assert!(dql.contains("description:\"residential\""));
        assert!(dql.contains("description:\"home renovation\""));
    }

    // ---- FIX #1+#2 COMBINED TEST: V2 expansion with excluded locations ----

    #[test]
    fn test_build_dql_v2_expansion_with_exclusions() {
        let request = ApolloOrgSearchRequest {
            organization_industry_tag_ids: Some(vec!["Software Companies".to_string()]),
            organization_locations: Some(vec![
                "United States".to_string(),
                "Germany".to_string(),
                "France".to_string(),
            ]),
            organization_not_locations: Some(vec!["China".to_string()]),
            organization_num_employees_ranges: Some(vec!["51-200".to_string()]),
            ..default_request()
        };

        let dql = DiffbotClient::build_dql_query(&request);

        // Locations should be OR'd
        assert!(dql.contains("or(location.country.name:\"united states\""));
        assert!(dql.contains("location.country.name:\"germany\""));
        assert!(dql.contains("location.country.name:\"france\""));

        // Excluded location should be NOT'd
        assert!(dql.contains("not(location.country.name:\"china\")"));
    }

    #[test]
    fn test_compute_offset() {
        let mut req = ApolloOrgSearchRequest {
            page: 1,
            per_page: 25,
            ..default_request()
        };
        assert_eq!(DiffbotClient::compute_offset(&req), 0);

        req.page = 2;
        assert_eq!(DiffbotClient::compute_offset(&req), 25);

        req.page = 5;
        req.per_page = 100;
        assert_eq!(DiffbotClient::compute_offset(&req), 400);
    }

    #[test]
    fn test_parse_employee_ranges_single() {
        let ranges = vec!["51-200".to_string()];
        let (min, max) = parse_employee_ranges(&ranges);
        assert_eq!(min, Some(51));
        assert_eq!(max, Some(200));
    }

    #[test]
    fn test_parse_employee_ranges_multiple() {
        let ranges = vec![
            "51-200".to_string(),
            "201-500".to_string(),
            "501-1000".to_string(),
        ];
        let (min, max) = parse_employee_ranges(&ranges);
        assert_eq!(min, Some(51));
        assert_eq!(max, Some(1000));
    }

    #[test]
    fn test_parse_employee_ranges_open_ended() {
        let ranges = vec!["10001+".to_string()];
        let (min, max) = parse_employee_ranges(&ranges);
        assert_eq!(min, Some(10001));
        assert_eq!(max, None);
    }

    #[test]
    fn test_escape_dql() {
        assert_eq!(escape_dql("hello"), "hello");
        assert_eq!(escape_dql("O'Brien"), "O'Brien"); // single quotes OK
        assert_eq!(escape_dql("say \"hi\""), "say \\\"hi\\\"");
    }
}
