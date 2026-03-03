// src/jobs/company_fetcher/providers/mod.rs
//
// Organization Search Provider abstraction.
//
// Defines a trait that both Apollo and Diffbot implement, so the orchestrator
// can swap providers without changing any fetch logic.
//
// The trait mirrors the exact call signature the orchestrator already uses:
//   provider.search_page(&request) -> Result<(Vec<ApolloOrganization>, PageFetchMeta), ApolloApiError>

pub mod diffbot_client;
pub mod diffbot_converter;
pub mod diffbot_models;

use async_trait::async_trait;

use super::apollo_client::{ApolloApiError, ApolloClient, PageFetchMeta};
use super::models::ApolloOrgSearchRequest;
use super::models::ApolloOrganization;

pub use diffbot_converter::EnrichmentData;

// ============================================================================
// Trait
// ============================================================================

/// Provider-agnostic interface for organization search.
///
/// Both Apollo and Diffbot implement this. The orchestrator calls
/// `provider.search_page(...)` without knowing which provider is active.
#[async_trait]
pub trait OrgSearchProvider: Send + Sync {
    /// Fetch a single page of organization search results.
    ///
    /// Input: Apollo-format request (the orchestrator always builds these).
    /// Output: Apollo-format organizations + pagination metadata.
    ///
    /// Diffbot implementations translate the request to DQL internally
    /// and convert results back to Apollo shapes.
    async fn search_page(
        &self,
        request: &ApolloOrgSearchRequest,
    ) -> Result<(Vec<ApolloOrganization>, PageFetchMeta), ApolloApiError>;

    /// Drain any enrichment data accumulated during the last `search_page` call.
    ///
    /// Returns `(external_org_id, enrichment_data)` pairs.
    /// The orchestrator calls this AFTER `process_batch()` so that company rows
    /// already exist in the DB when enrichment upserts run.
    ///
    /// Default: returns empty (Apollo doesn't produce enrichment inline).
    fn drain_pending_enrichments(&self) -> Vec<(String, EnrichmentData)> {
        vec![]
    }

    /// Human-readable provider name for logging.
    fn provider_name(&self) -> &str;

    /// Check that the provider is properly configured (API key present, etc.).
    fn validate(&self) -> Result<(), ApolloApiError>;
}

// ============================================================================
// Apollo Provider (blanket impl on existing ApolloClient)
// ============================================================================

/// ApolloClient already has `search_page` with the exact signature.
/// This impl just delegates.
#[async_trait]
impl OrgSearchProvider for ApolloClient {
    async fn search_page(
        &self,
        request: &ApolloOrgSearchRequest,
    ) -> Result<(Vec<ApolloOrganization>, PageFetchMeta), ApolloApiError> {
        // Delegate to existing ApolloClient::search_page
        ApolloClient::search_page(self, request).await
    }

    fn provider_name(&self) -> &str {
        "apollo"
    }

    fn validate(&self) -> Result<(), ApolloApiError> {
        ApolloClient::validate(self)
    }
}

// ============================================================================
// Provider Enum (for config-driven dispatch)
// ============================================================================

/// Which organization search provider to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrgProviderKind {
    Apollo,
    Diffbot,
}

impl OrgProviderKind {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "diffbot" => OrgProviderKind::Diffbot,
            _ => OrgProviderKind::Apollo,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            OrgProviderKind::Apollo => "apollo",
            OrgProviderKind::Diffbot => "diffbot",
        }
    }
}
