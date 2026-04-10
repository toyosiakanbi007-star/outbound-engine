// src/ai/mod.rs

use std::sync::Arc;

use async_trait::async_trait;

use crate::config::Config;
use crate::news::models::NewsItem;

/// Dynamic LLM client trait object.
pub type DynLlmClient = Arc<dyn LlmClient>;

/// Abstraction for any LLM provider we might use (Bedrock, OpenAI, etc.)
#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Generate web search queries for a given company.
    ///
    /// These are used to search for news / signals.
    async fn generate_search_queries(
        &self,
        company_name: &str,
        domain: &str,
        industry: Option<&str>,
        location: Option<&str>,
    ) -> anyhow::Result<Vec<String>>;

    /// Given a page of text, extract structured news items.
    ///
    /// In a real implementation, this would run a "page -> news JSON" prompt.
    async fn extract_news_from_page(
        &self,
        company_name: &str,
        domain: &str,
        url: &str,
        page_text: &str,
    ) -> anyhow::Result<Vec<NewsItem>>;
}

/// Simple dummy implementation.
///
/// This lets us wire the rest of the pipeline without calling a real LLM yet.
pub struct DummyLlmClient;

impl DummyLlmClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl LlmClient for DummyLlmClient {
    async fn generate_search_queries(
        &self,
        company_name: &str,
        domain: &str,
        _industry: Option<&str>,
        _location: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
        // Very basic, but enough to test end-to-end flow.
        let queries = vec![
            format!("{company_name} {domain} funding"),
            format!("{company_name} {domain} product launch"),
            format!("{company_name} {domain} partnership"),
            format!("{company_name} {domain} hiring"),
        ];

        Ok(queries)
    }

    async fn extract_news_from_page(
        &self,
        _company_name: &str,
        _domain: &str,
        _url: &str,
        _page_text: &str,
    ) -> anyhow::Result<Vec<NewsItem>> {
        // For now, we don't extract anything.
        // When we plug in Bedrock, this will:
        //  - Truncate / clean the page text
        //  - Run the "extract news" prompt
        //  - Parse JSON into Vec<NewsItem>
        Ok(Vec::new())
    }
}

/// Factory function to build an LLM client from config.
///
/// For now:
/// - if LLM_PROVIDER is "dummy" or unset → DummyLlmClient
/// - later we can add "bedrock", "openai", etc.
pub fn build_llm_client(cfg: &Config) -> DynLlmClient {
    match cfg.llm_provider.as_deref() {
        Some("dummy") | None => Arc::new(DummyLlmClient::new()),
        // TODO: add `Some("bedrock") => Arc::new(BedrockLlmClient::from_config(cfg)?)` etc.
        Some(_other) => Arc::new(DummyLlmClient::new()),
    }
}
