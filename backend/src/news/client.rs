// src/news/client.rs

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::news::models::NewsItem;

/// Request payload we send to the news sourcing service.
///
/// This matches the JSON contract we defined for /news/fetch.
#[derive(Debug, Clone, Serialize)]
pub struct NewsFetchRequest {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: String,
    pub industry: Option<String>,
    pub country: Option<String>,
    /// Maximum number of normalized news items we want back.
    pub max_results: Option<u32>,
}

/// Response shape we expect from the news sourcing service.
///
/// { "company_id": "...", "news_items": [ ... ] }
#[derive(Debug, Clone, Deserialize)]
pub struct NewsFetchResponse {
    pub company_id: Uuid,
    pub news_items: Vec<NewsItem>,
}

/// Trait that abstracts over "something that can fetch news for a company".
///
/// This lets you:
/// - use an HTTP service (Lambda + API Gateway) today,
/// - swap to another provider or local service later,
/// - without changing worker or DB code.
#[async_trait]
pub trait NewsSourcingClient: Send + Sync {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>>;
}

/// Convenience type alias for a trait object we can share across tasks.
pub type DynNewsSourcingClient = Arc<dyn NewsSourcingClient + Send + Sync>;

/// HTTP-based implementation of NewsSourcingClient.
///
/// It calls an external service with POST /news/fetch.
#[derive(Clone)]
pub struct HttpNewsSourcingClient {
    http: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
}

impl HttpNewsSourcingClient {
    /// Build the client from environment variables.
    ///
    /// Required:
    /// - NEWS_SERVICE_BASE_URL (e.g. "https://news-service.my-domain.com")
    ///
    /// Optional:
    /// - NEWS_SERVICE_API_KEY (sent as `x-api-key` header)
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("NEWS_SERVICE_BASE_URL")
            .map_err(|_| anyhow!("NEWS_SERVICE_BASE_URL env var is required for HttpNewsSourcingClient"))?;

        let api_key = std::env::var("NEWS_SERVICE_API_KEY").ok();

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            http,
            base_url,
            api_key,
        })
    }

    /// Helper to wrap this client into a shared trait object.
    pub fn into_dyn(self) -> DynNewsSourcingClient {
        Arc::new(self)
    }
}

#[async_trait]
impl NewsSourcingClient for HttpNewsSourcingClient {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>> {
        // Build URL: "<base>/news/fetch"
        let url = format!("{}/news/fetch", self.base_url.trim_end_matches('/'));

        let mut builder = self.http.post(&url).json(req);

        // Optional API key header
        if let Some(ref key) = self.api_key {
            builder = builder.header("x-api-key", key);
        }

        let resp = builder.send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            // 404 / 400 / 500 etc.
            return Err(anyhow!(
                "news service returned non-success status {}: {}",
                status,
                body
            ));
        }

        // Try to parse the JSON into our response struct
        let parsed: NewsFetchResponse = resp.json().await?;

        // (Optional) basic sanity check
        if parsed.company_id != req.company_id {
            // Not fatal, but suspicious - log it if you want
            tracing::warn!(
                "news service returned company_id {}, but request was for {}",
                parsed.company_id,
                req.company_id
            );
        }

        Ok(parsed.news_items)
    }
}
