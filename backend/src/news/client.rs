// src/news/client.rs

use crate::news::models::NewsItem;
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Request we send to the news service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsFetchRequest {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: String,
    pub industry: Option<String>,
    pub country: Option<String>,
    pub max_results: Option<u32>,
}

/// Response we expect from the news service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsFetchResponse {
    pub company_id: Uuid,
    pub news_items: Vec<NewsItem>,
}

#[async_trait]
pub trait NewsSourcingClient: Send + Sync {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, anyhow::Error>;
}

/// Convenient type alias for dyn client.
pub type DynNewsSourcingClient = Arc<dyn NewsSourcingClient>;

/// HTTP implementation that calls an external /news/fetch endpoint.
#[derive(Clone)]
pub struct HttpNewsSourcingClient {
    http: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
}

impl HttpNewsSourcingClient {
    /// Create a new client from base_url + optional API key.
    pub fn new(base_url: String, api_key: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            api_key,
        }
    }

    /// Create a client from optional config values.
    /// Returns None if base_url is missing.
    pub fn from_config(
        base_url: Option<String>,
        api_key: Option<String>,
    ) -> Option<DynNewsSourcingClient> {
        let base_url = base_url?;
        let client = HttpNewsSourcingClient::new(base_url, api_key);
        Some(Arc::new(client) as DynNewsSourcingClient)
    }
}

#[async_trait]
impl NewsSourcingClient for HttpNewsSourcingClient {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, anyhow::Error> {
        // Compose URL: {base_url}/news/fetch
        let url = format!("{}/news/fetch", self.base_url.trim_end_matches('/'));

        let mut builder = self.http.post(&url).json(req);

        // Optional bearer token header
        if let Some(ref key) = self.api_key {
            builder = builder.header("Authorization", format!("Bearer {}", key));
        }

        let resp = builder.send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "news service error: status={} body={}",
                status,
                body
            ));
        }

        let parsed: NewsFetchResponse = resp.json().await?;
        Ok(parsed.news_items)
    }
}
