// src/news/client.rs

use crate::config::Config;
use crate::news::models::NewsItem;

use async_trait::async_trait;
use aws_config;
use aws_sdk_lambda::{primitives::Blob, Client as LambdaClient};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

/// Request we send to the news service Lambda (matches what we POST from Rust).
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

/// High-level response type used by HTTP/mock endpoints, etc.
/// This is what `routes/news.rs` imports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsFetchResponse {
    pub company_id: Uuid,
    pub news_items: Vec<NewsItem>,
}

/// Raw item as returned by Lambda JSON (wire format).
/// NOTE: `date` is a String here to match the Lambda payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNewsItem {
    pub company_id: Uuid,
    pub title: String,
    pub date: Option<String>,
    pub location: Option<String>,
    pub summary: Option<String>,
    pub url: String,
    pub source_type: String,
    pub source_name: Option<String>,
    pub tags: Vec<String>,
    pub confidence: f32,
}

/// Raw response shape from Lambda.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNewsFetchResponse {
    pub company_id: Uuid,
    pub news_items: Vec<RawNewsItem>,
}

#[derive(Debug, Error)]
pub enum NewsError {
    #[error("lambda error: {0}")]
    Lambda(String),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[async_trait]
pub trait NewsSourcingClient: Send + Sync {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, NewsError>;
}

/// Convenient type alias for dyn client.
pub type DynNewsSourcingClient = Arc<dyn NewsSourcingClient>;

/// Lambda-based implementation that calls the news-service Lambda directly.
#[derive(Clone)]
pub struct LambdaNewsSourcingClient {
    client: LambdaClient,
    function_name: String,
}

impl LambdaNewsSourcingClient {
    pub fn new(client: LambdaClient, function_name: String) -> Self {
        Self {
            client,
            function_name,
        }
    }
}

/// No-op client used when Lambda is not configured (returns zero news).
#[derive(Clone)]
pub struct NoopNewsSourcingClient;

impl NoopNewsSourcingClient {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NewsSourcingClient for NoopNewsSourcingClient {
    async fn fetch_news_for_company(
        &self,
        _req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, NewsError> {
        Ok(vec![])
    }
}

/// Build the news client from config + environment.
///
/// If `NEWS_LAMBDA_FUNCTION_NAME` is set -> use Lambda client  
/// Otherwise -> use Noop client (worker will log “no news” and continue)
pub async fn build_news_client(_cfg: &Config) -> Result<DynNewsSourcingClient, NewsError> {
    if let Ok(function_name) = std::env::var("NEWS_LAMBDA_FUNCTION_NAME") {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        info!(
            "Initializing LambdaNewsSourcingClient for function={} in region={}",
            function_name, region
        );

        let conf = aws_config::load_from_env().await;
        let client = LambdaClient::new(&conf);
        let lambda_client = LambdaNewsSourcingClient::new(client, function_name);
        Ok(Arc::new(lambda_client) as DynNewsSourcingClient)
    } else {
        warn!("NEWS_LAMBDA_FUNCTION_NAME not set; using NoopNewsSourcingClient (no news will be fetched)");
        Ok(Arc::new(NoopNewsSourcingClient::new()) as DynNewsSourcingClient)
    }
}

#[async_trait]
impl NewsSourcingClient for LambdaNewsSourcingClient {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, NewsError> {
        // 1) Serialize request to JSON bytes
        let payload_bytes = serde_json::to_vec(req)?;

        // 2) Invoke Lambda
        let resp = self
            .client
            .invoke()
            .function_name(&self.function_name)
            .payload(Blob::new(payload_bytes))
            .send()
            .await
            .map_err(|e| NewsError::Lambda(format!("{e:?}")))?;

        // 3) Extract raw payload bytes
        let raw_bytes = resp
            .payload
            .map(|b| b.into_inner())
            .unwrap_or_default();

        if raw_bytes.is_empty() {
            warn!("news lambda returned empty payload");
            return Ok(vec![]);
        }

        let raw_str = String::from_utf8(raw_bytes.clone())?;
        info!(
            target: "backend::news::client",
            "news lambda raw payload: {}",
            raw_str
        );

        // Helper: map RawNewsFetchResponse -> Vec<NewsItem>
        fn map_raw_response(raw: RawNewsFetchResponse) -> Vec<NewsItem> {
            raw.news_items
                .into_iter()
                .map(|r| RawNewsItem::into_news_item(r))
                .collect()
        }

        // ---- Primary parse path: direct raw response ----
        if let Ok(parsed) = serde_json::from_slice::<RawNewsFetchResponse>(&raw_bytes) {
            return Ok(map_raw_response(parsed));
        }

        warn!(
            target: "backend::news::client",
            "failed to parse lambda payload as RawNewsFetchResponse directly; trying wrapper/body fallbacks"
        );

        // ---- Fallback 1: parse as generic Value, handle wrappers ----
        if let Ok(v) = serde_json::from_str::<Value>(&raw_str) {
            // Case 1: already looks like { company_id, news_items }
            if v.get("company_id").is_some() && v.get("news_items").is_some() {
                if let Ok(parsed) = serde_json::from_value::<RawNewsFetchResponse>(v.clone()) {
                    return Ok(map_raw_response(parsed));
                }
            }

            // Case 2: API Gateway style: { statusCode, body: "<json string>" }
            if let Some(body) = v.get("body") {
                if let Some(body_str) = body.as_str() {
                    info!(
                        target: "backend::news::client",
                        "news lambda wrapper body string: {}",
                        body_str
                    );
                    if let Ok(parsed) = serde_json::from_str::<RawNewsFetchResponse>(body_str) {
                        return Ok(map_raw_response(parsed));
                    }
                }
            }
        }

        // ---- Fallback 2: trim at last closing brace (in case of trailing junk) ----
        if let Some(pos) = raw_str.rfind('}') {
            let trimmed = &raw_str[..=pos];
            if let Ok(parsed) = serde_json::from_str::<RawNewsFetchResponse>(trimmed) {
                warn!(
                    target: "backend::news::client",
                    "parsed lambda payload successfully after trimming trailing data"
                );
                return Ok(map_raw_response(parsed));
            }
        }

        // ---- If everything fails, log and treat as no items ----
        warn!(
            target: "backend::news::client",
            "all attempts to parse lambda payload failed; treating as 0 items"
        );

        Ok(vec![])
    }
}

/// Helper: convert wire/raw item into internal NewsItem, with flexible date parsing.
impl RawNewsItem {
    fn into_news_item(self) -> NewsItem {
        let parsed_date = self
            .date
            .as_ref()
            .and_then(|s| parse_flexible_date(s));

        NewsItem {
            company_id: self.company_id,
            title: self.title,
            date: parsed_date,       // Option<DateTime<Utc>>
            location: self.location, // Option<String>
            summary: self.summary,   // Option<String>
            url: self.url,
            source_type: self.source_type,
            source_name: self.source_name,
            tags: self.tags,
            confidence: self.confidence,
        }
    }
}

/// Try to parse either "YYYY-MM-DD" or full ISO8601.
/// If it fails, returns None instead of blowing up.
fn parse_flexible_date(s: &str) -> Option<DateTime<Utc>> {
    // Try full RFC3339/ISO8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try plain date "YYYY-MM-DD"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(naive_dt) = d.and_hms_opt(0, 0, 0) {
            return Some(DateTime::<Utc>::from_utc(naive_dt, Utc));
        }
    }

    None
}
