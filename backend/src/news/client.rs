// src/news/client.rs

use crate::config::Config;
use crate::news::models::NewsItem;

use async_trait::async_trait;
use aws_config;
use aws_sdk_lambda::{primitives::Blob, Client as LambdaClient};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

/// Request we send to the news service (Lambda or Azure HTTP).
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

/// Raw item as returned by the news microservice JSON (wire format).
/// NOTE: `date` is a String here to match the Lambda/Azure payload.
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

/// Raw response shape from the news microservice.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNewsFetchResponse {
    pub company_id: Uuid,
    pub news_items: Vec<RawNewsItem>,
}

#[derive(Debug, Error)]
pub enum NewsError {
    #[error("lambda error: {0}")]
    Lambda(String),

    #[error("http error: {0}")]
    Http(String),

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

/// Azure HTTP-based implementation that calls an Azure Function / HTTP endpoint.
#[derive(Clone)]
pub struct AzureHttpNewsSourcingClient {
    http: HttpClient,
    base_url: String,
    api_key: Option<String>,
}

impl AzureHttpNewsSourcingClient {
    pub fn new(http: HttpClient, base_url: String, api_key: Option<String>) -> Self {
        Self {
            http,
            base_url,
            api_key,
        }
    }
}

/// No-op client used when no backend is configured (returns zero news).
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
/// Precedence:
/// 1. If `NEWS_AZURE_FUNCTION_URL` (or `Config.news_service_base_url`) is set -> Azure HTTP client
/// 2. Else if `NEWS_LAMBDA_FUNCTION_NAME` is set -> Lambda client
/// 3. Else -> Noop client (worker will log “no news” and continue)
pub async fn build_news_client(cfg: &Config) -> Result<DynNewsSourcingClient, NewsError> {
    // --- 1) Azure HTTP function? ---
    // Env takes precedence; fall back to config.news_service_base_url / _api_key.
    let azure_url_env = std::env::var("NEWS_AZURE_FUNCTION_URL").ok();
    let azure_key_env = std::env::var("NEWS_AZURE_FUNCTION_KEY").ok();

    let azure_url = azure_url_env.or_else(|| cfg.news_service_base_url.clone());
    let azure_key = azure_key_env.or_else(|| cfg.news_service_api_key.clone());

    if let Some(base_url) = azure_url {
        info!(
            "Initializing AzureHttpNewsSourcingClient with base_url={}",
            base_url
        );
        let http = HttpClient::new();
        let client = AzureHttpNewsSourcingClient::new(http, base_url, azure_key);
        return Ok(Arc::new(client) as DynNewsSourcingClient);
    }

    // --- 2) AWS Lambda? ---
    if let Ok(function_name) = std::env::var("NEWS_LAMBDA_FUNCTION_NAME") {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        info!(
            "Initializing LambdaNewsSourcingClient for function={} in region={}",
            function_name, region
        );

        let conf = aws_config::load_from_env().await;
        let client = LambdaClient::new(&conf);
        let lambda_client = LambdaNewsSourcingClient::new(client, function_name);
        return Ok(Arc::new(lambda_client) as DynNewsSourcingClient);
    }

    // --- 3) Fallback: Noop client ---
    warn!(
        "No NEWS_AZURE_FUNCTION_URL / NEWS_LAMBDA_FUNCTION_NAME configured; \
         using NoopNewsSourcingClient (no news will be fetched)"
    );
    Ok(Arc::new(NoopNewsSourcingClient::new()) as DynNewsSourcingClient)
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

        // Shared parse logic
        let items = parse_news_items_from_bytes(&raw_bytes, &raw_str);
        Ok(items)
    }
}

#[async_trait]
impl NewsSourcingClient for AzureHttpNewsSourcingClient {
    async fn fetch_news_for_company(
        &self,
        req: &NewsFetchRequest,
    ) -> Result<Vec<NewsItem>, NewsError> {
        // Treat base_url as the full function URL.
        let url = self.base_url.trim_end_matches('/').to_string();

        let mut request_builder = self.http.post(&url).json(req);

        // Azure Function key, if any (standard header).
        if let Some(ref key) = self.api_key {
            request_builder = request_builder.header("x-functions-key", key);
        }

        let resp = request_builder
            .send()
            .await
            .map_err(|e| NewsError::Http(e.to_string()))?;

        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| NewsError::Http(e.to_string()))?;
        let raw_bytes = bytes.to_vec();

        if !status.is_success() {
            let body_str = String::from_utf8_lossy(&raw_bytes);
            warn!(
                target: "backend::news::client",
                "Azure news function returned non-success status={} body={}",
                status,
                body_str
            );
            return Err(NewsError::Http(format!(
                "Azure function status={} body={}",
                status, body_str
            )));
        }

        if raw_bytes.is_empty() {
            warn!("Azure news function returned empty payload");
            return Ok(vec![]);
        }

        let raw_str = String::from_utf8(raw_bytes.clone())?;
        info!(
            target: "backend::news::client",
            "Azure news function raw payload: {}",
            raw_str
        );

        // Shared parse logic
        let items = parse_news_items_from_bytes(&raw_bytes, &raw_str);
        Ok(items)
    }
}

/// Shared helper: convert wire/raw item into internal NewsItem, with flexible date parsing.
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

/// Shared parsing logic used by both Lambda and Azure HTTP clients.
///
/// Tries several shapes:
/// - direct `RawNewsFetchResponse`
/// - wrapper with `body` string (API Gateway-style)
/// - trimmed-at-last-`}` in case of trailing junk
fn parse_news_items_from_bytes(raw_bytes: &[u8], raw_str: &str) -> Vec<NewsItem> {
    // Local helper: map RawNewsFetchResponse -> Vec<NewsItem>
    fn map_raw_response(raw: RawNewsFetchResponse) -> Vec<NewsItem> {
        raw.news_items
            .into_iter()
            .map(RawNewsItem::into_news_item)
            .collect()
    }

    // ---- Primary parse path: direct raw response ----
    if let Ok(parsed) = serde_json::from_slice::<RawNewsFetchResponse>(raw_bytes) {
        return map_raw_response(parsed);
    }

    warn!(
        target: "backend::news::client",
        "failed to parse payload as RawNewsFetchResponse directly; trying wrapper/body fallbacks"
    );

    // ---- Fallback 1: parse as generic Value, handle wrappers ----
    if let Ok(v) = serde_json::from_str::<Value>(raw_str) {
        // Case 1: already looks like { company_id, news_items }
        if v.get("company_id").is_some() && v.get("news_items").is_some() {
            if let Ok(parsed) = serde_json::from_value::<RawNewsFetchResponse>(v.clone()) {
                return map_raw_response(parsed);
            }
        }

        // Case 2: API Gateway style: { statusCode, body: "<json string>" }
        if let Some(body) = v.get("body") {
            if let Some(body_str) = body.as_str() {
                info!(
                    target: "backend::news::client",
                    "news wrapper body string: {}",
                    body_str
                );
                if let Ok(parsed) = serde_json::from_str::<RawNewsFetchResponse>(body_str) {
                    return map_raw_response(parsed);
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
                "parsed payload successfully after trimming trailing data"
            );
            return map_raw_response(parsed);
        }
    }

    // ---- If everything fails, log and treat as no items ----
    warn!(
        target: "backend::news::client",
        "all attempts to parse news payload failed; treating as 0 items"
    );
    Vec::new()
}
