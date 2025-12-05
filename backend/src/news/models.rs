// src/news/models.rs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Canonical news item used between Rust and the news sourcing service.
///
/// This is the shape we expect from the Lambda / external service.
/// It is also very close to the columns in `company_news`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsItem {
    pub company_id: Uuid,

    pub title: String,
    /// Optional ISO timestamp when the news was published.
    pub date: Option<DateTime<Utc>>,
    pub location: Option<String>,
    pub summary: Option<String>,

    pub url: String,
    /// e.g. "company_site" | "press_release" | "media" | "blog" | "forum" | "other"
    pub source_type: String,
    /// e.g. "Acme Logistics", "TechCrunch"
    pub source_name: Option<String>,

    /// Classification tags, e.g. ["funding", "series_a", "product_launch"]
    pub tags: Vec<String>,
    /// Confidence score 0.0–1.0
    pub confidence: f32,
}

/// Row mapping for the `company_news` table.
///
/// This is what you'll typically use when reading from the DB.
/// When inserting, you can either:
/// - use `sqlx::query!` with explicit fields, or
/// - write helper functions to convert from `NewsItem` to insert parameters.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CompanyNews {
    pub id: Uuid,
    pub client_id: Uuid,
    pub company_id: Uuid,

    pub title: String,
    pub published_at: Option<DateTime<Utc>>,
    pub location: Option<String>,
    pub summary: Option<String>,

    pub url: String,
    pub source_type: String,
    pub source_name: Option<String>,

    pub tags: Vec<String>,
    pub confidence: f32,

    /// Optional raw JSON snapshot of the news item
    pub raw_item: Option<serde_json::Value>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Row mapping for the `company_news_raw` table.
///
/// This is for debug / audit information about where we got a page,
/// how it was searched, and where raw artifacts are stored (S3 keys).
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CompanyNewsRaw {
    pub id: Uuid,
    pub client_id: Uuid,
    pub company_id: Uuid,

    pub run_id: Uuid,
    pub url: String,

    pub search_query: Option<String>,
    pub search_rank: Option<i32>,
    pub source_name: Option<String>,

    pub html_s3_key: Option<String>,
    pub extraction_s3_key: Option<String>,
    pub raw_metadata: Option<serde_json::Value>,

    pub created_at: DateTime<Utc>,
}
