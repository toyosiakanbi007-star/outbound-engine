// src/news/listener.rs
//
// Postgres LISTEN/NOTIFY subscriber for real-time news alerts.
//
// PURPOSE:
// - Azure Function writes news directly to Postgres (bypasses 230s timeout)
// - Azure Function sends NOTIFY news_ready after insert
// - This listener receives the notification and prints the news items
//
// USAGE:
// - Run as separate process: MODE=listener cargo run
// - Or integrate into worker: spawn listener task alongside job loop
//
// ENV VARS:
// - DATABASE_URL: Postgres connection string
// - NEWS_LISTEN_CHANNEL: Channel name (default: "news_ready")
// - NEWS_PRINT_LIMIT: Max news items to print per notification (default: 10)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgListener, PgPool};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Payload sent by Azure Function via NOTIFY
#[derive(Debug, Clone, Deserialize)]
pub struct NewsReadyPayload {
    pub company_id: Uuid,
    pub client_id: Uuid,
    pub count: i32,
    pub timestamp: Option<String>,
}

/// News item from company_news table
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct CompanyNewsRow {
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
    pub created_at: DateTime<Utc>,
}

/// Company info for context
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CompanyInfo {
    pub id: Uuid,
    pub name: String,
    pub domain: Option<String>,
}

/// Run the news listener loop.
///
/// This function never returns (infinite loop).
/// Call it in a separate task or process.
pub async fn run_news_listener(pool: PgPool) -> anyhow::Result<()> {
    let channel = std::env::var("NEWS_LISTEN_CHANNEL").unwrap_or_else(|_| "news_ready".to_string());
    let print_limit: i32 = std::env::var("NEWS_PRINT_LIMIT")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    info!(
        "Starting news listener on channel '{}' (print_limit={})",
        channel, print_limit
    );

    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen(&channel).await?;

    info!("Subscribed to Postgres LISTEN {}", channel);

    loop {
        match listener.recv().await {
            Ok(notification) => {
                let payload_str = notification.payload();
                info!("Received notification on '{}': {}", channel, payload_str);

                match serde_json::from_str::<NewsReadyPayload>(payload_str) {
                    Ok(payload) => {
                        handle_news_ready(&pool, &payload, print_limit).await;
                    }
                    Err(e) => {
                        warn!("Failed to parse notification payload: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving notification: {:?}", e);
                // Sleep briefly before retrying
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Handle a news_ready notification by fetching and printing the news items.
async fn handle_news_ready(pool: &PgPool, payload: &NewsReadyPayload, limit: i32) {
    info!(
        "📰 NEWS READY: company_id={} client_id={} count={}",
        payload.company_id, payload.client_id, payload.count
    );

    // Fetch company info for context
    let company = fetch_company_info(pool, payload.company_id).await;
    let company_name = company
        .as_ref()
        .map(|c| c.name.as_str())
        .unwrap_or("Unknown");
    let company_domain = company
        .as_ref()
        .and_then(|c| c.domain.as_deref())
        .unwrap_or("?");

    println!();
    println!("{}", "=".repeat(60));
    println!("🏢 COMPANY: {} ({})", company_name, company_domain);
    println!("   ID: {}", payload.company_id);
    println!("   New items: {}", payload.count);
    println!("{}", "=".repeat(60));

    // Fetch the latest news items
    match fetch_latest_news(pool, payload.company_id, limit).await {
        Ok(items) => {
            if items.is_empty() {
                println!("   (no news items found in database)");
            } else {
                for (i, item) in items.iter().enumerate() {
                    print_news_item(i + 1, item);
                }
            }
        }
        Err(e) => {
            error!("Failed to fetch news items: {:?}", e);
            println!("   ❌ Error fetching news: {}", e);
        }
    }

    println!("{}", "=".repeat(60));
    println!();
}

/// Fetch company info from database.
async fn fetch_company_info(pool: &PgPool, company_id: Uuid) -> Option<CompanyInfo> {
    sqlx::query_as::<_, CompanyInfo>(
        r#"
        SELECT id, name, domain
        FROM companies
        WHERE id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

/// Fetch latest news items for a company.
async fn fetch_latest_news(
    pool: &PgPool,
    company_id: Uuid,
    limit: i32,
) -> Result<Vec<CompanyNewsRow>, sqlx::Error> {
    sqlx::query_as::<_, CompanyNewsRow>(
        r#"
        SELECT
            id, client_id, company_id, title, published_at, location,
            summary, url, source_type, source_name, tags, confidence, created_at
        FROM company_news
        WHERE company_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(company_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Pretty-print a news item.
fn print_news_item(index: usize, item: &CompanyNewsRow) {
    let date_str = item
        .published_at
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "?".to_string());

    let tags_str = if item.tags.is_empty() {
        "-".to_string()
    } else {
        item.tags.join(", ")
    };

    let confidence_bar = confidence_to_bar(item.confidence);

    println!();
    println!("   [{:>2}] 📰 {}", index, item.title);
    println!("       📅 Date: {}", date_str);
    println!("       🔗 URL: {}", truncate_url(&item.url, 60));
    println!(
        "       📂 Type: {} | Source: {}",
        item.source_type,
        item.source_name.as_deref().unwrap_or("-")
    );
    println!("       🏷️  Tags: {}", tags_str);
    println!(
        "       📊 Confidence: {} ({:.0}%)",
        confidence_bar,
        item.confidence * 100.0
    );

    if let Some(summary) = &item.summary {
        if !summary.is_empty() {
            let truncated = if summary.len() > 150 {
                format!("{}...", &summary[..150])
            } else {
                summary.clone()
            };
            println!("       📝 Summary: {}", truncated);
        }
    }
}

/// Convert confidence score to visual bar.
fn confidence_to_bar(conf: f32) -> String {
    let filled = (conf * 10.0).round() as usize;
    let filled = filled.min(10); // Clamp to max 10
    let empty = 10 - filled;
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

/// Truncate URL for display.
fn truncate_url(url: &str, max_len: usize) -> String {
    if url.len() <= max_len {
        url.to_string()
    } else {
        format!("{}...", &url[..max_len])
    }
}

// =============================================================================
// Integration with main.rs
// =============================================================================

/// Add this to your main.rs to support listener mode:
///
/// ```rust
/// // In main.rs, add to the mode match:
/// 
/// match mode.as_str() {
///     "worker" => { ... }
///     "listener" => {
///         news::listener::run_news_listener(pool).await?;
///     }
///     "worker+listener" => {
///         // Run both in parallel
///         let pool_clone = pool.clone();
///         let listener_handle = tokio::spawn(async move {
///             if let Err(e) = news::listener::run_news_listener(pool_clone).await {
///                 error!("Listener error: {:?}", e);
///             }
///         });
///         
///         let worker_handle = tokio::spawn(async move {
///             worker::run_worker(pool, news_client).await
///         });
///         
///         // Wait for both (they run forever)
///         tokio::select! {
///             _ = listener_handle => { tracing::warn!("Listener exited"); }
///             _ = worker_handle => { tracing::warn!("Worker exited"); }
///         }
///     }
///     _ => { run_server(...).await?; }
/// }
/// ```
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_bar() {
        assert_eq!(confidence_to_bar(0.0), "[░░░░░░░░░░]");
        assert_eq!(confidence_to_bar(0.5), "[█████░░░░░]");
        assert_eq!(confidence_to_bar(1.0), "[██████████]");
        assert_eq!(confidence_to_bar(0.85), "[████████░░]");
    }

    #[test]
    fn test_truncate_url() {
        let short = "https://example.com";
        assert_eq!(truncate_url(short, 50), short);

        let long = "https://example.com/very/long/path/that/exceeds/the/limit";
        assert_eq!(truncate_url(long, 30), "https://example.com/very/long/...");
    }
}
