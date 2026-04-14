// src/jobs/onboarding/enrich_and_crawl.rs
//
// Job 2: ONBOARDING_ENRICH_AND_CRAWL
//
// 1. Call Diffbot Knowledge Graph to resolve the organization
// 2. Normalize the org profile (logo, socials, competitors, etc.)
// 3. Crawl the client's website (10-15 high-signal pages)
// 4. Store all artifacts
// 5. Enqueue GENERATE_DRAFTS

use reqwest::Client as HttpClient;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::models::*;
use super::site_crawler;

lazy_static::lazy_static! {
    static ref DIFFBOT_TOKEN: String = std::env::var("DIFFBOT_TOKEN").unwrap_or_default();
    static ref DIFFBOT_BASE_URL: String = std::env::var("DIFFBOT_BASE_URL")
        .unwrap_or_else(|_| "https://kg.diffbot.com/kg/v3/dql".to_string());
}

pub async fn run_enrich_and_crawl(
    pool: &PgPool,
    http_client: &HttpClient,
    payload: EnrichAndCrawlPayload,
    worker_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = payload.run_id;
    let client_id = payload.client_id;
    let name = &payload.client_name;
    let domain = &payload.client_domain;

    info!("Worker {}: enrich+crawl for {} ({})", worker_id, name, domain);

    // ========================================================================
    // STEP 1: Diffbot Organization Enrichment
    // ========================================================================
    let diffbot_profile = if !DIFFBOT_TOKEN.is_empty() {
        match diffbot_org_lookup(http_client, name, domain).await {
            Ok((raw, normalized)) => {
                // Store raw artifact
                store_artifact(pool, run_id, "diffbot_org_raw", None, None, &raw).await;

                // Store normalized artifact
                let norm_json = serde_json::to_value(&normalized).unwrap_or(json!({}));
                store_artifact(pool, run_id, "diffbot_org_normalized", None, None, &norm_json).await;

                // Update run with diffbot URI
                if let Some(ref uri) = normalized.diffbot_uri {
                    sqlx::query("UPDATE client_onboarding_runs SET diffbot_uri = $1, updated_at = NOW() WHERE id = $2")
                        .bind(uri)
                        .bind(run_id)
                        .execute(pool)
                        .await
                        .ok();
                }

                info!("Worker {}: Diffbot resolved {} → {:?}", worker_id, domain, normalized.name);
                Some(normalized)
            }
            Err(e) => {
                warn!("Worker {}: Diffbot lookup failed for {}: {}", worker_id, domain, e);
                None
            }
        }
    } else {
        warn!("Worker {}: DIFFBOT_TOKEN not set, skipping org enrichment", worker_id);
        None
    };

    // ========================================================================
    // STEP 2: Client Website Crawl
    // ========================================================================
    info!("Worker {}: starting website crawl for {}", worker_id, domain);
    let crawled_pages = site_crawler::crawl_client_site(http_client, domain, name).await;

    // Store each page as an artifact
    for page in &crawled_pages {
        let page_json = serde_json::to_value(page).unwrap_or(json!({}));
        if page.fetch_error.is_none() {
            store_artifact(
                pool, run_id, "site_page_summary",
                Some(&page.url), Some(&page.page_type),
                &page_json,
            ).await;
        } else {
            store_artifact(
                pool, run_id, "site_page_raw",
                Some(&page.url), Some(&page.page_type),
                &page_json,
            ).await;
        }
    }

    let success_count = crawled_pages.iter().filter(|p| p.fetch_error.is_none()).count();
    info!("Worker {}: crawled {} pages ({} successful) for {}", worker_id, crawled_pages.len(), success_count, domain);

    // ========================================================================
    // STEP 3: Build Knowledge Pack (stored on the run for the next job)
    // ========================================================================
    // Load operator input from run
    let run = sqlx::query_as::<_, OnboardingRunRow>(
        "SELECT * FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;

    let knowledge_pack = KnowledgePack {
        company_name: name.clone(),
        domain: domain.clone(),
        diffbot_profile,
        crawled_pages,
        operator_note: run.operator_note,
        target_country: None,    // TODO: extract from operator_note or input
        known_competitors: None, // TODO: extract from input
        known_customers: None,
        tone_override: None,
    };

    let kp_json = serde_json::to_value(&knowledge_pack)?;

    // Store as artifact
    store_artifact(pool, run_id, "knowledge_pack", None, None, &kp_json).await;

    // Also store on the run itself for quick access by next job
    sqlx::query(
        "UPDATE client_onboarding_runs SET knowledge_pack = $1, updated_at = NOW() WHERE id = $2"
    )
    .bind(&kp_json)
    .bind(run_id)
    .execute(pool)
    .await?;

    // ========================================================================
    // STEP 4: Enqueue GENERATE_DRAFTS
    // ========================================================================
    sqlx::query(
        "UPDATE client_onboarding_runs SET status = 'generating', updated_at = NOW() WHERE id = $1"
    )
    .bind(run_id)
    .execute(pool)
    .await?;

    let next_payload = GenerateDraftsPayload { client_id, run_id };

    sqlx::query(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'onboarding_generate_drafts', $2, 'pending', NOW())"#,
    )
    .bind(client_id)
    .bind(json!(next_payload))
    .execute(pool)
    .await?;

    info!("Worker {}: enrich+crawl complete for run {}, generate_drafts enqueued", worker_id, run_id);

    Ok(())
}

// ============================================================================
// Diffbot Organization Lookup
// ============================================================================

async fn diffbot_org_lookup(
    http_client: &HttpClient,
    name: &str,
    domain: &str,
) -> Result<(JsonValue, DiffbotOrgProfile), String> {
    let query = format!(
        "type:Organization strict:name:\"{}\" homepageUri:\"{}\"",
        name.replace('"', ""), domain.replace('"', "")
    );

    let url = format!(
        "{}?type=query&token={}&query={}&size=1",
        *DIFFBOT_BASE_URL,
        *DIFFBOT_TOKEN,
        urlencoding::encode(&query),
    );

    let resp = http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("Diffbot request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Diffbot returned {}: {}", status, &body[..body.len().min(300)]));
    }

    let raw: JsonValue = resp.json().await
        .map_err(|e| format!("Failed to parse Diffbot response: {}", e))?;

    // Extract first entity
    let entity = raw["data"].as_array()
        .and_then(|arr| arr.first())
        .cloned()
        .unwrap_or(json!({}));

    let profile = normalize_diffbot_entity(&entity);

    Ok((raw, profile))
}

fn normalize_diffbot_entity(entity: &JsonValue) -> DiffbotOrgProfile {
    DiffbotOrgProfile {
        name: entity["name"].as_str().map(String::from),
        homepage: entity["homepageUri"].as_str().map(String::from),
        description: entity["description"].as_str().map(String::from),
        short_description: entity["shortDescription"].as_str().map(String::from),
        logo: entity["logo"].as_str().map(String::from),
        industries: json_str_vec(&entity["industries"]),
        categories: json_str_vec(&entity["categories"]),
        employee_count: entity["nbEmployees"].as_i64()
            .or_else(|| entity["nbEmployeesMax"].as_i64()),
        employee_range: entity["nbEmployeesRange"].as_str().map(String::from),
        location: extract_location(entity),
        founding_date: entity["foundingDate"].as_str().map(String::from)
            .or_else(|| entity["founding"].as_str().map(String::from)),
        social_links: extract_socials(entity),
        competitors: extract_competitors(entity),
        similar_companies: json_str_vec(&entity["similarTo"]),
        diffbot_uri: entity["diffbotUri"].as_str().map(String::from),
        stock_symbol: entity["stock"]["symbol"].as_str().map(String::from),
        revenue_range: entity["revenueRange"].as_str().map(String::from)
            .or_else(|| entity["revenue"]["range"].as_str().map(String::from)),
    }
}

fn json_str_vec(val: &JsonValue) -> Vec<String> {
    match val {
        JsonValue::Array(arr) => arr.iter().filter_map(|v| {
            v.as_str().map(String::from)
                .or_else(|| v["name"].as_str().map(String::from))
        }).collect(),
        JsonValue::String(s) => vec![s.clone()],
        _ => vec![],
    }
}

fn extract_location(entity: &JsonValue) -> Option<String> {
    let loc = &entity["location"];
    if loc.is_null() { return entity["headquarters"].as_str().map(String::from); }

    let parts: Vec<&str> = [
        loc["city"].as_str(),
        loc["region"].as_str(),
        loc["country"].as_str(),
    ].into_iter().flatten().collect();

    if parts.is_empty() {
        loc.as_str().map(String::from)
    } else {
        Some(parts.join(", "))
    }
}

fn extract_socials(entity: &JsonValue) -> Vec<SocialLink> {
    let mut links = Vec::new();

    for (platform, key) in &[
        ("linkedin", "linkedInUri"),
        ("twitter", "twitterUri"),
        ("facebook", "facebookUri"),
        ("github", "githubUri"),
        ("crunchbase", "crunchbaseUri"),
    ] {
        if let Some(url) = entity[key].as_str() {
            links.push(SocialLink {
                platform: platform.to_string(),
                url: url.to_string(),
            });
        }
    }

    links
}

fn extract_competitors(entity: &JsonValue) -> Vec<CompetitorInfo> {
    entity["competitors"].as_array()
        .map(|arr| arr.iter().map(|c| CompetitorInfo {
            name: c["name"].as_str().unwrap_or("").to_string(),
            domain: c["homepageUri"].as_str().map(String::from),
            summary: c["description"].as_str().map(String::from)
                .or_else(|| c["shortDescription"].as_str().map(String::from)),
        }).collect())
        .unwrap_or_default()
}

// ============================================================================
// Helpers
// ============================================================================

async fn store_artifact(
    pool: &PgPool,
    run_id: Uuid,
    artifact_type: &str,
    page_url: Option<&str>,
    page_type: Option<&str>,
    content: &JsonValue,
) {
    if let Err(e) = sqlx::query(
        r#"INSERT INTO client_onboarding_artifacts (run_id, artifact_type, page_url, page_type, content_json)
           VALUES ($1, $2, $3, $4, $5)"#
    )
    .bind(run_id)
    .bind(artifact_type)
    .bind(page_url)
    .bind(page_type)
    .bind(content)
    .execute(pool)
    .await
    {
        error!("Failed to store artifact {}: {:?}", artifact_type, e);
    }
}
