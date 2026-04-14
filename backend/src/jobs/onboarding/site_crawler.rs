// src/jobs/onboarding/site_crawler.rs
//
// Rust-native website crawler for client onboarding.
// Crawls ~10-20 high-signal pages from the client's own website.
//
// Strategy:
//   1. Fetch homepage → extract nav/footer links + common path probes
//   2. Classify each URL by page type
//   3. Prioritize and deduplicate
//   4. Fetch top N pages
//   5. For each page, call LLM to generate structured summary

use reqwest::Client as HttpClient;
use serde_json::{json, Value as JsonValue};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use super::llm_client;
use super::models::CrawledPage;

/// Maximum pages to crawl per client
const MAX_PAGES: usize = 15;

/// Timeout per page fetch
const FETCH_TIMEOUT_SECS: u64 = 15;

/// Common high-signal paths to probe even if not found in nav
const COMMON_PATHS: &[&str] = &[
    "/", "/product", "/platform", "/features", "/solutions",
    "/use-cases", "/pricing", "/integrations", "/customers",
    "/case-studies", "/security", "/trust", "/compliance",
    "/docs", "/about", "/about-us", "/privacy", "/terms",
];

// ============================================================================
// Public interface
// ============================================================================

/// Crawl a client's website and return structured page summaries.
pub async fn crawl_client_site(
    http_client: &HttpClient,
    domain: &str,
    company_name: &str,
) -> Vec<CrawledPage> {
    let base_url = format!("https://{}", domain.trim_start_matches("https://").trim_start_matches("http://"));

    // Step 1: Discover URLs
    info!("Discovering URLs for {}", domain);
    let urls = discover_urls(http_client, &base_url).await;
    info!("Found {} candidate URLs for {}", urls.len(), domain);

    // Step 2: Fetch and summarize each page
    let mut results = Vec::new();
    for (url, page_type) in urls.into_iter().take(MAX_PAGES) {
        let page = fetch_and_summarize(http_client, &url, &page_type, company_name).await;
        results.push(page);
    }

    let successful = results.iter().filter(|p| p.fetch_error.is_none()).count();
    info!("Crawled {} pages for {} ({} successful)", results.len(), domain, successful);

    results
}

// ============================================================================
// URL Discovery
// ============================================================================

async fn discover_urls(http_client: &HttpClient, base_url: &str) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut candidates: Vec<(String, String, i32)> = Vec::new(); // (url, type, priority)

    // 1. Add common path probes
    for path in COMMON_PATHS {
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);
        if seen.insert(url.clone()) {
            let page_type = classify_path(path);
            let priority = page_type_priority(&page_type);
            candidates.push((url, page_type, priority));
        }
    }

    // 2. Fetch homepage and extract links
    if let Ok(homepage_html) = fetch_page_html(http_client, base_url).await {
        let links = extract_same_domain_links(&homepage_html, base_url);
        for link in links {
            if seen.insert(link.clone()) {
                let path = Url::parse(&link)
                    .ok()
                    .and_then(|u| Some(u.path().to_string()))
                    .unwrap_or_default();
                let page_type = classify_path(&path);
                let priority = page_type_priority(&page_type);
                candidates.push((link, page_type, priority));
            }
        }
    }

    // 3. Try sitemap
    let sitemap_url = format!("{}/sitemap.xml", base_url.trim_end_matches('/'));
    if let Ok(sitemap_xml) = fetch_page_html(http_client, &sitemap_url).await {
        let sitemap_links = extract_sitemap_urls(&sitemap_xml, base_url);
        for link in sitemap_links {
            if seen.insert(link.clone()) {
                let path = Url::parse(&link)
                    .ok()
                    .and_then(|u| Some(u.path().to_string()))
                    .unwrap_or_default();
                let page_type = classify_path(&path);
                let priority = page_type_priority(&page_type);
                candidates.push((link, page_type, priority));
            }
        }
    }

    // Sort by priority (lower number = higher priority)
    candidates.sort_by_key(|(_, _, p)| *p);

    // Deduplicate by page_type — keep max 2 per type (except homepage)
    let mut type_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut filtered = Vec::new();
    for (url, ptype, _) in candidates {
        let count = type_counts.entry(ptype.clone()).or_insert(0);
        if ptype == "homepage" || *count < 2 {
            *count += 1;
            filtered.push((url, ptype));
        }
    }

    filtered
}

fn classify_path(path: &str) -> String {
    let p = path.to_lowercase();
    if p == "/" || p.is_empty() { return "homepage".into(); }
    if p.contains("pricing") { return "pricing".into(); }
    if p.contains("product") || p.contains("platform") { return "product".into(); }
    if p.contains("feature") { return "features".into(); }
    if p.contains("solution") || p.contains("use-case") { return "solutions".into(); }
    if p.contains("integrat") { return "integrations".into(); }
    if p.contains("customer") || p.contains("case-stud") { return "customers".into(); }
    if p.contains("security") || p.contains("trust") || p.contains("compliance") { return "security".into(); }
    if p.contains("doc") || p.contains("help") || p.contains("support") { return "docs".into(); }
    if p.contains("about") { return "about".into(); }
    if p.contains("privacy") || p.contains("terms") || p.contains("legal") { return "legal".into(); }
    if p.contains("blog") || p.contains("news") || p.contains("announce") { return "blog".into(); }
    if p.contains("career") || p.contains("jobs") { return "careers".into(); }
    "other".into()
}

fn page_type_priority(page_type: &str) -> i32 {
    match page_type {
        "homepage"      => 0,
        "product"       => 1,
        "features"      => 2,
        "solutions"     => 3,
        "pricing"       => 4,
        "integrations"  => 5,
        "customers"     => 6,
        "security"      => 7,
        "about"         => 8,
        "docs"          => 9,
        "legal"         => 10,
        "blog"          => 11,
        "careers"       => 12,
        _               => 15,
    }
}

// ============================================================================
// Page Fetching
// ============================================================================

async fn fetch_page_html(http_client: &HttpClient, url: &str) -> Result<String, String> {
    let resp = http_client
        .get(url)
        .header("User-Agent", "Mozilla/5.0 (compatible; OutboundEngine/1.0)")
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| format!("fetch error: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    resp.text().await.map_err(|e| format!("read error: {}", e))
}

/// Strip HTML tags and normalize whitespace to get clean text
fn html_to_text(html: &str) -> String {
    // Remove script/style blocks
    let mut text = html.to_string();

    // Simple regex-free tag stripping
    let mut result = String::with_capacity(text.len());
    let mut in_tag = false;
    let mut in_script = false;

    for c in text.chars() {
        match c {
            '<' => {
                in_tag = true;
            }
            '>' => {
                in_tag = false;
            }
            _ if in_tag => {}
            _ => {
                result.push(c);
            }
        }
    }

    // Collapse whitespace
    let mut cleaned = String::new();
    let mut last_was_space = false;
    for c in result.chars() {
        if c.is_whitespace() {
            if !last_was_space {
                cleaned.push(' ');
                last_was_space = true;
            }
        } else {
            cleaned.push(c);
            last_was_space = false;
        }
    }

    cleaned.trim().to_string()
}

// ============================================================================
// Link Extraction
// ============================================================================

fn extract_same_domain_links(html: &str, base_url: &str) -> Vec<String> {
    let base = Url::parse(base_url).unwrap_or_else(|_| Url::parse("https://example.com").unwrap());
    let host = base.host_str().unwrap_or("");

    let mut links = Vec::new();

    // Simple href extraction (no HTML parser dependency)
    for part in html.split("href=\"") {
        if let Some(end) = part.find('"') {
            let href = &part[..end];
            if let Ok(abs) = base.join(href) {
                if abs.host_str() == Some(host) {
                    let url_str = abs.as_str().to_string();
                    // Skip anchors, assets, and query-heavy URLs
                    if !url_str.contains('#')
                        && !url_str.ends_with(".css")
                        && !url_str.ends_with(".js")
                        && !url_str.ends_with(".png")
                        && !url_str.ends_with(".jpg")
                        && !url_str.ends_with(".svg")
                        && !url_str.ends_with(".pdf")
                        && !url_str.contains("?utm_")
                    {
                        links.push(url_str);
                    }
                }
            }
        }
    }

    links
}

fn extract_sitemap_urls(xml: &str, base_url: &str) -> Vec<String> {
    let base = Url::parse(base_url).unwrap_or_else(|_| Url::parse("https://example.com").unwrap());
    let host = base.host_str().unwrap_or("");

    let mut urls = Vec::new();

    for part in xml.split("<loc>") {
        if let Some(end) = part.find("</loc>") {
            let url = part[..end].trim();
            if let Ok(parsed) = Url::parse(url) {
                if parsed.host_str() == Some(host) {
                    urls.push(url.to_string());
                }
            }
        }
    }

    urls
}

// ============================================================================
// Page Summarization via LLM
// ============================================================================

async fn fetch_and_summarize(
    http_client: &HttpClient,
    url: &str,
    page_type: &str,
    company_name: &str,
) -> CrawledPage {
    // Fetch HTML
    let html = match fetch_page_html(http_client, url).await {
        Ok(h) => h,
        Err(e) => {
            debug!("Failed to fetch {}: {}", url, e);
            return CrawledPage {
                url: url.to_string(),
                page_type: page_type.to_string(),
                title: None,
                raw_text_length: 0,
                summary: None,
                product_claims: vec![],
                target_buyer_clues: vec![],
                industry_clues: vec![],
                use_case_clues: vec![],
                integration_clues: vec![],
                security_compliance_clues: vec![],
                gtm_motion_clues: vec![],
                recurring_terms: vec![],
                fetch_error: Some(e),
            };
        }
    };

    // Extract text
    let text = html_to_text(&html);
    let raw_text_length = text.len();

    // Extract title from <title> tag
    let title = html.split("<title>")
        .nth(1)
        .and_then(|s| s.split("</title>").next())
        .map(|s| s.trim().to_string());

    // Truncate for LLM (keep first ~4000 chars)
    let truncated = if text.len() > 4000 { &text[..4000] } else { &text };

    // LLM summarization
    let system_prompt = r#"You are analyzing a website page to understand a company's product, positioning, and market.
Extract structured information from the page content. Respond ONLY with JSON.

Return this exact structure:
{
  "summary": "2-3 sentence summary of what this page communicates",
  "product_claims": ["list of specific product/service claims made"],
  "target_buyer_clues": ["clues about who they sell to - roles, company types, sizes"],
  "industry_clues": ["industries they serve or mention"],
  "use_case_clues": ["specific use cases or problems they solve"],
  "integration_clues": ["tools, platforms, or systems they integrate with"],
  "security_compliance_clues": ["certifications, compliance frameworks, security features"],
  "gtm_motion_clues": ["self-serve, enterprise sales, PLG, channel, etc."],
  "recurring_terms": ["important domain-specific terms repeated on the page"]
}

If a field has no relevant data, use an empty array []."#;

    let user_prompt = format!(
        "Company: {}\nPage URL: {}\nPage type: {}\n\nPage content:\n{}",
        company_name, url, page_type, truncated
    );

    match llm_client::chat_completion_json(
        http_client, system_prompt, &user_prompt, 4000,
    ).await {
        Ok(parsed) => CrawledPage {
            url: url.to_string(),
            page_type: page_type.to_string(),
            title,
            raw_text_length,
            summary: parsed["summary"].as_str().map(|s| s.to_string()),
            product_claims: json_str_array(&parsed["product_claims"]),
            target_buyer_clues: json_str_array(&parsed["target_buyer_clues"]),
            industry_clues: json_str_array(&parsed["industry_clues"]),
            use_case_clues: json_str_array(&parsed["use_case_clues"]),
            integration_clues: json_str_array(&parsed["integration_clues"]),
            security_compliance_clues: json_str_array(&parsed["security_compliance_clues"]),
            gtm_motion_clues: json_str_array(&parsed["gtm_motion_clues"]),
            recurring_terms: json_str_array(&parsed["recurring_terms"]),
            fetch_error: None,
        },
        Err(e) => {
            warn!("LLM summarization failed for {}: {}", url, e);
            CrawledPage {
                url: url.to_string(),
                page_type: page_type.to_string(),
                title,
                raw_text_length,
                summary: Some(truncated[..truncated.len().min(500)].to_string()),
                product_claims: vec![],
                target_buyer_clues: vec![],
                industry_clues: vec![],
                use_case_clues: vec![],
                integration_clues: vec![],
                security_compliance_clues: vec![],
                gtm_motion_clues: vec![],
                recurring_terms: vec![],
                fetch_error: Some(format!("LLM error: {}", e)),
            }
        }
    }
}

fn json_str_array(val: &JsonValue) -> Vec<String> {
    val.as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}
