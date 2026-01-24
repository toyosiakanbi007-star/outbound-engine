// src/jobs/phase_b_controller.rs
//
// Phase B Controller for V3 Prequal Engine.
//
// PURPOSE:
// - Orchestrate Phase B after prequal qualifies
// - Call Apollo "Get Complete Organization Info" via /api/v1/organizations/{id}
// - First need to search for org ID using domain if not cached
// - Trigger employee metrics and funding analysis
// - Call Azure Function with mode="aggregate" to generate final hypotheses
//
// APOLLO API:
// - Organization Search: POST /api/v1/organizations/search (to get org ID from domain)
// - Get Complete Org: GET /api/v1/organizations/{id}?include_employee_metrics=true
//
// FLOW:
// 1. LISTEN prequal_ready notification
// 2. Check if qualifies=true
// 3. Get Apollo org_id (from cache or search by domain)
// 4. Call Apollo API for full org enrichment
// 5. Store enrichment in company_apollo_enrichment
// 6. Run ANALYZE_EMPLOYEE_METRICS
// 7. Run ANALYZE_FUNDING_EVENTS
// 8. HTTP call Azure Function with mode="aggregate"
// 9. Store aggregated results

use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::jobs::employee_metrics::{
    handle_analyze_employee_metrics, AnalyzeEmployeeMetricsPayload, EmployeeMetricsAnalysis,
};
use crate::jobs::funding_analysis::{
    handle_analyze_funding_events, AnalyzeFundingEventsPayload, FundingAnalysis,
};

// ----------------------------
// Config
// ----------------------------

fn get_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn get_env_bool(name: &str, default: bool) -> bool {
    let val = get_env(name, if default { "1" } else { "0" });
    matches!(val.to_lowercase().as_str(), "1" | "true" | "yes")
}

lazy_static::lazy_static! {
    static ref APOLLO_API_KEY: String = get_env("APOLLO_API_KEY", "");
    static ref AZURE_FUNCTION_URL: String = get_env("AZURE_FUNCTION_URL", "");
    static ref AZURE_FUNCTION_KEY: String = get_env("AZURE_FUNCTION_KEY", "");
    static ref PHASE_B_ENABLED: bool = get_env_bool("PHASE_B_ENABLED", true);
}

// ----------------------------
// Data Models
// ----------------------------

/// Payload for PHASE_B_ENRICH_APOLLO job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseBPayload {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: String,
    pub prequal_run_id: Uuid,
    pub prequal_score: f64,
    /// Optional: Apollo org ID if already known
    pub apollo_org_id: Option<String>,
}

/// Prequal notification from Postgres NOTIFY
#[derive(Debug, Clone, Deserialize)]
pub struct PrequalReadyNotification {
    pub company_id: Uuid,
    pub client_id: Uuid,
    pub run_id: Uuid,
    pub score: f64,
    pub qualifies: bool,
    pub phase: String,
    pub timestamp: String,
}

// ----------------------------
// Apollo API Models
// ----------------------------

/// Apollo Organization Search Response
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloSearchResponse {
    #[serde(default)]
    pub organizations: Vec<ApolloSearchOrg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApolloSearchOrg {
    pub id: String,
    pub name: Option<String>,
    pub primary_domain: Option<String>,
    pub website_url: Option<String>,
}

/// Apollo Get Complete Organization Response
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloOrgResponse {
    pub organization: Option<ApolloOrganization>,
}

/// Full Apollo Organization (from Get Complete Organization Info)
#[derive(Debug, Clone, Deserialize)]
pub struct ApolloOrganization {
    pub id: Option<String>,
    pub name: Option<String>,
    pub website_url: Option<String>,
    pub primary_domain: Option<String>,
    pub industry: Option<String>,
    pub estimated_num_employees: Option<i32>,
    pub short_description: Option<String>,
    pub founded_year: Option<i32>,
    pub linkedin_url: Option<String>,
    pub twitter_url: Option<String>,
    pub facebook_url: Option<String>,
    
    // Address
    pub street_address: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
    
    // Financials
    pub annual_revenue: Option<i64>,
    pub total_funding: Option<i64>,
    pub latest_funding_stage: Option<String>,
    pub latest_funding_round_date: Option<String>,
    
    // Technologies - array of strings
    #[serde(default)]
    pub technology_names: Vec<String>,
    
    // Funding events - array of objects
    #[serde(default)]
    pub funding_events: Vec<JsonValue>,
    
    // Employee metrics - array of monthly snapshots
    #[serde(default)]
    pub employee_metrics: Vec<JsonValue>,
    
    // Keywords
    #[serde(default)]
    pub keywords: Vec<String>,
}

/// Azure Function aggregate request
#[derive(Debug, Clone, Serialize)]
pub struct AggregateRequest {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: String,
    pub mode: String,
    pub run_id: Uuid,
    pub prequal_hypotheses: Vec<JsonValue>,
    pub prequal_evidence: Vec<JsonValue>,
    pub apollo_enrichment: JsonValue,
    pub employee_metrics_analysis: JsonValue,
    pub funding_analysis: JsonValue,
    pub client_context: JsonValue,
}

// ----------------------------
// Apollo API Client
// ----------------------------

/// Search for organization by domain to get Apollo org ID
pub async fn search_apollo_org_by_domain(
    http_client: &Client,
    domain: &str,
) -> Result<Option<String>, String> {
    if APOLLO_API_KEY.is_empty() {
        return Err("APOLLO_API_KEY not set".to_string());
    }
    
    let url = "https://api.apollo.io/api/v1/mixed_companies/search";
    
    let body = json!({
        "api_key": APOLLO_API_KEY.as_str(),
        "q_organization_domains": domain,
        "page": 1,
        "per_page": 1
    });
    
    let response = http_client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-cache")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Apollo search request failed: {}", e))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Apollo search error {}: {}", status, body));
    }
    
    let data: ApolloSearchResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse Apollo search response: {}", e))?;
    
    // Return first matching org ID
    Ok(data.organizations.first().map(|o| o.id.clone()))
}

/// Call Apollo API to get complete organization info
pub async fn fetch_apollo_org_info(
    http_client: &Client,
    org_id: &str,
) -> Result<ApolloOrganization, String> {
    if APOLLO_API_KEY.is_empty() {
        return Err("APOLLO_API_KEY not set".to_string());
    }
    
    // GET /api/v1/organizations/{id}
    let url = format!(
        "https://api.apollo.io/api/v1/organizations/{}",
        org_id
    );
    
    let response = http_client
        .get(&url)
        .header("Content-Type", "application/json")
        .header("Cache-Control", "no-cache")
        .query(&[("api_key", APOLLO_API_KEY.as_str())])
        .send()
        .await
        .map_err(|e| format!("Apollo API request failed: {}", e))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Apollo API error {}: {}", status, body));
    }
    
    let data: ApolloOrgResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse Apollo response: {}", e))?;
    
    data.organization.ok_or_else(|| "No organization data returned".to_string())
}

// ----------------------------
// Database Operations
// ----------------------------

/// Check if we have cached Apollo org_id for this company
pub async fn get_cached_apollo_org_id(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        r#"
        SELECT apollo_org_id
        FROM company_apollo_enrichment
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await?;
    
    Ok(row.and_then(|(id,)| id))
}

/// Store Apollo enrichment data
pub async fn store_apollo_enrichment(
    pool: &PgPool,
    company_id: Uuid,
    org: &ApolloOrganization,
) -> Result<(), sqlx::Error> {
    let funding_events = serde_json::to_value(&org.funding_events).unwrap_or(json!([]));
    let employee_metrics = serde_json::to_value(&org.employee_metrics).unwrap_or(json!([]));
    
    sqlx::query(
        r#"
        INSERT INTO company_apollo_enrichment (
            company_id,
            apollo_org_id,
            description,
            industry,
            estimated_num_employees,
            technologies,
            keywords,
            founded_year,
            annual_revenue,
            linkedin_url,
            city,
            state,
            country,
            funding_events,
            total_funding_raised,
            latest_funding_stage,
            employee_metrics,
            fetched_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
        ON CONFLICT (company_id) DO UPDATE SET
            apollo_org_id = EXCLUDED.apollo_org_id,
            description = EXCLUDED.description,
            industry = EXCLUDED.industry,
            estimated_num_employees = EXCLUDED.estimated_num_employees,
            technologies = EXCLUDED.technologies,
            keywords = EXCLUDED.keywords,
            founded_year = EXCLUDED.founded_year,
            annual_revenue = EXCLUDED.annual_revenue,
            linkedin_url = EXCLUDED.linkedin_url,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            funding_events = EXCLUDED.funding_events,
            total_funding_raised = EXCLUDED.total_funding_raised,
            latest_funding_stage = EXCLUDED.latest_funding_stage,
            employee_metrics = EXCLUDED.employee_metrics,
            fetched_at = NOW()
        "#,
    )
    .bind(company_id)
    .bind(&org.id)
    .bind(&org.short_description)
    .bind(&org.industry)
    .bind(org.estimated_num_employees)
    .bind(&org.technology_names)
    .bind(&org.keywords)
    .bind(org.founded_year)
    .bind(org.annual_revenue)
    .bind(&org.linkedin_url)
    .bind(&org.city)
    .bind(&org.state)
    .bind(&org.country)
    .bind(funding_events)
    .bind(org.total_funding)
    .bind(&org.latest_funding_stage)
    .bind(employee_metrics)
    .execute(pool)
    .await?;
    
    info!("Stored Apollo enrichment for company {}", company_id);
    
    Ok(())
}

/// Load prequal data for aggregate request
pub async fn load_prequal_data(
    pool: &PgPool,
    company_id: Uuid,
    run_id: Uuid,
) -> Result<(Vec<JsonValue>, Vec<JsonValue>), sqlx::Error> {
    // Load hypotheses
    let hypotheses: Vec<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'hypothesis', hypothesis,
            'why_now', why_now,
            'pain_category', pain_category,
            'pain_subtags', pain_subtags,
            'evidence', evidence,
            'confidence', confidence,
            'strength', strength
        )
        FROM company_pain_hypotheses
        WHERE company_id = $1 AND run_id = $2
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    
    // Load evidence
    let evidence: Vec<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'url', url,
            'snippet', snippet,
            'evidence_type', evidence_type,
            'pain_category', pain_category,
            'source_type', source_type,
            'date', date
        )
        FROM extracted_evidence
        WHERE company_id = $1 AND run_id = $2
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    
    Ok((
        hypotheses.into_iter().map(|(j,)| j).collect(),
        evidence.into_iter().map(|(j,)| j).collect(),
    ))
}

/// Load Apollo enrichment for aggregate request
pub async fn load_apollo_enrichment(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<JsonValue, sqlx::Error> {
    let row: Option<(JsonValue, JsonValue, JsonValue, Option<String>, Option<i32>)> = sqlx::query_as(
        r#"
        SELECT 
            COALESCE(to_jsonb(technologies), '[]'::jsonb) as technologies,
            COALESCE(employee_metrics_analysis, '{}'::jsonb) as emp_analysis,
            COALESCE(funding_analysis, '{}'::jsonb) as fund_analysis,
            industry,
            estimated_num_employees
        FROM company_apollo_enrichment
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await?;
    
    match row {
        Some((tech, emp, fund, industry, emp_count)) => Ok(json!({
            "technologies": tech,
            "employee_metrics_analysis": emp,
            "funding_analysis": fund,
            "industry": industry,
            "estimated_num_employees": emp_count,
        })),
        None => Ok(json!({})),
    }
}

/// Load client context from clients table
pub async fn load_client_context(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<JsonValue, sqlx::Error> {
    let row: Option<(Option<JsonValue>,)> = sqlx::query_as(
        r#"
        SELECT config
        FROM clients
        WHERE id = $1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    
    Ok(row.and_then(|(c,)| c).unwrap_or(json!({})))
}

/// Get company info
#[derive(Debug, sqlx::FromRow)]
pub struct CompanyInfo {
    pub id: Uuid,
    pub name: String,
    pub domain: String,
}

pub async fn get_company_info(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<Option<CompanyInfo>, sqlx::Error> {
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
}

// ----------------------------
// Azure Function Trigger
// ----------------------------

/// Call Azure Function with mode="aggregate"
pub async fn trigger_aggregate(
    http_client: &Client,
    request: &AggregateRequest,
) -> Result<JsonValue, String> {
    if AZURE_FUNCTION_URL.is_empty() {
        return Err("AZURE_FUNCTION_URL not set".to_string());
    }
    
    let url = format!("{}/api/news-fetch", AZURE_FUNCTION_URL.trim_end_matches('/'));
    
    let mut req_builder = http_client
        .post(&url)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(120))  // 2 min timeout for aggregate
        .json(request);
    
    // Add function key if configured
    if !AZURE_FUNCTION_KEY.is_empty() {
        req_builder = req_builder.header("x-functions-key", AZURE_FUNCTION_KEY.as_str());
    }
    
    let response = req_builder
        .send()
        .await
        .map_err(|e| format!("Azure Function request failed: {}", e))?;
    
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Azure Function error {}: {}", status, body));
    }
    
    let data: JsonValue = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse Azure Function response: {}", e))?;
    
    Ok(data)
}

// ----------------------------
// Main Job Handler
// ----------------------------

/// Handle PHASE_B_ENRICH_APOLLO job
pub async fn handle_phase_b_enrich(
    pool: &PgPool,
    http_client: &Client,
    payload: &PhaseBPayload,
    worker_id: &str,
) -> Result<JsonValue, Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Worker {}: starting Phase B enrichment for company {} (domain={}, prequal_score={:.2})",
        worker_id, payload.company_id, payload.domain, payload.prequal_score
    );
    
    if !*PHASE_B_ENABLED {
        warn!("Worker {}: Phase B disabled, skipping", worker_id);
        return Ok(json!({"status": "skipped", "reason": "phase_b_disabled"}));
    }
    
    // Step 1: Get Apollo org ID (from payload, cache, or search)
    let org_id = match &payload.apollo_org_id {
        Some(id) => id.clone(),
        None => {
            // Check cache first
            match get_cached_apollo_org_id(pool, payload.company_id).await? {
                Some(id) => {
                    info!("Worker {}: using cached Apollo org_id: {}", worker_id, id);
                    id
                }
                None => {
                    // Search by domain
                    info!("Worker {}: searching Apollo for domain {}", worker_id, payload.domain);
                    match search_apollo_org_by_domain(http_client, &payload.domain).await? {
                        Some(id) => {
                            info!("Worker {}: found Apollo org_id: {}", worker_id, id);
                            id
                        }
                        None => {
                            warn!("Worker {}: no Apollo org found for domain {}", worker_id, payload.domain);
                            return Ok(json!({
                                "status": "skipped",
                                "reason": "no_apollo_org_found",
                                "domain": &payload.domain
                            }));
                        }
                    }
                }
            }
        }
    };
    
    // Step 2: Fetch full org info from Apollo
    info!("Worker {}: fetching Apollo org info for {}", worker_id, org_id);
    let apollo_org = match fetch_apollo_org_info(http_client, &org_id).await {
        Ok(org) => org,
        Err(e) => {
            error!("Worker {}: Apollo API failed: {}", worker_id, e);
            return Err(e.into());
        }
    };
    
    // Step 3: Store Apollo enrichment
    store_apollo_enrichment(pool, payload.company_id, &apollo_org).await?;
    
    // Step 4: Run employee metrics analysis
    info!("Worker {}: analyzing employee metrics", worker_id);
    let emp_payload = AnalyzeEmployeeMetricsPayload {
        company_id: payload.company_id,
        client_id: payload.client_id,
    };
    let emp_analysis = handle_analyze_employee_metrics(pool, &emp_payload, worker_id).await?;
    
    // Step 5: Run funding analysis
    info!("Worker {}: analyzing funding events", worker_id);
    let fund_payload = AnalyzeFundingEventsPayload {
        company_id: payload.company_id,
        client_id: payload.client_id,
    };
    let fund_analysis = handle_analyze_funding_events(pool, &fund_payload, worker_id).await?;
    
    // Step 6: Load prequal data
    info!("Worker {}: loading prequal data", worker_id);
    let (prequal_hypotheses, prequal_evidence) = 
        load_prequal_data(pool, payload.company_id, payload.prequal_run_id).await?;
    
    // Step 7: Load enrichment for request
    let apollo_enrichment = load_apollo_enrichment(pool, payload.company_id).await?;
    
    // Step 8: Load client context
    let client_context = load_client_context(pool, payload.client_id).await?;
    
    // Step 9: Trigger Azure Function aggregate
    info!("Worker {}: triggering Azure Function aggregate", worker_id);
    let aggregate_request = AggregateRequest {
        client_id: payload.client_id,
        company_id: payload.company_id,
        company_name: payload.company_name.clone(),
        domain: payload.domain.clone(),
        mode: "aggregate".to_string(),
        run_id: payload.prequal_run_id,
        prequal_hypotheses,
        prequal_evidence,
        apollo_enrichment,
        employee_metrics_analysis: serde_json::to_value(&emp_analysis)?,
        funding_analysis: serde_json::to_value(&fund_analysis)?,
        client_context,
    };
    
    let aggregate_result = trigger_aggregate(http_client, &aggregate_request).await?;
    
    info!(
        "Worker {}: Phase B complete for company {} (aggregate hypotheses={})",
        worker_id,
        payload.company_id,
        aggregate_result.get("pain_hypotheses").and_then(|h| h.as_array()).map(|a| a.len()).unwrap_or(0)
    );
    
    Ok(aggregate_result)
}

// ----------------------------
// Prequal Listener
// ----------------------------

/// Handle prequal_ready notification and queue Phase B if qualified
pub async fn handle_prequal_notification(
    pool: &PgPool,
    notification: &PrequalReadyNotification,
    worker_id: &str,
) -> Result<bool, sqlx::Error> {
    info!(
        "Worker {}: received prequal notification for company {} (qualifies={}, score={:.2})",
        worker_id, notification.company_id, notification.qualifies, notification.score
    );
    
    if !notification.qualifies {
        info!(
            "Worker {}: company {} did not qualify (score={:.2}), skipping Phase B",
            worker_id, notification.company_id, notification.score
        );
        return Ok(false);
    }
    
    // Get company info
    let company = match get_company_info(pool, notification.company_id).await? {
        Some(c) => c,
        None => {
            warn!(
                "Worker {}: company {} not found, skipping Phase B",
                worker_id, notification.company_id
            );
            return Ok(false);
        }
    };
    
    // Queue Phase B job
    let payload = PhaseBPayload {
        client_id: notification.client_id,
        company_id: notification.company_id,
        company_name: company.name,
        domain: company.domain,
        prequal_run_id: notification.run_id,
        prequal_score: notification.score,
        apollo_org_id: None,  // Will be looked up
    };
    
    let payload_json = serde_json::to_value(&payload)
        .map_err(|e| sqlx::Error::Protocol(format!("JSON serialization failed: {}", e)))?;
    
    sqlx::query(
        r#"
        INSERT INTO jobs (
            client_id,
            job_type,
            status,
            payload,
            created_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        "#,
    )
    .bind(notification.client_id)
    .bind("PHASE_B_ENRICH_APOLLO")
    .bind("pending")
    .bind(payload_json)
    .execute(pool)
    .await?;
    
    info!(
        "Worker {}: queued PHASE_B_ENRICH_APOLLO job for company {} (score={:.2})",
        worker_id, notification.company_id, notification.score
    );
    
    Ok(true)
}
