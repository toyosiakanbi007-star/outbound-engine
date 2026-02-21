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
// - Store aggregate results in aggregate_results table
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
// 8. Load V3 prequal data (v3_hypotheses, v3_evidence, v3_analysis_runs)
// 9. Load Phase 0 offer-fit result (if available)
// 10. HTTP call Azure Function with mode="aggregate"
// 11. Store aggregated results in aggregate_results table
// 12. Update v3_analysis_runs with aggregate status
//
// V3 CHANGES:
// - Uses v3_hypotheses, v3_evidence, v3_analysis_runs tables instead of V2 tables
// - AggregateRequest now uses prequal_output structure matching aggregator_v3.py
// - Stores results in aggregate_results table
// - Includes Phase 0 ICP decision in aggregate request

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

/// Azure Function aggregate request (V3 format)
/// 
/// This structure matches what aggregator_v3.py expects in run_aggregator()
#[derive(Debug, Clone, Serialize)]
pub struct AggregateRequest {
    pub client_id: Uuid,
    pub company_id: Uuid,
    pub company_name: String,
    pub domain: String,
    pub mode: String,  // "aggregate"
    pub run_id: Uuid,
    
    // V3: Full prequal output structure (matches aggregator_v3.py expectations)
    pub prequal_output: JsonValue,  // Contains: analysis_run, hypotheses, evidence
    
    // Apollo enrichment data
    pub apollo_enrichment: JsonValue,
    pub employee_metrics_analysis: JsonValue,
    pub funding_analysis: JsonValue,
    
    // Client context (ICP profile, tech preferences, offer)
    pub client_context: JsonValue,
    
    // Optional: Phase 0 ICP result if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase0_icp: Option<JsonValue>,
}

/// Aggregate result from Azure Function (for storing)
#[derive(Debug, Clone, Deserialize)]
pub struct AggregateResultResponse {
    pub final_score: Option<f64>,
    pub tier: Option<String>,
    pub qualifies: Option<bool>,
    pub do_not_outreach: Option<bool>,
    pub needs_review: Option<bool>,
    pub score_breakdown: Option<JsonValue>,
    pub unified_hypotheses: Option<Vec<JsonValue>>,
    pub tech_context: Option<JsonValue>,
    pub final_offer_fit: Option<JsonValue>,
    pub contact_suggestions: Option<JsonValue>,
    pub decision_notes: Option<Vec<String>>,
    #[serde(rename = "_meta")]
    pub meta: Option<JsonValue>,
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

/// Load V3 prequal data for aggregate request
/// 
/// Returns: (hypotheses, evidence, analysis_run)
/// Uses V3 tables: v3_hypotheses, v3_evidence, v3_analysis_runs
pub async fn load_prequal_data_v3(
    pool: &PgPool,
    company_id: Uuid,
    run_id: Uuid,
) -> Result<(Vec<JsonValue>, Vec<JsonValue>, Option<JsonValue>), sqlx::Error> {
    // Load V3 hypotheses with full confidence breakdown
    let hypotheses: Vec<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'id', id,
            'hypothesis', hypothesis,
            'pain_category', pain_category,
            'pain_type', pain_type,
            'pain_subtags', pain_subtags,
            'raw_confidence', raw_confidence,
            'staleness_penalty', staleness_penalty,
            'corroboration_penalty', corroboration_penalty,
            'final_confidence', final_confidence,
            'corroborated', corroborated,
            'corroborated_by', corroborated_by,
            'corroboration_needed', corroboration_needed,
            'why_now_text', why_now_text,
            'why_now_type', why_now_type,
            'why_now_date', why_now_date,
            'why_now_days_until', why_now_days_until,
            'why_now_urgency', why_now_urgency,
            'evidence', evidence,
            'evidence_summary', evidence_summary,
            'offer_fit_strength', offer_fit_strength,
            'do_not_outreach', do_not_outreach,
            'recommended_personas', recommended_personas,
            'suggested_offer_fit', suggested_offer_fit,
            'strength', strength
        )
        FROM v3_hypotheses
        WHERE company_id = $1 AND run_id = $2
        ORDER BY final_confidence DESC
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    
    // Load V3 evidence with date/tier info
    let evidence: Vec<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'id', id,
            'url', url,
            'verbatim_quote', verbatim_quote,
            'extracted_date', extracted_date,
            'date_source', date_source,
            'date_confidence', date_confidence,
            'observed_at', observed_at,
            'days_ago', days_ago,
            'staleness_multiplier', staleness_multiplier,
            'source_tier', source_tier,
            'source_type', source_type,
            'source_domain', source_domain,
            'evidence_status', evidence_status,
            'can_drive_high_confidence', can_drive_high_confidence,
            'content_hash', content_hash,
            'is_syndication', is_syndication,
            'evidence_type', evidence_type,
            'pain_category', pain_category,
            'pain_subtags', pain_subtags,
            'pain_indicators', pain_indicators,
            'confidence', confidence
        )
        FROM v3_evidence
        WHERE company_id = $1 AND run_id = $2
        ORDER BY confidence DESC, source_tier ASC
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    
    // Load V3 analysis run summary
    let analysis_run: Option<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'run_id', run_id,
            'client_id', client_id,
            'company_id', company_id,
            'raw_score', raw_score,
            'staleness_adjusted_score', staleness_adjusted_score,
            'final_score', final_score,
            'evidence_summary', evidence_summary,
            'recency_summary', recency_summary,
            'gates', gates,
            'gates_passed', gates_passed,
            'gates_failed', gates_failed,
            'qualifies', qualifies,
            'hypothesis_count', hypothesis_count,
            'strong_fit_count', strong_fit_count,
            'do_not_outreach', do_not_outreach,
            'phase', phase,
            'started_at', started_at,
            'completed_at', completed_at,
            'duration_ms', duration_ms,
            'created_at', created_at
        )
        FROM v3_analysis_runs
        WHERE company_id = $1 AND run_id = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    
    Ok((
        hypotheses.into_iter().map(|(j,)| j).collect(),
        evidence.into_iter().map(|(j,)| j).collect(),
        analysis_run.map(|(j,)| j),
    ))
}

/// Load Phase 0 offer-fit result if available
pub async fn load_phase0_result(
    pool: &PgPool,
    company_id: Uuid,
    run_id: Uuid,
) -> Result<Option<JsonValue>, sqlx::Error> {
    let row: Option<(JsonValue,)> = sqlx::query_as(
        r#"
        SELECT json_build_object(
            'fit', CASE WHEN icp_fit = 'qualify' THEN true ELSE false END,
            'icp_fit', icp_fit,
            'score', confidence,
            'fit_strength', fit_strength,
            'reasons', reasons,
            'disqualify_reasons', disqualify_reasons,
            'missing_info', missing_info,
            'recommended_angles', recommended_angles,
            'needs_review', needs_review,
            'do_not_outreach', do_not_outreach,
            'pipeline_continued', pipeline_continued
        )
        FROM offer_fit_decisions
        WHERE company_id = $1 AND run_id = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(company_id)
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    
    Ok(row.map(|(j,)| j))
}

/// Load Apollo enrichment for aggregate request (V3 enhanced)
pub async fn load_apollo_enrichment_v3(
    pool: &PgPool,
    company_id: Uuid,
) -> Result<JsonValue, sqlx::Error> {
    let row: Option<(
        Option<String>,      // apollo_org_id
        Option<String>,      // description
        Option<String>,      // industry
        Option<i32>,         // estimated_num_employees
        Vec<String>,         // technologies
        Vec<String>,         // keywords
        Option<i32>,         // founded_year
        Option<i64>,         // annual_revenue
        Option<String>,      // linkedin_url
        Option<String>,      // city
        Option<String>,      // state
        Option<String>,      // country
        JsonValue,           // funding_events
        Option<i64>,         // total_funding_raised
        Option<String>,      // latest_funding_stage
        JsonValue,           // employee_metrics
        JsonValue,           // employee_metrics_analysis
        JsonValue,           // funding_analysis
    )> = sqlx::query_as(
        r#"
        SELECT 
            apollo_org_id,
            description,
            industry,
            estimated_num_employees,
            COALESCE(technologies, '{}') as technologies,
            COALESCE(keywords, '{}') as keywords,
            founded_year,
            annual_revenue,
            linkedin_url,
            city,
            state,
            country,
            COALESCE(funding_events, '[]'::jsonb) as funding_events,
            total_funding_raised,
            latest_funding_stage,
            COALESCE(employee_metrics, '[]'::jsonb) as employee_metrics,
            COALESCE(employee_metrics_analysis, '{}'::jsonb) as emp_analysis,
            COALESCE(funding_analysis, '{}'::jsonb) as fund_analysis
        FROM company_apollo_enrichment
        WHERE company_id = $1
        "#,
    )
    .bind(company_id)
    .fetch_optional(pool)
    .await?;
    
    match row {
        Some((
            apollo_org_id,
            description,
            industry,
            emp_count,
            technologies,
            keywords,
            founded_year,
            annual_revenue,
            linkedin_url,
            city,
            state,
            country,
            funding_events,
            total_funding,
            latest_stage,
            employee_metrics,
            emp_analysis,
            fund_analysis,
        )) => Ok(json!({
            "apollo_org_id": apollo_org_id,
            "description": description,
            "industry": industry,
            "employee_count": emp_count,
            "technologies": technologies,
            "keywords": keywords,
            "founded_year": founded_year,
            "annual_revenue": annual_revenue,
            "linkedin_url": linkedin_url,
            "city": city,
            "state": state,
            "country": country,
            "funding_events": funding_events,
            "total_funding_raised": total_funding,
            "latest_funding_stage": latest_stage,
            "employee_metrics": employee_metrics,
            "employee_metrics_analysis": emp_analysis,
            "funding_analysis": fund_analysis,
        })),
        None => Ok(json!({})),
    }
}

/// Load client context from client_icp_profiles table (preferred) or clients table
pub async fn load_client_context_v3(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<JsonValue, sqlx::Error> {
    // Try client_icp_profiles first (V3 preferred source)
    let icp_row: Option<(JsonValue, JsonValue)> = sqlx::query_as(
        r#"
        SELECT 
            COALESCE(icp_json, '{}'::jsonb) as icp,
            COALESCE(tech_preferences, '{}'::jsonb) as tech_prefs
        FROM client_icp_profiles
        WHERE client_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    
    if let Some((icp, tech_prefs)) = icp_row {
        return Ok(json!({
            "icp": icp,
            "tech_preferences": tech_prefs,
        }));
    }
    
    // Fallback to clients.config
    let client_row: Option<(Option<JsonValue>,)> = sqlx::query_as(
        r#"
        SELECT config
        FROM clients
        WHERE id = $1
        "#,
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    
    Ok(client_row.and_then(|(c,)| c).unwrap_or(json!({})))
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

/// Store aggregate result in aggregate_results table
pub async fn store_aggregate_result(
    pool: &PgPool,
    run_id: Uuid,
    company_id: Uuid,
    client_id: Uuid,
    result: &JsonValue,
) -> Result<(), sqlx::Error> {
    let final_score = result.get("final_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let tier = result.get("tier").and_then(|v| v.as_str()).unwrap_or("D");
    let qualifies = result.get("qualifies").and_then(|v| v.as_bool()).unwrap_or(false);
    let do_not_outreach = result.get("do_not_outreach").and_then(|v| v.as_bool()).unwrap_or(false);
    let needs_review = result.get("needs_review").and_then(|v| v.as_bool()).unwrap_or(false);
    
    let score_breakdown = result.get("score_breakdown").cloned().unwrap_or(json!({}));
    let unified_hypotheses = result.get("unified_hypotheses").cloned().unwrap_or(json!([]));
    let tech_context = result.get("tech_context").cloned().unwrap_or(json!({}));
    let final_offer_fit = result.get("final_offer_fit").cloned().unwrap_or(json!({}));
    let contact_suggestions = result.get("contact_suggestions").cloned().unwrap_or(json!({}));
    
    // Extract arrays
    let decision_notes: Vec<String> = result
        .get("decision_notes")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    
    let recommended_personas: Vec<String> = contact_suggestions
        .get("personas")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    
    let title_keywords: Vec<String> = contact_suggestions
        .get("title_keywords")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    
    let title_exact: Vec<String> = contact_suggestions
        .get("title_exact")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    
    // Extract metadata
    let meta = result.get("_meta").cloned().unwrap_or(json!({}));
    let llm_calls = meta.get("llm_calls").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let duration_ms = meta.get("duration_ms").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    
    sqlx::query(
        r#"
        INSERT INTO aggregate_results (
            run_id,
            company_id,
            client_id,
            final_score,
            tier,
            qualifies,
            do_not_outreach,
            needs_review,
            score_breakdown,
            output_json,
            unified_hypotheses,
            tech_fit,
            offer_fit,
            recommended_personas,
            title_keywords,
            title_exact,
            decision_notes,
            llm_calls,
            duration_ms,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW(), NOW())
        ON CONFLICT (run_id) DO UPDATE SET
            final_score = EXCLUDED.final_score,
            tier = EXCLUDED.tier,
            qualifies = EXCLUDED.qualifies,
            do_not_outreach = EXCLUDED.do_not_outreach,
            needs_review = EXCLUDED.needs_review,
            score_breakdown = EXCLUDED.score_breakdown,
            output_json = EXCLUDED.output_json,
            unified_hypotheses = EXCLUDED.unified_hypotheses,
            tech_fit = EXCLUDED.tech_fit,
            offer_fit = EXCLUDED.offer_fit,
            recommended_personas = EXCLUDED.recommended_personas,
            title_keywords = EXCLUDED.title_keywords,
            title_exact = EXCLUDED.title_exact,
            decision_notes = EXCLUDED.decision_notes,
            llm_calls = EXCLUDED.llm_calls,
            duration_ms = EXCLUDED.duration_ms,
            updated_at = NOW()
        "#,
    )
    .bind(run_id)
    .bind(company_id)
    .bind(client_id)
    .bind(final_score)
    .bind(tier)
    .bind(qualifies)
    .bind(do_not_outreach)
    .bind(needs_review)
    .bind(&score_breakdown)
    .bind(result)  // Full output_json
    .bind(&unified_hypotheses)
    .bind(&tech_context)
    .bind(&final_offer_fit)
    .bind(&recommended_personas)
    .bind(&title_keywords)
    .bind(&title_exact)
    .bind(&decision_notes)
    .bind(llm_calls)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    
    info!(
        "Stored aggregate result: run_id={} score={:.2} tier={} qualifies={}",
        run_id, final_score, tier, qualifies
    );
    
    Ok(())
}

/// Update v3_analysis_runs with aggregate status
pub async fn update_analysis_run_aggregate_status(
    pool: &PgPool,
    run_id: Uuid,
    status: &str,
    score: Option<f64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE v3_analysis_runs SET
            aggregate_completed_at = NOW(),
            aggregate_status = $2,
            aggregate_score = $3
        WHERE run_id = $1
        "#,
    )
    .bind(run_id)
    .bind(status)
    .bind(score)
    .execute(pool)
    .await?;
    
    Ok(())
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
        .timeout(std::time::Duration::from_secs(180))  // 3 min timeout for aggregate (5 LLM calls)
        .json(request);
    
    // Add function key if configured
    if !AZURE_FUNCTION_KEY.is_empty() {
        req_builder = req_builder.header("x-functions-key", AZURE_FUNCTION_KEY.as_str());
    }
    
    info!("Calling Azure Function aggregate: {}", url);
    
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

/// Handle PHASE_B_ENRICH_APOLLO job (V3 version)
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
                            // Update status to skipped
                            let _ = update_analysis_run_aggregate_status(
                                pool, 
                                payload.prequal_run_id, 
                                "skipped_no_apollo", 
                                None
                            ).await;
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
            let _ = update_analysis_run_aggregate_status(
                pool, 
                payload.prequal_run_id, 
                "failed_apollo_api", 
                None
            ).await;
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
    
    // Step 6: Load V3 prequal data
    info!("Worker {}: loading V3 prequal data", worker_id);
    let (prequal_hypotheses, prequal_evidence, analysis_run) = 
        load_prequal_data_v3(pool, payload.company_id, payload.prequal_run_id).await?;
    
    info!(
        "Worker {}: loaded {} hypotheses, {} evidence items",
        worker_id, prequal_hypotheses.len(), prequal_evidence.len()
    );
    
    // Build prequal_output structure for aggregator
    let prequal_output = json!({
        "analysis_run": analysis_run,
        "hypotheses": prequal_hypotheses,
        "evidence": prequal_evidence,
    });
    
    // Step 7: Load Apollo enrichment (full V3 version)
    let apollo_enrichment = load_apollo_enrichment_v3(pool, payload.company_id).await?;
    
    // Step 8: Load client context (with ICP profile)
    let client_context = load_client_context_v3(pool, payload.client_id).await?;
    
    // Step 9: Load Phase 0 result if available
    let phase0_icp = load_phase0_result(pool, payload.company_id, payload.prequal_run_id).await?;
    
    // Step 10: Build aggregate request
    info!("Worker {}: triggering Azure Function aggregate", worker_id);
    let aggregate_request = AggregateRequest {
        client_id: payload.client_id,
        company_id: payload.company_id,
        company_name: payload.company_name.clone(),
        domain: payload.domain.clone(),
        mode: "aggregate".to_string(),
        run_id: payload.prequal_run_id,
        prequal_output,
        apollo_enrichment,
        employee_metrics_analysis: serde_json::to_value(&emp_analysis)?,
        funding_analysis: serde_json::to_value(&fund_analysis)?,
        client_context,
        phase0_icp,
    };
    
    // Step 11: Call Azure Function
    let aggregate_result = match trigger_aggregate(http_client, &aggregate_request).await {
        Ok(result) => result,
        Err(e) => {
            error!("Worker {}: Azure Function aggregate failed: {}", worker_id, e);
            let _ = update_analysis_run_aggregate_status(
                pool, 
                payload.prequal_run_id, 
                "failed_aggregate", 
                None
            ).await;
            return Err(e.into());
        }
    };
    
    // Step 12: Store aggregate result
    let final_score = aggregate_result.get("final_score").and_then(|v| v.as_f64());
    let tier = aggregate_result.get("tier").and_then(|v| v.as_str()).unwrap_or("D");
    let qualifies = aggregate_result.get("qualifies").and_then(|v| v.as_bool()).unwrap_or(false);
    let unified_count = aggregate_result
        .get("unified_hypotheses")
        .and_then(|h| h.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    
    // Store in aggregate_results table
    if let Err(e) = store_aggregate_result(
        pool,
        payload.prequal_run_id,
        payload.company_id,
        payload.client_id,
        &aggregate_result,
    ).await {
        error!("Worker {}: failed to store aggregate result: {:?}", worker_id, e);
        // Don't fail the job, just log
    }
    
    // Update v3_analysis_runs with aggregate status
    if let Err(e) = update_analysis_run_aggregate_status(
        pool,
        payload.prequal_run_id,
        "success",
        final_score,
    ).await {
        error!("Worker {}: failed to update analysis run status: {:?}", worker_id, e);
    }
    
    info!(
        "Worker {}: Phase B complete for company {} (score={:.2}, tier={}, qualifies={}, unified_hypotheses={})",
        worker_id,
        payload.company_id,
        final_score.unwrap_or(0.0),
        tier,
        qualifies,
        unified_count
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
