// src/routes/companies.rs
//
// Company endpoints: list/search, hybrid detail, lazy sub-resources, actions.
//
// Data is returned as raw JSON from PostgreSQL using row_to_json/json_agg
// to avoid struct mismatch with evolving table schemas.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::FromRow;
use tracing::{error, info};
use uuid::Uuid;

use crate::AppState;

// ============================================================================
// Models (only for list/detail — sub-resources use raw JSON)
// ============================================================================

#[derive(Debug, Serialize, FromRow)]
pub struct CompanyRow {
    pub id: Uuid,
    pub client_id: Uuid,
    pub name: String,
    pub domain: Option<String>,
    pub industry: Option<String>,
    pub employee_count: Option<i32>,
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub source: Option<String>,
    pub linkedin_url: Option<String>,
    pub website_url: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct CompanyListItem {
    pub id: Uuid,
    pub client_id: Uuid,
    pub name: String,
    pub domain: Option<String>,
    pub industry: Option<String>,
    pub employee_count: Option<i32>,
    pub country: Option<String>,
    pub source: Option<String>,
    pub latest_status: Option<String>,
    pub latest_score: Option<f64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct CandidateSummary {
    pub id: Uuid,
    pub run_id: Uuid,
    pub status: String,
    pub industry: Option<String>,
    pub variant: Option<String>,
    pub was_duplicate: bool,
    pub prequal_attempts: i32,
    pub prequal_completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct PrequalSummary {
    pub id: Uuid,
    pub run_id: Uuid,
    pub score: f64,
    pub qualifies: bool,
    pub gates_passed: Option<Vec<String>>,
    pub gates_failed: Option<Vec<String>>,
    pub why_now_indicators: Option<Vec<String>>,
    pub evidence_count: Option<i32>,
    pub distinct_sources: Option<i32>,
    pub prequal_reasons: Option<JsonValue>,
    pub offer_fit_tags: Option<JsonValue>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct ListCompaniesQuery {
    pub client_id: Option<Uuid>,
    pub status: Option<String>,
    pub industry: Option<String>,
    pub search: Option<String>,
    pub min_score: Option<f64>,
    pub max_score: Option<f64>,
    /// Tier filter: "1" (score >= 0.75), "2" (0.50-0.74), "3" (< 0.50)
    pub tier: Option<String>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
    #[serde(default = "default_sort_by")]
    pub sort_by: String,
    #[serde(default = "default_sort_order")]
    pub sort_order: String,
}

fn default_page() -> i64 { 1 }
fn default_per_page() -> i64 { 50 }
fn default_sort_by() -> String { "created_at".into() }
fn default_sort_order() -> String { "desc".into() }

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/companies — list/search companies with latest prequal status
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListCompaniesQuery>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let offset = (params.page - 1).max(0) * params.per_page;
    let search_pattern = params.search.as_ref().map(|s| format!("%{}%", s));

    // Resolve tier to score range (tier overrides min/max_score if set)
    let (min_score, max_score) = match params.tier.as_deref() {
        Some("1") => (Some(0.75_f64), None),                 // Tier 1: score >= 0.75
        Some("2") => (Some(0.50_f64), Some(0.7499_f64)),     // Tier 2: 0.50 - 0.74
        Some("3") => (None, Some(0.4999_f64)),                // Tier 3: < 0.50
        _ => (params.min_score, params.max_score),
    };

    let companies = sqlx::query_as::<_, CompanyListItem>(
        r#"SELECT DISTINCT ON (c.id)
                  c.id, c.client_id, c.name, c.domain, c.industry,
                  c.employee_count, c.country, c.source,
                  cc.status AS latest_status,
                  cp.score AS latest_score,
                  c.created_at
           FROM companies c
           LEFT JOIN company_candidates cc ON cc.company_id = c.id
               AND cc.id = (SELECT id FROM company_candidates WHERE company_id = c.id ORDER BY created_at DESC LIMIT 1)
           LEFT JOIN company_prequal cp ON cp.company_id = c.id
               AND cp.id = (SELECT id FROM company_prequal WHERE company_id = c.id ORDER BY created_at DESC LIMIT 1)
           WHERE ($1::uuid IS NULL OR c.client_id = $1)
             AND ($2::text IS NULL OR cc.status = $2)
             AND ($3::text IS NULL OR c.industry = $3)
             AND ($4::text IS NULL OR c.name ILIKE $4 OR c.domain ILIKE $4)
             AND ($5::float8 IS NULL OR cp.score >= $5)
             AND ($6::float8 IS NULL OR cp.score <= $6)
           ORDER BY c.id, c.created_at DESC
           LIMIT $7 OFFSET $8"#,
    )
    .bind(params.client_id)
    .bind(params.status.as_deref())
    .bind(params.industry.as_deref())
    .bind(search_pattern.as_deref())
    .bind(min_score)
    .bind(max_score)
    .bind(params.per_page)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to list companies: {:?}", e);
        db_error("Failed to list companies")
    })?;

    Ok(Json(json!({
        "data": companies,
        "meta": { "page": params.page, "per_page": params.per_page }
    })))
}

/// GET /api/companies/:id — hybrid: basic company + latest candidate + latest prequal
pub async fn get(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let company = sqlx::query_as::<_, CompanyRow>(
        r#"SELECT id, client_id, name, domain, industry, employee_count,
                  country, region, city, source, linkedin_url, website_url,
                  created_at, updated_at
           FROM companies WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch company: {:?}", e);
        db_error("Failed to fetch company")
    })?
    .ok_or_else(|| not_found("Company not found"))?;

    let latest_candidate = sqlx::query_as::<_, CandidateSummary>(
        r#"SELECT id, run_id, status, industry, variant, was_duplicate,
                  prequal_attempts, prequal_completed_at, created_at
           FROM company_candidates
           WHERE company_id = $1
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let latest_prequal = sqlx::query_as::<_, PrequalSummary>(
        r#"SELECT id, run_id, score, qualifies, gates_passed, gates_failed,
                  why_now_indicators, evidence_count, distinct_sources,
                  prequal_reasons, offer_fit_tags, created_at
           FROM company_prequal
           WHERE company_id = $1
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let run_history_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM company_candidates WHERE company_id = $1",
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .unwrap_or(0);

    // Also fetch offer_fit and snapshot for the overview tab
    let offer_fit: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT row_to_json(t) FROM (
            SELECT icp_fit, fit_strength, reasons, disqualify_reasons,
                   missing_info, confidence, recommended_angles,
                   needs_review, do_not_outreach, created_at
            FROM offer_fit_decisions
            WHERE company_id = $1
            ORDER BY created_at DESC LIMIT 1
        ) t"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let snapshot: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT row_to_json(t) FROM (
            SELECT company_name, domain, hq_location, industry_guess,
                   business_model_guess, what_they_sell, who_they_sell_to,
                   gtm_motion, keywords, signals_of_client_product_need,
                   snapshot_version, created_at
            FROM company_snapshots
            WHERE company_id = $1
            ORDER BY created_at DESC LIMIT 1
        ) t"#,
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    Ok(Json(json!({
        "data": {
            "company": company,
            "latest_candidate": latest_candidate,
            "latest_prequal": latest_prequal,
            "offer_fit": offer_fit,
            "snapshot": snapshot,
            "run_history_count": run_history_count,
        }
    })))
}

/// GET /api/companies/:id/hypotheses — from v3_hypotheses table
///
/// Uses json_agg(row_to_json(...)) to return complete rows as JSON,
/// avoiding struct mismatch with evolving schema.
pub async fn hypotheses(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let result: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT COALESCE(json_agg(t ORDER BY t.confidence DESC), '[]'::json)
           FROM (
               SELECT
                   hypothesis, pain_category, pain_type, pain_subtags,
                   raw_confidence, staleness_penalty, corroboration_penalty,
                   final_confidence AS confidence,
                   corroborated, corroborated_by, corroboration_needed,
                   why_now_text AS why_now, why_now_type, why_now_date, why_now_urgency,
                   evidence, evidence_summary,
                   offer_fit_strength, do_not_outreach,
                   recommended_personas, suggested_offer_fit,
                   strength, phase, created_at
               FROM v3_hypotheses
               WHERE company_id = $1
           ) t"#,
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch hypotheses: {:?}", e);
        db_error("Failed to fetch hypotheses")
    })?;

    Ok(Json(json!({ "data": result.unwrap_or(json!([])) })))
}

/// GET /api/companies/:id/news — from company_news table
pub async fn news(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let result: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT COALESCE(json_agg(t ORDER BY t.date DESC NULLS LAST), '[]'::json)
           FROM (
               SELECT
                   company_id, title,
                   published_at AS date,
                   url, summary, source_type, source_name,
                   tags, confidence, location
               FROM company_news
               WHERE company_id = $1
           ) t"#,
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch news: {:?}", e);
        db_error("Failed to fetch news")
    })?;

    Ok(Json(json!({ "data": result.unwrap_or(json!([])) })))
}

/// GET /api/companies/:id/evidence — from v3_evidence table
pub async fn evidence(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let result: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT COALESCE(json_agg(t ORDER BY t.source_tier ASC, t.days_ago ASC NULLS LAST), '[]'::json)
           FROM (
               SELECT
                   url, verbatim_quote, source_type, source_tier,
                   extracted_date AS date, days_ago, staleness_multiplier,
                   pain_indicators, created_at
               FROM v3_evidence
               WHERE company_id = $1
           ) t"#,
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch evidence: {:?}", e);
        db_error("Failed to fetch evidence")
    })?;

    Ok(Json(json!({ "data": result.unwrap_or(json!([])) })))
}

/// GET /api/companies/:id/discovered-urls
pub async fn discovered_urls(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let result: Option<JsonValue> = sqlx::query_scalar(
        r#"SELECT COALESCE(json_agg(t ORDER BY t.rank_score DESC NULLS LAST), '[]'::json)
           FROM (
               SELECT url, source_type, lane, rank_score, title, discovered_at
               FROM discovered_urls
               WHERE company_id = $1
           ) t"#,
    )
    .bind(id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch discovered URLs: {:?}", e);
        db_error("Failed to fetch discovered URLs")
    })?;

    Ok(Json(json!({ "data": result.unwrap_or(json!([])) })))
}

/// GET /api/companies/:id/candidates — all candidate records (cross-run)
pub async fn candidates(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    let rows = sqlx::query_as::<_, CandidateSummary>(
        r#"SELECT id, run_id, status, industry, variant, was_duplicate,
                  prequal_attempts, prequal_completed_at, created_at
           FROM company_candidates
           WHERE company_id = $1
           ORDER BY created_at DESC"#,
    )
    .bind(id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        error!("Failed to fetch candidates: {:?}", e);
        db_error("Failed to fetch candidates")
    })?;

    Ok(Json(json!({ "data": rows })))
}

/// POST /api/companies/:id/actions/rerun-prequal
pub async fn rerun_prequal(
    State(state): State<AppState>,
    Path(company_id): Path<Uuid>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    let client_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT client_id FROM companies WHERE id = $1",
    )
    .bind(company_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("Failed to find company: {:?}", e); db_error("DB error") })?
    .ok_or_else(|| not_found("Company not found"))?;

    // Reset latest candidate to 'new' so dispatch picks it up
    let affected = sqlx::query(
        r#"UPDATE company_candidates
           SET status = 'new', prequal_started_at = NULL, prequal_last_error = NULL
           WHERE id = (
               SELECT id FROM company_candidates
               WHERE company_id = $1 AND client_id = $2
                 AND status IN ('qualified', 'disqualified', 'prequal_queued')
               ORDER BY created_at DESC LIMIT 1
           )"#,
    )
    .bind(company_id)
    .bind(client_id)
    .execute(&state.db)
    .await
    .map_err(|e| { error!("Failed to reset candidate: {:?}", e); db_error("Failed to reset") })?;

    info!("Reset prequal for company {} ({} rows)", company_id, affected.rows_affected());

    Ok((StatusCode::ACCEPTED, Json(json!({
        "data": { "company_id": company_id, "rows_affected": affected.rows_affected(),
                  "message": "Candidate reset — will be picked up by next prequal dispatch" }
    }))))
}

/// POST /api/companies/:id/actions/rerun-fetch
pub async fn rerun_fetch(
    State(state): State<AppState>,
    Path(company_id): Path<Uuid>,
) -> Result<(StatusCode, Json<JsonValue>), (StatusCode, Json<JsonValue>)> {
    let client_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT client_id FROM companies WHERE id = $1",
    )
    .bind(company_id)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| { error!("Failed to find company: {:?}", e); db_error("DB error") })?
    .ok_or_else(|| not_found("Company not found"))?;

    let payload = json!({ "client_id": client_id, "company_id": company_id });

    let job_id = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'fetch_news', $2, 'pending', NOW()) RETURNING id"#,
    )
    .bind(client_id)
    .bind(&payload)
    .fetch_one(&state.db)
    .await
    .map_err(|e| { error!("Failed to enqueue: {:?}", e); db_error("Failed to enqueue") })?;

    info!("Enqueued fetch_news job {} for company {}", job_id, company_id);

    Ok((StatusCode::ACCEPTED, Json(json!({
        "data": { "job_id": job_id, "message": "News fetch job enqueued" }
    }))))
}

/// DELETE /api/companies/:id — delete a company and all related data
pub async fn delete(
    State(state): State<AppState>,
    Path(company_id): Path<Uuid>,
) -> Result<Json<JsonValue>, (StatusCode, Json<JsonValue>)> {
    // Companies table has ON DELETE CASCADE on most FK relationships,
    // but we explicitly clean up to be safe and provide counts.
    let mut tx = state.db.begin().await.map_err(|e| {
        error!("Failed to start transaction: {:?}", e);
        db_error("Failed to start transaction")
    })?;

    // Delete related data (CASCADE handles most, but explicit for counts)
    let candidates_deleted = sqlx::query("DELETE FROM company_candidates WHERE company_id = $1")
        .bind(company_id).execute(&mut *tx).await.map(|r| r.rows_affected()).unwrap_or(0);
    let prequal_deleted = sqlx::query("DELETE FROM company_prequal WHERE company_id = $1")
        .bind(company_id).execute(&mut *tx).await.map(|r| r.rows_affected()).unwrap_or(0);
    let hypotheses_deleted = sqlx::query("DELETE FROM v3_hypotheses WHERE company_id = $1")
        .bind(company_id).execute(&mut *tx).await.map(|r| r.rows_affected()).unwrap_or(0);
    let evidence_deleted = sqlx::query("DELETE FROM v3_evidence WHERE company_id = $1")
        .bind(company_id).execute(&mut *tx).await.map(|r| r.rows_affected()).unwrap_or(0);
    let news_deleted = sqlx::query("DELETE FROM company_news WHERE company_id = $1")
        .bind(company_id).execute(&mut *tx).await.map(|r| r.rows_affected()).unwrap_or(0);

    // Delete the company itself
    let company_deleted = sqlx::query("DELETE FROM companies WHERE id = $1")
        .bind(company_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            error!("Failed to delete company: {:?}", e);
            db_error("Failed to delete company")
        })?;

    if company_deleted.rows_affected() == 0 {
        return Err(not_found("Company not found"));
    }

    tx.commit().await.map_err(|e| {
        error!("Failed to commit delete: {:?}", e);
        db_error("Failed to commit delete")
    })?;

    info!("Deleted company {} (candidates={}, prequal={}, hypotheses={}, evidence={}, news={})",
        company_id, candidates_deleted, prequal_deleted, hypotheses_deleted, evidence_deleted, news_deleted);

    Ok(Json(json!({
        "data": {
            "company_id": company_id,
            "deleted": true,
            "counts": {
                "candidates": candidates_deleted,
                "prequal": prequal_deleted,
                "hypotheses": hypotheses_deleted,
                "evidence": evidence_deleted,
                "news": news_deleted,
            }
        }
    })))
}

fn db_error(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": { "code": "db_error", "message": msg } })))
}

fn not_found(msg: &str) -> (StatusCode, Json<JsonValue>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": { "code": "not_found", "message": msg } })))
}
