// src/jobs/onboarding/generate_drafts.rs
//
// Job 3: ONBOARDING_GENERATE_DRAFTS
//
// Takes the knowledge_pack from the run, calls Azure OpenAI to generate:
//   1. draft client_configs.config
//   2. draft client_configs.prequal_config
//   3. draft client_icp_profiles.icp_json
//   4. review notes (assumptions, missing info, confidence)
//   5. brand profile (logo, socials, summary)
//
// Stores everything as artifacts + on the run record.

use reqwest::Client as HttpClient;
use serde_json::{json, Value as JsonValue};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::llm_client;
use super::models::*;

pub async fn run_generate_drafts(
    pool: &PgPool,
    http_client: &HttpClient,
    payload: GenerateDraftsPayload,
    worker_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let run_id = payload.run_id;
    let client_id = payload.client_id;

    info!("Worker {}: generating drafts for run {}", worker_id, run_id);

    // Load knowledge pack from run
    let kp_json: Option<JsonValue> = sqlx::query_scalar(
        "SELECT knowledge_pack FROM client_onboarding_runs WHERE id = $1"
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;

    let kp_json = kp_json.ok_or("No knowledge pack found on run — enrich step may have failed")?;

    let knowledge_pack: KnowledgePack = serde_json::from_value(kp_json.clone())
        .map_err(|e| format!("Failed to parse knowledge pack: {}", e))?;

    // ========================================================================
    // Build the LLM prompt
    // ========================================================================
    let system_prompt = build_system_prompt();
    let user_prompt = build_user_prompt(&knowledge_pack);

    info!("Worker {}: calling Azure OpenAI for draft generation (prompt ~{} chars)",
        worker_id, user_prompt.len());

    // Call LLM
    let result = llm_client::chat_completion_json(
        http_client,
        &system_prompt,
        &user_prompt,
        30000, // large output for all 3 configs + review notes + brand profile
    ).await?;

    let llm_model = llm_client::model_name();

    // ========================================================================
    // Extract the 5 outputs
    // ========================================================================
    let draft_config = result.get("draft_config").cloned().unwrap_or(json!({}));
    let draft_prequal_config = result.get("draft_prequal_config").cloned().unwrap_or(json!({}));
    let draft_icp = result.get("draft_icp").cloned().unwrap_or(json!({}));
    let review_notes = result.get("review_notes").cloned().unwrap_or(json!({}));

    // Build brand profile from Diffbot + LLM
    let brand_profile = build_brand_profile(&knowledge_pack, &result);

    // ========================================================================
    // Store artifacts
    // ========================================================================
    store_artifact(pool, run_id, "draft_config", &draft_config).await;
    store_artifact(pool, run_id, "draft_prequal_config", &draft_prequal_config).await;
    store_artifact(pool, run_id, "draft_icp", &draft_icp).await;
    store_artifact(pool, run_id, "review_notes", &review_notes).await;

    // ========================================================================
    // Update run with drafts
    // ========================================================================
    sqlx::query(
        r#"UPDATE client_onboarding_runs SET
            draft_config = $1,
            draft_prequal_config = $2,
            draft_icp = $3,
            review_notes = $4,
            brand_profile = $5,
            llm_model = $6,
            updated_at = NOW()
           WHERE id = $7"#
    )
    .bind(&draft_config)
    .bind(&draft_prequal_config)
    .bind(&draft_icp)
    .bind(&review_notes)
    .bind(&brand_profile)
    .bind(&llm_model)
    .bind(run_id)
    .execute(pool)
    .await?;

    // ========================================================================
    // Enqueue FINALIZE
    // ========================================================================
    let next_payload = FinalizeDraftPayload { client_id, run_id };

    sqlx::query(
        r#"INSERT INTO jobs (client_id, job_type, payload, status, run_at)
           VALUES ($1, 'onboarding_finalize_draft', $2, 'pending', NOW())"#,
    )
    .bind(client_id)
    .bind(json!(next_payload))
    .execute(pool)
    .await?;

    info!("Worker {}: drafts generated for run {}, finalize enqueued", worker_id, run_id);

    Ok(())
}

// ============================================================================
// Prompt construction
// ============================================================================

fn build_system_prompt() -> String {
    r##"You are an expert B2B outbound strategist and sales engineer. Your job is to analyze a company's product, positioning, and market to generate configuration for an automated outbound prospecting engine.

You will receive a "knowledge pack" containing:
- Company name and domain
- Diffbot organization profile (if available)
- Summaries of 10-15 pages from the company's website
- Operator notes (if any)

You must generate 4 JSON objects:

## 1. draft_config
Configuration for the outbound engine. Include:
- brand_name: the company's actual brand name
- niche: 1-2 sentence description of their niche/category
- offer: 2-3 sentence description of what they sell and the core value proposition
- tone: recommended outreach tone (professional, consultative, casual-expert, etc.)
- calendar_link: "[PLACEHOLDER - operator must set]"
- sender_name: "[PLACEHOLDER - operator must set]"
- sender_email: "[PLACEHOLDER - operator must set]"
- offer_capabilities: array of specific capabilities/features they offer (be thorough, 5-15 items)
- outreach_angles: array of 3-5 potential outreach angles based on their product
- signal_preferences: { strong_signals: [...], weak_signals: [...] }
- source_preferences: { prioritize: [...], deprioritize: [...] }

### Pain Category Fit Classification
Based on the company's actual product/service, classify which pain categories are strong, medium, or weak fit:
- strong_fit_categories: array of pain categories where the company's product DIRECTLY solves the pain (e.g., a surveillance vendor → "security_compliance" is strong)
- medium_fit_categories: array of pain categories where the product has INDIRECT relevance
- weak_fit_categories: array of pain categories where the product has NO meaningful connection
Available categories: reliability_uptime, operational_visibility, cost_pressure, scaling_capacity, workflow_efficiency, sales_pipeline, customer_churn, security_compliance, tech_debt_migration, talent_execution_gaps, competitive_pressure, market_expansion

### Technology Preferences
Based on the company's integrations, product docs, and case studies, infer:
- technology_preferences:
  - must_support: array of technologies a prospect MUST use for the product to be relevant (e.g., ["Salesforce", "HubSpot"] for a CRM integration tool)
  - nice_to_have: array of technologies that indicate a good fit but aren't required
  - avoid: array of technologies that signal the prospect is unlikely to buy (e.g., they already use a direct competitor)
  - competitor_tech: object mapping competitor names to their product names (e.g., {"Competitor A": "CompProduct"})
  - confidence: 0.0-1.0 how confident you are in these tech preferences
  - evidence_urls: array of URLs from the knowledge pack that informed these preferences

## 2. draft_prequal_config
Sensible defaults for the prequal pipeline:
- autopilot_enabled: true
- batch_size: 10
- max_batches_per_dispatch: 5
- dispatch_interval_minutes: 30
- max_attempts_per_company: 3
- azure_function_timeout_secs: 300

Adjust batch_size and dispatch_interval based on how niche/broad the ICP is.

## 3. draft_icp
Ideal Customer Profile for discovery and qualification:
- target_roles: array of 5-8 likely buyer/decision-maker titles
- preferred_personas: array of 3-5 ideal personas to reach out to (e.g., "VP of Security", "Head of Compliance")
- non_preferred_personas: array of personas to AVOID (e.g., "Intern", "Junior Developer")
- target_profile:
  - industries: array of objects with { name, priority (1-3), diffbot_category, include_keywords_any, exclude_keywords_any }
  - company_sizes: array of size bands like "51-200", "201-500"
  - locations: array of countries/regions
  - excluded_locations: array if needed
- must_have_any: array of 3-5 requirements (at least one must be true)
- strong_signals: array of high-value signals to look for
- weak_signals: array of lower-value signals
- disqualify_if: array of automatic disqualifiers
- pain_categories_priority: array of ALL relevant pain categories ordered by relevance to this client's product. Most relevant first. This drives scoring weights.
- discovery_keywords: array of keyword families useful for finding prospects (e.g., ["compliance automation", "trade surveillance", "regulatory reporting"])
- exclusion_keywords: array of keywords that indicate a bad fit

## 4. review_notes
For the human operator reviewing the output:
- assumptions: array of assumptions you made
- missing_information: array of things you couldn't determine
- uncertain_sections: array of sections with low confidence
- recommended_review_points: array of things the operator should verify
- confidence_by_section: { config: 0.0-1.0, prequal: 0.0-1.0, icp: 0.0-1.0, tech_preferences: 0.0-1.0 }
- icp_rationale: 2-3 sentences explaining why you chose this ICP
- tech_preferences_rationale: 1-2 sentences explaining how you inferred technology preferences

## Rules
- Base EVERYTHING on evidence from the knowledge pack
- Do NOT invent data not supported by evidence
- If information is missing, use reasonable defaults and flag in review_notes
- Industries must be specific and evidence-based, not generic
- Pain categories must relate to actual product capabilities
- Disqualify rules should prevent wasting credits on bad fits
- Technology preferences should come from integrations pages, docs, case studies, partner pages, or job posts
- Every section must be practically useful for B2B outbound
- strong_fit_categories MUST match the company's actual product — do not use global defaults

Respond with a single JSON object containing all 4 keys: draft_config, draft_prequal_config, draft_icp, review_notes."##.to_string()
}

fn build_user_prompt(kp: &KnowledgePack) -> String {
    let mut parts = Vec::new();

    parts.push(format!("# Company: {} ({})", kp.company_name, kp.domain));

    // Operator notes
    if let Some(ref note) = kp.operator_note {
        parts.push(format!("\n## Operator Notes\n{}", note));
    }
    if let Some(ref country) = kp.target_country {
        parts.push(format!("Target country: {}", country));
    }
    if let Some(ref comps) = kp.known_competitors {
        parts.push(format!("Known competitors: {}", comps.join(", ")));
    }

    // Diffbot profile
    if let Some(ref profile) = kp.diffbot_profile {
        parts.push("\n## Diffbot Organization Profile".to_string());
        if let Some(ref name) = profile.name {
            parts.push(format!("Name: {}", name));
        }
        if let Some(ref desc) = profile.description {
            parts.push(format!("Description: {}", &desc[..desc.len().min(500)]));
        }
        if !profile.industries.is_empty() {
            parts.push(format!("Industries: {}", profile.industries.join(", ")));
        }
        if !profile.categories.is_empty() {
            parts.push(format!("Categories: {}", profile.categories.join(", ")));
        }
        if let Some(count) = profile.employee_count {
            parts.push(format!("Employees: ~{}", count));
        }
        if let Some(ref loc) = profile.location {
            parts.push(format!("Location: {}", loc));
        }
        if !profile.competitors.is_empty() {
            let comp_names: Vec<&str> = profile.competitors.iter().map(|c| c.name.as_str()).collect();
            parts.push(format!("Competitors: {}", comp_names.join(", ")));
        }
    }

    // Crawled pages
    parts.push("\n## Website Pages Analyzed".to_string());
    for page in &kp.crawled_pages {
        if page.fetch_error.is_some() { continue; }

        parts.push(format!("\n### {} ({})", page.page_type, page.url));
        if let Some(ref summary) = page.summary {
            parts.push(format!("Summary: {}", summary));
        }
        if !page.product_claims.is_empty() {
            parts.push(format!("Product claims: {}", page.product_claims.join("; ")));
        }
        if !page.target_buyer_clues.is_empty() {
            parts.push(format!("Buyer clues: {}", page.target_buyer_clues.join("; ")));
        }
        if !page.industry_clues.is_empty() {
            parts.push(format!("Industry clues: {}", page.industry_clues.join("; ")));
        }
        if !page.use_case_clues.is_empty() {
            parts.push(format!("Use cases: {}", page.use_case_clues.join("; ")));
        }
        if !page.integration_clues.is_empty() {
            parts.push(format!("Integrations: {}", page.integration_clues.join("; ")));
        }
        if !page.security_compliance_clues.is_empty() {
            parts.push(format!("Security/compliance: {}", page.security_compliance_clues.join("; ")));
        }
        if !page.gtm_motion_clues.is_empty() {
            parts.push(format!("GTM motion: {}", page.gtm_motion_clues.join("; ")));
        }
    }

    parts.join("\n")
}

// ============================================================================
// Brand profile
// ============================================================================

fn build_brand_profile(kp: &KnowledgePack, llm_result: &JsonValue) -> JsonValue {
    let mut bp = json!({
        "company_name": kp.company_name,
        "domain": kp.domain,
    });

    if let Some(ref profile) = kp.diffbot_profile {
        if let Some(ref logo) = profile.logo {
            bp["logo_url"] = json!(logo);
        }
        if let Some(ref desc) = profile.description {
            bp["description"] = json!(desc);
        }
        if let Some(ref short) = profile.short_description {
            bp["short_description"] = json!(short);
        }
        if let Some(count) = profile.employee_count {
            bp["employee_count"] = json!(count);
        }
        if let Some(ref loc) = profile.location {
            bp["location"] = json!(loc);
        }
        if let Some(ref founding) = profile.founding_date {
            bp["founded"] = json!(founding);
        }
        if !profile.social_links.is_empty() {
            bp["social_links"] = serde_json::to_value(&profile.social_links).unwrap_or(json!([]));
        }
        if !profile.competitors.is_empty() {
            bp["competitors"] = serde_json::to_value(&profile.competitors).unwrap_or(json!([]));
        }
        if !profile.industries.is_empty() {
            bp["industries"] = json!(profile.industries);
        }
    }

    // Add niche from draft config if available
    if let Some(niche) = llm_result["draft_config"]["niche"].as_str() {
        bp["niche"] = json!(niche);
    }

    bp
}

// ============================================================================
// Helpers
// ============================================================================

async fn store_artifact(pool: &PgPool, run_id: Uuid, artifact_type: &str, content: &JsonValue) {
    if let Err(e) = sqlx::query(
        r#"INSERT INTO client_onboarding_artifacts (run_id, artifact_type, content_json)
           VALUES ($1, $2, $3)"#
    )
    .bind(run_id)
    .bind(artifact_type)
    .bind(content)
    .execute(pool)
    .await
    {
        error!("Failed to store {} artifact: {:?}", artifact_type, e);
    }
}
