// src/jobs/company_fetcher/enrichment.rs
//
// Upsert enrichment data into the `company_apollo_enrichment` table.
//
// This module is provider-agnostic — it accepts EnrichmentData (built by
// diffbot_converter or a future Apollo enrichment converter) and writes it
// to the existing table schema from migrations 0015 + 0016.
//
// TABLE SCHEMA (for reference):
//   company_apollo_enrichment (
//     id UUID PK,
//     company_id UUID NOT NULL UNIQUE,
//     apollo_org_id VARCHAR(100),
//     description TEXT,
//     industry VARCHAR(200),
//     estimated_num_employees INT,
//     technologies TEXT[],
//     keywords TEXT[],
//     founded_year INT,
//     annual_revenue BIGINT,
//     linkedin_url VARCHAR(500),
//     city VARCHAR(200),
//     state VARCHAR(200),
//     country VARCHAR(200),
//     funding_events JSONB DEFAULT '[]',
//     total_funding_raised BIGINT,
//     last_funding_date DATE,
//     last_funding_type VARCHAR(100),
//     latest_funding_stage VARCHAR(100),
//     employee_metrics JSONB DEFAULT '[]',
//     employee_metrics_analysis JSONB DEFAULT '{}',
//     funding_analysis JSONB DEFAULT '{}',
//     fetched_at TIMESTAMPTZ DEFAULT NOW(),
//     analyzed_at TIMESTAMPTZ,
//   )
//
// UPSERT KEY: company_id (UNIQUE constraint)
// When a row already exists, we refresh all enrichment columns + fetched_at.

use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::{debug, warn};
use uuid::Uuid;

use super::providers::diffbot_converter::EnrichmentData;

// ============================================================================
// Public API
// ============================================================================

/// Upsert an enrichment record for a company.
///
/// The company must already exist in the `companies` table (the company_fetcher
/// dedup pipeline handles this before we get here).
///
/// If company_id is not yet known, use `upsert_enrichment_by_external_id` which
/// looks up the company first.
///
/// ARGS:
///   pool      — Postgres connection pool
///   client_id — Client scope (for logging only; not in enrichment table)
///   company_id — FK to companies.id
///   data      — Enrichment data built by the converter
pub async fn upsert_enrichment_for_company(
    pool: &PgPool,
    company_id: Uuid,
    data: &EnrichmentData,
) -> Result<Uuid, sqlx::Error> {
    let new_id = Uuid::new_v4();

    // Parse last_funding_date string → NaiveDate (if present)
    let funding_date: Option<NaiveDate> = data
        .last_funding_date
        .as_ref()
        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

    let result = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO company_apollo_enrichment (
            id,
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
            last_funding_date,
            last_funding_type,
            latest_funding_stage,
            employee_metrics,
            employee_metrics_analysis,
            funding_analysis,
            match_metadata,
            fetched_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23, NOW()
        )
        ON CONFLICT (company_id) DO UPDATE SET
            apollo_org_id            = COALESCE(EXCLUDED.apollo_org_id, company_apollo_enrichment.apollo_org_id),
            description              = COALESCE(EXCLUDED.description, company_apollo_enrichment.description),
            industry                 = COALESCE(EXCLUDED.industry, company_apollo_enrichment.industry),
            estimated_num_employees  = COALESCE(EXCLUDED.estimated_num_employees, company_apollo_enrichment.estimated_num_employees),
            technologies             = COALESCE(EXCLUDED.technologies, company_apollo_enrichment.technologies),
            keywords                 = COALESCE(EXCLUDED.keywords, company_apollo_enrichment.keywords),
            founded_year             = COALESCE(EXCLUDED.founded_year, company_apollo_enrichment.founded_year),
            annual_revenue           = COALESCE(EXCLUDED.annual_revenue, company_apollo_enrichment.annual_revenue),
            linkedin_url             = COALESCE(EXCLUDED.linkedin_url, company_apollo_enrichment.linkedin_url),
            city                     = COALESCE(EXCLUDED.city, company_apollo_enrichment.city),
            state                    = COALESCE(EXCLUDED.state, company_apollo_enrichment.state),
            country                  = COALESCE(EXCLUDED.country, company_apollo_enrichment.country),
            funding_events           = COALESCE(EXCLUDED.funding_events, company_apollo_enrichment.funding_events),
            total_funding_raised     = COALESCE(EXCLUDED.total_funding_raised, company_apollo_enrichment.total_funding_raised),
            last_funding_date        = COALESCE(EXCLUDED.last_funding_date, company_apollo_enrichment.last_funding_date),
            last_funding_type        = COALESCE(EXCLUDED.last_funding_type, company_apollo_enrichment.last_funding_type),
            latest_funding_stage     = COALESCE(EXCLUDED.latest_funding_stage, company_apollo_enrichment.latest_funding_stage),
            employee_metrics         = COALESCE(EXCLUDED.employee_metrics, company_apollo_enrichment.employee_metrics),
            employee_metrics_analysis = COALESCE(EXCLUDED.employee_metrics_analysis, company_apollo_enrichment.employee_metrics_analysis),
            funding_analysis         = COALESCE(EXCLUDED.funding_analysis, company_apollo_enrichment.funding_analysis),
            match_metadata           = EXCLUDED.match_metadata,
            fetched_at               = NOW()
        RETURNING id
        "#,
    )
    .bind(new_id)                                    // $1  id
    .bind(company_id)                                // $2  company_id
    .bind(&data.apollo_org_id)                       // $3  apollo_org_id (Diffbot entity ID)
    .bind(data.description.as_deref())               // $4  description
    .bind(data.industry.as_deref())                  // $5  industry
    .bind(data.estimated_num_employees)              // $6  estimated_num_employees
    .bind(&data.technologies)                        // $7  technologies TEXT[]
    .bind(&data.keywords)                            // $8  keywords TEXT[]
    .bind(data.founded_year)                         // $9  founded_year
    .bind(data.annual_revenue)                       // $10 annual_revenue
    .bind(data.linkedin_url.as_deref())              // $11 linkedin_url
    .bind(data.city.as_deref())                      // $12 city
    .bind(data.state.as_deref())                     // $13 state
    .bind(data.country.as_deref())                   // $14 country
    .bind(&data.funding_events)                      // $15 funding_events JSONB
    .bind(data.total_funding_raised)                 // $16 total_funding_raised
    .bind(funding_date)                              // $17 last_funding_date DATE
    .bind(data.last_funding_type.as_deref())         // $18 last_funding_type
    .bind(data.latest_funding_stage.as_deref())      // $19 latest_funding_stage
    .bind(&data.employee_metrics)                    // $20 employee_metrics JSONB
    .bind(&data.employee_metrics_analysis)           // $21 employee_metrics_analysis JSONB
    .bind(&data.funding_analysis)                    // $22 funding_analysis JSONB
    .bind(&data.match_metadata)                      // $23 match_metadata JSONB
    .fetch_one(pool)
    .await?;

    debug!(
        "Upserted enrichment {} for company {} (provider_org_id={})",
        result, company_id, data.apollo_org_id
    );

    Ok(result)
}

/// Upsert enrichment by external org ID (looks up company_id first).
///
/// This is used when the caller has the Diffbot/Apollo org ID but not the
/// internal company UUID. It queries the companies table to find the match.
///
/// Returns None if the company doesn't exist (no enrichment written).
pub async fn upsert_enrichment(
    pool: &PgPool,
    client_id: Uuid,
    external_org_id: &str,
    data: &EnrichmentData,
) -> Result<Option<Uuid>, sqlx::Error> {
    // Look up company by external_id within client scope
    let company_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT id FROM companies
        WHERE client_id = $1 AND external_id = $2
        LIMIT 1
        "#,
    )
    .bind(client_id)
    .bind(external_org_id)
    .fetch_optional(pool)
    .await?;

    match company_id {
        Some(cid) => {
            let enrichment_id = upsert_enrichment_for_company(pool, cid, data).await?;
            Ok(Some(enrichment_id))
        }
        None => {
            warn!(
                "Cannot write enrichment: no company found for client_id={} external_id={}",
                client_id, external_org_id
            );
            Ok(None)
        }
    }
}

/// Batch-write enrichments for multiple organizations after a search page.
///
/// Silently skips companies that don't exist yet (they'll be enriched
/// after the dedup/upsert pipeline creates them).
///
/// Returns the count of successfully written enrichment rows.
pub async fn batch_write_enrichments(
    pool: &PgPool,
    client_id: Uuid,
    enrichments: &[(String, EnrichmentData)],  // (external_org_id, data)
) -> i32 {
    let mut written = 0;

    for (external_id, data) in enrichments {
        match upsert_enrichment(pool, client_id, external_id, data).await {
            Ok(Some(_)) => written += 1,
            Ok(None) => {
                debug!("Skipped enrichment for {} (company not yet created)", external_id);
            }
            Err(e) => {
                warn!(
                    "Failed to write enrichment for external_id={}: {}",
                    external_id, e
                );
            }
        }
    }

    if written > 0 {
        debug!(
            "Wrote {}/{} enrichment rows for client {}",
            written,
            enrichments.len(),
            client_id
        );
    }

    written
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Verify EnrichmentData can be constructed with all fields.
    #[test]
    fn test_enrichment_data_construction() {
        let data = EnrichmentData {
            apollo_org_id: "TEST123".to_string(),
            description: Some("A test company".to_string()),
            industry: Some("software".to_string()),
            estimated_num_employees: Some(150),
            technologies: vec!["React".to_string(), "PostgreSQL".to_string()],
            keywords: vec!["saas".to_string(), "b2b".to_string()],
            founded_year: Some(2020),
            annual_revenue: Some(25_000_000),
            linkedin_url: Some("https://linkedin.com/company/test".to_string()),
            city: Some("San Francisco".to_string()),
            state: Some("California".to_string()),
            country: Some("United States".to_string()),
            funding_events: json!([{
                "date": "2023-06-15",
                "amount": 10000000,
                "series": "Series A",
                "investors": ["Sequoia"]
            }]),
            total_funding_raised: Some(10_000_000),
            last_funding_date: Some("2023-06-15".to_string()),
            last_funding_type: Some("Series A".to_string()),
            latest_funding_stage: Some("Series A".to_string()),
            employee_metrics: json!([{"source": "diffbot", "total_employees": 150}]),
            employee_metrics_analysis: json!({}),
            funding_analysis: json!({}),
            raw_provider_json: json!({"provider": "diffbot"}),
            match_metadata: json!({"provider": "diffbot", "query_variant": "tight"}),
        };

        assert_eq!(data.apollo_org_id, "TEST123");
        assert_eq!(data.technologies.len(), 2);
        assert!(data.total_funding_raised.is_some());
    }

    /// Verify NaiveDate parsing for funding dates.
    #[test]
    fn test_funding_date_parsing() {
        let good = "2023-06-15";
        let parsed = NaiveDate::parse_from_str(good, "%Y-%m-%d");
        assert!(parsed.is_ok());
        assert_eq!(parsed.unwrap().to_string(), "2023-06-15");

        let bad = "not-a-date";
        let parsed = NaiveDate::parse_from_str(bad, "%Y-%m-%d");
        assert!(parsed.is_err());

        // None input → None output (our actual logic)
        let none_date: Option<&str> = None;
        let result: Option<NaiveDate> = none_date
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
        assert!(result.is_none());
    }
}
