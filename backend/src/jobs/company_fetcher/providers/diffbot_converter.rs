// src/jobs/company_fetcher/providers/diffbot_converter.rs
//
// Converts Diffbot Knowledge Graph Organization entities into:
//   1. ApolloOrganization structs (for the existing company_fetcher pipeline)
//   2. Enrichment JSON (for upsert into company_apollo_enrichment table)
//
// DESIGN:
//   The converter is pure and stateless — no DB, no HTTP.
//   It takes a &DiffbotEntity and produces mapped outputs.
//
// IDENTITY MAPPING:
//   diffbot.id             → apollo.id (external_id in companies table)
//   diffbot.homepageUri    → apollo.primary_domain (normalized)
//   diffbot.linkedInUri    → apollo.linkedin_url
//   diffbot.allNames[0]    → apollo.name (fallback to diffbot.name)
//   diffbot.nbEmployees    → apollo.estimated_num_employees
//   diffbot.descriptors[0] → apollo.industry
//   diffbot.categories     → apollo.industries
//
// ENRICHMENT INCLUDES (per user spec):
//   technographics, nbEmployees, homepageUri, description,
//   employeeCategories, allNames, linkedInUri, yearlyRevenues (most recent)
//   descriptors (first = industry), categories

use serde_json::json;
use std::collections::HashMap;
use tracing::debug;

use super::diffbot_models::DiffbotEntity;
use crate::jobs::company_fetcher::models::{ApolloOrganization, normalize_domain};

// ============================================================================
// Apollo Organization Conversion
// ============================================================================

/// Convert a Diffbot entity to an Apollo-compatible organization.
///
/// The output can be consumed directly by the existing dedup → upsert pipeline.
pub fn to_apollo_organization(entity: &DiffbotEntity) -> ApolloOrganization {
    let id = entity.entity_id();

    // Name: prefer allNames[0] (per user spec), fall back to name
    let name = entity
        .all_names
        .first()
        .cloned()
        .or_else(|| entity.name.clone());

    // Domain: normalize homepageUri
    let primary_domain = entity
        .homepage_uri
        .as_ref()
        .map(|uri| normalize_domain(uri));

    // Website URL: build from domain
    let website_url = primary_domain
        .as_ref()
        .map(|d| format!("https://{}", d));

    // LinkedIn: use as-is, normalize later in ApolloOrganization methods
    let linkedin_url = entity.linkedin_uri.as_ref().map(|uri| {
        if uri.starts_with("http") {
            uri.clone()
        } else {
            format!("https://{}", uri)
        }
    });

    // Industry: first descriptor (per user spec, more consistent than categories)
    let industry = entity.primary_industry();

    // Industries: all category names
    let industries = entity.industry_tags();

    // Employee count
    let estimated_num_employees = entity.employee_count();

    // Location
    let country = entity.country_name();
    let state = entity.state_name();
    let city = entity.city_name();
    let postal_code = entity
        .location
        .as_ref()
        .and_then(|loc| loc.postal_code.clone());
    let raw_address = entity
        .location
        .as_ref()
        .and_then(|loc| loc.address.clone());

    // Financials
    let annual_revenue = entity.latest_annual_revenue();
    let total_funding = entity.total_funding();

    let latest_investment = entity.latest_investment();
    let latest_funding_round_date = latest_investment
        .and_then(|inv| inv.date.as_ref())
        .and_then(|d| d.to_iso_date());
    let latest_funding_stage = latest_investment
        .and_then(|inv| inv.series.clone());

    // Description
    let short_description = entity.description.clone().or(entity.summary.clone());

    // Keywords: descriptors
    let keywords = entity.keyword_tags();

    // Founded year
    let founded_year = entity.founded_year();

    // Logo
    let logo_url = entity.logo.clone();

    // Total funding display string
    let total_funding_printed = total_funding.map(format_funding_amount);

    debug!(
        "Converted Diffbot entity {} → Apollo org (name={:?}, domain={:?}, employees={:?})",
        id,
        name,
        primary_domain,
        estimated_num_employees,
    );

    ApolloOrganization {
        id,
        name,
        primary_domain,
        website_url,
        linkedin_url,
        linkedin_uid: None,
        industry,
        industries,
        estimated_num_employees,
        founded_year,
        city,
        state,
        country,
        postal_code,
        raw_address,
        street_address: None,
        short_description,
        seo_description: None,
        keywords,
        phone: None,
        logo_url,
        annual_revenue,
        total_funding,
        total_funding_printed,
        latest_funding_round_date,
        latest_funding_stage,
        industry_tag_id: None,
        industry_tag_hash: None,
    }
}

// ============================================================================
// Enrichment JSON Builder
// ============================================================================

/// Build the enrichment data for company_apollo_enrichment table.
///
/// This produces values matching the existing column schema:
///   technologies TEXT[], estimated_num_employees INT, industry VARCHAR,
///   city/state/country VARCHAR, funding_events JSONB, total_funding_raised BIGINT,
///   last_funding_date DATE, last_funding_type VARCHAR, latest_funding_stage VARCHAR,
///   employee_metrics JSONB, keywords TEXT[], founded_year INT,
///   annual_revenue BIGINT, linkedin_url VARCHAR, description TEXT
///
/// Also builds a raw_provider_json snapshot for diagnostics.
pub struct EnrichmentData {
    // Direct column values
    pub apollo_org_id: String,
    pub description: Option<String>,
    pub industry: Option<String>,
    pub estimated_num_employees: Option<i32>,
    pub technologies: Vec<String>,
    pub keywords: Vec<String>,
    pub founded_year: Option<i32>,
    pub annual_revenue: Option<i64>,
    pub linkedin_url: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub country: Option<String>,
    pub funding_events: serde_json::Value,
    pub total_funding_raised: Option<i64>,
    pub last_funding_date: Option<String>,
    pub last_funding_type: Option<String>,
    pub latest_funding_stage: Option<String>,
    pub employee_metrics: serde_json::Value,
    /// Pre-computed Phase B-compatible analysis stubs (empty until Phase B runs)
    pub employee_metrics_analysis: serde_json::Value,
    pub funding_analysis: serde_json::Value,
    /// Raw Diffbot entity snapshot for diagnostics/re-processing.
    pub raw_provider_json: serde_json::Value,
    /// Match metadata: which keywords matched, query variant, DQL used, etc.
    /// Populated by the orchestrator after search_page, not by the converter.
    pub match_metadata: serde_json::Value,
}

/// Build enrichment data from a Diffbot entity.
pub fn build_enrichment_data(entity: &DiffbotEntity) -> EnrichmentData {
    // Technologies from technographics
    let technologies = entity.technology_names();

    // Funding events: convert Diffbot investments[] to Apollo-like JSONB array
    let funding_events = build_funding_events(entity);

    // Latest investment
    let latest_inv = entity.latest_investment();
    let last_funding_date = latest_inv
        .and_then(|inv| inv.date.as_ref())
        .and_then(|d| d.to_iso_date());
    let last_funding_type = latest_inv.and_then(|inv| inv.series.clone());
    let latest_funding_stage = last_funding_type.clone();

    // Employee metrics: build from employeeCategories
    let employee_metrics = build_employee_metrics(entity);

    // LinkedIn
    let linkedin_url = entity.linkedin_uri.as_ref().map(|uri| {
        if uri.starts_with("http") {
            uri.clone()
        } else {
            format!("https://{}", uri)
        }
    });

    // Build raw snapshot (selected fields, not the entire 12K entity)
    let raw_provider_json = build_raw_snapshot(entity);

    EnrichmentData {
        apollo_org_id: entity.entity_id(),
        description: entity.description.clone().or(entity.summary.clone()),
        industry: entity.primary_industry(),
        estimated_num_employees: entity.employee_count(),
        technologies,
        keywords: entity.keyword_tags(),
        founded_year: entity.founded_year(),
        annual_revenue: entity.latest_annual_revenue(),
        linkedin_url,
        city: entity.city_name(),
        state: entity.state_name(),
        country: entity.country_name(),
        funding_events,
        total_funding_raised: entity.total_funding(),
        last_funding_date,
        last_funding_type,
        latest_funding_stage,
        employee_metrics,
        employee_metrics_analysis: json!({}),
        funding_analysis: json!({}),
        raw_provider_json,
        match_metadata: json!({}),
    }
}

// ============================================================================
// Internal Helpers
// ============================================================================

/// Convert Diffbot investments[] to Apollo-like funding_events JSONB.
fn build_funding_events(entity: &DiffbotEntity) -> serde_json::Value {
    let events: Vec<serde_json::Value> = entity
        .investments
        .iter()
        .map(|inv| {
            let investors: Vec<String> = inv
                .investors
                .iter()
                .filter_map(|i| i.name.clone())
                .collect();

            json!({
                "date": inv.date.as_ref().and_then(|d| d.to_iso_date()),
                "amount": inv.amount.as_ref().and_then(|a| a.value).map(|v| v as i64),
                "currency": inv.amount.as_ref().and_then(|a| a.currency.clone()).unwrap_or_else(|| "USD".to_string()),
                "series": inv.series,
                "investors": investors,
            })
        })
        .collect();

    serde_json::Value::Array(events)
}

/// Build employee_metrics JSONB from Diffbot employeeCategories.
///
/// Apollo stores this as monthly snapshots. We store the Diffbot
/// department breakdown as a single snapshot instead.
fn build_employee_metrics(entity: &DiffbotEntity) -> serde_json::Value {
    if entity.employee_categories.is_empty() {
        return json!([]);
    }

    // Build a single "snapshot" with department breakdown
    let departments: Vec<serde_json::Value> = entity
        .employee_categories
        .iter()
        .filter(|ec| ec.nb_employees.unwrap_or(0) > 0)
        .map(|ec| {
            json!({
                "department": ec.category,
                "count": ec.nb_employees,
            })
        })
        .collect();

    json!([{
        "source": "diffbot",
        "total_employees": entity.employee_count(),
        "departments": departments,
    }])
}

/// Build a compact raw snapshot of the Diffbot entity for storage.
/// Not the full 12K entity — just the fields that are useful for debugging
/// or re-processing.
fn build_raw_snapshot(entity: &DiffbotEntity) -> serde_json::Value {
    json!({
        "provider": "diffbot",
        "entity_id": entity.entity_id(),
        "name": entity.name,
        "all_names": entity.all_names,
        "homepage_uri": entity.homepage_uri,
        "linkedin_uri": entity.linkedin_uri,
        "description": entity.description,
        "summary": entity.summary,
        "nb_employees": entity.nb_employees,
        "nb_employees_min": entity.nb_employees_min,
        "nb_employees_max": entity.nb_employees_max,
        "descriptors": &entity.descriptors[..entity.descriptors.len().min(20)],
        "categories": entity.categories.iter().map(|c| json!({
            "name": c.name,
            "level": c.level,
            "is_primary": c.is_primary,
        })).collect::<Vec<_>>(),
        "industries": entity.industries,
        "is_public": entity.is_public,
        "is_acquired": entity.is_acquired,
        "is_dissolved": entity.is_dissolved,
        "founded_year": entity.founded_year(),
        "monthly_traffic": entity.monthly_traffic,
        "monthly_traffic_growth": entity.monthly_traffic_growth,
        "importance": entity.importance,
        "yearly_revenues": entity.yearly_revenues.iter().take(3).map(|yr| json!({
            "year": yr.year,
            "value": yr.revenue.as_ref().and_then(|r| r.value),
            "currency": yr.revenue.as_ref().and_then(|r| r.currency.clone()),
        })).collect::<Vec<_>>(),
        "total_investment": entity.total_investment.as_ref().map(|ti| json!({
            "value": ti.value,
            "currency": ti.currency,
        })),
        "technographics_count": entity.technographics.len(),
        "employee_categories_count": entity.employee_categories.len(),
    })
}

/// Format a funding amount to display string (e.g. 19100000 → "$19.1M").
fn format_funding_amount(amount: i64) -> String {
    if amount >= 1_000_000_000 {
        format!("${:.1}B", amount as f64 / 1_000_000_000.0)
    } else if amount >= 1_000_000 {
        format!("${:.1}M", amount as f64 / 1_000_000.0)
    } else if amount >= 1_000 {
        format!("${:.0}K", amount as f64 / 1_000.0)
    } else {
        format!("${}", amount)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::diffbot_models::*;

    fn make_test_entity() -> DiffbotEntity {
        DiffbotEntity {
            id: Some("TEST_ID".to_string()),
            name: Some("TestCorp".to_string()),
            all_names: vec!["TestCorp Inc.".to_string(), "TestCorp".to_string()],
            homepage_uri: Some("www.testcorp.com".to_string()),
            linkedin_uri: Some("linkedin.com/company/testcorp".to_string()),
            nb_employees: Some(150),
            descriptors: vec!["software".to_string(), "saas".to_string()],
            categories: vec![DiffbotCategory {
                name: Some("Software Companies".to_string()),
                is_primary: Some(true),
                ..Default::default()
            }],
            location: Some(DiffbotLocation {
                country: Some(DiffbotNamedEntity {
                    name: Some("United States".to_string()),
                    ..Default::default()
                }),
                region: Some(DiffbotNamedEntity {
                    name: Some("California".to_string()),
                    ..Default::default()
                }),
                city: Some(DiffbotNamedEntity {
                    name: Some("San Francisco".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            technographics: vec![
                DiffbotTechnographic {
                    technology: Some(DiffbotTechRef {
                        name: Some("React".to_string()),
                        ..Default::default()
                    }),
                    categories: vec!["JavaScript Frameworks".to_string()],
                },
                DiffbotTechnographic {
                    technology: Some(DiffbotTechRef {
                        name: Some("PostgreSQL".to_string()),
                        ..Default::default()
                    }),
                    categories: vec!["Databases".to_string()],
                },
            ],
            yearly_revenues: vec![DiffbotYearlyRevenue {
                revenue: Some(DiffbotMoneyAmount {
                    currency: Some("USD".to_string()),
                    value: Some(25_000_000.0),
                }),
                year: Some(2024),
                ..Default::default()
            }],
            total_investment: Some(DiffbotMoneyAmount {
                currency: Some("USD".to_string()),
                value: Some(10_000_000.0),
            }),
            investments: vec![DiffbotInvestment {
                date: Some(DiffbotDate {
                    str: Some("d2023-06-15".to_string()),
                    precision: Some(3),
                    timestamp: Some(1686787200000),
                }),
                amount: Some(DiffbotMoneyAmount {
                    currency: Some("USD".to_string()),
                    value: Some(10_000_000.0),
                }),
                series: Some("Series A".to_string()),
                investors: vec![DiffbotInvestor {
                    name: Some("Sequoia".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            founding_date: Some(DiffbotDate {
                str: Some("d2020-01-01".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_to_apollo_organization() {
        let entity = make_test_entity();
        let apollo = to_apollo_organization(&entity);

        assert_eq!(apollo.id, "TEST_ID");
        // allNames[0] = "TestCorp Inc." (preferred over name)
        assert_eq!(apollo.name, Some("TestCorp Inc.".to_string()));
        assert_eq!(apollo.primary_domain, Some("testcorp.com".to_string()));
        assert_eq!(
            apollo.website_url,
            Some("https://testcorp.com".to_string())
        );
        assert_eq!(
            apollo.linkedin_url,
            Some("https://linkedin.com/company/testcorp".to_string())
        );
        assert_eq!(apollo.estimated_num_employees, Some(150));
        assert_eq!(apollo.industry, Some("software".to_string()));
        assert!(apollo.industries.contains(&"Software Companies".to_string()));
        assert_eq!(apollo.country, Some("United States".to_string()));
        assert_eq!(apollo.state, Some("California".to_string()));
        assert_eq!(apollo.city, Some("San Francisco".to_string()));
        assert_eq!(apollo.annual_revenue, Some(25_000_000));
        assert_eq!(apollo.total_funding, Some(10_000_000));
        assert_eq!(
            apollo.latest_funding_round_date,
            Some("2023-06-15".to_string())
        );
        assert_eq!(
            apollo.latest_funding_stage,
            Some("Series A".to_string())
        );
        assert_eq!(apollo.founded_year, Some(2020));
    }

    #[test]
    fn test_build_enrichment_data() {
        let entity = make_test_entity();
        let enrichment = build_enrichment_data(&entity);

        assert_eq!(enrichment.apollo_org_id, "TEST_ID");
        assert_eq!(enrichment.technologies, vec!["React", "PostgreSQL"]);
        assert_eq!(enrichment.estimated_num_employees, Some(150));
        assert_eq!(enrichment.industry, Some("software".to_string()));
        assert_eq!(enrichment.annual_revenue, Some(25_000_000));
        assert_eq!(enrichment.total_funding_raised, Some(10_000_000));
        assert_eq!(enrichment.last_funding_date, Some("2023-06-15".to_string()));
        assert_eq!(enrichment.latest_funding_stage, Some("Series A".to_string()));
        assert_eq!(enrichment.founded_year, Some(2020));

        // Funding events should have 1 entry
        let events = enrichment.funding_events.as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["series"], "Series A");
        assert_eq!(events[0]["investors"][0], "Sequoia");

        // Raw snapshot should contain provider tag
        assert_eq!(enrichment.raw_provider_json["provider"], "diffbot");
    }

    #[test]
    fn test_handles_empty_entity() {
        let entity = DiffbotEntity::default();
        let apollo = to_apollo_organization(&entity);
        // Should not panic — just have None/empty fields
        assert!(apollo.name.is_none());
        assert!(apollo.primary_domain.is_none());
        assert!(apollo.estimated_num_employees.is_none());

        let enrichment = build_enrichment_data(&entity);
        assert!(enrichment.technologies.is_empty());
        assert!(enrichment.annual_revenue.is_none());
    }

    #[test]
    fn test_format_funding() {
        assert_eq!(format_funding_amount(19_100_000), "$19.1M");
        assert_eq!(format_funding_amount(1_500_000_000), "$1.5B");
        assert_eq!(format_funding_amount(500_000), "$500K");
        assert_eq!(format_funding_amount(999), "$999");
    }
}
