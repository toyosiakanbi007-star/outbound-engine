// src/jobs/company_fetcher/providers/diffbot_models.rs
//
// Deserialization structs for Diffbot Knowledge Graph API responses.
//
// These mirror the JSON shape returned by:
//   POST https://kg.diffbot.com/kg/v3/dql?type=query&token=...
//
// Only fields we actually use are modeled. Everything else is ignored
// via #[serde(default)] so we never crash on missing/extra fields.

use serde::Deserialize;

// ============================================================================
// Top-Level Response
// ============================================================================

/// Top-level Diffbot KG DQL response.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffbotKGResponse {
    /// Number of matching entities in the KG (not the number returned).
    #[serde(default)]
    pub hits: i64,

    /// Number of entities in this response page.
    #[serde(default)]
    pub results: i32,

    /// The entity records.
    #[serde(default)]
    pub data: Vec<DiffbotDataWrapper>,

    /// Error message if query failed.
    #[serde(default)]
    pub error: Option<String>,

    /// Error code if query failed.
    #[serde(default)]
    pub error_code: Option<i32>,
}

/// Each item in `data` wraps an `entity` object.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffbotDataWrapper {
    #[serde(default)]
    pub entity: Option<DiffbotEntity>,
}

// ============================================================================
// Organization Entity
// ============================================================================

/// A Diffbot Organization entity with the fields we care about.
///
/// Fields are all optional because Diffbot entities can be incomplete.
/// We use #[serde(default)] everywhere for robustness.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotEntity {
    /// Diffbot entity ID (e.g. "EPAPM5D5TOs6ZhS7ioX01Gw").
    pub id: Option<String>,

    /// Diffbot URI (e.g. "http://diffbot.com/entity/EPAPM5D5TOs6ZhS7ioX01Gw").
    #[serde(rename = "diffbotUri")]
    pub diffbot_uri: Option<String>,

    /// Primary display name.
    pub name: Option<String>,

    /// Full legal name (e.g. "SEACHANGE INTERNATIONAL, INC.").
    #[serde(rename = "fullName")]
    pub full_name: Option<String>,

    /// All known names for this entity.
    #[serde(rename = "allNames")]
    pub all_names: Vec<String>,

    /// Short summary/tagline.
    pub summary: Option<String>,

    /// Longer description.
    pub description: Option<String>,

    // ---- Website / Social ----

    /// Primary website domain (e.g. "seachange.com").
    #[serde(rename = "homepageUri")]
    pub homepage_uri: Option<String>,

    /// LinkedIn company page URI.
    #[serde(rename = "linkedInUri")]
    pub linkedin_uri: Option<String>,

    /// Facebook URI.
    #[serde(rename = "facebookUri")]
    pub facebook_uri: Option<String>,

    /// Crunchbase URI.
    #[serde(rename = "crunchbaseUri")]
    pub crunchbase_uri: Option<String>,

    /// Logo URL.
    pub logo: Option<String>,

    // ---- Employees ----

    /// Estimated number of current employees.
    #[serde(rename = "nbEmployees")]
    pub nb_employees: Option<i32>,

    /// Min employee estimate.
    #[serde(rename = "nbEmployeesMin")]
    pub nb_employees_min: Option<i32>,

    /// Max employee estimate.
    #[serde(rename = "nbEmployeesMax")]
    pub nb_employees_max: Option<i32>,

    /// Active employee edges in the KG.
    #[serde(rename = "nbActiveEmployeeEdges")]
    pub nb_active_employee_edges: Option<i32>,

    /// Employee breakdown by department/function.
    #[serde(rename = "employeeCategories")]
    pub employee_categories: Vec<DiffbotEmployeeCategory>,

    // ---- Location ----

    /// Primary location (structured).
    pub location: Option<DiffbotLocation>,

    /// All locations.
    pub locations: Vec<DiffbotLocationEntry>,

    // ---- Industry / Classification ----

    /// Diffbot category taxonomy (e.g. "Software Companies", "Technology Companies").
    pub categories: Vec<DiffbotCategory>,

    /// Industry names derived from categories.
    pub industries: Vec<String>,

    /// Free-text descriptor tags (e.g. "telecommunications", "video", "prepackaged software").
    pub descriptors: Vec<String>,

    /// Diffbot's own classification.
    #[serde(rename = "diffbotClassification")]
    pub diffbot_classification: Vec<DiffbotClassificationEntry>,

    // ---- Financials ----

    /// Current/latest annual revenue.
    pub revenue: Option<DiffbotMoneyAmount>,

    /// Historical yearly revenues.
    #[serde(rename = "yearlyRevenues")]
    pub yearly_revenues: Vec<DiffbotYearlyRevenue>,

    /// Market capitalization.
    pub capitalization: Option<DiffbotMoneyAmount>,

    // ---- Funding ----

    /// Total investment raised.
    #[serde(rename = "totalInvestment")]
    pub total_investment: Option<DiffbotMoneyAmount>,

    /// Individual funding rounds.
    pub investments: Vec<DiffbotInvestment>,

    /// Number of unique investors.
    #[serde(rename = "nbUniqueInvestors")]
    pub nb_unique_investors: Option<i32>,

    // ---- IPO / Public ----

    /// IPO details.
    pub ipo: Option<DiffbotIpo>,

    /// Whether the company is publicly traded.
    #[serde(rename = "isPublic")]
    pub is_public: Option<bool>,

    /// Stock info.
    pub stock: Option<DiffbotStock>,

    // ---- Company Status ----

    /// Whether the company has been acquired.
    #[serde(rename = "isAcquired")]
    pub is_acquired: Option<bool>,

    /// Whether the company is dissolved.
    #[serde(rename = "isDissolved")]
    pub is_dissolved: Option<bool>,

    /// Whether the company is a nonprofit.
    #[serde(rename = "isNonProfit")]
    pub is_non_profit: Option<bool>,

    // ---- Founding ----

    /// Founding date.
    #[serde(rename = "foundingDate")]
    pub founding_date: Option<DiffbotDate>,

    // ---- Tech Stack ----

    /// Technologies used by this company (detected via website crawling).
    pub technographics: Vec<DiffbotTechnographic>,

    // ---- Relationships ----

    /// Known suppliers.
    pub suppliers: Vec<DiffbotRelatedEntity>,

    /// Known customers.
    pub customers: Vec<DiffbotRelatedEntity>,

    /// Known competitors.
    pub competitors: Vec<DiffbotRelatedEntity>,

    /// Known partnerships.
    pub partnerships: Vec<DiffbotRelatedEntity>,

    /// Subsidiaries.
    pub subsidiaries: Vec<DiffbotRelatedEntity>,

    // ---- Web Traffic ----

    /// Estimated monthly website traffic.
    #[serde(rename = "monthlyTraffic")]
    pub monthly_traffic: Option<i64>,

    /// Monthly traffic growth rate.
    #[serde(rename = "monthlyTrafficGrowth")]
    pub monthly_traffic_growth: Option<f64>,

    // ---- Contact ----

    /// Email addresses.
    #[serde(rename = "emailAddresses")]
    pub email_addresses: Vec<DiffbotEmailAddress>,

    /// Phone numbers.
    #[serde(rename = "phoneNumbers")]
    pub phone_numbers: Vec<DiffbotPhoneNumber>,

    // ---- KG Metadata ----

    /// Entity importance score (0-100+).
    pub importance: Option<f64>,

    /// Entity type (should be "Organization").
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
}

// ============================================================================
// Nested Types
// ============================================================================

/// Primary location structure.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotLocation {
    pub country: Option<DiffbotNamedEntity>,
    pub city: Option<DiffbotNamedEntity>,
    pub region: Option<DiffbotNamedEntity>,
    #[serde(rename = "subregion")]
    pub sub_region: Option<DiffbotNamedEntity>,
    #[serde(rename = "metroArea")]
    pub metro_area: Option<DiffbotNamedEntity>,
    pub address: Option<String>,
    #[serde(rename = "postalCode")]
    pub postal_code: Option<String>,
    pub street: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    #[serde(rename = "isCurrent")]
    pub is_current: Option<bool>,
    #[serde(rename = "isPrimary")]
    pub is_primary: Option<bool>,
}

/// Location entry in the locations array.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotLocationEntry {
    pub country: Option<DiffbotNamedEntity>,
    pub city: Option<DiffbotNamedEntity>,
    pub region: Option<DiffbotNamedEntity>,
    pub address: Option<String>,
    #[serde(rename = "isCurrent")]
    pub is_current: Option<bool>,
    #[serde(rename = "isPrimary")]
    pub is_primary: Option<bool>,
}

/// A named entity reference (used for country, city, region, etc.).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotNamedEntity {
    pub name: Option<String>,
    pub summary: Option<String>,
    #[serde(rename = "diffbotUri")]
    pub diffbot_uri: Option<String>,
    #[serde(rename = "targetDiffbotId")]
    pub target_diffbot_id: Option<String>,
}

/// Organization category.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotCategory {
    pub name: Option<String>,
    pub level: Option<i32>,
    #[serde(rename = "isPrimary")]
    pub is_primary: Option<bool>,
    #[serde(rename = "diffbotUri")]
    pub diffbot_uri: Option<String>,
}

/// Diffbot classification entry.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotClassificationEntry {
    pub name: Option<String>,
    #[serde(rename = "isPrimary")]
    pub is_primary: Option<bool>,
}

/// Employee category (department breakdown).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotEmployeeCategory {
    pub category: Option<String>,
    #[serde(rename = "nbEmployees")]
    pub nb_employees: Option<i32>,
    #[serde(rename = "firstHireDate")]
    pub first_hire_date: Option<DiffbotDate>,
}

/// A money amount with currency.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotMoneyAmount {
    pub currency: Option<String>,
    pub value: Option<f64>,
}

/// A Diffbot date (has multiple precision levels).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotDate {
    /// Human-readable date string (e.g. "d2021-03-30").
    pub str: Option<String>,
    /// Precision: 1=year, 2=month, 3=day.
    pub precision: Option<i32>,
    /// Unix timestamp in milliseconds.
    pub timestamp: Option<i64>,
}

/// Yearly revenue entry.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotYearlyRevenue {
    pub revenue: Option<DiffbotMoneyAmount>,
    pub year: Option<i32>,
    #[serde(rename = "isCurrent")]
    pub is_current: Option<bool>,
    #[serde(rename = "filingDate")]
    pub filing_date: Option<DiffbotDate>,
}

/// Individual funding round / investment.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotInvestment {
    pub date: Option<DiffbotDate>,
    pub amount: Option<DiffbotMoneyAmount>,
    pub series: Option<String>,
    #[serde(rename = "isCurrent")]
    pub is_current: Option<bool>,
    pub investors: Vec<DiffbotInvestor>,
}

/// Investor in a funding round.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotInvestor {
    pub name: Option<String>,
    pub summary: Option<String>,
    #[serde(rename = "targetDiffbotId")]
    pub target_diffbot_id: Option<String>,
}

/// IPO details.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotIpo {
    pub date: Option<DiffbotDate>,
    #[serde(rename = "stockExchange")]
    pub stock_exchange: Option<String>,
}

/// Stock info.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotStock {
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    #[serde(rename = "isCurrent")]
    pub is_current: Option<bool>,
}

/// Technology entry from technographics.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotTechnographic {
    pub technology: Option<DiffbotTechRef>,
    pub categories: Vec<String>,
}

/// Technology reference.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotTechRef {
    pub name: Option<String>,
    #[serde(rename = "diffbotUri")]
    pub diffbot_uri: Option<String>,
}

/// Related entity (supplier, customer, competitor, partner, subsidiary).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotRelatedEntity {
    pub name: Option<String>,
    pub summary: Option<String>,
    #[serde(rename = "targetDiffbotId")]
    pub target_diffbot_id: Option<String>,
}

/// Email address entry.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotEmailAddress {
    #[serde(rename = "contactString")]
    pub contact_string: Option<String>,
}

/// Phone number entry.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DiffbotPhoneNumber {
    pub string: Option<String>,
    #[serde(rename = "digits")]
    pub digits: Option<String>,
}

// ============================================================================
// Helper Methods
// ============================================================================

impl DiffbotEntity {
    /// Get the best available entity ID.
    pub fn entity_id(&self) -> String {
        self.id
            .clone()
            .or_else(|| {
                self.diffbot_uri.as_ref().and_then(|uri| {
                    uri.rsplit('/').next().map(|s| s.to_string())
                })
            })
            .unwrap_or_else(|| format!("diffbot-unknown-{}", uuid::Uuid::new_v4()))
    }

    /// Get employee count with fallback logic.
    ///
    /// Priority: nbEmployees > midpoint(nbEmployeesMin, nbEmployeesMax) > None
    pub fn employee_count(&self) -> Option<i32> {
        self.nb_employees.or_else(|| {
            match (self.nb_employees_min, self.nb_employees_max) {
                (Some(min), Some(max)) => Some((min + max) / 2),
                (Some(n), None) | (None, Some(n)) => Some(n),
                _ => None,
            }
        })
    }

    /// Get the primary industry from descriptors (first entry).
    /// Falls back to first category name if no descriptors.
    pub fn primary_industry(&self) -> Option<String> {
        self.descriptors.first().cloned().or_else(|| {
            self.categories
                .iter()
                .find(|c| c.is_primary == Some(true))
                .and_then(|c| c.name.clone())
                .or_else(|| self.categories.first().and_then(|c| c.name.clone()))
        })
    }

    /// Get all industry/category names (for Apollo `industries` field).
    pub fn industry_tags(&self) -> Vec<String> {
        let mut tags: Vec<String> = self
            .categories
            .iter()
            .filter_map(|c| c.name.clone())
            .collect();

        // Also include industries if different
        for ind in &self.industries {
            if !tags.contains(ind) {
                tags.push(ind.clone());
            }
        }

        tags
    }

    /// Get the most recent yearly revenue in USD.
    pub fn latest_annual_revenue(&self) -> Option<i64> {
        // Revenue from yearly_revenues sorted by year descending
        self.yearly_revenues
            .iter()
            .filter_map(|yr| {
                yr.revenue
                    .as_ref()
                    .and_then(|r| r.value)
                    .map(|v| (yr.year.unwrap_or(0), v as i64))
            })
            .max_by_key(|(year, _)| *year)
            .map(|(_, value)| value)
            .or_else(|| {
                // Fallback to top-level revenue
                self.revenue
                    .as_ref()
                    .and_then(|r| r.value)
                    .map(|v| v as i64)
            })
    }

    /// Get total funding raised.
    pub fn total_funding(&self) -> Option<i64> {
        self.total_investment
            .as_ref()
            .and_then(|ti| ti.value)
            .map(|v| v as i64)
    }

    /// Get the most recent investment (by date).
    pub fn latest_investment(&self) -> Option<&DiffbotInvestment> {
        self.investments
            .iter()
            .filter(|inv| inv.date.is_some())
            .max_by_key(|inv| {
                inv.date
                    .as_ref()
                    .and_then(|d| d.timestamp)
                    .unwrap_or(0)
            })
    }

    /// Get the founding year.
    pub fn founded_year(&self) -> Option<i32> {
        self.founding_date.as_ref().and_then(|d| {
            d.str.as_ref().and_then(|s| {
                // Format: "d1993-01-01" — extract year after 'd'
                s.strip_prefix('d')
                    .and_then(|rest| rest.split('-').next())
                    .and_then(|y| y.parse::<i32>().ok())
            })
        })
    }

    /// Extract technology names from technographics.
    pub fn technology_names(&self) -> Vec<String> {
        self.technographics
            .iter()
            .filter_map(|t| {
                t.technology
                    .as_ref()
                    .and_then(|tech| tech.name.clone())
            })
            .collect()
    }

    /// Extract keywords from descriptors.
    pub fn keyword_tags(&self) -> Vec<String> {
        self.descriptors.clone()
    }

    /// Get country name from primary location.
    pub fn country_name(&self) -> Option<String> {
        self.location
            .as_ref()
            .and_then(|loc| loc.country.as_ref())
            .and_then(|c| c.name.clone())
    }

    /// Get state/region name from primary location.
    pub fn state_name(&self) -> Option<String> {
        self.location
            .as_ref()
            .and_then(|loc| loc.region.as_ref())
            .and_then(|r| r.name.clone())
    }

    /// Get city name from primary location.
    pub fn city_name(&self) -> Option<String> {
        self.location
            .as_ref()
            .and_then(|loc| loc.city.as_ref())
            .and_then(|c| c.name.clone())
    }
}

impl DiffbotDate {
    /// Convert to ISO date string (YYYY-MM-DD).
    pub fn to_iso_date(&self) -> Option<String> {
        self.str.as_ref().and_then(|s| {
            s.strip_prefix('d').map(|rest| rest.to_string())
        })
    }

    /// Extract just the year.
    pub fn year(&self) -> Option<i32> {
        self.str.as_ref().and_then(|s| {
            s.strip_prefix('d')
                .and_then(|rest| rest.split('-').next())
                .and_then(|y| y.parse::<i32>().ok())
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diffbot_date_parsing() {
        let date = DiffbotDate {
            str: Some("d2021-03-30".to_string()),
            precision: Some(3),
            timestamp: Some(1617062400000),
        };
        assert_eq!(date.to_iso_date(), Some("2021-03-30".to_string()));
        assert_eq!(date.year(), Some(2021));
    }

    #[test]
    fn test_entity_id_extraction() {
        let mut entity = DiffbotEntity::default();
        entity.id = Some("EPAPM5D5TOs6ZhS7ioX01Gw".to_string());
        assert_eq!(entity.entity_id(), "EPAPM5D5TOs6ZhS7ioX01Gw");
    }

    #[test]
    fn test_entity_id_from_uri() {
        let mut entity = DiffbotEntity::default();
        entity.diffbot_uri =
            Some("http://diffbot.com/entity/EPAPM5D5TOs6ZhS7ioX01Gw".to_string());
        assert_eq!(entity.entity_id(), "EPAPM5D5TOs6ZhS7ioX01Gw");
    }

    #[test]
    fn test_employee_count_primary() {
        let mut entity = DiffbotEntity::default();
        entity.nb_employees = Some(107);
        entity.nb_employees_min = Some(100);
        entity.nb_employees_max = Some(200);
        assert_eq!(entity.employee_count(), Some(107));
    }

    #[test]
    fn test_employee_count_fallback() {
        let mut entity = DiffbotEntity::default();
        entity.nb_employees_min = Some(100);
        entity.nb_employees_max = Some(200);
        assert_eq!(entity.employee_count(), Some(150));
    }

    #[test]
    fn test_primary_industry_from_descriptors() {
        let mut entity = DiffbotEntity::default();
        entity.descriptors = vec!["telecommunications".into(), "video".into()];
        assert_eq!(
            entity.primary_industry(),
            Some("telecommunications".to_string())
        );
    }

    #[test]
    fn test_primary_industry_falls_back_to_categories() {
        let mut entity = DiffbotEntity::default();
        entity.categories = vec![DiffbotCategory {
            name: Some("Software Companies".to_string()),
            is_primary: Some(true),
            ..Default::default()
        }];
        assert_eq!(
            entity.primary_industry(),
            Some("Software Companies".to_string())
        );
    }

    #[test]
    fn test_deserialize_sample() {
        let json = r#"{
            "version": 3,
            "hits": 1,
            "results": 1,
            "data": [{
                "entity": {
                    "id": "TEST123",
                    "name": "TestCorp",
                    "homepageUri": "testcorp.com",
                    "nbEmployees": 50,
                    "descriptors": ["software"],
                    "categories": [{"name": "Software Companies", "isPrimary": true}],
                    "technographics": [
                        {"technology": {"name": "React"}, "categories": ["JavaScript Frameworks"]}
                    ],
                    "location": {
                        "country": {"name": "United States"},
                        "region": {"name": "California"},
                        "city": {"name": "San Francisco"}
                    },
                    "yearlyRevenues": [{"revenue": {"currency": "USD", "value": 10000000}, "year": 2024}],
                    "totalInvestment": {"currency": "USD", "value": 5000000},
                    "investments": [{
                        "date": {"str": "d2024-01-15", "precision": 3, "timestamp": 1705276800000},
                        "amount": {"currency": "USD", "value": 5000000},
                        "series": "Series A",
                        "investors": [{"name": "Sequoia"}]
                    }],
                    "foundingDate": {"str": "d2020-06-01", "precision": 3}
                }
            }]
        }"#;

        let response: DiffbotKGResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.hits, 1);
        assert_eq!(response.results, 1);
        assert_eq!(response.data.len(), 1);

        let entity = response.data[0].entity.as_ref().unwrap();
        assert_eq!(entity.name, Some("TestCorp".to_string()));
        assert_eq!(entity.employee_count(), Some(50));
        assert_eq!(entity.primary_industry(), Some("software".to_string()));
        assert_eq!(entity.country_name(), Some("United States".to_string()));
        assert_eq!(entity.state_name(), Some("California".to_string()));
        assert_eq!(entity.latest_annual_revenue(), Some(10_000_000));
        assert_eq!(entity.total_funding(), Some(5_000_000));
        assert_eq!(entity.founded_year(), Some(2020));
        assert_eq!(entity.technology_names(), vec!["React".to_string()]);

        let latest_inv = entity.latest_investment().unwrap();
        assert_eq!(latest_inv.series, Some("Series A".to_string()));
    }
}
