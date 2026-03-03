// src/jobs/company_fetcher/dedup.rs
//
// Deduplication and upsert logic for the Company Fetcher.
//
// RESPONSIBILITIES:
// - In-memory dedup within a batch (catch duplicates inside one Apollo page
//   before hitting the DB)
// - Build and execute upsert SQL against `companies` using cascading identity
//   resolution
// - Map Apollo org fields → companies table columns with normalization
//
// IDENTITY RESOLUTION ORDER (per the spec):
//   1. (client_id, external_id) — Apollo org ID, globally unique per provider
//   2. (client_id, domain)      — normalized domain, one company per domain
//   3. (client_id, linkedin_url)— fallback when domain is missing
//   4. (client_id, name, country, city) — last-resort fuzzy match (SELECT only)
//
// DB CONSTRAINTS (from migration 0022):
//   UNIQUE (client_id, external_id)  — uq_companies_client_external_id
//   UNIQUE (client_id, domain)       — uq_companies_client_domain
//   UNIQUE (client_id, linkedin_url) — uq_companies_client_linkedin
//
// PostgreSQL allows only ONE `ON CONFLICT` target per INSERT, so the strategy
// is two-phase:
//   Phase 1: SELECT to find existing row via cascading key lookup
//   Phase 2: If found → UPDATE; if not found → INSERT with best ON CONFLICT
//
// USAGE:
//   let mut deduper = BatchDeduper::new();
//   for org in apollo_orgs {
//       if deduper.is_duplicate(&org) { continue; }
//       let result = upsert_company(&pool, client_id, &org).await?;
//       // result.was_insert tells you if it was new
//   }

use sqlx::PgPool;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use std::collections::HashSet;

use super::models::{normalize_domain, ApolloOrganization, UpsertResult};

// ============================================================================
// In-Memory Batch Deduper
// ============================================================================

/// Tracks companies already seen within the current batch/page to avoid
/// sending duplicate upserts to the database.
///
/// Uses three identity sets (mirroring DB constraints):
/// - external_ids (Apollo org IDs)
/// - domains (normalized)
/// - linkedin_urls (normalized)
///
/// Call `is_duplicate()` before processing each org. If it returns true,
/// skip the org. If false, the org's identity keys are recorded.
#[derive(Debug)]
pub struct BatchDeduper {
    external_ids: HashSet<String>,
    domains: HashSet<String>,
    linkedin_urls: HashSet<String>,
    /// Total duplicates caught in memory (for telemetry).
    pub duplicates_caught: i32,
}

impl BatchDeduper {
    pub fn new() -> Self {
        Self {
            external_ids: HashSet::new(),
            domains: HashSet::new(),
            linkedin_urls: HashSet::new(),
            duplicates_caught: 0,
        }
    }

    /// Create with pre-allocated capacity (for large batches).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            external_ids: HashSet::with_capacity(cap),
            domains: HashSet::with_capacity(cap),
            linkedin_urls: HashSet::with_capacity(cap),
            duplicates_caught: 0,
        }
    }

    /// Check if this org has already been seen in this batch.
    /// If NOT a duplicate, records the org's identity keys and returns false.
    /// If IS a duplicate, increments the counter and returns true.
    pub fn is_duplicate(&mut self, org: &ApolloOrganization) -> bool {
        // Check external_id first (strongest identity)
        if self.external_ids.contains(&org.id) {
            self.duplicates_caught += 1;
            trace!("In-memory dupe by external_id: {}", org.id);
            return true;
        }

        // Check domain
        if let Some(domain) = org.normalized_domain() {
            if !domain.is_empty() && self.domains.contains(&domain) {
                self.duplicates_caught += 1;
                trace!(
                    "In-memory dupe by domain: {} (org: {})",
                    domain,
                    org.id
                );
                return true;
            }
        }

        // Check linkedin
        if let Some(linkedin) = org.normalized_linkedin() {
            if !linkedin.is_empty() && self.linkedin_urls.contains(&linkedin) {
                self.duplicates_caught += 1;
                trace!(
                    "In-memory dupe by linkedin: {} (org: {})",
                    linkedin,
                    org.id
                );
                return true;
            }
        }

        // Not a dupe — record all identity keys
        self.record(org);
        false
    }

    /// Record an org's identity keys without checking for duplicates.
    /// Used when pre-loading known companies from the DB.
    pub fn record(&mut self, org: &ApolloOrganization) {
        self.external_ids.insert(org.id.clone());

        if let Some(domain) = org.normalized_domain() {
            if !domain.is_empty() {
                self.domains.insert(domain);
            }
        }
        if let Some(linkedin) = org.normalized_linkedin() {
            if !linkedin.is_empty() {
                self.linkedin_urls.insert(linkedin);
            }
        }
    }

    /// Pre-seed the deduper with domains already known for this client.
    /// Called once before processing starts, using a DB query.
    pub fn seed_domains(&mut self, domains: impl IntoIterator<Item = String>) {
        for d in domains {
            let normalized = normalize_domain(&d);
            if !normalized.is_empty() {
                self.domains.insert(normalized);
            }
        }
    }

    /// Pre-seed with external IDs already known for this client.
    pub fn seed_external_ids(&mut self, ids: impl IntoIterator<Item = String>) {
        self.external_ids.extend(ids);
    }

    /// Pre-seed with LinkedIn URLs already known.
    pub fn seed_linkedin_urls(&mut self, urls: impl IntoIterator<Item = String>) {
        for u in urls {
            let normalized = u.to_lowercase().trim_end_matches('/').to_string();
            if !normalized.is_empty() {
                self.linkedin_urls.insert(normalized);
            }
        }
    }

    /// How many unique orgs have been recorded.
    pub fn unique_count(&self) -> usize {
        self.external_ids.len()
    }
}

impl Default for BatchDeduper {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// DB Upsert: Find Existing Company
// ============================================================================

/// Try to find an existing company in the DB via cascading identity resolution.
///
/// Resolution order:
///   1. external_id match  (strongest — Apollo org ID)
///   2. domain match       (normalized)
///   3. linkedin_url match (normalized)
///   4. name + country + city match (weakest — fuzzy last resort)
///
/// Returns Some(company_id) if found, None if no match.
pub async fn find_existing_company(
    pool: &PgPool,
    client_id: Uuid,
    org: &ApolloOrganization,
) -> Result<Option<Uuid>, sqlx::Error> {
    // 1. Match by external_id (Apollo org ID)
    let row = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM companies WHERE client_id = $1 AND external_id = $2 LIMIT 1",
    )
    .bind(client_id)
    .bind(&org.id)
    .fetch_optional(pool)
    .await?;

    if let Some(id) = row {
        trace!("Found by external_id={}: company={}", org.id, id);
        return Ok(Some(id));
    }

    // 2. Match by normalized domain
    if let Some(domain) = org.normalized_domain() {
        if !domain.is_empty() {
            let row = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM companies WHERE client_id = $1 AND domain = $2 LIMIT 1",
            )
            .bind(client_id)
            .bind(&domain)
            .fetch_optional(pool)
            .await?;

            if let Some(id) = row {
                trace!("Found by domain={}: company={}", domain, id);
                return Ok(Some(id));
            }
        }
    }

    // 3. Match by normalized linkedin_url
    if let Some(linkedin) = org.normalized_linkedin() {
        if !linkedin.is_empty() {
            let row = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM companies WHERE client_id = $1 AND linkedin_url = $2 LIMIT 1",
            )
            .bind(client_id)
            .bind(&linkedin)
            .fetch_optional(pool)
            .await?;

            if let Some(id) = row {
                trace!("Found by linkedin_url: company={}", id);
                return Ok(Some(id));
            }
        }
    }

    // 4. Last resort: name + country + city (fuzzy)
    if let Some(name) = org.name.as_deref() {
        if !name.is_empty() {
            let row = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id FROM companies
                WHERE client_id = $1
                  AND LOWER(name) = LOWER($2)
                  AND (country IS NOT DISTINCT FROM $3)
                  AND (city IS NOT DISTINCT FROM $4)
                LIMIT 1
                "#,
            )
            .bind(client_id)
            .bind(name)
            .bind(org.country.as_deref())
            .bind(org.city.as_deref())
            .fetch_optional(pool)
            .await?;

            if let Some(id) = row {
                debug!(
                    "Found by name+geo: name='{}', country={:?}, city={:?} → company={}",
                    name, org.country, org.city, id
                );
                return Ok(Some(id));
            }
        }
    }

    Ok(None)
}

// ============================================================================
// DB Upsert: Insert or Update Company
// ============================================================================

/// Upsert an Apollo organization into the `companies` table.
///
/// Strategy:
///   1. Check for existing row via `find_existing_company()`
///   2. If found → UPDATE enrichable fields, return (id, was_insert=false)
///   3. If not found → INSERT with ON CONFLICT on the best available constraint
///      as a race-condition safety net
///
/// Fields that are always updated on match (enrichment merge):
///   - name (if Apollo has a better/newer one)
///   - industry, employee_count, country, region, city
///   - linkedin_url, website_url
///   - updated_at
///
/// Fields set only on insert:
///   - id (new UUID), client_id, source, external_id, domain, created_at
pub async fn upsert_company(
    pool: &PgPool,
    client_id: Uuid,
    org: &ApolloOrganization,
) -> Result<UpsertResult, sqlx::Error> {
    let domain = org.normalized_domain();
    let linkedin = org.normalized_linkedin();
    let website_url = org
        .website_url
        .as_deref()
        .or(org.primary_domain.as_deref().map(|_| ""))
        .and_then(|_| org.website_url.clone());

    // Phase 1: Find existing
    let existing_id = find_existing_company(pool, client_id, org).await?;

    if let Some(company_id) = existing_id {
        // Phase 2a: UPDATE existing row with fresh Apollo data
        update_existing_company(pool, company_id, org, domain.as_deref(), linkedin.as_deref())
            .await?;

        return Ok(UpsertResult {
            company_id,
            was_insert: false,
        });
    }

    // Phase 2b: INSERT new row
    // Use ON CONFLICT on external_id as race-condition safety net
    // (another worker might have inserted between our SELECT and INSERT)
    let new_id = Uuid::new_v4();

    let result = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO companies (
            id, client_id, name, domain, industry, employee_count,
            country, region, city, source, external_id,
            linkedin_url, website_url,
            created_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, 'apollo', $10,
            $11, $12,
            NOW(), NOW()
        )
        ON CONFLICT (client_id, external_id) DO UPDATE SET
            name           = COALESCE(NULLIF(EXCLUDED.name, ''), companies.name),
            domain         = COALESCE(EXCLUDED.domain, companies.domain),
            industry       = COALESCE(EXCLUDED.industry, companies.industry),
            employee_count = COALESCE(EXCLUDED.employee_count, companies.employee_count),
            country        = COALESCE(EXCLUDED.country, companies.country),
            region         = COALESCE(EXCLUDED.region, companies.region),
            city           = COALESCE(EXCLUDED.city, companies.city),
            linkedin_url   = COALESCE(EXCLUDED.linkedin_url, companies.linkedin_url),
            website_url    = COALESCE(EXCLUDED.website_url, companies.website_url),
            updated_at     = NOW()
        RETURNING id
        "#,
    )
    .bind(new_id)
    .bind(client_id)
    .bind(org.name.as_deref().unwrap_or("Unknown"))
    .bind(domain.as_deref())
    .bind(org.industry.as_deref())
    .bind(org.estimated_num_employees)
    .bind(org.country.as_deref())
    .bind(org.state.as_deref())      // maps to `region` column
    .bind(org.city.as_deref())
    .bind(&org.id)                   // external_id = Apollo org ID
    .bind(linkedin.as_deref())
    .bind(website_url.as_deref())
    .fetch_one(pool)
    .await?;

    // If the returned ID equals our new_id, it was a genuine insert.
    // If it differs, ON CONFLICT fired and we got the existing row's ID.
    let was_insert = result == new_id;

    if !was_insert {
        debug!(
            "Race-condition upsert: ON CONFLICT matched existing company {} for Apollo org {}",
            result, org.id
        );
    }

    Ok(UpsertResult {
        company_id: result,
        was_insert,
    })
}

/// Update an existing company row with fresh data from Apollo.
/// Uses COALESCE to avoid overwriting good data with NULLs.
async fn update_existing_company(
    pool: &PgPool,
    company_id: Uuid,
    org: &ApolloOrganization,
    domain: Option<&str>,
    linkedin: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE companies SET
            name           = COALESCE(NULLIF($2, ''), name),
            domain         = COALESCE($3, domain),
            industry       = COALESCE($4, industry),
            employee_count = COALESCE($5, employee_count),
            country        = COALESCE($6, country),
            region         = COALESCE($7, region),
            city           = COALESCE($8, city),
            external_id    = COALESCE($9, external_id),
            linkedin_url   = COALESCE($10, linkedin_url),
            website_url    = COALESCE($11, website_url),
            updated_at     = NOW()
        WHERE id = $1
        "#,
    )
    .bind(company_id)
    .bind(org.name.as_deref().unwrap_or(""))
    .bind(domain)
    .bind(org.industry.as_deref())
    .bind(org.estimated_num_employees)
    .bind(org.country.as_deref())
    .bind(org.state.as_deref())      // maps to `region`
    .bind(org.city.as_deref())
    .bind(&org.id)                   // external_id
    .bind(linkedin)
    .bind(org.website_url.as_deref())
    .execute(pool)
    .await?;

    Ok(())
}

// ============================================================================
// Bulk Seed: Load Known Identity Keys
// ============================================================================

/// Load all known external_ids for a client from the DB.
/// Used to pre-seed the BatchDeduper before a run.
pub async fn load_known_external_ids(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT external_id FROM companies WHERE client_id = $1 AND external_id IS NOT NULL",
    )
    .bind(client_id)
    .fetch_all(pool)
    .await?;

    debug!(
        "Loaded {} known external_ids for client {}",
        rows.len(),
        client_id
    );
    Ok(rows)
}

/// Load all known domains for a client from the DB.
pub async fn load_known_domains(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT domain FROM companies WHERE client_id = $1 AND domain IS NOT NULL",
    )
    .bind(client_id)
    .fetch_all(pool)
    .await?;

    debug!(
        "Loaded {} known domains for client {}",
        rows.len(),
        client_id
    );
    Ok(rows)
}

/// Load all known LinkedIn URLs for a client from the DB.
pub async fn load_known_linkedin_urls(
    pool: &PgPool,
    client_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT linkedin_url FROM companies WHERE client_id = $1 AND linkedin_url IS NOT NULL",
    )
    .bind(client_id)
    .fetch_all(pool)
    .await?;

    debug!(
        "Loaded {} known linkedin_urls for client {}",
        rows.len(),
        client_id
    );
    Ok(rows)
}

/// Convenience: seed a BatchDeduper with all known identity keys for a client.
/// Call this once before starting the fetch loop.
pub async fn seed_deduper_from_db(
    pool: &PgPool,
    client_id: Uuid,
    deduper: &mut BatchDeduper,
) -> Result<(), sqlx::Error> {
    let external_ids = load_known_external_ids(pool, client_id).await?;
    let domains = load_known_domains(pool, client_id).await?;
    let linkedin_urls = load_known_linkedin_urls(pool, client_id).await?;

    let total = external_ids.len() + domains.len() + linkedin_urls.len();

    deduper.seed_external_ids(external_ids);
    deduper.seed_domains(domains);
    deduper.seed_linkedin_urls(linkedin_urls);

    debug!(
        "Seeded BatchDeduper with {} identity keys for client {}",
        total, client_id
    );
    Ok(())
}

// ============================================================================
// Batch Processing Helper
// ============================================================================

/// Result of processing a batch of Apollo orgs through dedup + upsert.
#[derive(Debug, Default)]
pub struct BatchResult {
    /// Companies that were newly inserted.
    pub inserted: Vec<UpsertResult>,
    /// Companies that matched an existing row (updated).
    pub updated: Vec<UpsertResult>,
    /// Orgs skipped by in-memory dedup (never hit DB).
    pub in_memory_dupes: i32,
    /// Orgs that had no usable identity (no id, no domain, no name).
    pub skipped_no_identity: i32,
}

impl BatchResult {
    pub fn total_processed(&self) -> usize {
        self.inserted.len() + self.updated.len()
    }

    pub fn new_count(&self) -> usize {
        self.inserted.len()
    }

    pub fn dupe_count(&self) -> usize {
        self.updated.len() + self.in_memory_dupes as usize
    }
}

/// Process a batch of Apollo organizations: dedup in memory, then upsert to DB.
///
/// Returns a BatchResult with counts of inserts, updates, and skips.
pub async fn process_batch(
    pool: &PgPool,
    client_id: Uuid,
    orgs: &[ApolloOrganization],
    deduper: &mut BatchDeduper,
) -> Result<BatchResult, sqlx::Error> {
    let mut result = BatchResult::default();

    for org in orgs {
        // Skip orgs with no usable data at all
        if org.name.is_none()
            && org.normalized_domain().is_none()
            && org.normalized_linkedin().is_none()
        {
            warn!(
                "Skipping Apollo org {} — no name, domain, or linkedin",
                org.id
            );
            result.skipped_no_identity += 1;
            continue;
        }

        // In-memory dedup
        if deduper.is_duplicate(org) {
            result.in_memory_dupes += 1;
            continue;
        }

        // DB upsert
        let upsert = upsert_company(pool, client_id, org).await?;

        if upsert.was_insert {
            result.inserted.push(upsert);
        } else {
            result.updated.push(upsert);
        }
    }

    debug!(
        "Batch processed: {} orgs → {} new, {} existing, {} in-mem dupes, {} skipped",
        orgs.len(),
        result.inserted.len(),
        result.updated.len(),
        result.in_memory_dupes,
        result.skipped_no_identity,
    );

    Ok(result)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper to build a minimal ApolloOrganization for testing.
    fn make_org(id: &str, domain: Option<&str>, linkedin: Option<&str>) -> ApolloOrganization {
        ApolloOrganization {
            id: id.to_string(),
            name: Some(format!("Company {}", id)),
            primary_domain: domain.map(String::from),
            website_url: None,
            linkedin_url: linkedin.map(String::from),
            linkedin_uid: None,
            industry: None,
            industries: vec![],
            estimated_num_employees: None,
            founded_year: None,
            city: None,
            state: None,
            country: None,
            postal_code: None,
            raw_address: None,
            street_address: None,
            short_description: None,
            seo_description: None,
            keywords: vec![],
            phone: None,
            logo_url: None,
            annual_revenue: None,
            total_funding: None,
            total_funding_printed: None,
            latest_funding_round_date: None,
            latest_funding_stage: None,
            industry_tag_id: None,
            industry_tag_hash: None,
        }
    }

    // ---- BatchDeduper Tests ----

    #[test]
    fn test_first_org_is_not_duplicate() {
        let mut deduper = BatchDeduper::new();
        let org = make_org("abc123", Some("acme.com"), None);
        assert!(!deduper.is_duplicate(&org));
        assert_eq!(deduper.unique_count(), 1);
    }

    #[test]
    fn test_same_external_id_is_duplicate() {
        let mut deduper = BatchDeduper::new();
        let org1 = make_org("abc123", Some("acme.com"), None);
        let org2 = make_org("abc123", Some("different.com"), None);

        assert!(!deduper.is_duplicate(&org1));
        assert!(deduper.is_duplicate(&org2));
        assert_eq!(deduper.duplicates_caught, 1);
    }

    #[test]
    fn test_same_domain_is_duplicate() {
        let mut deduper = BatchDeduper::new();
        let org1 = make_org("id1", Some("acme.com"), None);
        let org2 = make_org("id2", Some("www.Acme.com"), None); // normalizes to same

        assert!(!deduper.is_duplicate(&org1));
        assert!(deduper.is_duplicate(&org2));
        assert_eq!(deduper.duplicates_caught, 1);
    }

    #[test]
    fn test_same_linkedin_is_duplicate() {
        let mut deduper = BatchDeduper::new();
        let org1 = make_org("id1", None, Some("http://linkedin.com/company/acme/"));
        let org2 = make_org("id2", None, Some("http://linkedin.com/company/acme"));

        assert!(!deduper.is_duplicate(&org1));
        assert!(deduper.is_duplicate(&org2));
    }

    #[test]
    fn test_different_orgs_not_duplicate() {
        let mut deduper = BatchDeduper::new();
        let org1 = make_org("id1", Some("acme.com"), None);
        let org2 = make_org("id2", Some("globex.com"), None);
        let org3 = make_org("id3", None, Some("http://linkedin.com/company/initech"));

        assert!(!deduper.is_duplicate(&org1));
        assert!(!deduper.is_duplicate(&org2));
        assert!(!deduper.is_duplicate(&org3));
        assert_eq!(deduper.unique_count(), 3);
        assert_eq!(deduper.duplicates_caught, 0);
    }

    #[test]
    fn test_seed_domains_catches_future_orgs() {
        let mut deduper = BatchDeduper::new();
        deduper.seed_domains(vec!["acme.com".to_string()]);

        let org = make_org("new_id", Some("www.Acme.com"), None);
        assert!(deduper.is_duplicate(&org));
    }

    #[test]
    fn test_seed_external_ids() {
        let mut deduper = BatchDeduper::new();
        deduper.seed_external_ids(vec!["existing123".to_string()]);

        let org = make_org("existing123", Some("new-domain.com"), None);
        assert!(deduper.is_duplicate(&org));
    }

    #[test]
    fn test_seed_linkedin_urls() {
        let mut deduper = BatchDeduper::new();
        deduper.seed_linkedin_urls(vec![
            "http://www.linkedin.com/company/ACME/".to_string(),
        ]);

        let org = make_org("new_id", None, Some("http://www.linkedin.com/company/acme"));
        assert!(deduper.is_duplicate(&org));
    }

    #[test]
    fn test_with_capacity() {
        let deduper = BatchDeduper::with_capacity(5000);
        assert_eq!(deduper.unique_count(), 0);
        assert_eq!(deduper.duplicates_caught, 0);
    }

    #[test]
    fn test_empty_domain_not_treated_as_identity() {
        let mut deduper = BatchDeduper::new();
        // Org with empty-string domain after normalization
        let org1 = make_org("id1", Some(""), None);
        let org2 = make_org("id2", Some(""), None);

        assert!(!deduper.is_duplicate(&org1));
        // Different external_id, empty domain shouldn't match
        assert!(!deduper.is_duplicate(&org2));
    }

    // ---- BatchResult Tests ----

    #[test]
    fn test_batch_result_counts() {
        let mut result = BatchResult::default();
        result.inserted.push(UpsertResult {
            company_id: Uuid::new_v4(),
            was_insert: true,
        });
        result.inserted.push(UpsertResult {
            company_id: Uuid::new_v4(),
            was_insert: true,
        });
        result.updated.push(UpsertResult {
            company_id: Uuid::new_v4(),
            was_insert: false,
        });
        result.in_memory_dupes = 3;

        assert_eq!(result.total_processed(), 3);
        assert_eq!(result.new_count(), 2);
        assert_eq!(result.dupe_count(), 4); // 1 updated + 3 in-mem
    }
}
