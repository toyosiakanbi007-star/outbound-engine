// src/jobs/company_fetcher/query_variants.rs
//
// Query Variant Ladder for the Company Fetcher.
//
// RESPONSIBILITIES:
// - Build Apollo Organization Search requests for each variant level
// - Progressively broaden filters when earlier variants are exhausted
// - Manage the variant ladder lifecycle per industry
//
// VARIANT LADDER:
//   V1 Strict:         industry + base locations + base sizes
//   V2 Broaden Geo:    industry + (base + v2_locations) + base sizes
//   V3 Broaden Size:   industry + (base + v2_locations) + (base + v3_sizes)
//   V4 Keyword Assist: industry + keywords added to reduce taxonomy noise
//
// Each variant is tried in order. When Apollo results are exhausted for a
// variant (fewer results than per_page, or we've paged through all available),
// the orchestrator calls `advance_variant()` to move to the next one.
//
// USAGE:
//   let builder = VariantBuilder::new(&icp_profile);
//   let request = builder.build_request("fintech", QueryVariant::V1Strict, 1, 100);

use std::collections::HashMap;
use tracing::debug;

use super::models::{ApolloOrgSearchRequest, IcpFetchProfile, IndustrySpec, QueryVariant};

// ============================================================================
// VariantBuilder
// ============================================================================

/// Builds Apollo Organization Search requests for each variant level,
/// using the ICP profile to determine filters.
///
/// For Diffbot, also attaches per-industry keyword constraints from IndustrySpec.
///
/// Stateless — safe to reuse across industries and pages.
#[derive(Debug, Clone)]
pub struct VariantBuilder {
    /// Base locations from ICP (used in V1+).
    locations: Vec<String>,
    /// Excluded locations (used in all variants).
    excluded_locations: Vec<String>,
    /// Base company sizes from ICP (used in V1, V2).
    company_sizes: Vec<String>,
    /// V2 expansion: additional locations.
    v2_locations: Vec<String>,
    /// V3 expansion: additional size ranges.
    v3_sizes: Vec<String>,
    /// Keywords from ICP (used in V4).
    keywords: Vec<String>,
    /// Revenue filter (used in all variants).
    revenue_min: Option<i64>,
    revenue_max: Option<i64>,
    /// Per-industry keyword specs (name → IndustrySpec).
    industry_specs: HashMap<String, IndustrySpec>,
}

impl VariantBuilder {
    /// Create a builder from a parsed ICP profile.
    pub fn new(icp: &IcpFetchProfile) -> Self {
        let industry_specs: HashMap<String, IndustrySpec> = icp
            .industries
            .iter()
            .map(|s| (s.name.clone(), s.clone()))
            .collect();

        Self {
            locations: icp.locations.clone(),
            excluded_locations: icp.excluded_locations.clone(),
            company_sizes: icp.company_sizes.clone(),
            v2_locations: icp.v2_locations.clone(),
            v3_sizes: icp.v3_sizes.clone(),
            keywords: icp.keywords.clone(),
            revenue_min: icp.revenue_min,
            revenue_max: icp.revenue_max,
            industry_specs,
        }
    }

    /// Build an Apollo request for a specific industry, variant, and page.
    ///
    /// If the industry has an IndustrySpec with keyword constraints, those are
    /// attached to the request (the DQL builder will emit OR/NOT groups).
    ///
    /// # Arguments
    /// * `industry` — Industry name string (must match IndustrySpec.name).
    /// * `variant` — Which variant level to use.
    /// * `page` — Apollo page number (1-indexed).
    /// * `per_page` — Results per page (max 100).
    pub fn build_request(
        &self,
        industry: &str,
        variant: QueryVariant,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        let mut request = match variant {
            QueryVariant::V1Strict => self.build_v1(industry, page, per_page),
            QueryVariant::V2BroadenGeo => self.build_v2(industry, page, per_page),
            QueryVariant::V3BroadenSize => self.build_v3(industry, page, per_page),
            QueryVariant::V4KeywordAssist => self.build_v4(industry, page, per_page),
        };

        // Attach per-industry keyword constraints from IndustrySpec
        if let Some(spec) = self.industry_specs.get(industry) {
            // Use diffbot_category for the industry filter (overwrite the industry name)
            if spec.diffbot_category != spec.name {
                request.organization_industry_tag_ids = Some(vec![spec.diffbot_category.clone()]);
            }

            // Attach keyword constraints
            request = request
                .with_include_keywords_any(spec.include_keywords_any.clone())
                .with_include_keywords_all(spec.include_keywords_all.clone())
                .with_exclude_keywords_any(spec.exclude_keywords_any.clone())
                .with_variant_label("tight");
        }

        request
    }

    /// Build a "loose" variant of a request: same as tight but WITHOUT
    /// include_keywords_any (keeps exclude keywords for safety).
    ///
    /// Used as fallback when the tight variant returns too few results.
    pub fn build_loose_request(
        &self,
        industry: &str,
        variant: QueryVariant,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        let mut request = match variant {
            QueryVariant::V1Strict => self.build_v1(industry, page, per_page),
            QueryVariant::V2BroadenGeo => self.build_v2(industry, page, per_page),
            QueryVariant::V3BroadenSize => self.build_v3(industry, page, per_page),
            QueryVariant::V4KeywordAssist => self.build_v4(industry, page, per_page),
        };

        // Attach only exclude keywords (no include = broader results)
        if let Some(spec) = self.industry_specs.get(industry) {
            if spec.diffbot_category != spec.name {
                request.organization_industry_tag_ids = Some(vec![spec.diffbot_category.clone()]);
            }

            request = request
                .with_exclude_keywords_any(spec.exclude_keywords_any.clone())
                .with_variant_label("loose");
        }

        request
    }

    /// Whether a given industry has include keyword constraints
    /// (meaning tight→loose fallback is applicable).
    pub fn industry_has_include_keywords(&self, industry: &str) -> bool {
        self.industry_specs
            .get(industry)
            .map(|s| s.has_include_constraints())
            .unwrap_or(false)
    }

    /// Check whether a given variant can actually broaden anything compared to
    /// the previous variant. If not, the orchestrator should skip it.
    ///
    /// Example: V2 is pointless if v2_locations is empty (nothing new to add).
    pub fn variant_has_effect(&self, variant: QueryVariant) -> bool {
        match variant {
            // V1 always has effect — it's the base query.
            QueryVariant::V1Strict => true,
            // V2 only adds value if there are expansion locations.
            QueryVariant::V2BroadenGeo => !self.v2_locations.is_empty(),
            // V3 only adds value if there are expansion sizes.
            QueryVariant::V3BroadenSize => !self.v3_sizes.is_empty(),
            // V4 only adds value if there are keywords to inject.
            QueryVariant::V4KeywordAssist => !self.keywords.is_empty(),
        }
    }

    /// Return the next variant in the ladder that actually has effect,
    /// or None if all variants are exhausted.
    ///
    /// Starts checking from the variant *after* `current`.
    pub fn next_effective_variant(&self, current: QueryVariant) -> Option<QueryVariant> {
        let ladder = QueryVariant::ladder();
        let current_idx = ladder
            .iter()
            .position(|v| *v == current)
            .unwrap_or(ladder.len());

        ladder
            .iter()
            .skip(current_idx + 1)
            .find(|v| self.variant_has_effect(**v))
            .copied()
    }

    // ========================================================================
    // V1: Strict — base industry + geo + size
    // ========================================================================

    fn build_v1(
        &self,
        industry: &str,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        debug!(
            "V1 Strict: industry={}, locations={:?}, sizes={:?}",
            industry, self.locations, self.company_sizes
        );

        ApolloOrgSearchRequest::new(page, per_page)
            .with_industries(vec![industry.to_string()])
            .with_locations(self.locations.clone())
            .with_excluded_locations(self.excluded_locations.clone())
            .with_employee_ranges(self.company_sizes.clone())
            .with_revenue(self.revenue_min, self.revenue_max)
    }

    // ========================================================================
    // V2: Broaden Geo — add v2_locations to base locations
    // ========================================================================

    fn build_v2(
        &self,
        industry: &str,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        let mut expanded_locations = self.locations.clone();
        for loc in &self.v2_locations {
            if !expanded_locations.contains(loc) {
                expanded_locations.push(loc.clone());
            }
        }

        debug!(
            "V2 Broaden Geo: industry={}, locations={:?} (+{} new), sizes={:?}",
            industry,
            expanded_locations,
            self.v2_locations.len(),
            self.company_sizes
        );

        ApolloOrgSearchRequest::new(page, per_page)
            .with_industries(vec![industry.to_string()])
            .with_locations(expanded_locations)
            .with_excluded_locations(self.excluded_locations.clone())
            .with_employee_ranges(self.company_sizes.clone())
            .with_revenue(self.revenue_min, self.revenue_max)
    }

    // ========================================================================
    // V3: Broaden Size — add v3_sizes to base + expanded geo
    // ========================================================================

    fn build_v3(
        &self,
        industry: &str,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        // Keep expanded geo from V2
        let mut expanded_locations = self.locations.clone();
        for loc in &self.v2_locations {
            if !expanded_locations.contains(loc) {
                expanded_locations.push(loc.clone());
            }
        }

        // Expand sizes
        let mut expanded_sizes = self.company_sizes.clone();
        for size in &self.v3_sizes {
            if !expanded_sizes.contains(size) {
                expanded_sizes.push(size.clone());
            }
        }

        debug!(
            "V3 Broaden Size: industry={}, locations={:?}, sizes={:?} (+{} new)",
            industry,
            expanded_locations,
            expanded_sizes,
            self.v3_sizes.len()
        );

        ApolloOrgSearchRequest::new(page, per_page)
            .with_industries(vec![industry.to_string()])
            .with_locations(expanded_locations)
            .with_excluded_locations(self.excluded_locations.clone())
            .with_employee_ranges(expanded_sizes)
            .with_revenue(self.revenue_min, self.revenue_max)
    }

    // ========================================================================
    // V4: Keyword Assist — same as V3 but with keyword tags
    // ========================================================================

    fn build_v4(
        &self,
        industry: &str,
        page: i32,
        per_page: i32,
    ) -> ApolloOrgSearchRequest {
        // Keep all V3 expansions
        let mut expanded_locations = self.locations.clone();
        for loc in &self.v2_locations {
            if !expanded_locations.contains(loc) {
                expanded_locations.push(loc.clone());
            }
        }

        let mut expanded_sizes = self.company_sizes.clone();
        for size in &self.v3_sizes {
            if !expanded_sizes.contains(size) {
                expanded_sizes.push(size.clone());
            }
        }

        debug!(
            "V4 Keyword Assist: industry={}, keywords={:?}, locations={:?}, sizes={:?}",
            industry, self.keywords, expanded_locations, expanded_sizes
        );

        ApolloOrgSearchRequest::new(page, per_page)
            .with_industries(vec![industry.to_string()])
            .with_locations(expanded_locations)
            .with_excluded_locations(self.excluded_locations.clone())
            .with_employee_ranges(expanded_sizes)
            .with_keywords(self.keywords.clone())
            .with_revenue(self.revenue_min, self.revenue_max)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_icp() -> IcpFetchProfile {
        IcpFetchProfile {
            industries: vec![
                IndustrySpec::from_category_string("fintech"),
                IndustrySpec::from_category_string("healthcare_saas"),
            ],
            company_sizes: vec!["51-200".into(), "201-500".into()],
            locations: vec!["United States".into(), "Canada".into()],
            excluded_locations: vec!["China".into()],
            keywords: vec!["SaaS".into(), "platform".into()],
            revenue_min: Some(1_000_000),
            revenue_max: Some(500_000_000),
            v2_locations: vec!["Germany".into(), "France".into()],
            v3_sizes: vec!["11-50".into(), "501-1000".into()],
        }
    }

    fn make_icp_minimal() -> IcpFetchProfile {
        IcpFetchProfile {
            industries: vec![
                IndustrySpec::from_category_string("cybersecurity"),
            ],
            company_sizes: vec!["201-500".into()],
            locations: vec!["United States".into()],
            excluded_locations: vec![],
            keywords: vec![],
            revenue_min: None,
            revenue_max: None,
            v2_locations: vec![],
            v3_sizes: vec![],
        }
    }

    // ---- V1 Strict ----

    #[test]
    fn test_v1_uses_base_filters() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V1Strict, 1, 100);

        assert_eq!(req.page, 1);
        assert_eq!(req.per_page, 100);
        assert_eq!(
            req.organization_industry_tag_ids,
            Some(vec!["fintech".into()])
        );
        assert_eq!(
            req.organization_locations,
            Some(vec!["United States".into(), "Canada".into()])
        );
        assert_eq!(
            req.organization_not_locations,
            Some(vec!["China".into()])
        );
        assert_eq!(
            req.organization_num_employees_ranges,
            Some(vec!["51-200".into(), "201-500".into()])
        );
        // V1 should NOT include keywords
        assert!(req.q_organization_keyword_tags.is_none());
        // Revenue should be present
        assert!(req.revenue_range.is_some());
    }

    // ---- V2 Broaden Geo ----

    #[test]
    fn test_v2_expands_locations() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V2BroadenGeo, 1, 100);

        let locs = req.organization_locations.unwrap();
        assert!(locs.contains(&"United States".into()));
        assert!(locs.contains(&"Canada".into()));
        assert!(locs.contains(&"Germany".into()));
        assert!(locs.contains(&"France".into()));
        assert_eq!(locs.len(), 4);

        // Sizes should still be base
        let sizes = req.organization_num_employees_ranges.unwrap();
        assert_eq!(sizes.len(), 2);
        assert!(!sizes.contains(&"11-50".into()));
    }

    #[test]
    fn test_v2_no_duplicate_locations() {
        let mut icp = make_icp();
        // Add a v2 location that's already in base
        icp.v2_locations.push("United States".into());

        let builder = VariantBuilder::new(&icp);
        let req = builder.build_request("fintech", QueryVariant::V2BroadenGeo, 1, 100);

        let locs = req.organization_locations.unwrap();
        let us_count = locs.iter().filter(|l| *l == "United States").count();
        assert_eq!(us_count, 1, "Should not duplicate locations");
    }

    // ---- V3 Broaden Size ----

    #[test]
    fn test_v3_expands_both() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V3BroadenSize, 1, 100);

        // Locations should include V2 expansions
        let locs = req.organization_locations.unwrap();
        assert_eq!(locs.len(), 4);
        assert!(locs.contains(&"Germany".into()));

        // Sizes should include V3 expansions
        let sizes = req.organization_num_employees_ranges.unwrap();
        assert_eq!(sizes.len(), 4);
        assert!(sizes.contains(&"11-50".into()));
        assert!(sizes.contains(&"501-1000".into()));
    }

    // ---- V4 Keyword Assist ----

    #[test]
    fn test_v4_adds_keywords() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V4KeywordAssist, 1, 100);

        let kw = req.q_organization_keyword_tags.unwrap();
        assert!(kw.contains(&"SaaS".into()));
        assert!(kw.contains(&"platform".into()));

        // Should still have expanded locations + sizes from V3
        let locs = req.organization_locations.unwrap();
        assert_eq!(locs.len(), 4);
        let sizes = req.organization_num_employees_ranges.unwrap();
        assert_eq!(sizes.len(), 4);
    }

    // ---- Variant Effectiveness ----

    #[test]
    fn test_variant_has_effect_full_icp() {
        let builder = VariantBuilder::new(&make_icp());
        assert!(builder.variant_has_effect(QueryVariant::V1Strict));
        assert!(builder.variant_has_effect(QueryVariant::V2BroadenGeo));
        assert!(builder.variant_has_effect(QueryVariant::V3BroadenSize));
        assert!(builder.variant_has_effect(QueryVariant::V4KeywordAssist));
    }

    #[test]
    fn test_variant_has_effect_minimal_icp() {
        let builder = VariantBuilder::new(&make_icp_minimal());
        assert!(builder.variant_has_effect(QueryVariant::V1Strict));
        assert!(!builder.variant_has_effect(QueryVariant::V2BroadenGeo)); // no v2_locations
        assert!(!builder.variant_has_effect(QueryVariant::V3BroadenSize)); // no v3_sizes
        assert!(!builder.variant_has_effect(QueryVariant::V4KeywordAssist)); // no keywords
    }

    // ---- Next Effective Variant ----

    #[test]
    fn test_next_effective_full() {
        let builder = VariantBuilder::new(&make_icp());

        assert_eq!(
            builder.next_effective_variant(QueryVariant::V1Strict),
            Some(QueryVariant::V2BroadenGeo)
        );
        assert_eq!(
            builder.next_effective_variant(QueryVariant::V2BroadenGeo),
            Some(QueryVariant::V3BroadenSize)
        );
        assert_eq!(
            builder.next_effective_variant(QueryVariant::V3BroadenSize),
            Some(QueryVariant::V4KeywordAssist)
        );
        assert_eq!(
            builder.next_effective_variant(QueryVariant::V4KeywordAssist),
            None
        );
    }

    #[test]
    fn test_next_effective_skips_useless_variants() {
        // Minimal ICP: V2, V3, V4 all have no effect
        let builder = VariantBuilder::new(&make_icp_minimal());

        assert_eq!(
            builder.next_effective_variant(QueryVariant::V1Strict),
            None, // Should skip V2, V3, V4 since none have effect
        );
    }

    #[test]
    fn test_next_effective_skips_middle() {
        // ICP with keywords but no geo/size expansion
        let mut icp = make_icp_minimal();
        icp.keywords = vec!["SaaS".into()];

        let builder = VariantBuilder::new(&icp);

        // V1 → skip V2 (no v2_locations) → skip V3 (no v3_sizes) → V4 (has keywords)
        assert_eq!(
            builder.next_effective_variant(QueryVariant::V1Strict),
            Some(QueryVariant::V4KeywordAssist)
        );
    }

    // ---- Page / Per-Page ----

    #[test]
    fn test_page_number_passed_through() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V1Strict, 42, 50);
        assert_eq!(req.page, 42);
        assert_eq!(req.per_page, 50);
    }

    // ---- Revenue filter ----

    #[test]
    fn test_revenue_filter_present() {
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V1Strict, 1, 100);
        let rev = req.revenue_range.unwrap();
        assert_eq!(rev.min, Some(1_000_000));
        assert_eq!(rev.max, Some(500_000_000));
    }

    #[test]
    fn test_revenue_filter_absent() {
        let builder = VariantBuilder::new(&make_icp_minimal());
        let req = builder.build_request("cybersecurity", QueryVariant::V1Strict, 1, 100);
        assert!(req.revenue_range.is_none());
    }

    // ---- IndustrySpec Keyword Constraints ----

    fn make_icp_with_keywords() -> IcpFetchProfile {
        IcpFetchProfile {
            industries: vec![
                IndustrySpec {
                    name: "Lending".to_string(),
                    diffbot_category: "Currency And Lending Services".to_string(),
                    include_keywords_any: vec!["loan".into(), "lending".into(), "credit".into()],
                    include_keywords_all: vec![],
                    exclude_keywords_any: vec!["crypto exchange".into(), "forex signals".into()],
                    priority: 0.9,
                },
                IndustrySpec {
                    name: "Construction".to_string(),
                    diffbot_category: "Infrastructure Construction Companies".to_string(),
                    include_keywords_any: vec!["contract".into(), "design".into(), "build".into()],
                    include_keywords_all: vec!["construction".into()],
                    exclude_keywords_any: vec!["residential".into(), "roofing".into()],
                    priority: 0.8,
                },
            ],
            company_sizes: vec!["51-200".into()],
            locations: vec!["United States".into()],
            excluded_locations: vec![],
            keywords: vec![],
            revenue_min: None,
            revenue_max: None,
            v2_locations: vec![],
            v3_sizes: vec![],
        }
    }

    #[test]
    fn test_tight_request_attaches_all_keywords() {
        let builder = VariantBuilder::new(&make_icp_with_keywords());
        let req = builder.build_request("Lending", QueryVariant::V1Strict, 1, 100);

        // diffbot_category should override the industry tag
        assert_eq!(
            req.organization_industry_tag_ids,
            Some(vec!["Currency And Lending Services".into()])
        );

        // include_any keywords attached
        let inc_any = req.include_keywords_any.unwrap();
        assert_eq!(inc_any.len(), 3);
        assert_eq!(inc_any[0], "loan");
        assert_eq!(inc_any[1], "lending");
        assert_eq!(inc_any[2], "credit");

        // exclude keywords attached
        let exc = req.exclude_keywords_any.unwrap();
        assert_eq!(exc.len(), 2);
        assert_eq!(exc[0], "crypto exchange");
        assert_eq!(exc[1], "forex signals");

        // variant label is "tight"
        assert_eq!(req.query_variant_label, Some("tight".to_string()));

        // include_all should be None (empty vec in spec)
        assert!(req.include_keywords_all.is_none());
    }

    #[test]
    fn test_tight_request_with_include_all() {
        let builder = VariantBuilder::new(&make_icp_with_keywords());
        let req = builder.build_request("Construction", QueryVariant::V1Strict, 1, 100);

        // include_all should be present
        let inc_all = req.include_keywords_all.unwrap();
        assert_eq!(inc_all.len(), 1);
        assert_eq!(inc_all[0], "construction");

        // include_any also present
        let inc_any = req.include_keywords_any.unwrap();
        assert_eq!(inc_any.len(), 3);
        assert_eq!(inc_any[0], "contract");
    }

    #[test]
    fn test_loose_request_drops_include_any_keeps_exclude() {
        let builder = VariantBuilder::new(&make_icp_with_keywords());
        let req = builder.build_loose_request("Lending", QueryVariant::V1Strict, 1, 100);

        // include_any should NOT be present (loose drops it)
        assert!(req.include_keywords_any.is_none());

        // include_all should NOT be present
        assert!(req.include_keywords_all.is_none());

        // exclude keywords SHOULD still be present
        let exc = req.exclude_keywords_any.unwrap();
        assert_eq!(exc.len(), 2);
        assert_eq!(exc[0], "crypto exchange");

        // variant label is "loose"
        assert_eq!(req.query_variant_label, Some("loose".to_string()));

        // diffbot_category still overrides
        assert_eq!(
            req.organization_industry_tag_ids,
            Some(vec!["Currency And Lending Services".into()])
        );
    }

    #[test]
    fn test_industry_has_include_keywords() {
        let builder = VariantBuilder::new(&make_icp_with_keywords());
        assert!(builder.industry_has_include_keywords("Lending"));
        assert!(builder.industry_has_include_keywords("Construction"));
    }

    #[test]
    fn test_industry_without_keywords_no_fallback_needed() {
        let builder = VariantBuilder::new(&make_icp());
        // from_category_string has no keywords
        assert!(!builder.industry_has_include_keywords("fintech"));
        assert!(!builder.industry_has_include_keywords("healthcare_saas"));
    }

    #[test]
    fn test_plain_industry_no_keyword_override() {
        // Industries from from_category_string should NOT get keyword fields
        let builder = VariantBuilder::new(&make_icp());
        let req = builder.build_request("fintech", QueryVariant::V1Strict, 1, 100);

        assert!(req.include_keywords_any.is_none());
        assert!(req.include_keywords_all.is_none());
        assert!(req.exclude_keywords_any.is_none());
        // name == diffbot_category, so no override
        assert_eq!(
            req.organization_industry_tag_ids,
            Some(vec!["fintech".into()])
        );
    }
}
