-- V3 Hardening Schema Migration
-- File: 0018_v3_hardening_schema.sql
-- 
-- This migration adds V3 hardening tables and columns for:
-- - Date tracking with staleness decay
-- - Source tier classification  
-- - Corroboration tracking
-- - Syndication deduplication
-- - Offer-fit scoring
-- - Recency gates
--
-- ADDITIVE: All V2 tables preserved, new V3 tables added

-- ============================================================================
-- PART 1: Add V3 columns to existing tables
-- ============================================================================

-- 1.1 Add V3 columns to discovered_urls
ALTER TABLE discovered_urls 
ADD COLUMN IF NOT EXISTS source_tier INTEGER DEFAULT 3,
ADD COLUMN IF NOT EXISTS url_date TEXT,
ADD COLUMN IF NOT EXISTS days_ago INTEGER;

-- 1.2 Add V3 columns to company_prequal
ALTER TABLE company_prequal
ADD COLUMN IF NOT EXISTS raw_score DOUBLE PRECISION DEFAULT 0,
ADD COLUMN IF NOT EXISTS staleness_adjusted_score DOUBLE PRECISION DEFAULT 0,
ADD COLUMN IF NOT EXISTS evidence_summary JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS recency_summary JSONB DEFAULT '{}';

-- ============================================================================
-- PART 2: Create V3 Evidence Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS v3_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    url TEXT NOT NULL,
    
    -- Verbatim quote (MUST be exact text from page)
    verbatim_quote TEXT,
    
    -- Date handling
    extracted_date TEXT,  -- YYYY-MM-DD
    date_source TEXT DEFAULT 'none',  -- meta_tag, byline, content, url_pattern, inferred, none
    date_confidence TEXT DEFAULT 'none',  -- high, medium, low, none
    observed_at TEXT,  -- When we saw it (fallback)
    days_ago INTEGER,
    staleness_multiplier DOUBLE PRECISION DEFAULT 0.5,
    
    -- Source classification
    source_tier INTEGER DEFAULT 3 CHECK (source_tier BETWEEN 1 AND 3),
    source_type TEXT DEFAULT 'other',
    source_domain TEXT,
    
    -- Verification status
    evidence_status TEXT DEFAULT 'verified',  -- verified, unverified, blocked
    can_drive_high_confidence BOOLEAN DEFAULT TRUE,
    
    -- Deduplication
    content_hash TEXT,
    is_syndication BOOLEAN DEFAULT FALSE,
    canonical_source_id UUID,
    
    -- Pain classification
    evidence_type TEXT DEFAULT 'signal',
    pain_category TEXT,
    pain_subtags TEXT[] DEFAULT '{}',
    pain_indicators TEXT[] DEFAULT '{}',
    
    -- Score
    confidence DOUBLE PRECISION DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    CONSTRAINT fk_v3_evidence_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- Indexes for v3_evidence
CREATE INDEX IF NOT EXISTS idx_v3_evidence_company_id ON v3_evidence(company_id);
CREATE INDEX IF NOT EXISTS idx_v3_evidence_run_id ON v3_evidence(run_id);
CREATE INDEX IF NOT EXISTS idx_v3_evidence_content_hash ON v3_evidence(content_hash) WHERE content_hash IS NOT NULL AND content_hash != '';
CREATE INDEX IF NOT EXISTS idx_v3_evidence_source_tier ON v3_evidence(source_tier);
CREATE INDEX IF NOT EXISTS idx_v3_evidence_days_ago ON v3_evidence(days_ago) WHERE days_ago IS NOT NULL;

-- Partial unique index for syndication deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_v3_evidence_dedup 
ON v3_evidence(company_id, content_hash) 
WHERE content_hash IS NOT NULL AND content_hash != '';

-- ============================================================================
-- PART 3: Create V3 Hypotheses Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS v3_hypotheses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id TEXT NOT NULL,
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    
    -- Core hypothesis
    hypothesis TEXT NOT NULL,
    pain_category TEXT NOT NULL,
    pain_type TEXT DEFAULT 'structural',  -- event or structural
    pain_subtags TEXT[] DEFAULT '{}',
    
    -- Confidence breakdown
    raw_confidence DOUBLE PRECISION DEFAULT 0.5,
    staleness_penalty DOUBLE PRECISION DEFAULT 1.0,
    corroboration_penalty DOUBLE PRECISION DEFAULT 1.0,
    final_confidence DOUBLE PRECISION DEFAULT 0.5,
    
    -- Corroboration tracking
    corroborated BOOLEAN DEFAULT FALSE,
    corroborated_by TEXT[] DEFAULT '{}',  -- List of domains
    corroboration_needed TEXT,  -- What's needed
    
    -- Structured why-now
    why_now_text TEXT,
    why_now_type TEXT DEFAULT 'structural',  -- deadline, event, window, structural
    why_now_date TEXT,
    why_now_days_until INTEGER,
    why_now_urgency TEXT DEFAULT 'medium',  -- high, medium, low
    
    -- Evidence bundle (JSONB for flexibility)
    evidence JSONB DEFAULT '[]',
    evidence_summary JSONB DEFAULT '{}',
    
    -- Offer fit
    offer_fit_strength TEXT DEFAULT 'medium',  -- strong, medium, weak
    do_not_outreach BOOLEAN DEFAULT FALSE,
    
    -- Personas
    recommended_personas TEXT[] DEFAULT '{}',
    suggested_offer_fit TEXT,
    
    -- Legacy compat
    strength TEXT DEFAULT 'medium',  -- low, medium, high
    phase TEXT DEFAULT 'prequal',  -- prequal or aggregate
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    CONSTRAINT fk_v3_hypotheses_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- Indexes for v3_hypotheses
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_company_id ON v3_hypotheses(company_id);
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_run_id ON v3_hypotheses(run_id);
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_pain_category ON v3_hypotheses(pain_category);
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_final_confidence ON v3_hypotheses(final_confidence);
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_offer_fit ON v3_hypotheses(offer_fit_strength);
CREATE INDEX IF NOT EXISTS idx_v3_hypotheses_do_not_outreach ON v3_hypotheses(do_not_outreach) WHERE do_not_outreach = TRUE;

-- ============================================================================
-- PART 4: Create V3 Analysis Runs Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS v3_analysis_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id TEXT NOT NULL,
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    
    -- Scores
    raw_score DOUBLE PRECISION DEFAULT 0,
    staleness_adjusted_score DOUBLE PRECISION DEFAULT 0,
    final_score DOUBLE PRECISION DEFAULT 0,
    
    -- Summaries (JSONB)
    evidence_summary JSONB DEFAULT '{}',
    recency_summary JSONB DEFAULT '{}',
    
    -- Gates
    gates JSONB DEFAULT '{}',
    gates_passed TEXT[] DEFAULT '{}',
    gates_failed TEXT[] DEFAULT '{}',
    qualifies BOOLEAN DEFAULT FALSE,
    
    -- Hypothesis summary
    hypothesis_count INTEGER DEFAULT 0,
    strong_fit_count INTEGER DEFAULT 0,
    do_not_outreach BOOLEAN DEFAULT FALSE,
    
    -- Timing
    started_at TEXT,
    completed_at TEXT,
    duration_ms INTEGER,
    
    -- Phase
    phase TEXT DEFAULT 'prequal',
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    CONSTRAINT fk_v3_analysis_runs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- Indexes for v3_analysis_runs
CREATE INDEX IF NOT EXISTS idx_v3_analysis_runs_company_id ON v3_analysis_runs(company_id);
CREATE INDEX IF NOT EXISTS idx_v3_analysis_runs_run_id ON v3_analysis_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_v3_analysis_runs_qualifies ON v3_analysis_runs(qualifies);
CREATE INDEX IF NOT EXISTS idx_v3_analysis_runs_final_score ON v3_analysis_runs(final_score);

-- Unique constraint for company + run
CREATE UNIQUE INDEX IF NOT EXISTS idx_v3_analysis_runs_unique 
ON v3_analysis_runs(company_id, run_id);

-- ============================================================================
-- PART 5: Source Domain Tiers Reference Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS source_domain_tiers (
    domain TEXT PRIMARY KEY,
    tier INTEGER NOT NULL CHECK (tier BETWEEN 1 AND 3),
    category TEXT,  -- official, media, forum, aggregator, etc.
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed known domains
INSERT INTO source_domain_tiers (domain, tier, category, notes) VALUES
    -- Tier 1: Official/Primary sources
    ('sec.gov', 1, 'official', 'SEC filings'),
    ('prnewswire.com', 1, 'official', 'Official press releases'),
    ('businesswire.com', 1, 'official', 'Official press releases'),
    ('globenewswire.com', 1, 'official', 'Official press releases'),
    ('accesswire.com', 1, 'official', 'Official press releases'),
    ('marketwatch.com', 1, 'official', 'Financial news with official data'),
    
    -- Tier 2: Major media
    ('reuters.com', 2, 'media', 'Major wire service'),
    ('bloomberg.com', 2, 'media', 'Financial news'),
    ('techcrunch.com', 2, 'media', 'Tech news'),
    ('wsj.com', 2, 'media', 'Wall Street Journal'),
    ('nytimes.com', 2, 'media', 'New York Times'),
    ('forbes.com', 2, 'media', 'Business magazine'),
    ('venturebeat.com', 2, 'media', 'Tech news'),
    ('theinformation.com', 2, 'media', 'Tech news'),
    ('axios.com', 2, 'media', 'News'),
    ('cnbc.com', 2, 'media', 'Financial news'),
    ('ft.com', 2, 'media', 'Financial Times'),
    ('bbc.com', 2, 'media', 'BBC News'),
    ('wired.com', 2, 'media', 'Tech magazine'),
    ('theverge.com', 2, 'media', 'Tech news'),
    ('arstechnica.com', 2, 'media', 'Tech news'),
    ('zdnet.com', 2, 'media', 'Tech news'),
    
    -- Tier 3: Forums, aggregators (implicit default)
    ('reddit.com', 3, 'forum', 'Community discussions'),
    ('news.ycombinator.com', 3, 'forum', 'Hacker News'),
    ('glassdoor.com', 3, 'review', 'Employee reviews'),
    ('g2.com', 3, 'review', 'Software reviews'),
    ('capterra.com', 3, 'review', 'Software reviews'),
    ('trustpilot.com', 3, 'review', 'Customer reviews')
ON CONFLICT (domain) DO NOTHING;

-- ============================================================================
-- PART 6: Helper Functions
-- ============================================================================

-- Function to compute staleness multiplier
CREATE OR REPLACE FUNCTION compute_staleness_multiplier(days_old INTEGER)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF days_old IS NULL THEN
        RETURN 0.5;  -- Unknown date = medium penalty
    ELSIF days_old <= 30 THEN
        RETURN 1.0;
    ELSIF days_old <= 90 THEN
        RETURN 0.8;
    ELSIF days_old <= 180 THEN
        RETURN 0.6;
    ELSIF days_old <= 365 THEN
        RETURN 0.35;
    ELSE
        RETURN 0.15;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get source tier from domain
CREATE OR REPLACE FUNCTION get_source_tier(domain_name TEXT)
RETURNS INTEGER AS $$
DECLARE
    tier_result INTEGER;
BEGIN
    SELECT tier INTO tier_result
    FROM source_domain_tiers
    WHERE domain_name LIKE '%' || domain || '%'
    ORDER BY LENGTH(domain) DESC
    LIMIT 1;
    
    RETURN COALESCE(tier_result, 3);  -- Default to Tier 3
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to check if pain category requires strict corroboration
CREATE OR REPLACE FUNCTION requires_strict_corroboration(pain_cat TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN pain_cat IN (
        'reliability_uptime',
        'security_compliance', 
        'customer_churn',
        'cost_pressure'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- PART 7: Useful Views
-- ============================================================================

-- View: V3 evidence with computed fields
CREATE OR REPLACE VIEW v3_evidence_computed AS
SELECT 
    e.*,
    compute_staleness_multiplier(e.days_ago) AS computed_staleness,
    CASE 
        WHEN e.source_tier = 1 THEN 'tier1_official'
        WHEN e.source_tier = 2 THEN 'tier2_media'
        ELSE 'tier3_other'
    END AS tier_label,
    CASE
        WHEN e.days_ago IS NULL THEN 'undated'
        WHEN e.days_ago <= 30 THEN 'fresh'
        WHEN e.days_ago <= 90 THEN 'recent'
        WHEN e.days_ago <= 180 THEN 'aging'
        ELSE 'stale'
    END AS recency_label
FROM v3_evidence e;

-- View: V3 hypothesis quality summary
CREATE OR REPLACE VIEW v3_hypothesis_quality AS
SELECT
    h.company_id,
    h.run_id,
    COUNT(*) AS total_hypotheses,
    COUNT(*) FILTER (WHERE h.corroborated) AS corroborated_count,
    COUNT(*) FILTER (WHERE h.offer_fit_strength = 'strong') AS strong_fit_count,
    COUNT(*) FILTER (WHERE h.do_not_outreach) AS do_not_outreach_count,
    AVG(h.raw_confidence) AS avg_raw_confidence,
    AVG(h.final_confidence) AS avg_final_confidence,
    AVG(h.staleness_penalty) AS avg_staleness_penalty,
    AVG(h.corroboration_penalty) AS avg_corroboration_penalty
FROM v3_hypotheses h
GROUP BY h.company_id, h.run_id;

-- View: Company prequal with V3 scores
CREATE OR REPLACE VIEW v3_prequal_summary AS
SELECT
    p.company_id,
    p.run_id,
    p.score,
    p.raw_score,
    p.staleness_adjusted_score,
    p.qualifies,
    p.evidence_count,
    p.distinct_sources,
    p.gates_passed,
    p.gates_failed,
    p.evidence_summary,
    p.recency_summary,
    ar.hypothesis_count,
    ar.strong_fit_count,
    ar.do_not_outreach AS run_do_not_outreach,
    ar.duration_ms
FROM company_prequal p
LEFT JOIN v3_analysis_runs ar ON p.company_id = ar.company_id AND p.run_id = ar.run_id;

-- ============================================================================
-- PART 8: Grants (adjust role names as needed)
-- ============================================================================

-- Grant permissions to application role (adjust 'app_user' to your role)
-- GRANT SELECT, INSERT, UPDATE ON v3_evidence TO app_user;
-- GRANT SELECT, INSERT, UPDATE ON v3_hypotheses TO app_user;
-- GRANT SELECT, INSERT, UPDATE ON v3_analysis_runs TO app_user;
-- GRANT SELECT ON source_domain_tiers TO app_user;
-- GRANT SELECT ON v3_evidence_computed TO app_user;
-- GRANT SELECT ON v3_hypothesis_quality TO app_user;
-- GRANT SELECT ON v3_prequal_summary TO app_user;

-- ============================================================================
-- DONE
-- ============================================================================
