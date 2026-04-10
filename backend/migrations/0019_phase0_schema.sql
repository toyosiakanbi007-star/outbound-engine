-- Phase 0: Offer-Fit Gate Schema Migration
-- File: 0019_phase0_schema.sql
-- 
-- This migration adds tables for:
-- - Company snapshots (cached company profiles)
-- - Offer-fit decisions (qualify/disqualify/uncertain)
-- - Snapshot cache metadata (TTL, versioning)
--
-- Integrates with existing V3 hardening tables

-- ============================================================================
-- PART 1: Company Snapshots Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS company_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID,  -- NULL if snapshot is domain-only (pre-company creation)
    domain TEXT NOT NULL,
    run_id UUID,
    
    -- Core company info
    company_name TEXT NOT NULL,
    hq_location TEXT,
    
    -- Business classification
    industry_guess TEXT[] DEFAULT '{}',
    business_model_guess TEXT,  -- SaaS|fintech|marketplace|manufacturing|services|hardware|other
    what_they_sell TEXT,
    who_they_sell_to TEXT,  -- B2B|B2C|mixed|unknown
    gtm_motion TEXT[] DEFAULT '{}',  -- self_serve, sales_led, enterprise, channel
    
    -- Keywords and signals
    keywords TEXT[] DEFAULT '{}',
    signals_of_client_product_need TEXT[] DEFAULT '{}',  -- Extracted signals relevant to offers
    
    -- Evidence with verbatim quotes
    evidence JSONB DEFAULT '[]',  -- [{url, verbatim_quote}]
    
    -- URLs fetched for this snapshot
    source_urls TEXT[] DEFAULT '{}',
    urls_fetched INTEGER DEFAULT 0,
    urls_blocked INTEGER DEFAULT 0,
    
    -- Extraction metadata
    extraction_model TEXT,  -- Which LLM model was used
    extraction_prompt_version TEXT DEFAULT 'v1',
    extraction_confidence DOUBLE PRECISION DEFAULT 0.5,
    
    -- Cache metadata
    snapshot_version TEXT DEFAULT 'v1',  -- For cache invalidation on schema/prompt changes
    cached_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '7 days'),
    cache_hit_count INTEGER DEFAULT 0,
    last_cache_hit_at TIMESTAMPTZ,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign key (optional - snapshot can exist before company record)
    CONSTRAINT fk_snapshot_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE SET NULL
);

-- Indexes for company_snapshots
CREATE INDEX IF NOT EXISTS idx_snapshot_domain ON company_snapshots(domain);
CREATE INDEX IF NOT EXISTS idx_snapshot_company_id ON company_snapshots(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_snapshot_expires ON company_snapshots(expires_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_version ON company_snapshots(snapshot_version);

-- Unique constraint for cache key (domain + version)
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_cache_key 
ON company_snapshots(domain, snapshot_version);

-- ============================================================================
-- PART 2: Offer-Fit Decisions Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS offer_fit_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID,  -- NULL if decision is pre-company creation
    client_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    run_id UUID NOT NULL,
    snapshot_id UUID,  -- Reference to the snapshot used
    
    -- Core decision
    icp_fit TEXT NOT NULL CHECK (icp_fit IN ('qualify', 'disqualify', 'uncertain')),
    fit_strength TEXT CHECK (fit_strength IN ('strong', 'medium', 'weak')),
    
    -- Reasoning
    reasons TEXT[] DEFAULT '{}',
    disqualify_reasons TEXT[] DEFAULT '{}',
    missing_info TEXT[] DEFAULT '{}',
    
    -- Confidence and evidence
    confidence DOUBLE PRECISION DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 1),
    evidence JSONB DEFAULT '[]',  -- [{url, verbatim_quote}]
    
    -- Outreach guidance (if qualifies)
    recommended_angles TEXT[] DEFAULT '{}',
    
    -- Flags
    needs_review BOOLEAN DEFAULT FALSE,  -- True if uncertain after reduced run
    do_not_outreach BOOLEAN DEFAULT FALSE,  -- True if disqualified
    
    -- Pipeline control
    pipeline_continued BOOLEAN DEFAULT FALSE,  -- Did we continue to full V3?
    reduced_limits_applied BOOLEAN DEFAULT FALSE,  -- Was this an uncertain+reduced run?
    
    -- Evaluation metadata
    evaluation_model TEXT,
    evaluation_prompt_version TEXT DEFAULT 'v1',
    evaluation_duration_ms INTEGER,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    CONSTRAINT fk_offer_fit_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE SET NULL,
    CONSTRAINT fk_offer_fit_snapshot FOREIGN KEY (snapshot_id)
        REFERENCES company_snapshots(id) ON DELETE SET NULL
);

-- Indexes for offer_fit_decisions
CREATE INDEX IF NOT EXISTS idx_offer_fit_domain ON offer_fit_decisions(domain);
CREATE INDEX IF NOT EXISTS idx_offer_fit_company ON offer_fit_decisions(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_offer_fit_client ON offer_fit_decisions(client_id);
CREATE INDEX IF NOT EXISTS idx_offer_fit_icp ON offer_fit_decisions(icp_fit);
CREATE INDEX IF NOT EXISTS idx_offer_fit_run ON offer_fit_decisions(run_id);
CREATE INDEX IF NOT EXISTS idx_offer_fit_needs_review ON offer_fit_decisions(needs_review) WHERE needs_review = TRUE;
CREATE INDEX IF NOT EXISTS idx_offer_fit_do_not_outreach ON offer_fit_decisions(do_not_outreach) WHERE do_not_outreach = TRUE;

-- Unique constraint for client + domain + run (one decision per run)
CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_fit_unique_run
ON offer_fit_decisions(client_id, domain, run_id);

-- ============================================================================
-- PART 3: Snapshot Cache Metadata Table (for cache management)
-- ============================================================================

CREATE TABLE IF NOT EXISTS snapshot_cache_meta (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Current active version
    current_version TEXT NOT NULL DEFAULT 'v1',
    
    -- Version history
    version_history JSONB DEFAULT '[]',  -- [{version, activated_at, reason}]
    
    -- Cache settings
    default_ttl_days INTEGER DEFAULT 7,
    max_cache_size INTEGER DEFAULT 10000,  -- Max cached snapshots
    
    -- Stats
    total_snapshots INTEGER DEFAULT 0,
    total_cache_hits INTEGER DEFAULT 0,
    total_cache_misses INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default cache meta if not exists
INSERT INTO snapshot_cache_meta (current_version, default_ttl_days)
VALUES ('v1', 7)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- PART 4: Helper Functions
-- ============================================================================

-- Function to get valid cached snapshot
CREATE OR REPLACE FUNCTION get_cached_snapshot(
    p_domain TEXT,
    p_version TEXT DEFAULT NULL
)
RETURNS TABLE (
    id UUID,
    company_name TEXT,
    industry_guess TEXT[],
    business_model_guess TEXT,
    what_they_sell TEXT,
    who_they_sell_to TEXT,
    evidence JSONB,
    cached_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ
) AS $$
DECLARE
    v_version TEXT;
BEGIN
    -- Get current version if not specified
    IF p_version IS NULL THEN
        SELECT current_version INTO v_version FROM snapshot_cache_meta LIMIT 1;
    ELSE
        v_version := p_version;
    END IF;
    
    RETURN QUERY
    SELECT 
        s.id,
        s.company_name,
        s.industry_guess,
        s.business_model_guess,
        s.what_they_sell,
        s.who_they_sell_to,
        s.evidence,
        s.cached_at,
        s.expires_at
    FROM company_snapshots s
    WHERE s.domain = p_domain
      AND s.snapshot_version = v_version
      AND s.expires_at > NOW()
    ORDER BY s.cached_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to increment cache hit
CREATE OR REPLACE FUNCTION record_cache_hit(p_snapshot_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE company_snapshots
    SET cache_hit_count = cache_hit_count + 1,
        last_cache_hit_at = NOW()
    WHERE id = p_snapshot_id;
    
    UPDATE snapshot_cache_meta
    SET total_cache_hits = total_cache_hits + 1,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to invalidate snapshots by version
CREATE OR REPLACE FUNCTION invalidate_snapshot_cache(p_old_version TEXT, p_reason TEXT DEFAULT 'manual')
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Mark old snapshots as expired
    UPDATE company_snapshots
    SET expires_at = NOW()
    WHERE snapshot_version = p_old_version
      AND expires_at > NOW();
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Log the invalidation
    UPDATE snapshot_cache_meta
    SET version_history = version_history || jsonb_build_object(
            'version', p_old_version,
            'invalidated_at', NOW(),
            'reason', p_reason,
            'count', v_count
        ),
        updated_at = NOW();
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up expired snapshots
CREATE OR REPLACE FUNCTION cleanup_expired_snapshots(p_older_than_days INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    DELETE FROM company_snapshots
    WHERE expires_at < NOW() - (p_older_than_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Update stats
    UPDATE snapshot_cache_meta
    SET total_snapshots = (SELECT COUNT(*) FROM company_snapshots),
        updated_at = NOW();
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PART 5: Views for Analytics
-- ============================================================================

-- View: Offer-fit summary by client
CREATE OR REPLACE VIEW offer_fit_summary_by_client AS
SELECT
    client_id,
    COUNT(*) AS total_decisions,
    COUNT(*) FILTER (WHERE icp_fit = 'qualify') AS qualified,
    COUNT(*) FILTER (WHERE icp_fit = 'disqualify') AS disqualified,
    COUNT(*) FILTER (WHERE icp_fit = 'uncertain') AS uncertain,
    COUNT(*) FILTER (WHERE needs_review) AS needs_review,
    AVG(confidence) AS avg_confidence,
    COUNT(*) FILTER (WHERE pipeline_continued) AS pipeline_continued
FROM offer_fit_decisions
GROUP BY client_id;

-- View: Snapshot cache stats
CREATE OR REPLACE VIEW snapshot_cache_stats AS
SELECT
    COUNT(*) AS total_snapshots,
    COUNT(*) FILTER (WHERE expires_at > NOW()) AS active_snapshots,
    COUNT(*) FILTER (WHERE expires_at <= NOW()) AS expired_snapshots,
    SUM(cache_hit_count) AS total_cache_hits,
    AVG(cache_hit_count) AS avg_hits_per_snapshot,
    COUNT(DISTINCT domain) AS unique_domains,
    MAX(cached_at) AS most_recent_cache,
    MIN(cached_at) AS oldest_cache
FROM company_snapshots;

-- View: Disqualification reasons analysis
CREATE OR REPLACE VIEW disqualification_reasons_analysis AS
SELECT
    unnest(disqualify_reasons) AS reason,
    COUNT(*) AS occurrences,
    AVG(confidence) AS avg_confidence
FROM offer_fit_decisions
WHERE icp_fit = 'disqualify'
GROUP BY unnest(disqualify_reasons)
ORDER BY occurrences DESC;

-- ============================================================================
-- PART 6: Triggers
-- ============================================================================

-- Trigger to update snapshot count in cache meta
CREATE OR REPLACE FUNCTION update_snapshot_count()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE snapshot_cache_meta
    SET total_snapshots = (SELECT COUNT(*) FROM company_snapshots),
        updated_at = NOW();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_snapshot_count ON company_snapshots;
CREATE TRIGGER trg_update_snapshot_count
AFTER INSERT OR DELETE ON company_snapshots
FOR EACH STATEMENT
EXECUTE FUNCTION update_snapshot_count();

-- ============================================================================
-- DONE
-- ============================================================================

SELECT 'Phase 0 Schema Migration Complete' AS status;
