-- Aggregate Results Schema Migration
-- File: 0020_create_aggregate_results.sql
-- 
-- This migration adds tables for:
-- - aggregate_results: Final scoring output from aggregator_v3
-- - Adds aggregate_* columns to v3_analysis_runs for tracking
--
-- The aggregate_results table stores the "golden" decision package after
-- merging prequal hypotheses + Apollo enrichment + client ICP.

-- ============================================================================
-- PART 1: Create aggregate_results Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS aggregate_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    company_id UUID NOT NULL,
    client_id UUID NOT NULL,
    
    -- Final score and tier
    final_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    tier TEXT NOT NULL DEFAULT 'D' CHECK (tier IN ('A', 'B', 'C', 'D')),
    qualifies BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Flags
    do_not_outreach BOOLEAN DEFAULT FALSE,
    needs_review BOOLEAN DEFAULT FALSE,
    
    -- Score breakdown (JSONB)
    score_breakdown JSONB DEFAULT '{}',
    -- Contains: phase0_icp, prequal, evidence_quality, why_now_urgency, 
    --           firmographic_fit, tech_fit, penalties, rationales
    
    -- Full output (JSONB) - complete aggregator result
    output_json JSONB DEFAULT '{}',
    -- Contains: unified_hypotheses, tech_context, final_offer_fit,
    --           contact_suggestions, summaries, decision_notes
    
    -- Unified hypotheses (denormalized for easier querying)
    unified_hypotheses JSONB DEFAULT '[]',
    
    -- Tech fit result
    tech_fit JSONB DEFAULT '{}',
    
    -- Offer fit result  
    offer_fit JSONB DEFAULT '{}',
    
    -- Contact suggestions (personas, titles)
    recommended_personas TEXT[] DEFAULT '{}',
    title_keywords TEXT[] DEFAULT '{}',
    title_exact TEXT[] DEFAULT '{}',
    
    -- Decision notes
    decision_notes TEXT[] DEFAULT '{}',
    
    -- Caps applied (e.g., "tier3_only", "job_only")
    caps_applied TEXT[] DEFAULT '{}',
    
    -- Input timestamps (for tracing data lineage)
    input_prequal_completed_at TIMESTAMPTZ,
    input_apollo_fetched_at TIMESTAMPTZ,
    
    -- Processing metadata
    llm_calls INTEGER DEFAULT 0,
    duration_ms INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    CONSTRAINT fk_aggregate_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE,
    CONSTRAINT fk_aggregate_client FOREIGN KEY (client_id) 
        REFERENCES clients(id) ON DELETE CASCADE
);

-- Primary lookup: by run_id (unique)
CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregate_results_run_id 
ON aggregate_results(run_id);

-- Company lookup
CREATE INDEX IF NOT EXISTS idx_aggregate_results_company_id 
ON aggregate_results(company_id);

-- Client lookup
CREATE INDEX IF NOT EXISTS idx_aggregate_results_client_id 
ON aggregate_results(client_id);

-- Tier filter (for batch processing by tier)
CREATE INDEX IF NOT EXISTS idx_aggregate_results_tier 
ON aggregate_results(tier);

-- Qualifies filter
CREATE INDEX IF NOT EXISTS idx_aggregate_results_qualifies 
ON aggregate_results(qualifies) WHERE qualifies = TRUE;

-- Do not outreach filter
CREATE INDEX IF NOT EXISTS idx_aggregate_results_dno 
ON aggregate_results(do_not_outreach) WHERE do_not_outreach = TRUE;

-- Needs review filter
CREATE INDEX IF NOT EXISTS idx_aggregate_results_review 
ON aggregate_results(needs_review) WHERE needs_review = TRUE;

-- Final score for ranking
CREATE INDEX IF NOT EXISTS idx_aggregate_results_score 
ON aggregate_results(final_score DESC);

-- Composite index for typical queries
CREATE INDEX IF NOT EXISTS idx_aggregate_results_client_tier_score
ON aggregate_results(client_id, tier, final_score DESC);

-- ============================================================================
-- PART 2: Add aggregate columns to v3_analysis_runs
-- ============================================================================

ALTER TABLE v3_analysis_runs
ADD COLUMN IF NOT EXISTS aggregate_completed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS aggregate_status TEXT,  -- pending, success, failed, skipped
ADD COLUMN IF NOT EXISTS aggregate_score DOUBLE PRECISION;

-- Index for pending aggregations
CREATE INDEX IF NOT EXISTS idx_v3_analysis_runs_aggregate_pending
ON v3_analysis_runs(qualifies, aggregate_status)
WHERE qualifies = TRUE AND aggregate_status IS NULL;

-- ============================================================================
-- PART 3: NOTIFY trigger for aggregate_ready
-- ============================================================================

CREATE OR REPLACE FUNCTION notify_aggregate_ready()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('aggregate_ready', json_build_object(
        'run_id', NEW.run_id,
        'company_id', NEW.company_id,
        'client_id', NEW.client_id,
        'qualifies', NEW.qualifies,
        'tier', NEW.tier,
        'final_score', NEW.final_score,
        'do_not_outreach', NEW.do_not_outreach,
        'needs_review', NEW.needs_review,
        'timestamp', NOW()
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_aggregate_ready ON aggregate_results;
CREATE TRIGGER trg_aggregate_ready
AFTER INSERT OR UPDATE ON aggregate_results
FOR EACH ROW
EXECUTE FUNCTION notify_aggregate_ready();

-- ============================================================================
-- PART 4: Views for Analytics
-- ============================================================================

-- View: Aggregate results summary by tier
CREATE OR REPLACE VIEW aggregate_tier_summary AS
SELECT
    tier,
    COUNT(*) AS count,
    AVG(final_score) AS avg_score,
    COUNT(*) FILTER (WHERE qualifies) AS qualified_count,
    COUNT(*) FILTER (WHERE do_not_outreach) AS dno_count,
    COUNT(*) FILTER (WHERE needs_review) AS review_count,
    AVG(duration_ms) AS avg_duration_ms,
    AVG(llm_calls) AS avg_llm_calls
FROM aggregate_results
GROUP BY tier
ORDER BY tier;

-- View: Aggregate results summary by client
CREATE OR REPLACE VIEW aggregate_client_summary AS
SELECT
    client_id,
    COUNT(*) AS total_aggregated,
    COUNT(*) FILTER (WHERE tier = 'A') AS tier_a,
    COUNT(*) FILTER (WHERE tier = 'B') AS tier_b,
    COUNT(*) FILTER (WHERE tier = 'C') AS tier_c,
    COUNT(*) FILTER (WHERE tier = 'D') AS tier_d,
    AVG(final_score) AS avg_score,
    COUNT(*) FILTER (WHERE qualifies) AS qualified,
    COUNT(*) FILTER (WHERE do_not_outreach) AS do_not_outreach
FROM aggregate_results
GROUP BY client_id;

-- View: Pending aggregations (prequal qualified but not yet aggregated)
CREATE OR REPLACE VIEW pending_aggregations AS
SELECT
    ar.run_id,
    ar.company_id,
    ar.client_id,
    ar.final_score AS prequal_score,
    ar.qualifies,
    ar.created_at AS prequal_completed_at,
    c.name AS company_name,
    c.domain
FROM v3_analysis_runs ar
JOIN companies c ON c.id = ar.company_id
WHERE ar.qualifies = TRUE
  AND ar.aggregate_status IS NULL
ORDER BY ar.final_score DESC, ar.created_at ASC;

-- ============================================================================
-- PART 5: Helper Functions
-- ============================================================================

-- Function to get aggregate result for a run
CREATE OR REPLACE FUNCTION get_aggregate_result(p_run_id UUID)
RETURNS TABLE (
    run_id UUID,
    company_id UUID,
    final_score DOUBLE PRECISION,
    tier TEXT,
    qualifies BOOLEAN,
    unified_hypotheses JSONB,
    recommended_personas TEXT[],
    decision_notes TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ar.run_id,
        ar.company_id,
        ar.final_score,
        ar.tier,
        ar.qualifies,
        ar.unified_hypotheses,
        ar.recommended_personas,
        ar.decision_notes
    FROM aggregate_results ar
    WHERE ar.run_id = p_run_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to get qualified companies for a client (for contact pull)
CREATE OR REPLACE FUNCTION get_qualified_for_contacts(
    p_client_id UUID,
    p_min_tier TEXT DEFAULT 'C',
    p_limit INTEGER DEFAULT 100
)
RETURNS TABLE (
    company_id UUID,
    run_id UUID,
    final_score DOUBLE PRECISION,
    tier TEXT,
    recommended_personas TEXT[],
    title_keywords TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ar.company_id,
        ar.run_id,
        ar.final_score,
        ar.tier,
        ar.recommended_personas,
        ar.title_keywords
    FROM aggregate_results ar
    WHERE ar.client_id = p_client_id
      AND ar.qualifies = TRUE
      AND ar.do_not_outreach = FALSE
      AND ar.tier <= p_min_tier  -- A < B < C < D
    ORDER BY ar.tier, ar.final_score DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- DONE
-- ============================================================================

SELECT 'Aggregate Results Schema Migration Complete' AS status;
