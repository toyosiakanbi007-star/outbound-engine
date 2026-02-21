-- 0023_create_company_fetch_tables.sql
--
-- PURPOSE:
-- Create tables for the Company Fetcher subsystem:
--   1. company_fetch_runs     — one row per fetcher execution
--   2. company_fetch_queries  — one row per Apollo API query attempt
--   3. company_candidates     — links companies to the run that discovered them
--   4. industry_yield_metrics — rolling stats for adaptive quota allocation
--
-- These tables enable:
--   - Full run telemetry (how many fetched, upserted, duped per run)
--   - Per-query Apollo request/response logging
--   - Attribution ("why is this company in the pipeline?")
--   - Adaptive industry quota planning based on downstream yield
--
-- DEPENDS ON: clients, companies (from earlier migrations)


-- ============================================================================
-- TABLE 1: company_fetch_runs
-- ============================================================================
-- Tracks a single Company Fetcher execution for one client.
-- Created when a discover_companies job starts; updated as pages are fetched.

CREATE TABLE IF NOT EXISTS company_fetch_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    
    -- Run configuration (snapshot of what was requested)
    batch_target    INT NOT NULL DEFAULT 2000,
    
    -- Quota plan: { "healthcare_saas": 400, "fintech": 350, ... }
    quota_plan      JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- Status lifecycle: pending → running → succeeded / failed / depleted / partial
    --   depleted = all industry variants exhausted before hitting batch_target
    --   partial  = stopped early (max runtime, too many dupes, etc.)
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN (
                        'pending', 'running', 'succeeded',
                        'failed', 'depleted', 'partial'
                    )),
    
    -- Counters (updated progressively as pages are fetched)
    raw_fetched     INT NOT NULL DEFAULT 0,     -- total orgs returned by Apollo (incl. dupes)
    unique_upserted INT NOT NULL DEFAULT 0,     -- new companies inserted or updated
    duplicates      INT NOT NULL DEFAULT 0,     -- skipped because already existed
    pages_fetched   INT NOT NULL DEFAULT 0,     -- total Apollo pages consumed
    api_credits_used INT NOT NULL DEFAULT 0,    -- Apollo credits consumed (if tracked)
    
    -- Per-industry summary (written at end of run)
    -- { "healthcare_saas": { "quota": 400, "fetched": 380, "upserted": 320, 
    --                         "depleted": false, "variants_used": ["v1","v2"] },
    --   ... }
    industry_summary JSONB DEFAULT '{}'::jsonb,
    
    -- Error info (if failed)
    error           JSONB DEFAULT NULL,
    -- { "message": "...", "apollo_status": 429, "at_page": 12, "industry": "..." }
    
    -- Timing
    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Find runs by client
CREATE INDEX IF NOT EXISTS idx_cfr_client_id
    ON company_fetch_runs(client_id);

-- Find active/recent runs
CREATE INDEX IF NOT EXISTS idx_cfr_status
    ON company_fetch_runs(status);

-- Latest runs first
CREATE INDEX IF NOT EXISTS idx_cfr_created_at
    ON company_fetch_runs(created_at DESC);

-- Composite: latest run per client
CREATE INDEX IF NOT EXISTS idx_cfr_client_created
    ON company_fetch_runs(client_id, created_at DESC);


-- ============================================================================
-- TABLE 2: company_fetch_queries
-- ============================================================================
-- Logs each Apollo Organization Search API call.
-- One run may produce many queries (industry × variant × page ranges).

CREATE TABLE IF NOT EXISTS company_fetch_queries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES company_fetch_runs(id) ON DELETE CASCADE,
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    
    -- What was queried
    industry        TEXT NOT NULL,
    variant         TEXT NOT NULL DEFAULT 'v1_strict',
                    -- v1_strict, v2_broaden_geo, v3_broaden_size, v4_keyword_assist
    
    -- Apollo request params (for debugging/replay)
    -- { "organization_industry_tag_ids": [...], "organization_locations": [...],
    --   "organization_num_employees_ranges": [...], "page": 1, "per_page": 100 }
    apollo_request  JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- Apollo response metadata (NOT the full org list — just stats)
    -- { "total_results": 4200, "total_pages": 42, "current_page": 1 }
    apollo_response_meta JSONB DEFAULT '{}'::jsonb,
    
    -- Pagination
    page_start      INT NOT NULL DEFAULT 1,     -- first page fetched in this query
    page_end        INT,                        -- last page fetched
    pages_fetched   INT NOT NULL DEFAULT 0,
    orgs_returned   INT NOT NULL DEFAULT 0,     -- total orgs across pages
    
    -- Status: running → succeeded / failed / exhausted
    --   exhausted = fewer results than expected (end of Apollo result set)
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN (
                        'running', 'succeeded', 'failed', 'exhausted'
                    )),
    error           TEXT,
    
    -- Timing
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Find queries by run
CREATE INDEX IF NOT EXISTS idx_cfq_run_id
    ON company_fetch_queries(run_id);

-- Find queries by client
CREATE INDEX IF NOT EXISTS idx_cfq_client_id
    ON company_fetch_queries(client_id);

-- Analytics: queries per industry
CREATE INDEX IF NOT EXISTS idx_cfq_industry
    ON company_fetch_queries(industry);

-- Analytics: queries per variant
CREATE INDEX IF NOT EXISTS idx_cfq_variant
    ON company_fetch_queries(variant);


-- ============================================================================
-- TABLE 3: company_candidates
-- ============================================================================
-- Links a company to the fetch run that discovered it.
-- Preserves attribution (which industry, variant, run) and tracks pipeline status.
-- A company can appear in multiple runs (different runs discover the same company),
-- but each (run_id, company_id) pair is unique.

CREATE TABLE IF NOT EXISTS company_candidates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    run_id          UUID NOT NULL REFERENCES company_fetch_runs(id) ON DELETE CASCADE,
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Attribution: how was this company discovered?
    industry        TEXT,
    variant         TEXT,
    source          TEXT NOT NULL DEFAULT 'apollo',
    
    -- Pipeline status for this candidate
    -- new → prequal_queued → prequal_done → qualified / disqualified
    status          TEXT NOT NULL DEFAULT 'new'
                    CHECK (status IN (
                        'new', 'prequal_queued', 'prequal_done',
                        'qualified', 'disqualified', 'skipped'
                    )),
    
    -- Was this an insert or did the company already exist?
    was_duplicate   BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Reference to the downstream prequal job (if queued)
    prequal_job_id  UUID REFERENCES jobs(id) ON DELETE SET NULL,
    
    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One candidate per company per run
CREATE UNIQUE INDEX IF NOT EXISTS idx_cc_run_company
    ON company_candidates(run_id, company_id);

-- Find candidates by company (cross-run)
CREATE INDEX IF NOT EXISTS idx_cc_company_id
    ON company_candidates(company_id);

-- Find candidates by client
CREATE INDEX IF NOT EXISTS idx_cc_client_id
    ON company_candidates(client_id);

-- Find candidates by run + status (for batch processing)
CREATE INDEX IF NOT EXISTS idx_cc_run_status
    ON company_candidates(run_id, status);

-- Find new candidates that need prequal
CREATE INDEX IF NOT EXISTS idx_cc_new
    ON company_candidates(status)
    WHERE status = 'new';

-- Attribution analytics: which industries/variants produce results
CREATE INDEX IF NOT EXISTS idx_cc_industry_variant
    ON company_candidates(industry, variant);


-- ============================================================================
-- TABLE 4: industry_yield_metrics
-- ============================================================================
-- Rolling aggregated stats per (client, industry) for adaptive quota planning.
-- Updated at the end of each company_fetch_run and when prequal results come in.
-- One row per (client_id, industry).

CREATE TABLE IF NOT EXISTS industry_yield_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    industry        TEXT NOT NULL,
    
    -- Fetch stats (rolling totals across all runs)
    total_fetched       INT NOT NULL DEFAULT 0,
    total_upserted      INT NOT NULL DEFAULT 0,
    total_duplicates    INT NOT NULL DEFAULT 0,
    
    -- Downstream yield (updated as prequal/aggregate results come in)
    prequal_passed      INT NOT NULL DEFAULT 0,     -- companies that passed prequal
    prequal_failed      INT NOT NULL DEFAULT 0,     -- companies that failed prequal
    tier_a_count        INT NOT NULL DEFAULT 0,
    tier_b_count        INT NOT NULL DEFAULT 0,
    tier_c_count        INT NOT NULL DEFAULT 0,
    tier_d_count        INT NOT NULL DEFAULT 0,
    
    -- Computed yield rates (updated when downstream stats change)
    prequal_pass_rate   DOUBLE PRECISION DEFAULT 0, -- prequal_passed / total_upserted
    tier_ab_rate        DOUBLE PRECISION DEFAULT 0, -- (tier_a + tier_b) / prequal_passed
    avg_aggregate_score DOUBLE PRECISION DEFAULT 0,
    
    -- Depletion tracking
    last_depleted_at    TIMESTAMPTZ,                -- when Apollo ran out of results
    depletion_count     INT NOT NULL DEFAULT 0,     -- how many times this industry depleted
    
    -- How many runs have contributed data
    run_count           INT NOT NULL DEFAULT 0,
    
    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per (client, industry)
CREATE UNIQUE INDEX IF NOT EXISTS idx_iym_client_industry
    ON industry_yield_metrics(client_id, industry);

-- Find metrics by client (for quota planning)
CREATE INDEX IF NOT EXISTS idx_iym_client_id
    ON industry_yield_metrics(client_id);

-- Find high-yield industries across all clients (for global insights)
CREATE INDEX IF NOT EXISTS idx_iym_tier_ab_rate
    ON industry_yield_metrics(tier_ab_rate DESC)
    WHERE tier_ab_rate > 0;


-- ============================================================================
-- VIEWS
-- ============================================================================

-- View: Latest fetch run per client
CREATE OR REPLACE VIEW v_latest_company_fetch_run AS
SELECT DISTINCT ON (client_id)
    cfr.*,
    cl.name AS client_name
FROM company_fetch_runs cfr
JOIN clients cl ON cl.id = cfr.client_id
ORDER BY client_id, cfr.created_at DESC;


-- View: Candidate pipeline funnel per run
CREATE OR REPLACE VIEW v_candidate_funnel AS
SELECT
    cc.run_id,
    cc.client_id,
    cc.industry,
    COUNT(*) AS total_candidates,
    COUNT(*) FILTER (WHERE cc.was_duplicate) AS duplicates,
    COUNT(*) FILTER (WHERE NOT cc.was_duplicate) AS new_companies,
    COUNT(*) FILTER (WHERE cc.status = 'prequal_queued') AS prequal_queued,
    COUNT(*) FILTER (WHERE cc.status = 'prequal_done') AS prequal_done,
    COUNT(*) FILTER (WHERE cc.status = 'qualified') AS qualified,
    COUNT(*) FILTER (WHERE cc.status = 'disqualified') AS disqualified
FROM company_candidates cc
GROUP BY cc.run_id, cc.client_id, cc.industry;


-- View: Industry yield leaderboard per client
CREATE OR REPLACE VIEW v_industry_yield_leaderboard AS
SELECT
    iym.client_id,
    cl.name AS client_name,
    iym.industry,
    iym.total_upserted,
    iym.prequal_passed,
    iym.prequal_pass_rate,
    iym.tier_ab_rate,
    iym.avg_aggregate_score,
    iym.depletion_count,
    iym.run_count
FROM industry_yield_metrics iym
JOIN clients cl ON cl.id = iym.client_id
ORDER BY iym.client_id, iym.tier_ab_rate DESC;


-- View: Apollo API usage per run (cost tracking)
CREATE OR REPLACE VIEW v_apollo_usage_per_run AS
SELECT
    cfq.run_id,
    cfq.client_id,
    COUNT(*) AS total_queries,
    SUM(cfq.pages_fetched) AS total_pages,
    SUM(cfq.orgs_returned) AS total_orgs_returned,
    COUNT(DISTINCT cfq.industry) AS industries_queried,
    COUNT(DISTINCT cfq.variant) AS variants_used,
    COUNT(*) FILTER (WHERE cfq.status = 'failed') AS failed_queries,
    COUNT(*) FILTER (WHERE cfq.status = 'exhausted') AS exhausted_queries,
    MIN(cfq.started_at) AS first_query_at,
    MAX(cfq.ended_at) AS last_query_at
FROM company_fetch_queries cfq
GROUP BY cfq.run_id, cfq.client_id;


-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function: Upsert industry yield metrics after a run completes
CREATE OR REPLACE FUNCTION upsert_industry_yield(
    p_client_id UUID,
    p_industry TEXT,
    p_fetched INT DEFAULT 0,
    p_upserted INT DEFAULT 0,
    p_duplicates INT DEFAULT 0,
    p_depleted BOOLEAN DEFAULT FALSE
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO industry_yield_metrics (
        client_id, industry,
        total_fetched, total_upserted, total_duplicates,
        depletion_count, run_count,
        last_depleted_at
    ) VALUES (
        p_client_id, p_industry,
        p_fetched, p_upserted, p_duplicates,
        CASE WHEN p_depleted THEN 1 ELSE 0 END,
        1,
        CASE WHEN p_depleted THEN NOW() ELSE NULL END
    )
    ON CONFLICT (client_id, industry)
    DO UPDATE SET
        total_fetched    = industry_yield_metrics.total_fetched + EXCLUDED.total_fetched,
        total_upserted   = industry_yield_metrics.total_upserted + EXCLUDED.total_upserted,
        total_duplicates = industry_yield_metrics.total_duplicates + EXCLUDED.total_duplicates,
        depletion_count  = industry_yield_metrics.depletion_count + EXCLUDED.depletion_count,
        run_count        = industry_yield_metrics.run_count + 1,
        last_depleted_at = CASE WHEN p_depleted THEN NOW()
                           ELSE industry_yield_metrics.last_depleted_at END,
        updated_at       = NOW();
END;
$$ LANGUAGE plpgsql;


-- Function: Update downstream yield metrics (called after prequal/aggregate completes)
CREATE OR REPLACE FUNCTION update_industry_downstream_yield(
    p_client_id UUID,
    p_industry TEXT,
    p_prequal_passed INT DEFAULT 0,
    p_prequal_failed INT DEFAULT 0,
    p_tier_a INT DEFAULT 0,
    p_tier_b INT DEFAULT 0,
    p_tier_c INT DEFAULT 0,
    p_tier_d INT DEFAULT 0,
    p_avg_score DOUBLE PRECISION DEFAULT 0
)
RETURNS VOID AS $$
BEGIN
    UPDATE industry_yield_metrics
    SET
        prequal_passed   = prequal_passed + p_prequal_passed,
        prequal_failed   = prequal_failed + p_prequal_failed,
        tier_a_count     = tier_a_count + p_tier_a,
        tier_b_count     = tier_b_count + p_tier_b,
        tier_c_count     = tier_c_count + p_tier_c,
        tier_d_count     = tier_d_count + p_tier_d,
        -- Recompute rates
        prequal_pass_rate = CASE
            WHEN total_upserted > 0
            THEN (prequal_passed + p_prequal_passed)::DOUBLE PRECISION / total_upserted
            ELSE 0 END,
        tier_ab_rate = CASE
            WHEN (prequal_passed + p_prequal_passed) > 0
            THEN (tier_a_count + p_tier_a + tier_b_count + p_tier_b)::DOUBLE PRECISION
                 / (prequal_passed + p_prequal_passed)
            ELSE 0 END,
        avg_aggregate_score = CASE
            WHEN p_avg_score > 0 AND (prequal_passed + p_prequal_passed) > 0
            THEN (avg_aggregate_score * prequal_passed + p_avg_score * p_prequal_passed)
                 / (prequal_passed + p_prequal_passed)
            ELSE avg_aggregate_score END,
        updated_at = NOW()
    WHERE client_id = p_client_id
      AND industry = p_industry;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- DONE
-- ============================================================================

SELECT '0023_create_company_fetch_tables: DONE' AS status;
