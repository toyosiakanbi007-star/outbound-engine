-- Migration: Fix all ON CONFLICT unique constraints
-- 
-- PROBLEM: 
-- Several INSERT ... ON CONFLICT statements fail with:
-- "there is no unique or exclusion constraint matching the ON CONFLICT specification"
--
-- AFFECTED TABLES:
-- 1. company_prequal        - ON CONFLICT (company_id, run_id)
-- 2. v3_analysis_runs       - ON CONFLICT (company_id, run_id) 
-- 3. company_snapshots      - ON CONFLICT (domain, snapshot_version)
-- 4. offer_fit_decisions    - ON CONFLICT (client_id, domain, run_id)
-- 5. aggregate_results      - ON CONFLICT (run_id)
--
-- RUN THIS MIGRATION:
-- psql $DATABASE_URL -f 0021_fix_on_conflict_constraints.sql

-- ============================================================
-- 1. company_prequal - UNIQUE(company_id, run_id)
-- ============================================================

CREATE TABLE IF NOT EXISTS company_prequal (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL,
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    score REAL,
    qualifies BOOLEAN DEFAULT FALSE,
    evidence_count INT DEFAULT 0,
    distinct_sources INT DEFAULT 0,
    why_now_indicators TEXT[],
    gates_passed TEXT[],
    gates_failed TEXT[],
    raw_score REAL,
    staleness_adjusted_score REAL,
    evidence_summary JSONB DEFAULT '{}',
    recency_summary JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'company_prequal_company_id_run_id_key'
    ) THEN
        ALTER TABLE company_prequal 
        ADD CONSTRAINT company_prequal_company_id_run_id_key 
        UNIQUE (company_id, run_id);
        RAISE NOTICE 'Added: company_prequal_company_id_run_id_key';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_company_prequal_client_id ON company_prequal(client_id);
CREATE INDEX IF NOT EXISTS idx_company_prequal_qualifies ON company_prequal(qualifies) WHERE qualifies = TRUE;


-- ============================================================
-- 2. v3_analysis_runs - UNIQUE(company_id, run_id)
-- ============================================================

-- Table should already exist from 0015_create_v3_tables.sql
-- Just add the constraint if missing

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'v3_analysis_runs') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'v3_analysis_runs_company_id_run_id_key'
        ) THEN
            ALTER TABLE v3_analysis_runs 
            ADD CONSTRAINT v3_analysis_runs_company_id_run_id_key 
            UNIQUE (company_id, run_id);
            RAISE NOTICE 'Added: v3_analysis_runs_company_id_run_id_key';
        END IF;
    ELSE
        RAISE NOTICE 'Table v3_analysis_runs does not exist - skipping';
    END IF;
END $$;


-- ============================================================
-- 3. company_snapshots - UNIQUE(domain, snapshot_version)
-- ============================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'company_snapshots') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'company_snapshots_domain_snapshot_version_key'
        ) THEN
            ALTER TABLE company_snapshots 
            ADD CONSTRAINT company_snapshots_domain_snapshot_version_key 
            UNIQUE (domain, snapshot_version);
            RAISE NOTICE 'Added: company_snapshots_domain_snapshot_version_key';
        END IF;
    ELSE
        RAISE NOTICE 'Table company_snapshots does not exist - skipping';
    END IF;
END $$;


-- ============================================================
-- 4. offer_fit_decisions - UNIQUE(client_id, domain, run_id)
-- ============================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'offer_fit_decisions') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'offer_fit_decisions_client_domain_run_key'
        ) THEN
            ALTER TABLE offer_fit_decisions 
            ADD CONSTRAINT offer_fit_decisions_client_domain_run_key 
            UNIQUE (client_id, domain, run_id);
            RAISE NOTICE 'Added: offer_fit_decisions_client_domain_run_key';
        END IF;
    ELSE
        RAISE NOTICE 'Table offer_fit_decisions does not exist - skipping';
    END IF;
END $$;


-- ============================================================
-- 5. aggregate_results - UNIQUE(run_id)
-- ============================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'aggregate_results') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'aggregate_results_run_id_key'
        ) THEN
            ALTER TABLE aggregate_results 
            ADD CONSTRAINT aggregate_results_run_id_key 
            UNIQUE (run_id);
            RAISE NOTICE 'Added: aggregate_results_run_id_key';
        END IF;
    ELSE
        RAISE NOTICE 'Table aggregate_results does not exist - skipping';
    END IF;
END $$;


-- ============================================================
-- VERIFICATION: List all unique constraints added
-- ============================================================

SELECT 
    t.relname AS table_name,
    c.conname AS constraint_name,
    pg_get_constraintdef(c.oid) AS constraint_definition
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
WHERE c.contype = 'u'
AND t.relname IN (
    'company_prequal', 
    'v3_analysis_runs', 
    'company_snapshots', 
    'offer_fit_decisions', 
    'aggregate_results'
)
ORDER BY t.relname, c.conname;
