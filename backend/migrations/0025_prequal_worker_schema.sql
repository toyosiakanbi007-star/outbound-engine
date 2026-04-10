-- 0025_prequal_worker_schema.sql
--
-- Prequal Worker: schema additions for batch prequal dispatch + execution.
--
-- Changes:
--   1. company_candidates: add prequal_attempts, prequal_last_error columns
--   2. client_configs: add prequal_config JSONB column
--   3. Indexes for efficient eligible-candidate selection

-- ============================================================
-- 1. company_candidates: prequal tracking columns
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'company_candidates'
                     AND column_name = 'prequal_attempts') THEN
        ALTER TABLE company_candidates
            ADD COLUMN prequal_attempts INT NOT NULL DEFAULT 0;
        RAISE NOTICE 'Added: company_candidates.prequal_attempts';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'company_candidates'
                     AND column_name = 'prequal_last_error') THEN
        ALTER TABLE company_candidates
            ADD COLUMN prequal_last_error TEXT;
        RAISE NOTICE 'Added: company_candidates.prequal_last_error';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'company_candidates'
                     AND column_name = 'prequal_started_at') THEN
        ALTER TABLE company_candidates
            ADD COLUMN prequal_started_at TIMESTAMPTZ;
        RAISE NOTICE 'Added: company_candidates.prequal_started_at';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'company_candidates'
                     AND column_name = 'prequal_completed_at') THEN
        ALTER TABLE company_candidates
            ADD COLUMN prequal_completed_at TIMESTAMPTZ;
        RAISE NOTICE 'Added: company_candidates.prequal_completed_at';
    END IF;
END $$;

-- ============================================================
-- 2. client_configs: prequal_config column
-- ============================================================
--
-- Stores per-client prequal worker settings:
-- {
--   "autopilot_enabled": true,
--   "batch_size": 5,
--   "max_batches_per_dispatch": 10,
--   "dispatch_interval_minutes": 15,
--   "max_per_industry_per_dispatch": null,
--   "max_attempts_per_company": 4,
--   "azure_function_timeout_secs": 120
-- }

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'client_configs'
                     AND column_name = 'prequal_config') THEN
        ALTER TABLE client_configs
            ADD COLUMN prequal_config JSONB DEFAULT '{}'::jsonb;
        RAISE NOTICE 'Added: client_configs.prequal_config';
    END IF;
END $$;

-- ============================================================
-- 3. Indexes for eligible-candidate selection
-- ============================================================

-- Fast lookup: "give me candidates that need prequal for this client"
-- Used by PREQUAL_DISPATCH with FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_cc_prequal_eligible
    ON company_candidates (client_id, created_at DESC)
    WHERE status IN ('new', 'prequal_queued')
      AND was_duplicate = FALSE;

-- Fast lookup: failed-retryable candidates
CREATE INDEX IF NOT EXISTS idx_cc_prequal_retryable
    ON company_candidates (client_id, prequal_attempts)
    WHERE status = 'new'
      AND prequal_attempts > 0
      AND was_duplicate = FALSE;

-- ============================================================
-- 4. Verify
-- ============================================================

DO $$
DECLARE col_count INT;
BEGIN
    SELECT COUNT(*) INTO col_count
    FROM information_schema.columns
    WHERE table_name = 'company_candidates'
      AND column_name IN ('prequal_attempts', 'prequal_last_error', 'prequal_started_at', 'prequal_completed_at');
    
    IF col_count = 4 THEN
        RAISE NOTICE 'OK: all 4 prequal tracking columns present on company_candidates';
    ELSE
        RAISE WARNING 'MISSING: expected 4 prequal columns, found %', col_count;
    END IF;
END $$;
