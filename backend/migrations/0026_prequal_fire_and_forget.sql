-- 0026_prequal_fire_and_forget.sql
--
-- Prequal Worker: move from synchronous HTTP response to DB-driven status updates.
--
-- PROBLEM:
--   The Rust prequal_worker sends HTTP requests to the Azure Function and blocks
--   waiting for the response to update company_candidates.status. But Azure Functions
--   often timeout (230s limit), and the Azure Function already writes results directly
--   to v3_analysis_runs + sends NOTIFY prequal_ready. The HTTP wait is wasted.
--
-- SOLUTION:
--   1. Trigger on v3_analysis_runs INSERT/UPDATE that auto-updates company_candidates.status
--   2. Rust worker fires HTTP requests without waiting for the response
--   3. prequal_listener also updates candidates from NOTIFY (belt-and-suspenders)
--   4. Stale-candidate recovery for edge cases (Azure Function crashes before writing)
--
-- CHANGES:
--   1. Trigger function: sync_candidate_status_from_analysis_run()
--   2. Trigger: trg_sync_candidate_status ON v3_analysis_runs
--   3. Index for efficient candidate lookup by (company_id, client_id, status)

-- ============================================================
-- 1. Trigger function: update company_candidates from v3_analysis_runs
-- ============================================================
--
-- When the Azure Function writes/updates a prequal analysis run, this trigger
-- automatically transitions the matching company_candidate from
-- 'prequal_queued' → 'qualified' or 'disqualified'.
--
-- Matching: company_id + client_id (cast from TEXT→UUID) + status still queued.
-- If no matching candidate is found (e.g., manual run), the trigger is a no-op.

CREATE OR REPLACE FUNCTION sync_candidate_status_from_analysis_run()
RETURNS TRIGGER AS $$
DECLARE
    new_status TEXT;
    client_uuid UUID;
BEGIN
    -- Only act on prequal-phase runs
    IF COALESCE(NEW.phase, 'prequal') <> 'prequal' THEN
        RETURN NEW;
    END IF;

    -- Only act when we have a definitive result (qualifies is not null)
    IF NEW.qualifies IS NULL THEN
        RETURN NEW;
    END IF;

    -- Determine new status
    IF NEW.qualifies = TRUE THEN
        new_status := 'qualified';
    ELSE
        new_status := 'disqualified';
    END IF;

    -- Safely cast client_id (TEXT in v3_analysis_runs → UUID in company_candidates)
    BEGIN
        client_uuid := NEW.client_id::UUID;
    EXCEPTION WHEN OTHERS THEN
        -- If client_id can't be cast to UUID, skip silently
        RETURN NEW;
    END;

    -- Update the most recent matching candidate that's still in-flight.
    -- Uses a subquery with LIMIT 1 to avoid updating multiple candidates
    -- for the same company across different runs.
    UPDATE company_candidates
    SET status              = new_status,
        prequal_completed_at = NOW(),
        prequal_last_error  = NULL
    WHERE id = (
        SELECT id
        FROM company_candidates
        WHERE company_id = NEW.company_id
          AND client_id  = client_uuid
          AND status IN ('prequal_queued', 'new')
        ORDER BY created_at DESC
        LIMIT 1
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 2. Create trigger on v3_analysis_runs
-- ============================================================

DROP TRIGGER IF EXISTS trg_sync_candidate_status ON v3_analysis_runs;

CREATE TRIGGER trg_sync_candidate_status
    AFTER INSERT OR UPDATE OF qualifies, final_score
    ON v3_analysis_runs
    FOR EACH ROW
    EXECUTE FUNCTION sync_candidate_status_from_analysis_run();

-- ============================================================
-- 3. Index for efficient candidate lookup in the trigger
-- ============================================================
--
-- The trigger does: WHERE company_id = X AND client_id = Y AND status IN (...)
-- This partial index covers exactly that query pattern.

CREATE INDEX IF NOT EXISTS idx_cc_company_client_inflight
    ON company_candidates (company_id, client_id, created_at DESC)
    WHERE status IN ('prequal_queued', 'new');

-- ============================================================
-- 4. Stale candidate recovery function
-- ============================================================
--
-- For edge cases where the Azure Function crashes before writing to
-- v3_analysis_runs. Candidates stuck in 'prequal_queued' for > threshold
-- get reset to 'new' so the dispatch can retry them.
--
-- Run periodically via cron or as part of PREQUAL_DISPATCH.

CREATE OR REPLACE FUNCTION recover_stale_prequal_candidates(
    stale_threshold_minutes INT DEFAULT 30
)
RETURNS INT AS $$
DECLARE
    recovered INT;
BEGIN
    WITH stale AS (
        SELECT id
        FROM company_candidates
        WHERE status = 'prequal_queued'
          AND prequal_started_at IS NOT NULL
          AND prequal_started_at < NOW() - (stale_threshold_minutes || ' minutes')::INTERVAL
          AND prequal_attempts < 4  -- don't retry exhausted candidates
        FOR UPDATE SKIP LOCKED
    )
    UPDATE company_candidates cc
    SET status             = 'new',
        prequal_last_error = 'stale: reset after ' || stale_threshold_minutes || 'min timeout',
        prequal_started_at = NULL
    FROM stale
    WHERE cc.id = stale.id;

    GET DIAGNOSTICS recovered = ROW_COUNT;
    RETURN recovered;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 5. Verify
-- ============================================================

DO $$
BEGIN
    -- Check trigger exists
    IF EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_sync_candidate_status'
    ) THEN
        RAISE NOTICE 'OK: trg_sync_candidate_status trigger created';
    ELSE
        RAISE WARNING 'MISSING: trg_sync_candidate_status trigger';
    END IF;

    -- Check function exists
    IF EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'sync_candidate_status_from_analysis_run'
    ) THEN
        RAISE NOTICE 'OK: sync_candidate_status_from_analysis_run function created';
    ELSE
        RAISE WARNING 'MISSING: sync_candidate_status_from_analysis_run function';
    END IF;

    -- Check recovery function exists
    IF EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'recover_stale_prequal_candidates'
    ) THEN
        RAISE NOTICE 'OK: recover_stale_prequal_candidates function created';
    ELSE
        RAISE WARNING 'MISSING: recover_stale_prequal_candidates function';
    END IF;
END $$;
