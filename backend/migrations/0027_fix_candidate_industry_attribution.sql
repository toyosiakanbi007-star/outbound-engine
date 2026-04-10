-- 0027_fix_candidate_industry_attribution.sql
--
-- Fixes the company_candidates unique constraint to allow the same company
-- to appear in multiple industries within a single run.
--
-- PROBLEM:
--   The old constraint (run_id, company_id) prevented a company from having
--   candidate rows under different industries.  The in-memory deduper in the
--   company_fetcher also swallowed cross-industry hits, but even if those
--   were to be inserted, the old constraint would collapse them into one row
--   keeping only the FIRST industry's attribution.
--
-- FIX:
--   1. Drop the old (run_id, company_id) unique index
--   2. Add a new (run_id, company_id, industry) unique index
--      → same company CAN have a candidate row per industry per run
--      → same company CANNOT have duplicate rows within the same industry
--   3. Add an index on (client_id, industry, status) for analytics queries
--      that group by industry to verify distribution
--
-- SAFE TO APPLY: existing data is untouched. The new constraint is strictly
-- less restrictive than the old one (it adds industry to the key).
-- No data migration needed.

BEGIN;

-- 1. Drop old unique constraint
--    (The index name depends on how it was created. Try both forms.)
DO $$
BEGIN
    -- Try dropping by constraint name
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_candidates_run_company'
        AND conrelid = 'company_candidates'::regclass
    ) THEN
        ALTER TABLE company_candidates DROP CONSTRAINT uq_candidates_run_company;
    END IF;

    -- Try dropping by index name (if created as a unique index, not constraint)
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE indexname = 'uq_candidates_run_company'
        AND tablename = 'company_candidates'
    ) THEN
        DROP INDEX uq_candidates_run_company;
    END IF;

    -- Also try the auto-generated name pattern
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE indexname = 'company_candidates_run_id_company_id_key'
        AND tablename = 'company_candidates'
    ) THEN
        DROP INDEX company_candidates_run_id_company_id_key;
    END IF;
END $$;

-- 2. Create new unique index: one candidate per (run, company, industry)
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidates_run_company_industry
    ON company_candidates (run_id, company_id, industry);

-- 3. Analytics index: lets you quickly check industry distribution
CREATE INDEX IF NOT EXISTS idx_candidates_client_industry_status
    ON company_candidates (client_id, industry, status);

COMMIT;
