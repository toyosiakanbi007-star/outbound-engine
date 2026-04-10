-- 0022_add_companies_unique_constraints.sql
--
-- PURPOSE:
-- Add partial unique constraints on `companies` to support ON CONFLICT upserts
-- in the Company Fetcher. Currently only non-unique indexes exist, so
-- INSERT ... ON CONFLICT cannot target them.
--
-- CONSTRAINTS ADDED:
-- 1. UNIQUE (client_id, external_id) WHERE external_id IS NOT NULL
--    → Primary dedupe key: Apollo org ID is globally unique per provider
-- 2. UNIQUE (client_id, domain) WHERE domain IS NOT NULL
--    → Secondary dedupe key: one company per domain per client
-- 3. UNIQUE (client_id, linkedin_url) WHERE linkedin_url IS NOT NULL
--    → Tertiary dedupe key: fallback when domain is missing
--
-- SAFETY:
-- Before adding each constraint, we deduplicate any existing rows that would
-- violate it (keeping the most recently updated row). This makes the migration
-- safe to run on databases with existing data.
--
-- IDEMPOTENT: Uses DO blocks with pg_constraint checks.

-- ============================================================================
-- STEP 1: Clean up duplicate (client_id, external_id) rows if any exist
-- ============================================================================

-- Keep the most recently updated row per (client_id, external_id), delete older dupes
DELETE FROM companies
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY client_id, external_id
                   ORDER BY updated_at DESC, created_at DESC
               ) AS rn
        FROM companies
        WHERE external_id IS NOT NULL
    ) ranked
    WHERE rn > 1
);

-- Add partial unique constraint
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_companies_client_external_id'
    ) THEN
        ALTER TABLE companies
        ADD CONSTRAINT uq_companies_client_external_id
        UNIQUE (client_id, external_id);
        RAISE NOTICE 'Added: uq_companies_client_external_id';
    END IF;
END $$;

-- Drop the old non-unique index (now redundant — the constraint creates its own)
DROP INDEX IF EXISTS idx_companies_client_external_id;


-- ============================================================================
-- STEP 2: Normalize domains before deduplication
-- ============================================================================

-- Normalize existing domains: lowercase + strip "www." prefix
UPDATE companies
SET domain = REGEXP_REPLACE(LOWER(TRIM(domain)), '^www\.', '')
WHERE domain IS NOT NULL
  AND domain != REGEXP_REPLACE(LOWER(TRIM(domain)), '^www\.', '');

-- Clean up duplicate (client_id, domain) rows
DELETE FROM companies
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY client_id, domain
                   ORDER BY updated_at DESC, created_at DESC
               ) AS rn
        FROM companies
        WHERE domain IS NOT NULL
    ) ranked
    WHERE rn > 1
);

-- Add partial unique constraint
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_companies_client_domain'
    ) THEN
        ALTER TABLE companies
        ADD CONSTRAINT uq_companies_client_domain
        UNIQUE (client_id, domain);
        RAISE NOTICE 'Added: uq_companies_client_domain';
    END IF;
END $$;

-- Drop the old non-unique index (now redundant)
DROP INDEX IF EXISTS idx_companies_client_domain;


-- ============================================================================
-- STEP 3: LinkedIn URL unique constraint (tertiary dedupe key)
-- ============================================================================

-- Normalize linkedin URLs: lowercase, trim trailing slash
UPDATE companies
SET linkedin_url = RTRIM(LOWER(TRIM(linkedin_url)), '/')
WHERE linkedin_url IS NOT NULL
  AND linkedin_url != RTRIM(LOWER(TRIM(linkedin_url)), '/');

-- Clean up duplicate (client_id, linkedin_url) rows
DELETE FROM companies
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY client_id, linkedin_url
                   ORDER BY updated_at DESC, created_at DESC
               ) AS rn
        FROM companies
        WHERE linkedin_url IS NOT NULL
    ) ranked
    WHERE rn > 1
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_companies_client_linkedin'
    ) THEN
        ALTER TABLE companies
        ADD CONSTRAINT uq_companies_client_linkedin
        UNIQUE (client_id, linkedin_url);
        RAISE NOTICE 'Added: uq_companies_client_linkedin';
    END IF;
END $$;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT
    t.relname AS table_name,
    c.conname AS constraint_name,
    pg_get_constraintdef(c.oid) AS constraint_definition
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
WHERE c.contype = 'u'
  AND t.relname = 'companies'
ORDER BY c.conname;

SELECT '0022_add_companies_unique_constraints: DONE' AS status;
