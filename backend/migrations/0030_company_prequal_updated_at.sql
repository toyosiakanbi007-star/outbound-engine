-- 0030_company_prequal_updated_at.sql
--
-- Add updated_at column to company_prequal.
--
-- The table was created in 0015 with only created_at.
-- Migration 0029 added prequal_reasons/offer_fit_tags/full_result columns
-- that are written via UPDATE after the initial INSERT, so we need
-- updated_at to track when the row was last modified.

BEGIN;

ALTER TABLE company_prequal
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Backfill: set updated_at = created_at for existing rows
UPDATE company_prequal SET updated_at = created_at WHERE updated_at IS NULL;

COMMIT;
