-- 0041_aggregate_extra_columns.sql
--
-- Add columns exposed by v2 aggregator (Group A-C + calibration):
--   - automation_confidence: high/medium/low (drives downstream layer behavior)
--   - review_reasons: exceptions that triggered needs_review (JSONB array of strings)
--   - do_not_outreach_reasons: why the company was blocked (JSONB array of strings)
--
-- These are stored flat (not just inside output_json) so we can filter/sort
-- via SQL (e.g., "give me all automation_confidence=high AND tier IN (A,B)").

ALTER TABLE aggregate_results
    ADD COLUMN IF NOT EXISTS automation_confidence TEXT,
    ADD COLUMN IF NOT EXISTS review_reasons JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS do_not_outreach_reasons JSONB DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_aggregate_results_automation_confidence
    ON aggregate_results (automation_confidence);

CREATE INDEX IF NOT EXISTS idx_aggregate_results_tier_confidence
    ON aggregate_results (tier, automation_confidence)
    WHERE qualifies = TRUE;
