-- 0029_prequal_reasons.sql
--
-- Add LLM-generated decision reasons and full result storage to company_prequal.
--
-- company_prequal already stores: score, qualifies, gates, evidence_summary, etc.
-- We add:
--   full_result     — complete Azure Function output (for audit/debugging)
--   prequal_reasons — LLM-generated _prequal_reasons block (for analytics/feedback loop)
--   offer_fit_tags  — phase0 LLM-generated qualification/disqualification tags

BEGIN;

-- Full Azure Function response (the entire JSON output)
ALTER TABLE company_prequal
    ADD COLUMN IF NOT EXISTS full_result JSONB;

-- LLM-generated prequal decision reasons
-- Structure: { qualifies, summary, reasons[], gates_failed_explained[], missing_info[], tags[], confidence }
ALTER TABLE company_prequal
    ADD COLUMN IF NOT EXISTS prequal_reasons JSONB;

-- Phase0 offer-fit tags from the same LLM call that does qualify/disqualify
-- Structure: ["icp_match", "strong_b2b_signal", "industry_fit", ...]
ALTER TABLE company_prequal
    ADD COLUMN IF NOT EXISTS offer_fit_tags JSONB;

-- GIN index for tag-based analytics on prequal_reasons.tags
CREATE INDEX IF NOT EXISTS idx_company_prequal_reason_tags
    ON company_prequal
    USING GIN ((prequal_reasons -> 'tags'))
    WHERE prequal_reasons IS NOT NULL;

-- GIN index for offer_fit_tags analytics
CREATE INDEX IF NOT EXISTS idx_company_prequal_offer_fit_tags
    ON company_prequal
    USING GIN (offer_fit_tags)
    WHERE offer_fit_tags IS NOT NULL;

COMMENT ON COLUMN company_prequal.full_result IS
    'Complete Azure Function prequal output JSON (news_items, pain_hypotheses, prequal, offer_fit, etc.)';

COMMENT ON COLUMN company_prequal.prequal_reasons IS
    'LLM-generated _prequal_reasons: summary, reasons, tags, confidence, missing_info';

COMMENT ON COLUMN company_prequal.offer_fit_tags IS
    'Phase0 LLM-generated qualification/disqualification tags from offer-fit evaluation';

COMMIT;
