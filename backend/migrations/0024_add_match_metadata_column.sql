-- 0024_add_match_metadata_column.sql
-- Adds match_metadata JSONB column to company_apollo_enrichment table.
--
-- This column stores query context for debugging and tuning:
--   {
--     "provider": "diffbot",
--     "matched_industry_name": "Currency And Lending Services",
--     "matched_diffbot_category": "Currency And Lending Services",
--     "include_keywords_any": ["loan", "lending"],
--     "include_keywords_all": [],
--     "exclude_keywords_any": ["crypto exchange"],
--     "query_variant": "tight",
--     "query_variant_ladder": "v1_strict"
--   }

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'company_apollo_enrichment'
                     AND column_name = 'match_metadata') THEN
        ALTER TABLE company_apollo_enrichment
            ADD COLUMN match_metadata JSONB DEFAULT '{}'::jsonb;
        RAISE NOTICE 'Added: match_metadata JSONB column';
    END IF;
END $$;
