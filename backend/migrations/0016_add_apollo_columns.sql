-- 0016_add_apollo_columns.sql
-- Adds additional Apollo fields to company_apollo_enrichment table
-- Safe to run on existing data

-- Add new columns if they don't exist
DO $$ 
BEGIN
    -- Keywords
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'keywords') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN keywords TEXT[];
    END IF;
    
    -- Founded year
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'founded_year') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN founded_year INT;
    END IF;
    
    -- Annual revenue
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'annual_revenue') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN annual_revenue BIGINT;
    END IF;
    
    -- LinkedIn URL
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'linkedin_url') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN linkedin_url VARCHAR(500);
    END IF;
    
    -- Location fields
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'city') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN city VARCHAR(200);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'state') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN state VARCHAR(200);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'country') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN country VARCHAR(200);
    END IF;
    
    -- Latest funding stage
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'company_apollo_enrichment' AND column_name = 'latest_funding_stage') THEN
        ALTER TABLE company_apollo_enrichment ADD COLUMN latest_funding_stage VARCHAR(100);
    END IF;
END $$;

-- Fix employee_metrics default if it was {} instead of []
-- This is safe - only affects future inserts
ALTER TABLE company_apollo_enrichment 
    ALTER COLUMN employee_metrics SET DEFAULT '[]'::jsonb;
