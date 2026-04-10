-- 0015_create_v3_tables.sql
-- V3 NewsFetcher Schema: Pain Hypotheses + Progressive Writes
--
-- Run this migration after your existing migrations.
-- These tables support the V3 prequal engine.

-- =============================================================================
-- Progressive Write Tables
-- =============================================================================

-- Discovered URLs (written during discovery phase)
CREATE TABLE IF NOT EXISTS discovered_urls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    url TEXT NOT NULL,
    source_type VARCHAR(50),
    lane VARCHAR(50),
    rank_score FLOAT DEFAULT 0.0,
    rank_reasons TEXT[],
    title VARCHAR(500),
    snippet TEXT,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Indexes
    CONSTRAINT fk_discovered_urls_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_discovered_urls_company_id ON discovered_urls(company_id);
CREATE INDEX IF NOT EXISTS idx_discovered_urls_run_id ON discovered_urls(run_id);
CREATE INDEX IF NOT EXISTS idx_discovered_urls_discovered_at ON discovered_urls(discovered_at DESC);


-- Fetch results (written after each URL fetch)
CREATE TABLE IF NOT EXISTS fetch_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    url TEXT NOT NULL,
    status_code INT,
    blocked BOOLEAN DEFAULT FALSE,
    block_reason VARCHAR(500),
    content_length INT,
    content_type VARCHAR(100),
    final_url TEXT,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_fetch_results_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fetch_results_company_id ON fetch_results(company_id);
CREATE INDEX IF NOT EXISTS idx_fetch_results_run_id ON fetch_results(run_id);
CREATE INDEX IF NOT EXISTS idx_fetch_results_fetched_at ON fetch_results(fetched_at DESC);


-- Extracted evidence (written after LLM extraction)
CREATE TABLE IF NOT EXISTS extracted_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    url TEXT NOT NULL,
    snippet TEXT NOT NULL,
    evidence_type VARCHAR(50),  -- 'pain_indicator', 'why_now', 'signal', 'context'
    pain_category VARCHAR(100),
    pain_subtags TEXT[],
    source_type VARCHAR(50),
    date VARCHAR(50),
    confidence FLOAT DEFAULT 0.0,
    extracted_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_extracted_evidence_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_extracted_evidence_company_id ON extracted_evidence(company_id);
CREATE INDEX IF NOT EXISTS idx_extracted_evidence_run_id ON extracted_evidence(run_id);
CREATE INDEX IF NOT EXISTS idx_extracted_evidence_pain_category ON extracted_evidence(pain_category);
CREATE INDEX IF NOT EXISTS idx_extracted_evidence_extracted_at ON extracted_evidence(extracted_at DESC);


-- =============================================================================
-- Prequal Results
-- =============================================================================

CREATE TABLE IF NOT EXISTS company_prequal (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL,
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    score FLOAT NOT NULL,
    qualifies BOOLEAN NOT NULL,
    evidence_count INT,
    distinct_sources INT,
    why_now_indicators TEXT[],
    gates_passed TEXT[],
    gates_failed TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_company_prequal_client FOREIGN KEY (client_id) 
        REFERENCES clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_company_prequal_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_company_prequal_company_id ON company_prequal(company_id);
CREATE INDEX IF NOT EXISTS idx_company_prequal_client_id ON company_prequal(client_id);
CREATE INDEX IF NOT EXISTS idx_company_prequal_qualifies ON company_prequal(qualifies) WHERE qualifies = TRUE;
CREATE INDEX IF NOT EXISTS idx_company_prequal_score ON company_prequal(score DESC);
CREATE INDEX IF NOT EXISTS idx_company_prequal_created_at ON company_prequal(created_at DESC);


-- =============================================================================
-- Pain Hypotheses
-- =============================================================================

-- Pain category enum type
DO $$ BEGIN
    CREATE TYPE pain_category_enum AS ENUM (
        'reliability_uptime',
        'operational_visibility',
        'cost_pressure',
        'scaling_capacity',
        'workflow_efficiency',
        'sales_pipeline',
        'customer_churn',
        'security_compliance',
        'tech_debt_migration',
        'talent_execution_gaps',
        'competitive_pressure',
        'market_expansion',
        'other'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;


CREATE TABLE IF NOT EXISTS company_pain_hypotheses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL,
    company_id UUID NOT NULL,
    run_id UUID NOT NULL,
    
    -- Hypothesis content
    hypothesis TEXT NOT NULL,
    why_now TEXT,
    pain_category VARCHAR(100) NOT NULL,  -- From pain_category_enum values
    pain_subtags TEXT[],  -- Freeform LLM subtags
    
    -- Evidence bundle (JSONB array)
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    
    -- Targeting
    recommended_personas TEXT[],
    suggested_offer_fit TEXT,
    
    -- Scoring
    confidence FLOAT DEFAULT 0.0,
    strength VARCHAR(20) DEFAULT 'medium',  -- low, medium, high
    
    -- Phase tracking
    phase VARCHAR(20) DEFAULT 'prequal',  -- prequal, aggregate
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT fk_pain_hypotheses_client FOREIGN KEY (client_id) 
        REFERENCES clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_pain_hypotheses_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_company_id ON company_pain_hypotheses(company_id);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_client_id ON company_pain_hypotheses(client_id);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_run_id ON company_pain_hypotheses(run_id);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_pain_category ON company_pain_hypotheses(pain_category);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_confidence ON company_pain_hypotheses(confidence DESC);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_phase ON company_pain_hypotheses(phase);
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_created_at ON company_pain_hypotheses(created_at DESC);

-- GIN index for evidence JSONB queries
CREATE INDEX IF NOT EXISTS idx_pain_hypotheses_evidence ON company_pain_hypotheses USING GIN (evidence);


-- =============================================================================
-- Apollo Enrichment Storage (for Phase B)
-- =============================================================================

CREATE TABLE IF NOT EXISTS company_apollo_enrichment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL UNIQUE,
    
    -- Apollo org data
    apollo_org_id VARCHAR(100),
    description TEXT,
    industry VARCHAR(200),
    estimated_num_employees INT,
    technologies TEXT[],
    keywords TEXT[],
    
    -- Company info
    founded_year INT,
    annual_revenue BIGINT,
    linkedin_url VARCHAR(500),
    
    -- Location
    city VARCHAR(200),
    state VARCHAR(200),
    country VARCHAR(200),
    
    -- Funding events (JSONB array - raw from Apollo)
    funding_events JSONB DEFAULT '[]'::jsonb,
    total_funding_raised BIGINT,
    last_funding_date DATE,
    last_funding_type VARCHAR(100),
    latest_funding_stage VARCHAR(100),
    
    -- Employee metrics (JSONB - raw from Apollo, array of monthly snapshots)
    employee_metrics JSONB DEFAULT '[]'::jsonb,
    
    -- Analysis results (computed by Rust)
    employee_metrics_analysis JSONB DEFAULT '{}'::jsonb,
    funding_analysis JSONB DEFAULT '{}'::jsonb,
    
    -- Timestamps
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    analyzed_at TIMESTAMPTZ,
    
    CONSTRAINT fk_apollo_enrichment_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_apollo_enrichment_company_id ON company_apollo_enrichment(company_id);
CREATE INDEX IF NOT EXISTS idx_apollo_enrichment_fetched_at ON company_apollo_enrichment(fetched_at DESC);


-- =============================================================================
-- Prequal Queue (companies awaiting Phase B)
-- =============================================================================

CREATE TABLE IF NOT EXISTS prequal_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id UUID NOT NULL,
    company_id UUID NOT NULL,
    prequal_id UUID NOT NULL,  -- Reference to company_prequal
    run_id UUID NOT NULL,
    
    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed
    
    -- Timestamps
    queued_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    -- Error tracking
    error_message TEXT,
    retry_count INT DEFAULT 0,
    
    CONSTRAINT fk_prequal_queue_client FOREIGN KEY (client_id) 
        REFERENCES clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_prequal_queue_company FOREIGN KEY (company_id) 
        REFERENCES companies(id) ON DELETE CASCADE,
    CONSTRAINT fk_prequal_queue_prequal FOREIGN KEY (prequal_id) 
        REFERENCES company_prequal(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_prequal_queue_status ON prequal_queue(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_prequal_queue_company_id ON prequal_queue(company_id);
CREATE INDEX IF NOT EXISTS idx_prequal_queue_queued_at ON prequal_queue(queued_at);


-- =============================================================================
-- Views for Analytics
-- =============================================================================

-- Latest prequal per company
CREATE OR REPLACE VIEW v_latest_company_prequal AS
SELECT DISTINCT ON (company_id)
    cp.*,
    c.name AS company_name,
    c.domain AS company_domain
FROM company_prequal cp
JOIN companies c ON c.id = cp.company_id
ORDER BY company_id, created_at DESC;


-- Qualified companies (ready for Phase B)
CREATE OR REPLACE VIEW v_qualified_companies AS
SELECT 
    cp.*,
    c.name AS company_name,
    c.domain AS company_domain,
    (SELECT COUNT(*) FROM company_pain_hypotheses ph WHERE ph.company_id = cp.company_id AND ph.run_id = cp.run_id) AS hypothesis_count
FROM company_prequal cp
JOIN companies c ON c.id = cp.company_id
WHERE cp.qualifies = TRUE
ORDER BY cp.score DESC, cp.created_at DESC;


-- Pain category distribution
CREATE OR REPLACE VIEW v_pain_category_stats AS
SELECT 
    pain_category,
    COUNT(*) AS hypothesis_count,
    AVG(confidence) AS avg_confidence,
    COUNT(DISTINCT company_id) AS company_count
FROM company_pain_hypotheses
WHERE created_at > NOW() - INTERVAL '30 days'
GROUP BY pain_category
ORDER BY hypothesis_count DESC;


-- =============================================================================
-- Notification trigger (optional - app-level NOTIFY preferred)
-- =============================================================================

-- Trigger function to notify on prequal completion
CREATE OR REPLACE FUNCTION notify_prequal_ready()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'prequal_ready',
        json_build_object(
            'company_id', NEW.company_id,
            'client_id', NEW.client_id,
            'run_id', NEW.run_id,
            'score', NEW.score,
            'qualifies', NEW.qualifies
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger (disabled by default - enable if you want DB-level notifications)
-- DROP TRIGGER IF EXISTS trg_prequal_ready ON company_prequal;
-- CREATE TRIGGER trg_prequal_ready
--     AFTER INSERT ON company_prequal
--     FOR EACH ROW
--     EXECUTE FUNCTION notify_prequal_ready();


-- =============================================================================
-- Grant permissions (adjust role names as needed)
-- =============================================================================

-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO your_app_role;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO your_app_role;
