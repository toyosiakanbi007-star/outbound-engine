-- 0017_create_crawl_queue_tables.sql
-- 
-- Tables for Windows VM custom crawler integration.
--
-- FLOW:
-- 1. Azure Function inserts blocked URLs into crawl_queue
-- 2. Windows VM crawler polls crawl_queue for pending work
-- 3. Crawler fetches with real browser, extracts with DeepSeek
-- 4. Results written to crawl_results
-- 5. Azure Function reads crawl_results and continues processing

-- Crawl Queue: URLs waiting to be crawled by Windows VM
CREATE TABLE IF NOT EXISTS crawl_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    client_id UUID,
    run_id VARCHAR(100) NOT NULL,
    
    -- URL details
    url VARCHAR(4000) NOT NULL,
    company_name VARCHAR(500) NOT NULL,
    domain VARCHAR(500) NOT NULL,
    source_type VARCHAR(50) DEFAULT 'other',
    
    -- Queue management
    status VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed
    priority INT DEFAULT 0,                -- Higher = process first
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    
    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ,              -- For delayed retries
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    -- Error tracking
    last_error TEXT,
    
    -- Original context (helps Azure Function continue)
    original_source_type VARCHAR(50),
    original_lane VARCHAR(50),
    original_rank_score FLOAT,
    
    CONSTRAINT crawl_queue_status_check CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Indexes for efficient polling
CREATE INDEX IF NOT EXISTS idx_crawl_queue_status_priority 
    ON crawl_queue(status, priority DESC, created_at ASC) 
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_crawl_queue_company_run 
    ON crawl_queue(company_id, run_id);

CREATE INDEX IF NOT EXISTS idx_crawl_queue_scheduled 
    ON crawl_queue(scheduled_at) 
    WHERE status = 'pending' AND scheduled_at IS NOT NULL;

-- Unique constraint to prevent duplicate URLs in same run
CREATE UNIQUE INDEX IF NOT EXISTS idx_crawl_queue_url_run 
    ON crawl_queue(company_id, run_id, url);


-- Crawl Results: Extracted events from custom crawler
-- Matches Azure Function's extraction format so it can seamlessly continue
CREATE TABLE IF NOT EXISTS crawl_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    
    -- URL info
    url VARCHAR(4000) NOT NULL,
    final_url VARCHAR(4000),
    
    -- Extracted event (matches Azure Function format)
    title VARCHAR(2000) NOT NULL,
    date DATE,
    summary TEXT,
    source_type VARCHAR(50) DEFAULT 'other',
    source_name VARCHAR(500),
    tags TEXT[] DEFAULT '{}',
    confidence FLOAT DEFAULT 0.5,
    location VARCHAR(500),
    
    -- V3 evidence fields
    evidence_snippets JSONB DEFAULT '[]',
    pain_indicators TEXT[] DEFAULT '{}',
    why_now_indicators TEXT[] DEFAULT '{}',
    
    -- Crawler metadata
    page_summary TEXT,
    relevance_score FLOAT DEFAULT 0.0,
    crawled_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Processing status
    processed_by_azure BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_crawl_results_company_run 
    ON crawl_results(company_id, run_id);

CREATE INDEX IF NOT EXISTS idx_crawl_results_unprocessed 
    ON crawl_results(company_id, run_id) 
    WHERE NOT processed_by_azure;


-- Helper function: Insert blocked URL into crawl queue
CREATE OR REPLACE FUNCTION queue_blocked_url(
    p_company_id UUID,
    p_client_id UUID,
    p_run_id VARCHAR(100),
    p_url VARCHAR(4000),
    p_company_name VARCHAR(500),
    p_domain VARCHAR(500),
    p_source_type VARCHAR(50) DEFAULT 'other',
    p_priority INT DEFAULT 0,
    p_original_lane VARCHAR(50) DEFAULT NULL,
    p_original_rank_score FLOAT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_id UUID;
BEGIN
    INSERT INTO crawl_queue (
        company_id, client_id, run_id, url, company_name, domain,
        source_type, priority, original_lane, original_rank_score
    ) VALUES (
        p_company_id, p_client_id, p_run_id, p_url, p_company_name, p_domain,
        p_source_type, p_priority, p_original_lane, p_original_rank_score
    )
    ON CONFLICT (company_id, run_id, url) DO NOTHING
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;


-- Helper function: Get unprocessed crawl results for a run
CREATE OR REPLACE FUNCTION get_crawl_results_for_run(
    p_company_id UUID,
    p_run_id VARCHAR(100)
) RETURNS TABLE (
    id UUID,
    url VARCHAR(4000),
    title VARCHAR(2000),
    date DATE,
    summary TEXT,
    source_type VARCHAR(50),
    source_name VARCHAR(500),
    tags TEXT[],
    confidence FLOAT,
    evidence_snippets JSONB,
    pain_indicators TEXT[],
    why_now_indicators TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cr.id,
        cr.url,
        cr.title,
        cr.date,
        cr.summary,
        cr.source_type,
        cr.source_name,
        cr.tags,
        cr.confidence,
        cr.evidence_snippets,
        cr.pain_indicators,
        cr.why_now_indicators
    FROM crawl_results cr
    WHERE cr.company_id = p_company_id
      AND cr.run_id = p_run_id
      AND NOT cr.processed_by_azure
    ORDER BY cr.confidence DESC;
END;
$$ LANGUAGE plpgsql;


-- Helper function: Mark crawl results as processed
CREATE OR REPLACE FUNCTION mark_crawl_results_processed(
    p_company_id UUID,
    p_run_id VARCHAR(100)
) RETURNS INT AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE crawl_results
    SET processed_by_azure = TRUE,
        processed_at = NOW()
    WHERE company_id = p_company_id
      AND run_id = p_run_id
      AND NOT processed_by_azure;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;


-- Notify channel for crawler coordination
-- Azure Function sends: NOTIFY crawl_requested, '{"company_id": "...", "run_id": "...", "url_count": 5}'
-- Crawler can LISTEN crawl_requested to wake up immediately instead of polling
