-- 0013_create_company_news_raw.sql
-- Raw metadata about each fetch run: S3 keys, search queries, etc.

CREATE TABLE IF NOT EXISTS company_news_raw (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id         UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    company_id        UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    run_id            UUID NOT NULL,          -- logical batch id per fetch run
    url               TEXT NOT NULL,          -- page URL

    search_query      TEXT,                   -- which search query produced this URL
    search_rank       INT,                    -- rank of this result in the search API
    source_name       TEXT,                   -- e.g. "bing", "serpapi", or domain hint

    html_s3_key       TEXT,                   -- S3 key where raw HTML is stored
    extraction_s3_key TEXT,                   -- S3 key where raw LLM extraction JSON is stored
    raw_metadata      JSONB,                  -- any extra structured debug info

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Look up all raw artifacts for a given company
CREATE INDEX IF NOT EXISTS idx_company_news_raw_company
    ON company_news_raw (company_id);

-- Look up all raw artifacts for a specific fetch run
CREATE INDEX IF NOT EXISTS idx_company_news_raw_run
    ON company_news_raw (run_id);

-- (Optional but useful) quickly filter by client+company
CREATE INDEX IF NOT EXISTS idx_company_news_raw_client_company
    ON company_news_raw (client_id, company_id);
