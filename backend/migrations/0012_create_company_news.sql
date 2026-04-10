-- 0012_create_company_news.sql
-- Normalized, deduplicated news / signals per company

CREATE TABLE IF NOT EXISTS company_news (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id    UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    company_id   UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- Core news fields
    title        TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    location     TEXT,
    summary      TEXT,

    url          TEXT NOT NULL,
    source_type  TEXT NOT NULL,            -- "company_site" | "press_release" | "media" | "blog" | "forum" | "other"
    source_name  TEXT,

    tags         TEXT[] NOT NULL DEFAULT '{}',   -- ["funding", "series_a", "product_launch"]
    confidence   REAL NOT NULL DEFAULT 0.0,      -- 0.0 – 1.0

    -- Optional raw JSON snapshot of the news item as returned by the news service
    raw_item     JSONB,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookups: show news for a company sorted by recency
CREATE INDEX IF NOT EXISTS idx_company_news_company_published
    ON company_news (company_id, published_at DESC);

-- Tag-based filtering: GIN index on text[]
CREATE INDEX IF NOT EXISTS idx_company_news_tags_gin
    ON company_news USING GIN (tags);

-- Dedupe prevention: one logical event per company/title/timestamp/source_name
-- Note: we use published_at directly (no ::date cast) to avoid non-IMMUTABLE functions in the index.
CREATE UNIQUE INDEX IF NOT EXISTS idx_company_news_dedupe
    ON company_news (company_id, lower(title), published_at, source_name);

