-- 0009_create_company_signals.sql

CREATE TABLE IF NOT EXISTS company_signals (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id    UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    company_id   UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    source       TEXT NOT NULL,     -- "news_api", "x_api", "website"
    signal_type  TEXT,              -- "funding", "hiring", etc. (optional in V0)

    headline     TEXT,
    url          TEXT,
    published_at TIMESTAMPTZ,
    raw_text     TEXT,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_signals_company
    ON company_signals (company_id);

CREATE INDEX IF NOT EXISTS idx_company_signals_client_company
    ON company_signals (client_id, company_id);
