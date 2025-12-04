-- 0009_create_companies.sql

CREATE TABLE IF NOT EXISTS companies (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,

    name            TEXT NOT NULL,
    domain          TEXT,              -- e.g. "acme.com"
    industry        TEXT,
    employee_count  INT,
    country         TEXT,
    region          TEXT,
    city            TEXT,

    source          TEXT,              -- e.g. "apollo", "manual"
    external_id     TEXT,              -- e.g. Apollo company ID

    linkedin_url    TEXT,
    website_url     TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- For quick lookup by (client, domain)
CREATE INDEX IF NOT EXISTS idx_companies_client_domain
    ON companies (client_id, domain);

-- For quick lookup by (client, external_id) from Apollo
CREATE INDEX IF NOT EXISTS idx_companies_client_external_id
    ON companies (client_id, external_id);
