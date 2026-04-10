CREATE TABLE IF NOT EXISTS contacts (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id   UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,

    email       TEXT NOT NULL,
    first_name  TEXT,
    last_name   TEXT,
    full_name   TEXT,
    title       TEXT,
    company     TEXT,

    -- e.g. "new", "enriched", "qualified", "unsubscribed"
    status      TEXT NOT NULL DEFAULT 'new',

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- No duplicate email per client
    CONSTRAINT uq_contacts_client_email UNIQUE (client_id, email)
);

CREATE INDEX IF NOT EXISTS idx_contacts_client_id
    ON contacts (client_id);

CREATE INDEX IF NOT EXISTS idx_contacts_client_status
    ON contacts (client_id, status);
