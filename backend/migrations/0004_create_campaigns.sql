CREATE TABLE IF NOT EXISTS campaigns (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,

    name            TEXT NOT NULL,
    -- e.g. "draft", "scheduled", "running", "paused", "completed"
    status          TEXT NOT NULL DEFAULT 'draft',

    subject_template    TEXT,
    body_template       TEXT,
    settings            JSONB,

    scheduled_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_campaigns_client_id
    ON campaigns (client_id);

CREATE INDEX IF NOT EXISTS idx_campaigns_client_status
    ON campaigns (client_id, status);

CREATE INDEX IF NOT EXISTS idx_campaigns_scheduled_at
    ON campaigns (scheduled_at);
