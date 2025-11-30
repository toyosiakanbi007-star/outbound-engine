CREATE TABLE IF NOT EXISTS jobs (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id   UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    campaign_id UUID     REFERENCES campaigns(id) ON DELETE SET NULL,
    contact_id  UUID     REFERENCES contacts(id)   ON DELETE SET NULL,

    -- e.g. "send_email", "enrich_contact", "ai_personalize"
    job_type    TEXT NOT NULL,

    -- e.g. "pending", "in_progress", "succeeded", "failed"
    status      TEXT NOT NULL DEFAULT 'pending',

    payload     JSONB,
    run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Find jobs to run
CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at
    ON jobs (status, run_at);

CREATE INDEX IF NOT EXISTS idx_jobs_client_id
    ON jobs (client_id);

CREATE INDEX IF NOT EXISTS idx_jobs_campaign_id
    ON jobs (campaign_id);

CREATE INDEX IF NOT EXISTS idx_jobs_contact_id
    ON jobs (contact_id);
