CREATE TABLE IF NOT EXISTS emails_sent (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    client_id       UUID NOT NULL REFERENCES clients(id)   ON DELETE CASCADE,
    campaign_id     UUID     REFERENCES campaigns(id)      ON DELETE SET NULL,
    contact_id      UUID     REFERENCES contacts(id)       ON DELETE SET NULL,
    job_id          UUID     REFERENCES jobs(id)           ON DELETE SET NULL,

    ses_message_id  TEXT,
    from_email      TEXT NOT NULL,
    to_email        TEXT NOT NULL,
    subject         TEXT,
    body_preview    TEXT,

    -- e.g. "sent", "bounced", "complaint"
    status          TEXT NOT NULL DEFAULT 'sent',
    error           TEXT,

    sent_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    opened_at       TIMESTAMPTZ,
    clicked_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_sent_ses_message_id
    ON emails_sent (ses_message_id);

CREATE INDEX IF NOT EXISTS idx_emails_sent_contact_id
    ON emails_sent (contact_id);

CREATE INDEX IF NOT EXISTS idx_emails_sent_campaign_id
    ON emails_sent (campaign_id);

CREATE INDEX IF NOT EXISTS idx_emails_sent_client_id_created_at
    ON emails_sent (client_id, created_at);
