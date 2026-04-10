-- 0010_create_lead_scores.sql

CREATE TABLE IF NOT EXISTS lead_scores (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id    UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    company_id   UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    fit_score    INT NOT NULL,
    intent_score INT NOT NULL,
    risk_score   INT NOT NULL,
    final_score  INT NOT NULL,

    summary      TEXT,
    scored_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_scores_client_company
    ON lead_scores (client_id, company_id);
