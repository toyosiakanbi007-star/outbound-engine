-- 0040_client_onboarding.sql
-- Client Onboarding AI subsystem tables

-- ============================================================================
-- Onboarding runs — one per onboarding attempt
-- ============================================================================
CREATE TABLE IF NOT EXISTS client_onboarding_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,

    -- Status lifecycle: pending → enriching → generating → review_ready → activated | failed
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','enriching','generating','review_ready','activated','failed')),

    -- Input
    input_name      TEXT NOT NULL,
    input_domain    TEXT NOT NULL,
    operator_note   TEXT,

    -- Diffbot resolution
    diffbot_uri     TEXT,

    -- LLM used for generation
    llm_model       TEXT,

    -- Generated drafts (stored directly for quick access)
    draft_config        JSONB,
    draft_prequal_config JSONB,
    draft_icp           JSONB,
    review_notes        JSONB,
    brand_profile       JSONB,       -- logo, socials, summary, competitors
    knowledge_pack      JSONB,       -- merged evidence for LLM input

    -- Timing
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    activated_at    TIMESTAMPTZ,
    error           TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_onboarding_runs_client ON client_onboarding_runs(client_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_runs_status ON client_onboarding_runs(status);

-- ============================================================================
-- Onboarding artifacts — raw evidence and intermediate outputs
-- ============================================================================
CREATE TABLE IF NOT EXISTS client_onboarding_artifacts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES client_onboarding_runs(id) ON DELETE CASCADE,

    artifact_type   TEXT NOT NULL
                    CHECK (artifact_type IN (
                        'diffbot_org_raw',
                        'diffbot_org_normalized',
                        'site_page_raw',
                        'site_page_summary',
                        'knowledge_pack',
                        'draft_config',
                        'draft_prequal_config',
                        'draft_icp',
                        'review_notes'
                    )),

    -- For site_page artifacts
    page_url        TEXT,
    page_type       TEXT,           -- homepage, product, pricing, security, etc.

    content_json    JSONB NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_onboarding_artifacts_run ON client_onboarding_artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_artifacts_type ON client_onboarding_artifacts(run_id, artifact_type);

-- ============================================================================
-- Add is_active to clients if not present (for draft clients)
-- ============================================================================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'clients' AND column_name = 'is_active'
    ) THEN
        ALTER TABLE clients ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE;
    END IF;
END $$;
