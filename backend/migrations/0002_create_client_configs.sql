CREATE TABLE IF NOT EXISTS client_configs (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id   UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    config      JSONB NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Only one active config per client
    CONSTRAINT uq_client_configs_client_active UNIQUE (client_id, is_active)
);

CREATE INDEX IF NOT EXISTS idx_client_configs_client_id
    ON client_configs (client_id);

CREATE INDEX IF NOT EXISTS idx_client_configs_client_version
    ON client_configs (client_id, version);
