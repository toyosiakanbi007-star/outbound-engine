-- 0008_create_client_icp_profiles.sql

CREATE TABLE IF NOT EXISTS client_icp_profiles (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id  UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    icp_json   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_client_icp_profiles_client
    ON client_icp_profiles (client_id);
