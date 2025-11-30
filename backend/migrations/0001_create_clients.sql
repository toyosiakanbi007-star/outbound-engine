-- Enable UUID generation (RDS supports this)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS clients (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- You probably want one row per "client name"
CREATE UNIQUE INDEX IF NOT EXISTS ux_clients_name ON clients (name);

-- Helpful if you later query only active clients
CREATE INDEX IF NOT EXISTS idx_clients_is_active ON clients (is_active);
