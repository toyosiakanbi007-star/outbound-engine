-- 0031_create_structured_logs.sql
--
-- Structured log storage for the control panel / debug UI.
--
-- Design: batch-inserted by a tracing subscriber in the Rust backend.
-- Not real-time; flushed every 5s or every 100 entries.
-- Retention: 30 days by default (cleanup via cron or background task).

CREATE TABLE IF NOT EXISTS structured_logs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level       TEXT NOT NULL CHECK (level IN ('trace','debug','info','warn','error')),
    service     TEXT NOT NULL DEFAULT 'backend',
    module      TEXT,

    -- Contextual foreign keys (all nullable — not all logs have these)
    -- No FK constraints: logs should never fail due to referential integrity
    run_id      UUID,
    company_id  UUID,
    client_id   UUID,
    job_id      UUID,

    message     TEXT NOT NULL,
    data_json   JSONB,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Time-based queries (most common access pattern)
CREATE INDEX IF NOT EXISTS idx_sl_timestamp
    ON structured_logs (timestamp DESC);

-- Filter by level (most common: errors and warnings only)
CREATE INDEX IF NOT EXISTS idx_sl_level
    ON structured_logs (level, timestamp DESC)
    WHERE level IN ('warn', 'error');

-- Contextual lookups (partial indexes — only index non-null rows)
CREATE INDEX IF NOT EXISTS idx_sl_run_id
    ON structured_logs (run_id, timestamp DESC)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sl_company_id
    ON structured_logs (company_id, timestamp DESC)
    WHERE company_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sl_client_id
    ON structured_logs (client_id, timestamp DESC)
    WHERE client_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sl_job_id
    ON structured_logs (job_id, timestamp DESC)
    WHERE job_id IS NOT NULL;

-- Full-text search on message
CREATE INDEX IF NOT EXISTS idx_sl_message_search
    ON structured_logs
    USING GIN (to_tsvector('english', message));

-- Verify
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'structured_logs') THEN
        RAISE NOTICE 'OK: structured_logs table created';
    ELSE
        RAISE WARNING 'MISSING: structured_logs table';
    END IF;
END $$;
