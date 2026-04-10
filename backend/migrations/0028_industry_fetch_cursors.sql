-- 0028_industry_fetch_cursors.sql
--
-- Persists pagination state across company_fetch runs so subsequent runs
-- resume where the last run left off instead of re-scanning from page 1.
--
-- CRITICAL FOR DIFFBOT: Diffbot charges 25 credits per entity RETURNED,
-- not per page. Without cursor persistence, every run re-fetches all
-- previously seen entities at full cost.
--
-- CRITICAL FOR APOLLO: Apollo charges 1 credit per page. Re-scanning
-- pages of already-known companies wastes credits even though the deduper
-- prevents DB inserts.
--
-- DESIGN:
--   One row per (client_id, industry, variant).
--   Stores the next page/offset to resume from, whether the variant is
--   exhausted, and whether the tight→loose fallback was triggered.
--   The orchestrator loads all cursors at run start, uses them for initial
--   page numbers, and saves them after each industry turn completes.

BEGIN;

CREATE TABLE IF NOT EXISTS industry_fetch_cursors (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id       UUID NOT NULL,
    industry        TEXT NOT NULL,
    variant         TEXT NOT NULL,         -- 'v1_strict', 'v2_broaden_geo', etc.

    -- Pagination state
    next_page       INT NOT NULL DEFAULT 1,
    total_fetched   INT NOT NULL DEFAULT 0,  -- cumulative entities fetched across all runs
    total_new       INT NOT NULL DEFAULT 0,  -- cumulative new (non-dupe) inserts

    -- Variant lifecycle
    exhausted       BOOLEAN NOT NULL DEFAULT FALSE,
    use_loose       BOOLEAN NOT NULL DEFAULT FALSE,  -- tight→loose fallback triggered

    -- Provider metadata (for detecting dataset changes)
    last_total_hits BIGINT,               -- provider's total_entries on last fetch
    last_run_id     UUID,                 -- which run last updated this cursor

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- One cursor per (client, industry, variant)
    CONSTRAINT uq_fetch_cursors_client_industry_variant
        UNIQUE (client_id, industry, variant)
);

-- Index for fast load of all cursors for a client
CREATE INDEX IF NOT EXISTS idx_fetch_cursors_client
    ON industry_fetch_cursors (client_id);

-- Index for cleanup queries (e.g., delete cursors for removed industries)
CREATE INDEX IF NOT EXISTS idx_fetch_cursors_client_industry
    ON industry_fetch_cursors (client_id, industry);

COMMIT;
