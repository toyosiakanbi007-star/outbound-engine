# Outbound Engine — Control Panel Architecture

## Decisions Summary

All architectural decisions confirmed with the operator:

| Decision | Choice |
|----------|--------|
| Auto mode UI | Autopilot toggle per client + "Run Full Pipeline" button + separate Discovery/Prequal triggers |
| Discovery trigger | Async job (enqueue `discover_companies`, worker picks up, frontend polls) |
| API location | Same Rust binary, `MODE=server` — add all routes to existing Axum server |
| PrequalConfig editing | All fields as form controls (autopilot, batch_size, max_batches, interval, timeout, max_attempts) |
| Structured logs | Postgres `structured_logs` table now, batch insert via tracing subscriber (not real-time) |
| Client config editor | Form fields for known stable keys + raw JSON fallback tab |
| Company detail loading | Hybrid: basic + prequal in one call; evidence/hypotheses/news as separate lazy-loaded endpoints |
| Azure Function health | Yes, health ping shown on dashboard |

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTEND (Next.js)                       │
│  App Router · TypeScript · Tailwind · shadcn/ui · TanStack Query│
│                                                                 │
│  Dashboard │ Clients │ Discovery │ Companies │ Prequal │ Logs   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ JSON API (HTTP)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   RUST BACKEND (MODE=server)                    │
│  Axum HTTP server · Same binary as worker                       │
│                                                                 │
│  /api/health          /api/clients        /api/companies        │
│  /api/discovery-runs  /api/prequal-runs   /api/logs             │
│  /api/queue-status    /api/workers        /api/pipeline          │
│                                                                 │
│  Reads/writes: Postgres (source of truth)                       │
│  Enqueues jobs: INSERT INTO jobs (workers pick up async)         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
│ RUST WORKER  │ │ PREQUAL      │ │ NEWS LISTENER    │
│ (MODE=worker)│ │ LISTENER     │ │ (LISTEN/NOTIFY)  │
│              │ │ (PG LISTEN)  │ │                  │
│ discover_    │ │              │ │                  │
│ companies    │ │ prequal_ready│ │ news_ready       │
│ prequal_     │ │ → Phase B    │ │ → insert news    │
│ dispatch     │ │   enqueue    │ │                  │
│ prequal_     │ │              │ │                  │
│ batch        │ │              │ │                  │
│ phase_b_*    │ │              │ │                  │
└──────┬───────┘ └──────────────┘ └──────────────────┘
       │
       │ HTTP POST (fire-and-forget for prequal)
       ▼
┌─────────────────────────────────────────────────────────────────┐
│              AZURE FUNCTION (Python v2)                         │
│  mode="prequal" → Phase0 offer-fit + news fetch + scoring      │
│  mode="aggregate" → Phase B aggregation                        │
│                                                                 │
│  Writes directly to: v3_analysis_runs, company_prequal,         │
│  extracted_evidence, v3_hypotheses, discovered_urls             │
│  Sends: NOTIFY prequal_ready                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Backend API Contract

All endpoints are prefixed with `/api`. Responses use standard JSON envelope:

```json
{
  "data": { ... },
  "meta": { "total": 100, "page": 1, "per_page": 50 }
}
```

Error responses:

```json
{
  "error": { "code": "not_found", "message": "Company not found" }
}
```

### 2.1 Health & Meta

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/health` | Server + DB health |
| `GET` | `/api/version` | Build version |
| `GET` | `/api/health/azure-function` | Ping Azure Function URL, return reachable/latency |

**`GET /api/health`** — enhanced from current:
```json
{
  "status": "ok",
  "env": "production",
  "db": "ok",
  "uptime_seconds": 12345,
  "version": "0.3.0"
}
```

**`GET /api/health/azure-function`** — new:
```json
{
  "reachable": true,
  "latency_ms": 142,
  "url": "https://xxx.azurewebsites.net/api/...",
  "checked_at": "2026-04-08T12:00:00Z"
}
```

Implementation: shallow reachability check only — HTTP GET with a 3s timeout. If the Azure Function exposes a dedicated `/health` or `/api/health` endpoint, prefer that. Otherwise, a TCP+TLS connection success + any HTTP response (even 401/404) counts as "reachable." The check must never send an expensive payload or trigger real processing. This is purely "is the Azure Function infrastructure up?"

### 2.2 Clients

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/clients` | List all clients (with summary stats) |
| `POST` | `/api/clients` | Create client |
| `GET` | `/api/clients/:id` | Client detail + recent runs + top companies |
| `PUT` | `/api/clients/:id` | Update client (name, is_active) |
| `GET` | `/api/clients/:id/config` | Get active client_config + prequal_config |
| `PUT` | `/api/clients/:id/config` | Update config JSON + prequal_config |
| `GET` | `/api/clients/:id/icp` | Get ICP profile (icp_json) |
| `PUT` | `/api/clients/:id/icp` | Update ICP profile |

**`GET /api/clients`** response:
```json
{
  "data": [
    {
      "id": "uuid",
      "name": "Acme Corp",
      "is_active": true,
      "created_at": "...",
      "stats": {
        "total_companies": 1234,
        "qualified_companies": 89,
        "disqualified_companies": 456,
        "pending_prequal": 23,
        "last_discovery_run": "2026-04-07T...",
        "last_prequal_run": "2026-04-07T...",
        "autopilot_enabled": true
      }
    }
  ]
}
```

**`POST /api/clients`** request body:
```json
{
  "name": "Acme Corp",
  "config": {
    "brand_name": "Acme",
    "niche": "SaaS security",
    "offer": "...",
    "tone": "professional",
    "calendar_link": "https://...",
    "sending_limits": { "daily_max": 100 }
  },
  "icp": {
    "target_profile": {
      "industries": [
        { "name": "healthcare_saas", "diffbot_category": "Software Companies", "include_keywords_any": ["health", "medical"] }
      ],
      "company_sizes": ["51-200", "201-500"],
      "locations": ["United States"]
    }
  },
  "prequal_config": {
    "autopilot_enabled": true,
    "batch_size": 5,
    "max_batches_per_dispatch": 10
  }
}
```

**`GET /api/clients/:id/config`** response:
```json
{
  "data": {
    "config_id": "uuid",
    "client_id": "uuid",
    "version": 3,
    "config": { "brand_name": "...", "niche": "...", ... },
    "prequal_config": {
      "autopilot_enabled": true,
      "batch_size": 5,
      "max_batches_per_dispatch": 10,
      "dispatch_interval_minutes": 15,
      "max_per_industry_per_dispatch": null,
      "max_attempts_per_company": 4,
      "azure_function_timeout_secs": 120
    },
    "is_active": true,
    "updated_at": "..."
  }
}
```

**Known stable config keys** (for form fields):
- `brand_name` (string)
- `niche` (string)
- `offer` (text/multiline)
- `tone` (string — "professional", "casual", "direct", etc.)
- `calendar_link` (url string)
- `sending_limits.daily_max` (integer)
- `sender_email` (string)
- `sender_name` (string)

Everything else falls through to the raw JSON editor tab.

### 2.3 Discovery Runs

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/clients/:id/discovery-runs` | Enqueue a discover_companies job |
| `GET` | `/api/discovery-runs` | List runs (filterable by client, status, date) |
| `GET` | `/api/discovery-runs/:id` | Run detail (counters, industry summary, timing) |
| `GET` | `/api/discovery-runs/:id/companies` | Paginated candidate companies from this run |

**`POST /api/clients/:id/discovery-runs`** — enqueues a `discover_companies` job:

Request body:
```json
{
  "batch_target": 500,
  "page_size": 100,
  "exploration_pct": 0.20,
  "max_runtime_seconds": 1800,
  "enqueue_prequal_jobs": true
}
```

All fields optional — defaults from `DiscoverCompaniesPayload` apply.

Response:
```json
{
  "data": {
    "job_id": "uuid",
    "run_id": null,
    "status": "pending",
    "message": "Discovery job enqueued"
  }
}
```

The frontend polls `GET /api/discovery-runs` or the specific run once the `run_id` appears in `company_fetch_runs`.

**`GET /api/discovery-runs/:id`** response:
```json
{
  "data": {
    "id": "uuid",
    "client_id": "uuid",
    "status": "succeeded",
    "batch_target": 2000,
    "quota_plan": { "healthcare_saas": 400, "fintech": 350 },
    "raw_fetched": 1856,
    "unique_upserted": 1204,
    "duplicates": 652,
    "pages_fetched": 42,
    "api_credits_used": 42,
    "industry_summary": { ... },
    "started_at": "...",
    "ended_at": "...",
    "elapsed_seconds": 340,
    "prequal_dispatch_job_id": "uuid or null",
    "error": null
  }
}
```

### 2.4 Companies

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/companies` | List/search companies (filterable) |
| `GET` | `/api/companies/:id` | Company detail (hybrid: basic + latest prequal) |
| `GET` | `/api/companies/:id/evidence` | Lazy-load: extracted evidence for this company |
| `GET` | `/api/companies/:id/hypotheses` | Lazy-load: pain hypotheses |
| `GET` | `/api/companies/:id/news` | Lazy-load: company_news items |
| `GET` | `/api/companies/:id/discovered-urls` | Lazy-load: discovered URLs from prequal runs |
| `GET` | `/api/companies/:id/candidates` | All company_candidate records (cross-run history) |
| `POST` | `/api/companies/:id/actions/rerun-prequal` | Enqueue prequal re-run for this company |
| `POST` | `/api/companies/:id/actions/rerun-fetch` | Enqueue news re-fetch |

**`GET /api/companies`** query params:
- `client_id` (uuid, required or optional depending on multi-tenant view)
- `status` (qualified, disqualified, new, prequal_queued, etc.)
- `industry` (string)
- `search` (fuzzy match on name/domain)
- `min_score` / `max_score` (float)
- `page` / `per_page`
- `sort_by` (name, score, created_at, updated_at)
- `sort_order` (asc, desc)

**`GET /api/companies/:id`** — hybrid response (basic + prequal in one call):
```json
{
  "data": {
    "company": {
      "id": "uuid",
      "client_id": "uuid",
      "name": "HealthTech Inc",
      "domain": "healthtech.io",
      "industry": "healthcare_saas",
      "employee_count": 120,
      "country": "United States",
      "region": "California",
      "city": "San Francisco",
      "source": "apollo",
      "linkedin_url": "...",
      "website_url": "...",
      "created_at": "...",
      "updated_at": "..."
    },
    "latest_candidate": {
      "id": "uuid",
      "run_id": "uuid",
      "status": "qualified",
      "industry": "healthcare_saas",
      "variant": "v1_tight",
      "was_duplicate": false,
      "prequal_attempts": 1,
      "prequal_completed_at": "...",
      "created_at": "..."
    },
    "latest_prequal": {
      "id": "uuid",
      "run_id": "uuid",
      "score": 0.82,
      "qualifies": true,
      "gates_passed": ["evidence_count", "distinct_sources", "why_now"],
      "gates_failed": [],
      "why_now_indicators": ["recent_funding", "hiring_surge"],
      "evidence_count": 8,
      "distinct_sources": 5,
      "prequal_reasons": {
        "summary": "Strong fit — recent Series B + aggressive hiring in compliance roles...",
        "reasons": ["...", "..."],
        "tags": ["funding_signal", "hiring_signal", "compliance_pain"],
        "confidence": 0.85
      },
      "offer_fit_tags": ["icp_match", "strong_b2b_signal"],
      "created_at": "..."
    },
    "run_history_count": 3
  }
}
```

Evidence, hypotheses, and news load separately via their own endpoints when the user scrolls to those sections.

**Design discipline:** `GET /api/companies/:id` must stay summary-focused. It returns the company row, the latest candidate record, and the latest prequal summary (score, qualifies, gates, reasons). It must NOT grow to include evidence lists, hypothesis arrays, news items, or full_result JSONB. Those belong in their lazy-loaded sub-endpoints. If the response payload exceeds ~5KB for a single company, something has leaked in.

### 2.5 Prequal Runs

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/clients/:id/prequal-dispatch` | Manually enqueue a PREQUAL_DISPATCH job |
| `GET` | `/api/prequal-runs` | List prequal dispatch + batch jobs |
| `GET` | `/api/prequal-runs/:id` | Dispatch or batch job detail |
| `GET` | `/api/companies/:id/latest-prequal` | Latest company_prequal result (with full_result JSON) |

**`POST /api/clients/:id/prequal-dispatch`** request:
```json
{
  "batch_size": 5,
  "max_batches": 10,
  "force": false,
  "filters": {
    "only_new_since": "2026-04-01T00:00:00Z",
    "industry_name": "healthcare_saas"
  },
  "source": "manual"
}
```

Response:
```json
{
  "data": {
    "job_id": "uuid",
    "status": "pending",
    "message": "Prequal dispatch job enqueued"
  }
}
```

### 2.6 Pipeline (Full Pipeline Trigger)

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/clients/:id/pipeline/run` | Enqueue discovery + auto-prequal chain |

**`POST /api/clients/:id/pipeline/run`** — the "Run Full Pipeline" button:

This endpoint:
1. Creates a `discover_companies` job with `force_full_pipeline: true` in the payload
2. The orchestrator reads this flag and enqueues `PREQUAL_DISPATCH` after discovery regardless of the stored `autopilot_enabled` setting
3. **Does NOT mutate the stored `prequal_config`** — the flag is run-scoped, not persistent
4. Returns the job_id so the frontend can track progress

Request body:
```json
{
  "batch_target": 500,
  "max_runtime_seconds": 1800
}
```

Response:
```json
{
  "data": {
    "discovery_job_id": "uuid",
    "force_full_pipeline": true,
    "message": "Full pipeline started: discovery → prequal (run-scoped, config unchanged)"
  }
}
```

The frontend can then poll both discovery runs and prequal runs for this client to show the chain progressing.

### 2.7 Queue & Workers

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/queue-status` | Aggregate job queue stats |
| `GET` | `/api/queue-status/jobs` | Paginated job list (filterable) |
| `GET` | `/api/workers` | Worker status (best-effort, see note) |

**`/api/workers` — best-effort without heartbeats table:** Until a `worker_heartbeats` table is added (Phase 3), this endpoint returns inferred worker info derived from the `jobs` table: distinct `assigned_worker` values from recently-running jobs, last seen timestamps, and job counts. This is approximate — a worker that has been idle for 10 minutes won't appear. The frontend should display this data with a "inferred from job activity" caveat. Do not block the frontend build on perfect worker telemetry.

**`GET /api/queue-status`** response:
```json
{
  "data": {
    "total_pending": 14,
    "total_running": 3,
    "total_failed_24h": 2,
    "total_completed_24h": 87,
    "by_type": {
      "discover_companies": { "pending": 1, "running": 1, "failed_24h": 0 },
      "prequal_dispatch": { "pending": 2, "running": 0, "failed_24h": 0 },
      "prequal_batch": { "pending": 8, "running": 2, "failed_24h": 1 },
      "phase_b_enrich_apollo": { "pending": 3, "running": 0, "failed_24h": 1 }
    },
    "oldest_pending_age_seconds": 45,
    "avg_completion_time_seconds": 23.5,
    "stuck_jobs": []
  }
}
```

**`GET /api/queue-status/jobs`** query params:
- `status` (pending, running, done, failed)
- `job_type` (discover_companies, prequal_dispatch, etc.)
- `client_id` (uuid)
- `since` (timestamp)
- `page` / `per_page`

### 2.8 Structured Logs

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/logs` | Query structured logs |
| `GET` | `/api/logs/stream` | Poll for latest logs (simple polling, not SSE yet) |

**`GET /api/logs`** query params:
- `run_id`, `company_id`, `client_id`, `job_id` (uuid filters)
- `level` (error, warn, info, debug)
- `service` / `module` (string filter)
- `search` (keyword search in message)
- `since` / `until` (timestamp range)
- `page` / `per_page`
- `order` (asc, desc — default desc)

Response:
```json
{
  "data": [
    {
      "id": "uuid",
      "timestamp": "2026-04-08T12:34:56Z",
      "level": "info",
      "service": "worker",
      "module": "company_fetcher::orchestrator",
      "run_id": "uuid",
      "company_id": null,
      "client_id": "uuid",
      "job_id": "uuid",
      "message": "Autopilot: enqueued PREQUAL_DISPATCH abc123 (42 new companies)",
      "data_json": { "prequal_dispatch_job_id": "abc123", "new_companies": 42 }
    }
  ],
  "meta": { "total": 5432, "page": 1, "per_page": 100 }
}
```

---

## 3. Structured Logs — DB Schema & Tracing Subscriber

### 3.1 Migration: `0031_create_structured_logs.sql`

```sql
CREATE TABLE IF NOT EXISTS structured_logs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level       TEXT NOT NULL CHECK (level IN ('trace','debug','info','warn','error')),
    service     TEXT NOT NULL DEFAULT 'backend',
    module      TEXT,
    
    -- Contextual foreign keys (all nullable — not all logs have these)
    run_id      UUID,
    company_id  UUID,
    client_id   UUID,
    job_id      UUID,
    
    message     TEXT NOT NULL,
    data_json   JSONB,
    
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Time-based queries (most common access pattern)
CREATE INDEX idx_sl_timestamp ON structured_logs (timestamp DESC);

-- Filter by level
CREATE INDEX idx_sl_level ON structured_logs (level, timestamp DESC)
    WHERE level IN ('warn', 'error');

-- Contextual lookups
CREATE INDEX idx_sl_run_id ON structured_logs (run_id, timestamp DESC)
    WHERE run_id IS NOT NULL;
CREATE INDEX idx_sl_company_id ON structured_logs (company_id, timestamp DESC)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_sl_client_id ON structured_logs (client_id, timestamp DESC)
    WHERE client_id IS NOT NULL;
CREATE INDEX idx_sl_job_id ON structured_logs (job_id, timestamp DESC)
    WHERE job_id IS NOT NULL;

-- Full-text search on message
CREATE INDEX idx_sl_message_search ON structured_logs
    USING GIN (to_tsvector('english', message));

-- Auto-cleanup: partition by month or add a retention policy later.
-- For now, a simple DELETE cron or a check in the batch inserter:
--   DELETE FROM structured_logs WHERE timestamp < NOW() - INTERVAL '30 days';
```

### 3.2 Batch Insert Tracing Subscriber

The subscriber collects log events in an in-memory buffer and flushes to Postgres periodically (every 5 seconds or when buffer hits 100 entries, whichever comes first).

Architecture:
```
tracing::info!("message", run_id = %id)
    │
    ▼
┌─────────────────────┐
│ DbLogLayer          │ ◄── implements tracing_subscriber::Layer
│ - Captures spans    │
│ - Reads span fields │
│   (run_id, etc.)    │
│ - Sends LogEntry to │
│   channel           │
└────────┬────────────┘
         │ mpsc channel
         ▼
┌─────────────────────┐
│ LogFlusher task      │ ◄── tokio::spawn background task
│ - Collects entries   │
│ - Batches by time/   │
│   count threshold    │
│ - INSERT INTO        │
│   structured_logs    │
│   (batch of N rows)  │
└─────────────────────┘
```

Key design decisions:
- Uses `tracing::Span` fields for context: workers already set `run_id`, `company_id`, `client_id`, `job_id` as span fields
- Non-blocking: the `DbLogLayer` sends to an async mpsc channel, never blocks the hot path
- Batch insert: uses a single `INSERT INTO ... VALUES ($1), ($2), ...` statement
- Graceful degradation: if the channel is full or DB insert fails, logs are dropped (console logging still works as primary)
- Retention: a daily cleanup task or cron deletes logs older than 30 days

### 3.3 How to attach context

In worker code, wrap operations in spans:

```rust
// Already largely done in the codebase via tracing::info! etc.
// The subscriber extracts these fields automatically.

let span = tracing::info_span!(
    "prequal_batch",
    run_id = %run_id,
    client_id = %client_id,
    job_id = %job.id,
);
let _guard = span.enter();

// All tracing events inside this scope get run_id/client_id/job_id attached
tracing::info!("Processing batch of {} candidates", batch.len());
```

---

## 4. Frontend Architecture

### 4.1 Tech Stack

| Layer | Choice |
|-------|--------|
| Framework | Next.js 14+ (App Router) |
| Language | TypeScript (strict) |
| Styling | Tailwind CSS |
| Components | shadcn/ui |
| Data fetching | TanStack Query (React Query v5) |
| Forms | React Hook Form + Zod validation |
| JSON editor | `@monaco-editor/react` (or `react-json-view-lite` for readonly tree) |
| Tables | TanStack Table (headless) + shadcn DataTable |
| Charts | Recharts (lightweight, already available) |
| Icons | Lucide React |

### 4.2 Directory Structure

```
frontend/
├── app/
│   ├── layout.tsx                  # Root layout (sidebar + header)
│   ├── page.tsx                    # Dashboard
│   ├── clients/
│   │   ├── page.tsx                # Clients list
│   │   └── [id]/
│   │       ├── page.tsx            # Client detail
│   │       ├── config/
│   │       │   └── page.tsx        # Config editor (forms + JSON)
│   │       └── icp/
│   │           └── page.tsx        # ICP editor
│   ├── discovery-runs/
│   │   ├── page.tsx                # Discovery runs list
│   │   └── [id]/
│   │       └── page.tsx            # Run detail + companies table
│   ├── companies/
│   │   ├── page.tsx                # Companies list/search
│   │   └── [id]/
│   │       └── page.tsx            # Company detail (tabbed)
│   ├── prequal-runs/
│   │   ├── page.tsx                # Prequal runs list
│   │   └── [id]/
│   │       └── page.tsx            # Prequal run detail
│   ├── queue/
│   │   └── page.tsx                # Queue + worker status
│   ├── logs/
│   │   └── page.tsx                # Structured log viewer
│   └── settings/
│       └── page.tsx                # Lightweight settings
├── components/
│   ├── layout/
│   │   ├── sidebar.tsx             # Left nav
│   │   ├── header.tsx              # Top bar + breadcrumbs
│   │   └── page-shell.tsx          # Wrapper (header + content area)
│   ├── ui/                         # shadcn primitives
│   ├── data-table/
│   │   ├── data-table.tsx          # Reusable table with sort/filter/paginate
│   │   ├── column-header.tsx
│   │   └── pagination.tsx
│   ├── json-viewer.tsx             # Collapsible JSON tree
│   ├── json-editor.tsx             # Monaco-based JSON editor
│   ├── status-badge.tsx            # Colored badges for statuses
│   ├── score-bar.tsx               # Visual score indicator
│   ├── stat-card.tsx               # Dashboard metric cards
│   ├── pipeline-trigger.tsx        # "Run Full Pipeline" button w/ config
│   ├── autopilot-toggle.tsx        # Per-client autopilot switch
│   └── log-viewer.tsx              # Filterable log viewer component
├── lib/
│   ├── api/
│   │   ├── client.ts               # Base fetch wrapper (error handling, auth)
│   │   ├── clients.ts              # Client CRUD hooks
│   │   ├── discovery.ts            # Discovery run hooks
│   │   ├── companies.ts            # Company hooks
│   │   ├── prequal.ts              # Prequal hooks
│   │   ├── queue.ts                # Queue/worker hooks
│   │   ├── logs.ts                 # Logs hooks
│   │   └── pipeline.ts             # Pipeline trigger hooks
│   ├── types/
│   │   ├── client.ts               # Client, ClientConfig, PrequalConfig types
│   │   ├── company.ts              # Company, Candidate, Prequal types
│   │   ├── discovery.ts            # DiscoveryRun, FetchQuery types
│   │   ├── job.ts                  # Job, JobType, JobStatus types
│   │   └── log.ts                  # StructuredLog type
│   └── utils/
│       ├── format.ts               # Date, number, duration formatters
│       └── polling.ts              # Polling interval helpers
└── next.config.ts                  # Proxy /api → Rust backend
```

### 4.3 API Client Layer

Base client with error handling:

```typescript
// lib/api/client.ts
const API_BASE = process.env.NEXT_PUBLIC_API_URL || '/api';

export async function apiFetch<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ error: { message: res.statusText } }));
    throw new ApiError(res.status, error.error?.message || 'Unknown error', error.error?.code);
  }

  return res.json();
}
```

TanStack Query hooks pattern:

```typescript
// lib/api/companies.ts
export function useCompanies(filters: CompanyFilters) {
  return useQuery({
    queryKey: ['companies', filters],
    queryFn: () => apiFetch<PaginatedResponse<Company>>('/companies?' + toParams(filters)),
  });
}

export function useCompanyDetail(id: string) {
  return useQuery({
    queryKey: ['company', id],
    queryFn: () => apiFetch<CompanyDetail>(`/companies/${id}`),
  });
}

// Lazy-loaded sub-resources (only fetch when tab is active)
export function useCompanyEvidence(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['company', id, 'evidence'],
    queryFn: () => apiFetch<Evidence[]>(`/companies/${id}/evidence`),
    enabled, // only fires when user opens the evidence tab
  });
}
```

Polling for active runs:

```typescript
export function useDiscoveryRun(id: string) {
  return useQuery({
    queryKey: ['discovery-run', id],
    queryFn: () => apiFetch<DiscoveryRun>(`/discovery-runs/${id}`),
    refetchInterval: (data) =>
      data?.status === 'running' ? 5000 : false, // poll every 5s while running
  });
}
```

### 4.4 Key Page Designs

#### Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│ Dashboard                                                    │
├─────────┬─────────┬─────────┬─────────┬─────────┬──────────┤
│ Clients │Companies│Qualified│ Pending │ Failed  │ Azure Fn │
│   12    │  4,231  │   342   │  Prequal│  Jobs   │  ● 142ms │
│         │         │         │    23   │    2    │          │
├─────────┴─────────┴─────────┴─────────┴─────────┴──────────┤
│                                                              │
│  Recent Runs                              Queue Overview     │
│  ┌──────────────────────────────┐  ┌───────────────────────┐│
│  │ Run abc123  ● succeeded      │  │ discover_companies: 1 ││
│  │ Client: Acme · 342 companies │  │ prequal_dispatch:   2 ││
│  │ 12 min ago                   │  │ prequal_batch:      8 ││
│  ├──────────────────────────────┤  │ phase_b:            3 ││
│  │ Run def456  ● running        │  │                       ││
│  │ Client: Beta · 89/500        │  │ Oldest pending: 45s   ││
│  │ Started 3 min ago            │  └───────────────────────┘│
│  └──────────────────────────────┘                            │
│                                                              │
│  Recent Errors                                               │
│  ┌──────────────────────────────────────────────────────────┐│
│  │ ⚠ prequal_batch job xyz failed: Azure Function timeout  ││
│  │   Client: Acme · Company: HealthTech · 8 min ago         ││
│  └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

#### Client Detail

```
┌─────────────────────────────────────────────────────────────┐
│ Clients > Acme Corp                              [Edit] [⚙] │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Autopilot: [● ON]     Niche: SaaS Security                │
│  Offer: Compliance automation for...                         │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐ │
│  │Companies │  │Qualified │  │Pending   │  │Last Run     │ │
│  │  1,234   │  │   89     │  │   23     │  │ 2h ago      │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────────┘ │
│                                                              │
│  [▶ Run Full Pipeline]  [▶ Discovery Only]  [▶ Prequal Only]│
│                                                              │
│  Recent Runs                                                 │
│  ┌────────┬──────────┬────────┬────────┬──────────┐         │
│  │ Type   │ Status   │ Result │ Time   │ Duration │         │
│  │ Disc.  │ ●done    │ 342 co │ 2h ago │ 12m      │         │
│  │ Preq.D │ ●done    │ 89/342 │ 1h ago │ 45m      │         │
│  └────────┴──────────┴────────┴────────┴──────────┘         │
│                                                              │
│  Top Qualified Companies                                     │
│  ┌────────────────┬────────┬────────┬──────────────┐        │
│  │ Company        │ Score  │ Status │ Why Now      │        │
│  │ HealthTech Inc │ 0.92   │ ●qual  │ Series B     │        │
│  │ SecureFlow     │ 0.87   │ ●qual  │ Hiring surge │        │
│  └────────────────┴────────┴────────┴──────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

#### Company Detail (Tabbed)

```
┌─────────────────────────────────────────────────────────────┐
│ Companies > HealthTech Inc                                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Domain: healthtech.io    Industry: healthcare_saas          │
│  Size: 120 employees      Location: San Francisco, US        │
│  Source: apollo            Score: ████████░░ 0.82             │
│  Status: ● qualified       Candidate: run abc123             │
│                                                              │
│  [▶ Rerun Prequal]  [▶ Rerun News Fetch]                    │
│                                                              │
│  ┌────────┬────────────┬──────────┬──────┬──────────────┐   │
│  │Overview│ Evidence   │Hypotheses│ News │ Raw JSON     │   │
│  └────────┴────────────┴──────────┴──────┴──────────────┘   │
│                                                              │
│  [Overview tab shown]                                        │
│  Prequal Score: 0.82 · Qualifies: Yes                       │
│  Gates Passed: evidence_count, distinct_sources, why_now     │
│  Offer Fit: icp_match, strong_b2b_signal                    │
│                                                              │
│  Summary: Strong fit — recent Series B funding + aggressive  │
│  hiring in compliance roles signals active pain in the       │
│  security/compliance domain...                               │
│                                                              │
│  Why-Now Indicators: recent_funding, hiring_surge            │
│                                                              │
│  Run History (3 runs)                                        │
│  ┌──────────┬────────┬────────┬──────────┐                  │
│  │ Run      │ Score  │ Result │ Date     │                  │
│  │ abc123   │ 0.82   │ qual.  │ Apr 7    │                  │
│  │ def456   │ 0.71   │ disq.  │ Mar 28   │                  │
│  └──────────┴────────┴────────┴──────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

### 4.5 Polling Strategy

| Page | What to poll | Interval | Condition |
|------|-------------|----------|-----------|
| Dashboard | `/api/queue-status` + `/api/health/azure-function` | 10s | Always |
| Discovery run detail | `/api/discovery-runs/:id` | 5s | While status = `running` |
| Prequal runs list | `/api/prequal-runs` | 10s | While any run is `pending` or `running` |
| Queue page | `/api/queue-status` + `/api/queue-status/jobs` | 5s | Always |
| Logs page | `/api/logs?since=<last_timestamp>` | 5s | When "follow" mode is on |

TanStack Query's `refetchInterval` handles this cleanly with conditional polling.

---

## 5. Backend Implementation Plan

### 5.1 New Rust Modules

```
src/
├── routes/
│   ├── mod.rs              # Route registration
│   ├── debug.rs            # (existing)
│   ├── news.rs             # (existing)
│   ├── clients.rs          # NEW: Client CRUD
│   ├── companies.rs        # NEW: Company endpoints
│   ├── discovery.rs        # NEW: Discovery run endpoints
│   ├── prequal.rs          # NEW: Prequal endpoints
│   ├── pipeline.rs         # NEW: Full pipeline trigger
│   ├── queue.rs            # NEW: Queue/worker status
│   ├── logs.rs             # NEW: Structured log queries
│   └── health.rs           # NEW: Enhanced health (incl. Azure Fn)
├── logging/
│   ├── mod.rs              # NEW: Logging module
│   ├── db_layer.rs         # NEW: Tracing subscriber → DB
│   └── flusher.rs          # NEW: Background batch inserter
```

### 5.2 Router Registration (updated `run_server`)

```rust
let app = Router::new()
    // Health
    .route("/api/health", get(routes::health::health))
    .route("/api/version", get(routes::health::version))
    .route("/api/health/azure-function", get(routes::health::azure_function_health))

    // Clients
    .route("/api/clients", get(routes::clients::list).post(routes::clients::create))
    .route("/api/clients/:id", get(routes::clients::get).put(routes::clients::update))
    .route("/api/clients/:id/config", get(routes::clients::get_config).put(routes::clients::update_config))
    .route("/api/clients/:id/icp", get(routes::clients::get_icp).put(routes::clients::update_icp))

    // Discovery
    .route("/api/clients/:id/discovery-runs", post(routes::discovery::create))
    .route("/api/discovery-runs", get(routes::discovery::list))
    .route("/api/discovery-runs/:id", get(routes::discovery::get))
    .route("/api/discovery-runs/:id/companies", get(routes::discovery::companies))

    // Companies
    .route("/api/companies", get(routes::companies::list))
    .route("/api/companies/:id", get(routes::companies::get))
    .route("/api/companies/:id/evidence", get(routes::companies::evidence))
    .route("/api/companies/:id/hypotheses", get(routes::companies::hypotheses))
    .route("/api/companies/:id/news", get(routes::companies::news))
    .route("/api/companies/:id/discovered-urls", get(routes::companies::discovered_urls))
    .route("/api/companies/:id/candidates", get(routes::companies::candidates))
    .route("/api/companies/:id/actions/rerun-prequal", post(routes::companies::rerun_prequal))
    .route("/api/companies/:id/actions/rerun-fetch", post(routes::companies::rerun_fetch))

    // Prequal
    .route("/api/clients/:id/prequal-dispatch", post(routes::prequal::dispatch))
    .route("/api/prequal-runs", get(routes::prequal::list))
    .route("/api/prequal-runs/:id", get(routes::prequal::get))
    .route("/api/companies/:id/latest-prequal", get(routes::prequal::latest_for_company))

    // Pipeline
    .route("/api/clients/:id/pipeline/run", post(routes::pipeline::run))

    // Queue
    .route("/api/queue-status", get(routes::queue::status))
    .route("/api/queue-status/jobs", get(routes::queue::jobs))
    .route("/api/workers", get(routes::queue::workers))

    // Logs
    .route("/api/logs", get(routes::logs::query))

    // Legacy
    .route("/debug/jobs/test-send", post(routes::debug::create_test_send_job_handler))
    .route("/news/fetch", post(routes::news::mock_fetch_news_handler))

    // CORS (for local frontend dev)
    .layer(CorsLayer::permissive())

    .with_state(state);
```

### 5.3 CORS

For local development, the Next.js frontend runs on `localhost:3001` while the Rust backend is on `localhost:3000`. Add `tower-http` CORS layer:

```toml
# Cargo.toml addition
tower-http = { version = "0.5", features = ["cors"] }
```

In production, the Next.js `next.config.ts` proxies `/api` to the Rust backend, so CORS isn't needed.

---

## 6. Phased Build Order

### Phase 1 — Foundation (Backend API + Frontend Shell)

**Backend:**
1. Migration `0031_create_structured_logs.sql`
2. `routes/health.rs` — enhanced health + Azure Function ping
3. `routes/clients.rs` — full Client CRUD + config + ICP endpoints
4. `routes/discovery.rs` — discovery run endpoints + job enqueue
5. `routes/prequal.rs` — prequal dispatch + listing
6. `routes/pipeline.rs` — full pipeline trigger
7. Updated `run_server()` with all routes + CORS
8. `logging/db_layer.rs` + `logging/flusher.rs` — batch log subscriber

**Frontend:**
1. Next.js app scaffold + Tailwind + shadcn/ui setup
2. Layout: sidebar + header + page-shell
3. Dashboard page (stat cards + recent runs + queue overview + Azure Fn status)
4. Clients list page + create client modal
5. Client detail page (info + stats + action buttons + recent runs)
6. Client config editor (form fields + raw JSON tab)
7. Discovery runs list + detail page
8. Prequal runs list + detail page

**Deliverables:** Operator can manage clients, trigger discovery/prequal, and see run status.

### Phase 2 — Deep Inspection

**Backend:**
1. `routes/companies.rs` — company list/search + hybrid detail + lazy sub-resources
2. `routes/queue.rs` — queue status + job listing
3. `routes/logs.rs` — structured log queries

**Frontend:**
1. Companies list page (filterable, searchable)
2. Company detail page (tabbed: overview, evidence, hypotheses, news, raw JSON)
3. Queue/worker status page
4. Logs page (filterable table + expandable JSON + follow mode)

**Deliverables:** Full visibility into companies, queue, and logs.

### Phase 3 — Polish

**Frontend:**
1. Config editor improvements (ICP industry builder, validation feedback)
2. Better table filters (saved filter presets, column visibility)
3. Client-scoped views (filter everything by selected client)
4. Polling refinements (reduce intervals when idle, increase when active)
5. Keyboard shortcuts for common actions
6. Toast notifications for completed/failed jobs
7. Dark mode (shadcn supports this out of the box)

**Backend:**
1. Log retention cleanup (cron or background task)
2. Worker heartbeat tracking (if we add `worker_heartbeats` table)
3. Bulk actions (batch rerun prequal for multiple companies)

---

## 7. Key Implementation Notes

### 7.1 Client Config Form — Known Keys

The form fields map to these JSON paths in `client_configs.config`:

| Field | JSON Path | Type | Validation |
|-------|-----------|------|------------|
| Brand Name | `brand_name` | string | required, 1-100 chars |
| Niche | `niche` | string | required |
| Offer | `offer` | text | required |
| Tone | `tone` | string | enum: professional, casual, direct, friendly |
| Calendar Link | `calendar_link` | url | valid URL |
| Sender Email | `sender_email` | string | valid email |
| Sender Name | `sender_name` | string | 1-100 chars |
| Daily Send Limit | `sending_limits.daily_max` | integer | 1-500 |

The raw JSON tab shows the full `config` JSONB and allows editing any field. On save, the form merges known fields into the JSON and sends the whole object.

### 7.2 PrequalConfig Form Fields

| Field | Key | Type | Range | Default |
|-------|-----|------|-------|---------|
| Autopilot | `autopilot_enabled` | toggle | bool | true |
| Batch Size | `batch_size` | number | 1-50 | 5 |
| Max Batches per Dispatch | `max_batches_per_dispatch` | number | 1-100 | 10 |
| Dispatch Interval (min) | `dispatch_interval_minutes` | number | 5-120 | 15 |
| Max per Industry | `max_per_industry_per_dispatch` | number | null or 1-500 | null |
| Max Attempts per Company | `max_attempts_per_company` | number | 1-10 | 4 |
| Azure Function Timeout (s) | `azure_function_timeout_secs` | number | 30-600 | 120 |

### 7.3 Status Color Mapping

| Status | Color | Badge Style |
|--------|-------|-------------|
| `new` | gray | outline |
| `pending` | yellow | outline |
| `running` | blue | filled, animated pulse |
| `prequal_queued` | indigo | outline |
| `qualified` | green | filled |
| `disqualified` | red | outline |
| `done` / `succeeded` | green | filled |
| `failed` | red | filled |
| `depleted` | amber | outline |
| `partial` | amber | outline |
| `skipped` | gray | outline |

### 7.4 "Run Full Pipeline" UX Flow

1. User clicks **"Run Full Pipeline"** on client detail page
2. Modal appears with optional overrides (batch_target, max_runtime)
3. Confirm → `POST /api/clients/:id/pipeline/run`
4. Frontend receives `discovery_job_id`
5. Dashboard/client detail shows a "Pipeline Running" banner
6. Frontend polls discovery runs for this client
7. When discovery completes and autopilot triggers prequal, the prequal runs appear
8. Each stage shows progress: Discovery ✓ → Prequal Dispatch ✓ → Batches 3/8 → ...
9. Pipeline banner updates with final stats when all batches complete

### 7.5 Company Detail — Lazy Loading Pattern

```
[Page loads]
  → GET /api/companies/:id           ← basic + prequal (immediate)
  
[User clicks "Evidence" tab]
  → GET /api/companies/:id/evidence   ← only now

[User clicks "Hypotheses" tab]
  → GET /api/companies/:id/hypotheses ← only now

[User clicks "News" tab]
  → GET /api/companies/:id/news       ← only now

[User clicks "Raw JSON" tab]
  → GET /api/companies/:id/latest-prequal  ← full_result JSONB
```

Each tab uses `useQuery` with `enabled: activeTab === 'evidence'` pattern.
