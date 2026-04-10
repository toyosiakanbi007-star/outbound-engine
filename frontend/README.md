# Outbound Engine — Control Panel Frontend

Operator control panel for the outbound engine. Built with Next.js 14 (App Router), TypeScript, Tailwind CSS, and TanStack Query.

## Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on **http://localhost:3001** and proxies `/api/*` to the Rust backend on **http://localhost:3000**.

## Prerequisites

- Node.js 18+
- Rust backend running in server mode: `MODE=server cargo run`
- Postgres with all migrations applied (including `0031_create_structured_logs.sql`)

## Architecture

```
Frontend (Next.js :3001)  →  /api/*  →  Rust Backend (Axum :3000)  →  Postgres
```

The frontend is a pure read/trigger UI. It never embeds business logic — all orchestration happens in the Rust backend and workers.

## Pages

| Route | Purpose |
|-------|---------|
| `/` | Dashboard — stats, recent runs, queue overview, Azure Fn health |
| `/clients` | Client list with summary stats |
| `/clients/[id]` | Client detail — stats, pipeline triggers, recent runs, top companies |
| `/clients/[id]/config` | Config editor — form fields + raw JSON + prequal config |
| `/discovery-runs` | Discovery run list with counters |
| `/discovery-runs/[id]` | Run detail — stats, industry summary, candidates table |
| `/companies` | Company search/filter with score and status |
| `/companies/[id]` | Company detail — tabbed: overview, evidence, hypotheses, news, raw JSON |
| `/prequal-runs` | Prequal job list (dispatch + batch) |
| `/prequal-runs/[id]` | Job detail with payload and error |
| `/queue` | Queue stats, worker status (inferred), job list with filters |
| `/logs` | Structured log viewer with level/module/context filters |
| `/settings` | Backend health and version info |

## Stack

- **Next.js 14** — App Router, server/client components
- **TanStack Query** — data fetching with conditional polling
- **Tailwind CSS** — utility-first styling
- **IBM Plex Sans / Mono** — typography (operator/industrial feel)
- **Lucide React** — icons

## Polling strategy

| Page | Interval | Condition |
|------|----------|-----------|
| Dashboard | 10-30s | Always |
| Discovery run detail | 5s | While `running` |
| Prequal runs list | 10s | While any pending/running |
| Queue page | 5-10s | Always |
| Logs page | Manual refresh | Button click |

## Key patterns

- **API client**: Single file `lib/api/client.ts` with all TanStack Query hooks
- **Lazy loading**: Company detail tabs use `enabled` flag — evidence/hypotheses/news only fetch when tab is active
- **Conditional polling**: `refetchInterval` returns `false` when runs are complete
- **Error handling**: API errors display inline, never crash the page
- **JSON inspection**: Every detail page has a raw JSON tab/section

## Development

```bash
# Frontend dev server (hot reload)
npm run dev

# Backend server mode (separate terminal)
cd ../backend
MODE=server cargo run
```

## Production

For production, configure the Next.js `rewrites` in `next.config.mjs` to point to your production backend URL, or deploy behind a reverse proxy (nginx, Caddy) that routes `/api/*` to the Rust backend.
