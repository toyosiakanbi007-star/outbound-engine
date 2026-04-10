// lib/types/index.ts — All shared types for the control panel

// ============================================================================
// API envelope
// ============================================================================
export interface ApiResponse<T> {
  data: T;
  meta?: { total: number; page: number; per_page: number };
}

export interface ApiError {
  error: { code: string; message: string };
}

// ============================================================================
// Client
// ============================================================================
export interface Client {
  id: string;
  name: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  stats?: ClientStats;
}

export interface ClientStats {
  total_companies: number;
  qualified_companies: number;
  disqualified_companies: number;
  pending_prequal: number;
  last_discovery_run: string | null;
  last_prequal_run: string | null;
  autopilot_enabled: boolean;
}

export interface ClientConfig {
  config_id: string;
  client_id: string;
  version: number;
  config: Record<string, unknown>;
  prequal_config: PrequalConfig;
  is_active: boolean;
  updated_at: string;
}

export interface PrequalConfig {
  autopilot_enabled: boolean;
  batch_size: number;
  max_batches_per_dispatch: number;
  dispatch_interval_minutes: number;
  max_per_industry_per_dispatch: number | null;
  max_attempts_per_company: number;
  azure_function_timeout_secs: number;
}

export interface IcpProfile {
  id: string;
  client_id: string;
  icp_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

// ============================================================================
// Discovery
// ============================================================================
export interface DiscoveryRun {
  id: string;
  client_id: string;
  status: string;
  batch_target: number;
  quota_plan: Record<string, number>;
  raw_fetched: number;
  unique_upserted: number;
  duplicates: number;
  pages_fetched: number;
  api_credits_used: number;
  industry_summary: Record<string, unknown> | null;
  error: Record<string, unknown> | null;
  started_at: string | null;
  ended_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface Candidate {
  id: string;
  company_id: string;
  company_name: string;
  company_domain: string | null;
  industry: string | null;
  variant: string | null;
  status: string;
  was_duplicate: boolean;
  created_at: string;
}

// ============================================================================
// Company
// ============================================================================
export interface Company {
  id: string;
  client_id: string;
  name: string;
  domain: string | null;
  industry: string | null;
  employee_count: number | null;
  country: string | null;
  region: string | null;
  city: string | null;
  source: string | null;
  linkedin_url: string | null;
  website_url: string | null;
  created_at: string;
  updated_at: string;
}

export interface CompanyListItem {
  id: string;
  client_id: string;
  name: string;
  domain: string | null;
  industry: string | null;
  employee_count: number | null;
  country: string | null;
  source: string | null;
  latest_status: string | null;
  latest_score: number | null;
  created_at: string;
}

export interface CompanyDetail {
  company: Company;
  latest_candidate: CandidateSummary | null;
  latest_prequal: PrequalSummary | null;
  run_history_count: number;
}

export interface CandidateSummary {
  id: string;
  run_id: string;
  status: string;
  industry: string | null;
  variant: string | null;
  was_duplicate: boolean;
  prequal_attempts: number;
  prequal_completed_at: string | null;
  created_at: string;
}

export interface PrequalSummary {
  id: string;
  run_id: string;
  score: number;
  qualifies: boolean;
  gates_passed: string[] | null;
  gates_failed: string[] | null;
  why_now_indicators: string[] | null;
  evidence_count: number | null;
  distinct_sources: number | null;
  prequal_reasons: {
    summary?: string;
    reasons?: string[];
    tags?: string[];
    confidence?: number;
  } | null;
  offer_fit_tags: string[] | null;
  created_at: string | null;
}

// ============================================================================
// Jobs / Queue
// ============================================================================
export interface Job {
  id: string;
  client_id: string;
  job_type: string;
  status: string;
  payload: Record<string, unknown>;
  assigned_worker: string | null;
  attempts: number;
  last_error: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

export interface QueueStatus {
  total_pending: number;
  total_running: number;
  total_failed_24h: number;
  total_completed_24h: number;
  by_type: Record<string, Record<string, number>>;
  oldest_pending_age_seconds: number;
  avg_completion_time_seconds: number;
  stuck_jobs: Job[];
}

export interface InferredWorker {
  assigned_worker: string;
  last_seen: string;
  jobs_running: number;
  jobs_completed_24h: number;
}

// ============================================================================
// Logs
// ============================================================================
export interface LogEntry {
  id: string;
  timestamp: string;
  level: string;
  service: string;
  module: string | null;
  run_id: string | null;
  company_id: string | null;
  client_id: string | null;
  job_id: string | null;
  message: string;
  data_json: Record<string, unknown> | null;
}

// ============================================================================
// Health
// ============================================================================
export interface HealthStatus {
  status: string;
  env: string;
  db: string;
  version: string;
}

export interface AzureFunctionHealth {
  reachable: boolean;
  status_code?: number;
  latency_ms: number | null;
  url: string | null;
  error?: string;
  checked_at: string;
}
