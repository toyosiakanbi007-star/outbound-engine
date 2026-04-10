// lib/api/client.ts — Base fetch wrapper + all TanStack Query hooks

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type {
  ApiResponse, Client, ClientConfig, IcpProfile,
  DiscoveryRun, Candidate, CompanyListItem, CompanyDetail,
  Job, QueueStatus, InferredWorker, LogEntry,
  HealthStatus, AzureFunctionHealth, PrequalSummary,
} from '@/lib/types';

const API_BASE = '/api';

class ApiError extends Error {
  constructor(public status: number, message: string, public code?: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: { message: res.statusText } }));
    throw new ApiError(res.status, body.error?.message || 'Unknown error', body.error?.code);
  }
  return res.json();
}

function toParams(obj: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(obj)) {
    if (v != null && v !== '') params.set(k, String(v));
  }
  return params.toString();
}

// ============================================================================
// Health
// ============================================================================
export function useHealth() {
  return useQuery({
    queryKey: ['health'],
    queryFn: () => apiFetch<HealthStatus>('/health'),
    refetchInterval: 30_000,
  });
}

export function useAzureFunctionHealth() {
  return useQuery({
    queryKey: ['health', 'azure-function'],
    queryFn: () => apiFetch<AzureFunctionHealth>('/health/azure-function'),
    refetchInterval: 15_000,
  });
}

// ============================================================================
// Clients
// ============================================================================
export function useClients(activeOnly = false) {
  return useQuery({
    queryKey: ['clients', { activeOnly }],
    queryFn: () => apiFetch<ApiResponse<Client[]>>(`/clients?active_only=${activeOnly}`),
  });
}

export function useClient(id: string) {
  return useQuery({
    queryKey: ['client', id],
    queryFn: () => apiFetch<{ data: { client: Client; stats: any; recent_runs: any[]; top_companies: any[] } }>(`/clients/${id}`),
    enabled: !!id,
  });
}

export function useCreateClient() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { name: string; config?: any; icp?: any; prequal_config?: any }) =>
      apiFetch<ApiResponse<Client>>('/clients', { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['clients'] }),
  });
}

export function useUpdateClient(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { name?: string; is_active?: boolean }) =>
      apiFetch(`/clients/${id}`, { method: 'PUT', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['clients'] });
      qc.invalidateQueries({ queryKey: ['client', id] });
    },
  });
}

export function useClientConfig(id: string) {
  return useQuery({
    queryKey: ['client', id, 'config'],
    queryFn: () => apiFetch<ApiResponse<ClientConfig>>(`/clients/${id}/config`),
    enabled: !!id,
  });
}

export function useUpdateClientConfig(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { config?: any; prequal_config?: any }) =>
      apiFetch(`/clients/${id}/config`, { method: 'PUT', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['client', id, 'config'] });
      qc.invalidateQueries({ queryKey: ['client', id] });
    },
  });
}

export function useClientIcp(id: string) {
  return useQuery({
    queryKey: ['client', id, 'icp'],
    queryFn: () => apiFetch<ApiResponse<IcpProfile>>(`/clients/${id}/icp`),
    enabled: !!id,
  });
}

export function useUpdateIcp(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { icp_json: any }) =>
      apiFetch(`/clients/${id}/icp`, { method: 'PUT', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['client', id, 'icp'] });
      qc.invalidateQueries({ queryKey: ['client', id] });
    },
  });
}

// ============================================================================
// Discovery
// ============================================================================
export function useDiscoveryRuns(filters: { client_id?: string; page?: number; per_page?: number } = {}) {
  return useQuery({
    queryKey: ['discovery-runs', filters],
    queryFn: () => apiFetch<ApiResponse<DiscoveryRun[]>>(`/discovery-runs?${toParams(filters as any)}`),
    refetchInterval: (query) => {
      const runs = query.state.data?.data;
      return runs?.some((r) => r.status === 'running') ? 5_000 : false;
    },
  });
}

export function useDiscoveryRun(id: string) {
  return useQuery({
    queryKey: ['discovery-run', id],
    queryFn: () => apiFetch<ApiResponse<DiscoveryRun>>(`/discovery-runs/${id}`),
    enabled: !!id,
    refetchInterval: (query) =>
      query.state.data?.data?.status === 'running' ? 5_000 : false,
  });
}

export function useDiscoveryRunCompanies(runId: string, page = 1) {
  return useQuery({
    queryKey: ['discovery-run', runId, 'companies', page],
    queryFn: () => apiFetch<ApiResponse<Candidate[]>>(`/discovery-runs/${runId}/companies?page=${page}&per_page=50`),
    enabled: !!runId,
  });
}

export function useCreateDiscoveryRun(clientId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { batch_target?: number; max_runtime_seconds?: number; enqueue_prequal_jobs?: boolean }) =>
      apiFetch(`/clients/${clientId}/discovery-runs`, { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['discovery-runs'] }),
  });
}

// ============================================================================
// Pipeline
// ============================================================================
export function useRunPipeline(clientId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { batch_target?: number; max_runtime_seconds?: number }) =>
      apiFetch(`/clients/${clientId}/pipeline/run`, { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['discovery-runs'] });
      qc.invalidateQueries({ queryKey: ['client', clientId] });
    },
  });
}

// ============================================================================
// Companies
// ============================================================================
export function useCompanies(filters: Record<string, unknown> = {}) {
  return useQuery({
    queryKey: ['companies', filters],
    queryFn: () => apiFetch<ApiResponse<CompanyListItem[]>>(`/companies?${toParams(filters)}`),
  });
}

export function useCompanyDetail(id: string) {
  return useQuery({
    queryKey: ['company', id],
    queryFn: () => apiFetch<ApiResponse<CompanyDetail>>(`/companies/${id}`),
    enabled: !!id,
  });
}

export function useCompanyEvidence(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['company', id, 'evidence'],
    queryFn: () => apiFetch<ApiResponse<any[]>>(`/companies/${id}/evidence`),
    enabled: enabled && !!id,
  });
}

export function useCompanyHypotheses(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['company', id, 'hypotheses'],
    queryFn: () => apiFetch<ApiResponse<any[]>>(`/companies/${id}/hypotheses`),
    enabled: enabled && !!id,
  });
}

export function useCompanyFullPrequal(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['company', id, 'full-prequal'],
    queryFn: () => apiFetch<ApiResponse<any>>(`/companies/${id}/latest-prequal`),
    enabled: enabled && !!id,
  });
}

export function useCompanyNews(id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['company', id, 'news'],
    queryFn: () => apiFetch<ApiResponse<any[]>>(`/companies/${id}/news`),
    enabled: enabled && !!id,
  });
}

export function useRerunPrequal(companyId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiFetch(`/companies/${companyId}/actions/rerun-prequal`, { method: 'POST' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['company', companyId] }),
  });
}

export function useDeleteCompany() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (companyId: string) =>
      apiFetch(`/companies/${companyId}`, { method: 'DELETE' }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['companies'] }),
  });
}

// ============================================================================
// Prequal
// ============================================================================
export function usePrequalRuns(filters: Record<string, unknown> = {}) {
  return useQuery({
    queryKey: ['prequal-runs', filters],
    queryFn: () => apiFetch<ApiResponse<Job[]>>(`/prequal-runs?${toParams(filters)}`),
    refetchInterval: (query) => {
      const jobs = query.state.data?.data;
      return jobs?.some((j) => j.status === 'pending' || j.status === 'running') ? 10_000 : false;
    },
  });
}

export function usePrequalRun(id: string) {
  return useQuery({
    queryKey: ['prequal-run', id],
    queryFn: () => apiFetch<ApiResponse<Job>>(`/prequal-runs/${id}`),
    enabled: !!id,
  });
}

export function useDispatchPrequal(clientId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { batch_size?: number; max_batches?: number; force?: boolean; source?: string }) =>
      apiFetch(`/clients/${clientId}/prequal-dispatch`, { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['prequal-runs'] }),
  });
}

// ============================================================================
// Queue
// ============================================================================
export function useQueueStatus() {
  return useQuery({
    queryKey: ['queue-status'],
    queryFn: () => apiFetch<ApiResponse<QueueStatus>>('/queue-status'),
    refetchInterval: 10_000,
  });
}

export function useQueueJobs(filters: Record<string, unknown> = {}) {
  return useQuery({
    queryKey: ['queue-jobs', filters],
    queryFn: () => apiFetch<ApiResponse<Job[]>>(`/queue-status/jobs?${toParams(filters)}`),
    refetchInterval: 5_000,
  });
}

export function useWorkers() {
  return useQuery({
    queryKey: ['workers'],
    queryFn: () => apiFetch<{ data: InferredWorker[]; meta: any }>('/workers'),
    refetchInterval: 15_000,
  });
}

// ============================================================================
// Logs
// ============================================================================
export function useLogs(filters: Record<string, unknown> = {}) {
  return useQuery({
    queryKey: ['logs', filters],
    queryFn: () => apiFetch<ApiResponse<LogEntry[]>>(`/logs?${toParams(filters)}`),
  });
}
