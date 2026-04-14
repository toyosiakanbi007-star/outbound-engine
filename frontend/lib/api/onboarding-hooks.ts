// Add these to frontend/lib/api/client.ts
// (alongside your existing hooks)

// ============================================================================
// Onboarding
// ============================================================================

export function useOnboardingRuns(filters: { client_id?: string; status?: string } = {}) {
  return useQuery({
    queryKey: ['onboarding-runs', filters],
    queryFn: () => apiFetch<ApiResponse<any[]>>(`/onboarding-runs?${toParams(filters)}`),
    refetchInterval: 5_000,
  });
}

export function useOnboardingRun(id: string) {
  return useQuery({
    queryKey: ['onboarding-run', id],
    queryFn: () => apiFetch<{ data: { run: any; artifact_counts: Record<string, number> } }>(`/onboarding-runs/${id}`),
    refetchInterval: 3_000,
  });
}

export function useOnboardingArtifacts(id: string, type?: string) {
  return useQuery({
    queryKey: ['onboarding-artifacts', id, type],
    queryFn: () => apiFetch<ApiResponse<any[]>>(`/onboarding-runs/${id}/artifacts${type ? `?artifact_type=${type}` : ''}`),
    enabled: !!id,
  });
}

export function useStartOnboarding(clientId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { client_name: string; client_domain: string; operator_note?: string }) =>
      apiFetch(`/clients/${clientId}/onboarding-runs`, { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['onboarding-runs'] });
    },
  });
}

export function useActivateOnboarding(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: {
      edited_config?: any;
      edited_prequal_config?: any;
      edited_icp?: any;
      run_discovery?: boolean;
      discovery_batch_target?: number;
    }) => apiFetch(`/onboarding-runs/${runId}/activate`, { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['onboarding-run', runId] });
      qc.invalidateQueries({ queryKey: ['clients'] });
    },
  });
}

export function useRegenerateOnboarding(runId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => apiFetch(`/onboarding-runs/${runId}/regenerate`, { method: 'POST' }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['onboarding-run', runId] });
    },
  });
}
