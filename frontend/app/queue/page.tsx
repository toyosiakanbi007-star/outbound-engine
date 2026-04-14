'use client';

import { useState } from 'react';
import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { useQueueStatus, useQueueJobs, useWorkers, useCancelJobs, useRetryJobs } from '@/lib/api/client';
import { timeAgo } from '@/lib/utils/format';
import { ListTodo, Play, AlertTriangle, CheckCircle, XCircle, RotateCcw } from 'lucide-react';

export default function QueuePage() {
  const { data: queueData } = useQueueStatus();
  const { data: workersData } = useWorkers();
  const cancelJobs = useCancelJobs();
  const retryJobs = useRetryJobs();
  const [jobStatus, setJobStatus] = useState('');
  const [jobType, setJobType] = useState('');
  const [page, setPage] = useState(1);
  const { data: jobsData } = useQueueJobs({ status: jobStatus || undefined, job_type: jobType || undefined, page, per_page: 30 });
  const [actionMsg, setActionMsg] = useState('');
  const [selectedJobs, setSelectedJobs] = useState<Set<string>>(new Set());

  const queue = queueData?.data;
  const workers = workersData?.data ?? [];
  const jobs = jobsData?.data ?? [];

  const handleCancelAll = async (type?: string) => {
    const label = type ? `all ${type} jobs` : 'all pending/running jobs';
    if (!confirm(`Cancel ${label}?`)) return;
    setActionMsg('');
    try {
      const result: any = await cancelJobs.mutateAsync({ job_type: type || undefined });
      setActionMsg(`Cancelled ${result.data?.jobs_cancelled ?? 0} jobs, reset ${result.data?.candidates_reset ?? 0} candidates`);
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleCancelOne = async (jobId: string) => {
    try { await cancelJobs.mutateAsync({ job_ids: [jobId] }); } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleRetryOne = async (jobId: string) => {
    setActionMsg('');
    try {
      const result: any = await retryJobs.mutateAsync({ job_ids: [jobId] });
      setActionMsg(`Requeued ${result.data?.jobs_retried ?? 0} job(s)`);
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleRetrySelected = async () => {
    if (selectedJobs.size === 0) return;
    setActionMsg('');
    try {
      const result: any = await retryJobs.mutateAsync({ job_ids: Array.from(selectedJobs) });
      setActionMsg(`Requeued ${result.data?.jobs_retried ?? 0} job(s)`);
      setSelectedJobs(new Set());
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleRetryAllType = async (type: string) => {
    if (!confirm(`Retry all failed ${type.replace(/_/g, ' ')} jobs?`)) return;
    setActionMsg('');
    try {
      const result: any = await retryJobs.mutateAsync({ job_type: type });
      setActionMsg(`Requeued ${result.data?.jobs_retried ?? 0} job(s), reset ${result.data?.candidates_reset ?? 0} candidates`);
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleRetryAllFailed = async () => {
    if (!confirm('Retry ALL failed jobs?')) return;
    setActionMsg('');
    try {
      const result: any = await retryJobs.mutateAsync({});
      setActionMsg(`Requeued ${result.data?.jobs_retried ?? 0} job(s), reset ${result.data?.candidates_reset ?? 0} candidates`);
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const toggleJobSelect = (id: string) => {
    const next = new Set(selectedJobs);
    if (next.has(id)) next.delete(id); else next.add(id);
    setSelectedJobs(next);
  };

  // Count failed by type for retry buttons
  const failedByType: Record<string, number> = {};
  if (queue?.by_type) {
    for (const [type, counts] of Object.entries(queue.by_type)) {
      const failed = (counts as any)?.failed ?? 0;
      if (failed > 0) failedByType[type] = failed;
    }
  }

  return (
    <PageShell
      title="Queue & Workers"
      actions={
        <div className="flex items-center gap-2 flex-wrap">
          {actionMsg && <span className={`text-xs ${actionMsg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>{actionMsg}</span>}
          {selectedJobs.size > 0 && (
            <button onClick={handleRetrySelected} disabled={retryJobs.isPending}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
              <RotateCcw className="w-3.5 h-3.5" /> Retry {selectedJobs.size} selected
            </button>
          )}
          {(queue?.total_failed_24h ?? 0) > 0 && (
            <button onClick={handleRetryAllFailed} disabled={retryJobs.isPending}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-blue-300 text-blue-600 hover:bg-blue-50 dark:border-blue-800 dark:hover:bg-blue-900/20 disabled:opacity-50">
              <RotateCcw className="w-3.5 h-3.5" /> Retry all failed
            </button>
          )}
          {(queue?.total_pending ?? 0) + (queue?.total_running ?? 0) > 0 && (
            <button onClick={() => handleCancelAll()} disabled={cancelJobs.isPending}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50">
              <XCircle className="w-3.5 h-3.5" /> Cancel all
            </button>
          )}
        </div>
      }
    >
      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Pending" value={queue?.total_pending ?? 0} icon={ListTodo} />
        <StatCard label="Running" value={queue?.total_running ?? 0} icon={Play} />
        <StatCard label="Failed (24h)" value={queue?.total_failed_24h ?? 0} icon={AlertTriangle}
          className={queue?.total_failed_24h ? 'border-red-200 dark:border-red-800' : ''} />
        <StatCard label="Completed (24h)" value={queue?.total_completed_24h ?? 0} icon={CheckCircle} />
      </div>

      {/* Quick action buttons by type */}
      {((queue?.total_pending ?? 0) + (queue?.total_running ?? 0) > 0 || Object.keys(failedByType).length > 0) && (
        <div className="flex flex-wrap gap-2 mb-6">
          {/* Cancel by type */}
          {['prequal_batch', 'prequal_dispatch', 'discover_companies', 'phase_b_enrich_apollo', 'fetch_news',
            'start_client_onboarding', 'onboarding_enrich_and_crawl', 'onboarding_generate_drafts'].map(t => {
            const counts = (queue?.by_type as any)?.[t];
            const active = (counts?.pending ?? 0) + (counts?.running ?? 0);
            if (active === 0) return null;
            return (
              <button key={`cancel-${t}`} onClick={() => handleCancelAll(t)} disabled={cancelJobs.isPending}
                className="px-2 py-1 text-xs border border-red-300 text-red-600 rounded hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-900/20 disabled:opacity-50">
                Cancel {t.replace(/_/g, ' ')} ({active})
              </button>
            );
          })}

          {/* Retry by type */}
          {Object.entries(failedByType).map(([t, count]) => (
            <button key={`retry-${t}`} onClick={() => handleRetryAllType(t)} disabled={retryJobs.isPending}
              className="px-2 py-1 text-xs border border-blue-300 text-blue-600 rounded hover:bg-blue-50 dark:border-blue-800 dark:hover:bg-blue-900/20 disabled:opacity-50">
              Retry {t.replace(/_/g, ' ')} ({count})
            </button>
          ))}
        </div>
      )}

      {/* Workers */}
      <div className="bg-card border border-border rounded-lg mb-6">
        <div className="px-4 py-3 border-b border-border"><h2 className="text-sm font-medium">Workers (inferred)</h2></div>
        {workers.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground text-center">No active workers</p>
        ) : (
          <table className="w-full text-sm">
            <thead><tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Worker</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Last seen</th>
              <th className="text-right px-4 py-2 font-medium text-muted-foreground">Running</th>
              <th className="text-right px-4 py-2 font-medium text-muted-foreground">Done (24h)</th>
            </tr></thead>
            <tbody className="divide-y divide-border">
              {workers.map((w: any) => (
                <tr key={w.assigned_worker}>
                  <td className="px-4 py-2 font-mono text-xs">{w.assigned_worker}</td>
                  <td className="px-4 py-2 text-xs text-muted-foreground">{timeAgo(w.last_seen)}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{w.jobs_running}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{w.jobs_completed_24h}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Stuck jobs */}
      {queue?.stuck_jobs?.length > 0 && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 mb-6">
          <h2 className="text-sm font-medium text-red-800 dark:text-red-300 mb-2">Stuck jobs ({queue.stuck_jobs.length})</h2>
          {queue.stuck_jobs.map((j: any) => (
            <div key={j.id} className="flex items-center justify-between py-1">
              <span className="text-xs font-mono text-red-700 dark:text-red-400">{j.job_type} · {j.id.slice(0, 10)} · {timeAgo(j.updated_at)}</span>
              <button onClick={() => handleCancelOne(j.id)} className="text-xs text-red-600 hover:text-red-800">Cancel</button>
            </div>
          ))}
        </div>
      )}

      {/* Job list */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center gap-3">
          <h2 className="text-sm font-medium">Jobs</h2>
          <select value={jobStatus} onChange={(e) => { setJobStatus(e.target.value); setPage(1); }}
            className="px-2 py-1 text-xs border border-input rounded bg-background">
            <option value="">All</option>
            <option value="pending">Pending</option>
            <option value="running">Running</option>
            <option value="done">Done</option>
            <option value="failed">Failed</option>
          </select>
          <select value={jobType} onChange={(e) => { setJobType(e.target.value); setPage(1); }}
            className="px-2 py-1 text-xs border border-input rounded bg-background">
            <option value="">All types</option>
            <option value="discover_companies">discover_companies</option>
            <option value="prequal_dispatch">prequal_dispatch</option>
            <option value="prequal_batch">prequal_batch</option>
            <option value="phase_b_enrich_apollo">phase_b_enrich</option>
            <option value="fetch_news">fetch_news</option>
            <option value="start_client_onboarding">onboarding_start</option>
            <option value="onboarding_enrich_and_crawl">onboarding_enrich</option>
            <option value="onboarding_generate_drafts">onboarding_generate</option>
            <option value="onboarding_finalize_draft">onboarding_finalize</option>
          </select>
        </div>
        <table className="w-full text-sm">
          <thead><tr className="border-b border-border bg-muted/50">
            <th className="w-8 px-4 py-2">
              <input type="checkbox"
                checked={selectedJobs.size > 0 && selectedJobs.size === jobs.filter((j: any) => j.status === 'failed').length}
                onChange={() => {
                  const failedIds = jobs.filter((j: any) => j.status === 'failed').map((j: any) => j.id);
                  if (selectedJobs.size === failedIds.length) setSelectedJobs(new Set());
                  else setSelectedJobs(new Set(failedIds));
                }} className="rounded border-input" />
            </th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">ID</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Type</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Status</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Worker</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Error</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Created</th>
            <th className="w-20 px-4 py-2"></th>
          </tr></thead>
          <tbody className="divide-y divide-border">
            {jobs.map((j: any) => (
              <tr key={j.id} className="hover:bg-muted/30">
                <td className="px-4 py-2">
                  {j.status === 'failed' && (
                    <input type="checkbox" checked={selectedJobs.has(j.id)}
                      onChange={() => toggleJobSelect(j.id)} className="rounded border-input" />
                  )}
                </td>
                <td className="px-4 py-2 font-mono text-xs">{j.id.slice(0, 10)}</td>
                <td className="px-4 py-2 font-mono text-xs">{j.job_type}</td>
                <td className="px-4 py-2"><StatusBadge status={j.status} /></td>
                <td className="px-4 py-2 text-xs text-muted-foreground">{j.assigned_worker ?? '—'}</td>
                <td className="px-4 py-2 text-xs text-red-600 truncate max-w-[200px]" title={j.last_error ?? ''}>{j.last_error ?? ''}</td>
                <td className="px-4 py-2 text-xs text-muted-foreground">{timeAgo(j.created_at)}</td>
                <td className="px-4 py-2 flex items-center gap-1">
                  {j.status === 'failed' && (
                    <button onClick={() => handleRetryOne(j.id)} title="Retry this job"
                      className="text-xs text-blue-600 hover:text-blue-800">Retry</button>
                  )}
                  {(j.status === 'pending' || j.status === 'running') && (
                    <button onClick={() => handleCancelOne(j.id)}
                      className="text-xs text-red-600 hover:text-red-800">Cancel</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="flex items-center justify-between px-4 py-3 border-t border-border">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Prev</button>
          <span className="text-xs text-muted-foreground">Page {page}</span>
          <button onClick={() => setPage(page + 1)} disabled={jobs.length < 30}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Next</button>
        </div>
      </div>
    </PageShell>
  );
}
