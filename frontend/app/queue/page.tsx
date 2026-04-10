'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { useQueueStatus, useQueueJobs, useWorkers } from '@/lib/api/client';
import { timeAgo, formatDuration } from '@/lib/utils/format';
import { ListTodo, Play, AlertTriangle, CheckCircle } from 'lucide-react';

export default function QueuePage() {
  const { data: queueData } = useQueueStatus();
  const { data: workersData } = useWorkers();
  const [jobStatus, setJobStatus] = useState('');
  const [jobType, setJobType] = useState('');
  const [page, setPage] = useState(1);
  const { data: jobsData } = useQueueJobs({ status: jobStatus || undefined, job_type: jobType || undefined, page, per_page: 30 });

  const queue = queueData?.data;
  const workers = workersData?.data ?? [];
  const jobs = jobsData?.data ?? [];

  return (
    <PageShell title="Queue & Workers">
      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Pending" value={queue?.total_pending ?? 0} icon={ListTodo} />
        <StatCard label="Running" value={queue?.total_running ?? 0} icon={Play} />
        <StatCard label="Failed (24h)" value={queue?.total_failed_24h ?? 0} icon={AlertTriangle}
          className={queue?.total_failed_24h ? 'border-red-200 dark:border-red-800' : ''} />
        <StatCard label="Completed (24h)" value={queue?.total_completed_24h ?? 0} icon={CheckCircle} />
      </div>

      {/* Workers */}
      <div className="bg-card border border-border rounded-lg mb-6">
        <div className="px-4 py-3 border-b border-border">
          <h2 className="text-sm font-medium">Workers (inferred from job activity)</h2>
        </div>
        {workers.length === 0 ? (
          <p className="px-4 py-6 text-sm text-muted-foreground text-center">No active workers detected</p>
        ) : (
          <table className="w-full text-sm">
            <thead><tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Worker ID</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Last seen</th>
              <th className="text-right px-4 py-2 font-medium text-muted-foreground">Running</th>
              <th className="text-right px-4 py-2 font-medium text-muted-foreground">Completed (24h)</th>
            </tr></thead>
            <tbody className="divide-y divide-border">
              {workers.map((w) => (
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

      {/* By-type breakdown */}
      {queue?.by_type && Object.keys(queue.by_type).length > 0 && (
        <div className="bg-card border border-border rounded-lg mb-6">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-medium">By type</h2>
          </div>
          <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-2">
            {Object.entries(queue.by_type).map(([type, counts]) => (
              <div key={type} className="flex items-center justify-between p-2 bg-muted/30 rounded text-sm">
                <span className="font-mono text-xs">{type}</span>
                <div className="flex gap-2 text-xs">
                  {Object.entries(counts as Record<string, number>).map(([s, c]) => (
                    <span key={s} className="tabular-nums">
                      <span className="text-muted-foreground">{s}:</span> {c}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Job list */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border flex items-center gap-3">
          <h2 className="text-sm font-medium">Jobs</h2>
          <select value={jobStatus} onChange={(e) => { setJobStatus(e.target.value); setPage(1); }}
            className="px-2 py-1 text-xs border border-input rounded bg-background">
            <option value="">All statuses</option>
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
            <option value="phase_b_enrich_apollo">phase_b_enrich_apollo</option>
            <option value="fetch_news">fetch_news</option>
          </select>
        </div>
        <table className="w-full text-sm">
          <thead><tr className="border-b border-border bg-muted/50">
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">ID</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Type</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Status</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Worker</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Error</th>
            <th className="text-left px-4 py-2 font-medium text-muted-foreground">Created</th>
          </tr></thead>
          <tbody className="divide-y divide-border">
            {jobs.map((j) => (
              <tr key={j.id} className="hover:bg-muted/30">
                <td className="px-4 py-2 font-mono text-xs">{j.id.slice(0, 10)}</td>
                <td className="px-4 py-2 font-mono text-xs">{j.job_type}</td>
                <td className="px-4 py-2"><StatusBadge status={j.status} /></td>
                <td className="px-4 py-2 text-xs text-muted-foreground">{j.assigned_worker ?? '—'}</td>
                <td className="px-4 py-2 text-xs text-red-600 truncate max-w-[200px]">{j.last_error ?? ''}</td>
                <td className="px-4 py-2 text-xs text-muted-foreground">{timeAgo(j.created_at)}</td>
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
