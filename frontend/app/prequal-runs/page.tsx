'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { usePrequalRuns } from '@/lib/api/client';
import { timeAgo, formatDuration } from '@/lib/utils/format';

export default function PrequalRunsPage() {
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState('');
  const { data, isLoading } = usePrequalRuns({ page, per_page: 30, status: statusFilter || undefined });
  const runs = data?.data ?? [];
  const total = data?.meta?.total ?? 0;

  return (
    <PageShell title="Prequal runs" description={`${total} job(s)`}>
      <div className="flex items-center gap-3 mb-4">
        <select value={statusFilter} onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All statuses</option>
          <option value="pending">Pending</option>
          <option value="running">Running</option>
          <option value="done">Done</option>
          <option value="failed">Failed</option>
        </select>
      </div>

      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Job ID</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Type</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Worker</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Attempts</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Error</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Created</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">Loading...</td></tr>
            ) : runs.length === 0 ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">No prequal jobs</td></tr>
            ) : (
              runs.map((job) => (
                <tr key={job.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3">
                    <Link href={`/prequal-runs/${job.id}`} className="font-mono text-xs text-primary hover:underline">
                      {job.id.slice(0, 12)}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-xs font-mono">{job.job_type.replace('prequal_', '')}</td>
                  <td className="px-4 py-3"><StatusBadge status={job.status} /></td>
                  <td className="px-4 py-3 text-xs font-mono text-muted-foreground">{job.assigned_worker ?? '—'}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{job.attempts}</td>
                  <td className="px-4 py-3 text-xs text-red-600 truncate max-w-[200px]">{job.last_error ?? ''}</td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(job.created_at)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {total > 30 && (
        <div className="flex items-center justify-between mt-4">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Previous</button>
          <span className="text-xs text-muted-foreground">Page {page}</span>
          <button onClick={() => setPage(page + 1)} disabled={runs.length < 30}
            className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Next</button>
        </div>
      )}
    </PageShell>
  );
}
