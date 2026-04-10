'use client';

// params destructured directly (Next.js 14)
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { JsonViewer } from '@/components/ui/json-viewer';
import { usePrequalRun } from '@/lib/api/client';
import { formatDate, timeAgo, formatDuration } from '@/lib/utils/format';

export default function PrequalRunDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = usePrequalRun(id);
  const job = data?.data;

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading job details...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load job: {(error as any)?.message ?? 'Unknown error'}</p><p className="text-xs text-muted-foreground mt-1">ID: {id}</p></PageShell>;
  if (!job) return <PageShell title="Not found"><p className="text-muted-foreground">Job not found</p></PageShell>;

  const elapsed = job.completed_at && job.created_at
    ? Math.round((new Date(job.completed_at).getTime() - new Date(job.created_at).getTime()) / 1000)
    : null;

  return (
    <PageShell title={`Prequal job ${job.id.slice(0, 12)}`} description={job.job_type}>
      <div className="flex items-center gap-3 mb-6">
        <StatusBadge status={job.status} />
        <span className="text-xs text-muted-foreground">{formatDate(job.created_at)}</span>
        {elapsed != null && <span className="text-xs text-muted-foreground">({formatDuration(elapsed)})</span>}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <div className="bg-card border border-border rounded-lg p-4 space-y-2 text-sm">
          <h3 className="font-medium mb-2">Job info</h3>
          <div className="flex justify-between"><span className="text-muted-foreground">Type</span><span className="font-mono text-xs">{job.job_type}</span></div>
          <div className="flex justify-between"><span className="text-muted-foreground">Status</span><StatusBadge status={job.status} /></div>
          <div className="flex justify-between"><span className="text-muted-foreground">Worker</span><span className="font-mono text-xs">{job.assigned_worker ?? '—'}</span></div>
          <div className="flex justify-between"><span className="text-muted-foreground">Attempts</span><span>{job.attempts}</span></div>
          <div className="flex justify-between"><span className="text-muted-foreground">Created</span><span className="text-xs">{formatDate(job.created_at)}</span></div>
          {job.completed_at && <div className="flex justify-between"><span className="text-muted-foreground">Completed</span><span className="text-xs">{formatDate(job.completed_at)}</span></div>}
        </div>

        {job.last_error && (
          <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
            <h3 className="text-sm font-medium text-red-800 dark:text-red-300 mb-2">Error</h3>
            <p className="text-xs text-red-700 dark:text-red-400 font-mono whitespace-pre-wrap">{job.last_error}</p>
          </div>
        )}
      </div>

      <h3 className="text-sm font-medium mb-2">Payload</h3>
      <JsonViewer data={job.payload} />
    </PageShell>
  );
}
