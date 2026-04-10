'use client';

import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { useClients, useQueueStatus, useAzureFunctionHealth, useDiscoveryRuns } from '@/lib/api/client';
import { timeAgo, formatDuration } from '@/lib/utils/format';
import {
  Users, Building2, ShieldCheck, Clock, AlertTriangle,
  Activity, Wifi, WifiOff,
} from 'lucide-react';
import Link from 'next/link';

export default function DashboardPage() {
  const { data: clientsData } = useClients();
  const { data: queueData } = useQueueStatus();
  const { data: azureHealth } = useAzureFunctionHealth();
  const { data: runsData } = useDiscoveryRuns({ per_page: 5 });

  const clients = clientsData?.data ?? [];
  const queue = queueData?.data;
  const azure = azureHealth;
  const runs = runsData?.data ?? [];

  const totalCompanies = clients.reduce((sum, c) => sum + (c.stats?.total_companies ?? 0), 0);
  const totalQualified = clients.reduce((sum, c) => sum + (c.stats?.qualified_companies ?? 0), 0);
  const totalPending = clients.reduce((sum, c) => sum + (c.stats?.pending_prequal ?? 0), 0);

  return (
    <PageShell title="Dashboard" description="System overview">
      {/* Top stats row */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-6">
        <StatCard label="Clients" value={clients.length} icon={Users} />
        <StatCard label="Companies" value={totalCompanies.toLocaleString()} icon={Building2} />
        <StatCard label="Qualified" value={totalQualified.toLocaleString()} icon={ShieldCheck} />
        <StatCard label="Pending prequal" value={totalPending.toLocaleString()} icon={Clock} />
        <StatCard
          label="Failed (24h)"
          value={queue?.total_failed_24h ?? 0}
          icon={AlertTriangle}
          className={queue?.total_failed_24h ? 'border-red-200 dark:border-red-900/50' : ''}
        />
        <AzureStatusCard azure={azure} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Recent runs */}
        <div className="lg:col-span-2 bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex items-center justify-between">
            <h2 className="text-sm font-medium">Recent discovery runs</h2>
            <Link href="/discovery-runs" className="text-xs text-primary hover:underline">View all</Link>
          </div>
          <div className="divide-y divide-border">
            {runs.length === 0 ? (
              <p className="px-4 py-8 text-sm text-muted-foreground text-center">No discovery runs yet</p>
            ) : (
              runs.map((run) => (
                <Link
                  key={run.id}
                  href={`/discovery-runs/${run.id}`}
                  className="flex items-center justify-between px-4 py-3 hover:bg-muted/50 transition-colors"
                >
                  <div className="flex items-center gap-3">
                    <StatusBadge status={run.status} />
                    <div>
                      <p className="text-sm font-medium font-mono">{run.id.slice(0, 8)}</p>
                      <p className="text-xs text-muted-foreground">
                        {run.unique_upserted} companies · {run.pages_fetched} pages
                      </p>
                    </div>
                  </div>
                  <span className="text-xs text-muted-foreground">{timeAgo(run.created_at)}</span>
                </Link>
              ))
            )}
          </div>
        </div>

        {/* Queue overview */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex items-center justify-between">
            <h2 className="text-sm font-medium">Queue</h2>
            <Link href="/queue" className="text-xs text-primary hover:underline">Details</Link>
          </div>
          <div className="p-4 space-y-3">
            {queue?.by_type && Object.entries(queue.by_type).length > 0 ? (
              Object.entries(queue.by_type).map(([type, counts]) => (
                <div key={type} className="flex items-center justify-between text-sm">
                  <span className="font-mono text-xs text-muted-foreground">{type}</span>
                  <div className="flex items-center gap-2">
                    {(counts as any).pending > 0 && (
                      <span className="text-xs text-amber-600">{(counts as any).pending} pending</span>
                    )}
                    {(counts as any).running > 0 && (
                      <span className="text-xs text-blue-600">{(counts as any).running} running</span>
                    )}
                    {(counts as any).failed > 0 && (
                      <span className="text-xs text-red-600">{(counts as any).failed} failed</span>
                    )}
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground text-center py-4">Queue empty</p>
            )}

            {queue && (
              <div className="pt-3 border-t border-border space-y-1">
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span>Oldest pending</span>
                  <span>{queue.oldest_pending_age_seconds}s</span>
                </div>
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span>Avg completion</span>
                  <span>{formatDuration(Math.round(queue.avg_completion_time_seconds))}</span>
                </div>
              </div>
            )}
          </div>

          {/* Stuck jobs warning */}
          {queue?.stuck_jobs && queue.stuck_jobs.length > 0 && (
            <div className="mx-4 mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-xs font-medium text-red-800 dark:text-red-300">
                {queue.stuck_jobs.length} stuck job(s)
              </p>
              {queue.stuck_jobs.slice(0, 3).map((j) => (
                <p key={j.id} className="text-xs text-red-600 dark:text-red-400 mt-1 font-mono">
                  {j.job_type} · {j.id.slice(0, 8)} · {timeAgo(j.updated_at)}
                </p>
              ))}
            </div>
          )}
        </div>
      </div>
    </PageShell>
  );
}

function AzureStatusCard({ azure }: { azure: any }) {
  if (!azure) {
    return <StatCard label="Azure Fn" value="..." icon={Activity} />;
  }
  return (
    <div className={`bg-card border rounded-lg p-4 ${azure.reachable ? 'border-border' : 'border-red-300 dark:border-red-800'}`}>
      <div className="flex items-center justify-between">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Azure Fn</p>
        {azure.reachable ? (
          <Wifi className="w-4 h-4 text-emerald-500" />
        ) : (
          <WifiOff className="w-4 h-4 text-red-500" />
        )}
      </div>
      <p className="mt-1 text-2xl font-semibold">
        {azure.reachable ? `${azure.latency_ms}ms` : 'Down'}
      </p>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {azure.reachable ? 'Reachable' : azure.error?.slice(0, 30) ?? 'Unreachable'}
      </p>
    </div>
  );
}
