'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { useClient, useRunPipeline, useCreateDiscoveryRun, useDispatchPrequal } from '@/lib/api/client';
import { timeAgo } from '@/lib/utils/format';
import { Play, Search, ShieldCheck, Settings, Building2, Target, Clock, Zap, BookOpen } from 'lucide-react';

export default function ClientDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = useClient(id);
  const runPipeline = useRunPipeline(id);
  const runDiscovery = useCreateDiscoveryRun(id);
  const runPrequal = useDispatchPrequal(id);
  const [actionMsg, setActionMsg] = useState('');

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading client...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load client: {(error as any)?.message ?? 'Unknown error'}</p><p className="text-xs text-muted-foreground mt-1">ID: {id}</p></PageShell>;
  if (!data) return <PageShell title="Client not found"><p className="text-muted-foreground">Not found</p></PageShell>;

  const { client, stats, recent_runs, top_companies } = data.data;

  const handleAction = async (action: 'pipeline' | 'discovery' | 'prequal') => {
    setActionMsg('');
    try {
      if (action === 'pipeline') {
        await runPipeline.mutateAsync({});
        setActionMsg('Full pipeline started');
      } else if (action === 'discovery') {
        await runDiscovery.mutateAsync({});
        setActionMsg('Discovery job enqueued');
      } else {
        await runPrequal.mutateAsync({ source: 'manual' });
        setActionMsg('Prequal dispatch enqueued');
      }
    } catch (e: any) {
      setActionMsg(`Error: ${e.message}`);
    }
  };

  return (
    <PageShell
      title={client.name}
      description={client.is_active ? 'Active' : 'Inactive'}
      actions={
        <div className="flex items-center gap-2">
          <Link href={`/clients/${id}/config`}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted transition-colors">
            <Settings className="w-3.5 h-3.5" /> Config
          </Link>
          <Link href={`/clients/${id}/icp`}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted transition-colors">
            <BookOpen className="w-3.5 h-3.5" /> ICP
          </Link>
        </div>
      }
    >
      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Companies" value={stats?.total_companies ?? 0} icon={Building2} />
        <StatCard label="Qualified" value={stats?.qualified_companies ?? 0} icon={Target} />
        <StatCard label="Pending" value={stats?.pending_prequal ?? 0} icon={Clock} />
        <StatCard
          label="Autopilot"
          value={stats?.autopilot_enabled ? 'ON' : 'OFF'}
          icon={Zap}
          className={stats?.autopilot_enabled ? 'border-emerald-200 dark:border-emerald-800' : ''}
        />
      </div>

      {/* Action buttons */}
      <div className="flex flex-wrap items-center gap-2 mb-6">
        <button
          onClick={() => handleAction('pipeline')}
          disabled={runPipeline.isPending}
          className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50"
        >
          <Play className="w-3.5 h-3.5" /> Run full pipeline
        </button>
        <button
          onClick={() => handleAction('discovery')}
          disabled={runDiscovery.isPending}
          className="flex items-center gap-1.5 px-3 py-2 text-sm rounded-md border border-border hover:bg-muted"
        >
          <Search className="w-3.5 h-3.5" /> Discovery only
        </button>
        <button
          onClick={() => handleAction('prequal')}
          disabled={runPrequal.isPending}
          className="flex items-center gap-1.5 px-3 py-2 text-sm rounded-md border border-border hover:bg-muted"
        >
          <ShieldCheck className="w-3.5 h-3.5" /> Prequal only
        </button>
        {actionMsg && (
          <span className={`text-xs ml-2 ${actionMsg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>
            {actionMsg}
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent runs */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-medium">Recent runs</h2>
          </div>
          <div className="divide-y divide-border">
            {(!recent_runs || recent_runs.length === 0) ? (
              <p className="px-4 py-6 text-sm text-muted-foreground text-center">No runs yet</p>
            ) : (
              recent_runs.map((run: any) => (
                <Link
                  key={run.id}
                  href={`/discovery-runs/${run.id}`}
                  className="flex items-center justify-between px-4 py-3 hover:bg-muted/50"
                >
                  <div className="flex items-center gap-2">
                    <StatusBadge status={run.status} />
                    <span className="text-xs font-mono">{run.id.slice(0, 8)}</span>
                    <span className="text-xs text-muted-foreground">{run.unique_upserted} co.</span>
                  </div>
                  <span className="text-xs text-muted-foreground">{timeAgo(run.created_at)}</span>
                </Link>
              ))
            )}
          </div>
        </div>

        {/* Top qualified companies */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-medium">Top qualified companies</h2>
          </div>
          <div className="divide-y divide-border">
            {(!top_companies || top_companies.length === 0) ? (
              <p className="px-4 py-6 text-sm text-muted-foreground text-center">No qualified companies yet</p>
            ) : (
              top_companies.slice(0, 8).map((c: any) => (
                <Link
                  key={c.id}
                  href={`/companies/${c.id}`}
                  className="flex items-center justify-between px-4 py-3 hover:bg-muted/50"
                >
                  <div>
                    <p className="text-sm font-medium">{c.name}</p>
                    <p className="text-xs text-muted-foreground">{c.domain} · {c.industry}</p>
                  </div>
                  <ScoreBar score={c.score} />
                </Link>
              ))
            )}
          </div>
        </div>
      </div>
    </PageShell>
  );
}
