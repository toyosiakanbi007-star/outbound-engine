'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { useClient, useRunPipeline, useCreateDiscoveryRun, useDispatchPrequal } from '@/lib/api/client';
import { timeAgo } from '@/lib/utils/format';
import { Play, Search, ShieldCheck, Settings, Building2, Target, Clock, Zap, BookOpen, FileEdit, X } from 'lucide-react';

export default function ClientDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = useClient(id);
  const runPipeline = useRunPipeline(id);
  const runDiscovery = useCreateDiscoveryRun(id);
  const runPrequal = useDispatchPrequal(id);
  const [actionMsg, setActionMsg] = useState('');
  const [showDiscoveryConfig, setShowDiscoveryConfig] = useState(false);

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading client...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load client: {(error as any)?.message ?? 'Unknown error'}</p></PageShell>;
  if (!data) return <PageShell title="Client not found"><p className="text-muted-foreground">Not found</p></PageShell>;

  const { client, stats, recent_runs, top_companies } = data.data;

  const handlePipeline = async () => {
    setShowDiscoveryConfig(true);
  };

  const handleDiscoveryWithConfig = async (config: any, pipeline: boolean) => {
    setActionMsg('');
    setShowDiscoveryConfig(false);
    try {
      if (pipeline) {
        config.force_full_pipeline = true;
      }
      await runDiscovery.mutateAsync(config);
      setActionMsg(pipeline ? 'Full pipeline started' : 'Discovery job enqueued');
    } catch (e: any) {
      setActionMsg(`Error: ${e.message}`);
    }
  };

  const handlePrequal = async () => {
    setActionMsg('');
    try {
      await runPrequal.mutateAsync({ source: 'manual' });
      setActionMsg('Prequal dispatch enqueued');
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
          <Link href={`/clients/${id}/manual-setup`}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted transition-colors">
            <FileEdit className="w-3.5 h-3.5" /> Manual Setup
          </Link>
        </div>
      }
    >
      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Companies" value={stats?.total_companies ?? 0} icon={Building2} />
        <StatCard label="Qualified" value={stats?.qualified_companies ?? 0} icon={Target} />
        <StatCard label="Pending" value={stats?.pending_prequal ?? 0} icon={Clock} />
        <StatCard label="Autopilot" value={stats?.autopilot_enabled ? 'ON' : 'OFF'} icon={Zap}
          className={stats?.autopilot_enabled ? 'border-emerald-200 dark:border-emerald-800' : ''} />
      </div>

      {/* Action buttons */}
      <div className="flex flex-wrap items-center gap-2 mb-6">
        <button onClick={handlePipeline}
          className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90">
          <Play className="w-3.5 h-3.5" /> Run pipeline
        </button>
        <button onClick={() => setShowDiscoveryConfig(true)}
          className="flex items-center gap-1.5 px-3 py-2 text-sm rounded-md border border-border hover:bg-muted">
          <Search className="w-3.5 h-3.5" /> Discovery
        </button>
        <button onClick={handlePrequal} disabled={runPrequal.isPending}
          className="flex items-center gap-1.5 px-3 py-2 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
          <ShieldCheck className="w-3.5 h-3.5" /> Prequal only
        </button>
        {actionMsg && (
          <span className={`text-xs ml-2 ${actionMsg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>{actionMsg}</span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent runs */}
        <div className="bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border"><h2 className="text-sm font-medium">Recent runs</h2></div>
          <div className="divide-y divide-border">
            {(!recent_runs || recent_runs.length === 0) ? (
              <p className="px-4 py-6 text-sm text-muted-foreground text-center">No runs yet</p>
            ) : (
              recent_runs.map((run: any) => (
                <Link key={run.id} href={`/discovery-runs/${run.id}`}
                  className="flex items-center justify-between px-4 py-3 hover:bg-muted/50">
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
          <div className="px-4 py-3 border-b border-border"><h2 className="text-sm font-medium">Top qualified</h2></div>
          <div className="divide-y divide-border">
            {(!top_companies || top_companies.length === 0) ? (
              <p className="px-4 py-6 text-sm text-muted-foreground text-center">No qualified companies</p>
            ) : (
              top_companies.slice(0, 8).map((c: any) => (
                <Link key={c.id} href={`/companies/${c.id}`}
                  className="flex items-center justify-between px-4 py-3 hover:bg-muted/50">
                  <div><p className="text-sm font-medium">{c.name}</p>
                    <p className="text-xs text-muted-foreground">{c.domain} · {c.industry}</p></div>
                  <ScoreBar score={c.score} />
                </Link>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Discovery config modal */}
      {showDiscoveryConfig && (
        <DiscoveryConfigModal
          onClose={() => setShowDiscoveryConfig(false)}
          onSubmit={handleDiscoveryWithConfig}
          isPending={runDiscovery.isPending}
        />
      )}
    </PageShell>
  );
}

// ============================================================================
// Discovery config modal with all DiscoverCompaniesPayload fields
// ============================================================================
function DiscoveryConfigModal({ onClose, onSubmit, isPending }: {
  onClose: () => void;
  onSubmit: (config: any, pipeline: boolean) => void;
  isPending: boolean;
}) {
  const [batchTarget, setBatchTarget] = useState(100);
  const [pageSize, setPageSize] = useState(100);
  const [explorationPct, setExplorationPct] = useState(0.20);
  const [minPerIndustry, setMinPerIndustry] = useState(10);
  const [maxPerIndustry, setMaxPerIndustry] = useState(50);
  const [maxPagesPerQuery, setMaxPagesPerQuery] = useState(10);
  const [maxRuntimeSecs, setMaxRuntimeSecs] = useState(1800);
  const [enqueuePrequal, setEnqueuePrequal] = useState(true);
  const [forceRescan, setForceRescan] = useState(false);

  const config = {
    batch_target: batchTarget,
    page_size: pageSize,
    exploration_pct: explorationPct,
    min_per_industry: minPerIndustry,
    max_per_industry: maxPerIndustry,
    max_pages_per_query: maxPagesPerQuery,
    max_runtime_seconds: maxRuntimeSecs,
    enqueue_prequal_jobs: enqueuePrequal,
    force_full_rescan: forceRescan,
  };

  // Estimate cost (Diffbot: 25 credits/entity, Apollo: 1 credit/page)
  const estimatedCredits = batchTarget * 25;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Discovery configuration</h2>
          <button onClick={onClose} className="p-1 text-muted-foreground hover:text-foreground"><X className="w-4 h-4" /></button>
        </div>

        <div className="p-5 space-y-4">
          {/* Warning for high batch target */}
          {batchTarget > 200 && (
            <div className="p-3 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-md">
              <p className="text-xs text-amber-800 dark:text-amber-300">
                Batch target of {batchTarget} may use ~{estimatedCredits.toLocaleString()} Diffbot credits.
                Use 50–200 for testing.
              </p>
            </div>
          )}

          <NumField label="Batch target (total companies)" value={batchTarget} onChange={setBatchTarget}
            min={10} max={5000} help="Total unique companies to discover. Start low (50-100) for testing." />

          <NumField label="Page size" value={pageSize} onChange={setPageSize}
            min={10} max={100} help="Results per API page. Max 100." />

          <NumField label="Min per industry" value={minPerIndustry} onChange={setMinPerIndustry}
            min={1} max={500} help="Minimum companies to fetch per industry." />

          <NumField label="Max per industry" value={maxPerIndustry} onChange={setMaxPerIndustry}
            min={10} max={2000} help="Cap per industry to prevent one industry dominating." />

          <NumField label="Max pages per query" value={maxPagesPerQuery} onChange={setMaxPagesPerQuery}
            min={1} max={100} help="Max API pages per query variant before trying next variant." />

          <NumField label="Max runtime (seconds)" value={maxRuntimeSecs} onChange={setMaxRuntimeSecs}
            min={60} max={7200} help="Hard wall-clock limit. Default 1800 (30 min)." />

          <div className="flex items-center justify-between">
            <div>
              <label className="text-sm">Exploration %</label>
              <p className="text-xs text-muted-foreground">Fraction of quota spread evenly vs yield-based</p>
            </div>
            <input type="number" step="0.05" min={0} max={1} value={explorationPct}
              onChange={(e) => setExplorationPct(parseFloat(e.target.value) || 0)}
              className="w-20 px-2 py-1 text-sm text-right border border-input rounded bg-background" />
          </div>

          <div className="flex items-center justify-between">
            <div>
              <label className="text-sm">Auto-enqueue prequal</label>
              <p className="text-xs text-muted-foreground">Run prequal on discovered companies</p>
            </div>
            <ToggleSwitch checked={enqueuePrequal} onChange={setEnqueuePrequal} />
          </div>

          <div className="flex items-center justify-between">
            <div>
              <label className="text-sm">Force full rescan</label>
              <p className="text-xs text-muted-foreground">Ignore cursors, re-fetch from page 1</p>
            </div>
            <ToggleSwitch checked={forceRescan} onChange={setForceRescan} />
          </div>
        </div>

        <div className="px-5 py-4 border-t border-border flex justify-between items-center">
          <p className="text-xs text-muted-foreground">
            Est. ~{estimatedCredits.toLocaleString()} Diffbot credits
          </p>
          <div className="flex gap-2">
            <button onClick={onClose} className="px-3 py-1.5 text-sm rounded-md hover:bg-muted">Cancel</button>
            <button onClick={() => onSubmit(config, false)} disabled={isPending}
              className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
              Discovery only
            </button>
            <button onClick={() => onSubmit(config, true)} disabled={isPending}
              className="px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50">
              Full pipeline
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function NumField({ label, value, onChange, min, max, help }: {
  label: string; value: number; onChange: (v: number) => void; min: number; max: number; help?: string;
}) {
  return (
    <div className="flex items-center justify-between">
      <div>
        <label className="text-sm">{label}</label>
        {help && <p className="text-xs text-muted-foreground">{help}</p>}
      </div>
      <input type="number" value={value} onChange={(e) => onChange(Number(e.target.value))}
        min={min} max={max}
        className="w-24 px-2 py-1 text-sm text-right border border-input rounded bg-background" />
    </div>
  );
}

function ToggleSwitch({ checked, onChange }: { checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <button onClick={() => onChange(!checked)}
      className={`relative w-10 h-5 rounded-full transition-colors ${checked ? 'bg-primary' : 'bg-muted'}`}>
      <div className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${checked ? 'left-5' : 'left-0.5'}`} />
    </button>
  );
}
