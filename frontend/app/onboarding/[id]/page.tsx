'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { PageShell } from '@/components/layout/page-shell';
import { JsonViewer, CollapsibleSection } from '@/components/ui/json-viewer';
import { useOnboardingRun, useActivateOnboarding, useRegenerateOnboarding } from '@/lib/api/client';
import { timeAgo, formatDate } from '@/lib/utils/format';
import { CheckCircle, RefreshCw, Sparkles, Play, Globe, Building2, Users, Shield, AlertTriangle, Edit3 } from 'lucide-react';

const TABS = ['overview', 'config', 'icp', 'prequal', 'review', 'artifacts'] as const;
type Tab = typeof TABS[number];

export default function OnboardingRunDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = useOnboardingRun(id);
  const activate = useActivateOnboarding(id);
  const regenerate = useRegenerateOnboarding(id);
  const router = useRouter();
  const [tab, setTab] = useState<Tab>('overview');
  const [actionMsg, setActionMsg] = useState('');

  // Editable drafts
  const [editedConfig, setEditedConfig] = useState<string>('');
  const [editedIcp, setEditedIcp] = useState<string>('');
  const [editedPrequal, setEditedPrequal] = useState<string>('');
  const [runDiscovery, setRunDiscovery] = useState(true);
  const [discoveryBatchTarget, setDiscoveryBatchTarget] = useState(100);

  const run = data?.data?.run;
  const artifactCounts = data?.data?.artifact_counts ?? {};

  // Initialize editors when drafts load
  useEffect(() => {
    if (run?.draft_config && !editedConfig) {
      setEditedConfig(JSON.stringify(run.draft_config, null, 2));
    }
    if (run?.draft_icp && !editedIcp) {
      setEditedIcp(JSON.stringify(run.draft_icp, null, 2));
    }
    if (run?.draft_prequal_config && !editedPrequal) {
      setEditedPrequal(JSON.stringify(run.draft_prequal_config, null, 2));
    }
  }, [run]);

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading onboarding run...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">{(error as any)?.message}</p></PageShell>;
  if (!run) return <PageShell title="Not found"><p className="text-muted-foreground">Run not found</p></PageShell>;

  const isReady = run.status === 'review_ready';
  const isActive = run.status === 'activated';
  const isProcessing = ['pending', 'enriching', 'generating'].includes(run.status);
  const brand = run.brand_profile ?? {};

  const handleActivate = async () => {
    setActionMsg('');
    try {
      const body: any = {
        run_discovery: runDiscovery,
        discovery_batch_target: discoveryBatchTarget,
      };
      // Parse edited JSONs
      try { body.edited_config = JSON.parse(editedConfig); } catch {}
      try { body.edited_icp = JSON.parse(editedIcp); } catch {}
      try { body.edited_prequal_config = JSON.parse(editedPrequal); } catch {}

      await activate.mutateAsync(body);
      setActionMsg('Activated! Config and ICP saved.');
    } catch (e: any) {
      setActionMsg(`Error: ${e.message}`);
    }
  };

  const handleRegenerate = async () => {
    setActionMsg('');
    try {
      await regenerate.mutateAsync();
      setActionMsg('Regeneration started — drafts will update shortly');
    } catch (e: any) {
      setActionMsg(`Error: ${e.message}`);
    }
  };

  return (
    <PageShell
      title={run.input_name}
      description={`${run.input_domain} · ${run.status.replace('_', ' ')}`}
      actions={
        <div className="flex items-center gap-2">
          {actionMsg && <span className={`text-xs ${actionMsg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>{actionMsg}</span>}
          {isReady && (
            <>
              <button onClick={handleRegenerate} disabled={regenerate.isPending}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
                <RefreshCw className="w-3.5 h-3.5" /> Regenerate
              </button>
              <button onClick={handleActivate} disabled={activate.isPending}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-emerald-600 text-white rounded-md hover:bg-emerald-700 disabled:opacity-50">
                <CheckCircle className="w-3.5 h-3.5" /> Activate
              </button>
            </>
          )}
        </div>
      }
    >
      {/* Progress indicator for processing runs */}
      {isProcessing && (
        <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
          <div className="flex items-center gap-3">
            <div className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
            <div>
              <p className="text-sm font-medium text-blue-800 dark:text-blue-300">
                {run.status === 'pending' ? 'Starting onboarding...' :
                 run.status === 'enriching' ? 'Researching company — Diffbot lookup + website crawl...' :
                 'Generating config and ICP drafts with AI...'}
              </p>
              <p className="text-xs text-blue-600 dark:text-blue-400 mt-0.5">This typically takes 2-5 minutes. Page auto-refreshes.</p>
            </div>
          </div>
        </div>
      )}

      {/* Brand header (when available) */}
      {brand.company_name && (
        <div className="flex items-start gap-4 mb-6 p-4 bg-card border border-border rounded-lg">
          {brand.logo_url && (
            <img src={brand.logo_url} alt="" className="w-12 h-12 rounded-lg object-contain bg-white border border-border" />
          )}
          <div className="flex-1 min-w-0">
            <h2 className="text-lg font-semibold">{brand.company_name}</h2>
            <p className="text-sm text-muted-foreground mt-0.5">{brand.niche || brand.short_description || brand.description?.slice(0, 200)}</p>
            <div className="flex flex-wrap gap-3 mt-2 text-xs text-muted-foreground">
              {brand.location && <span className="flex items-center gap-1"><Globe className="w-3 h-3" />{brand.location}</span>}
              {brand.employee_count && <span className="flex items-center gap-1"><Users className="w-3 h-3" />~{brand.employee_count} employees</span>}
              {brand.industries?.length > 0 && <span className="flex items-center gap-1"><Building2 className="w-3 h-3" />{brand.industries.join(', ')}</span>}
            </div>
            {brand.social_links?.length > 0 && (
              <div className="flex gap-2 mt-2">
                {brand.social_links.map((s: any) => (
                  <a key={s.platform} href={s.url} target="_blank" rel="noopener noreferrer"
                    className="text-xs text-primary hover:underline">{s.platform}</a>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-border mb-4 overflow-x-auto">
        {TABS.map(t => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-2 text-sm font-medium border-b-2 transition-colors capitalize whitespace-nowrap ${
              tab === t ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'
            }`}>
            {t === 'icp' ? 'ICP' : t === 'prequal' ? 'Prequal Config' : t}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'overview' && <OverviewTab run={run} artifactCounts={artifactCounts} />}
      {tab === 'config' && <JsonEditorTab label="Client Config" json={editedConfig} onChange={setEditedConfig} original={run.draft_config} />}
      {tab === 'icp' && <JsonEditorTab label="ICP Profile" json={editedIcp} onChange={setEditedIcp} original={run.draft_icp} />}
      {tab === 'prequal' && <JsonEditorTab label="Prequal Config" json={editedPrequal} onChange={setEditedPrequal} original={run.draft_prequal_config} />}
      {tab === 'review' && <ReviewTab notes={run.review_notes} />}
      {tab === 'artifacts' && <ArtifactsTab counts={artifactCounts} runId={id} />}

      {/* Activation panel */}
      {isReady && (
        <div className="mt-6 p-4 bg-emerald-50 dark:bg-emerald-900/10 border border-emerald-200 dark:border-emerald-800 rounded-lg">
          <h3 className="text-sm font-medium text-emerald-800 dark:text-emerald-300 mb-3">Ready to activate</h3>
          <div className="flex items-center gap-4 mb-3">
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={runDiscovery} onChange={e => setRunDiscovery(e.target.checked)}
                className="rounded" />
              Run discovery after activation
            </label>
            {runDiscovery && (
              <label className="flex items-center gap-2 text-sm">
                Batch target:
                <input type="number" value={discoveryBatchTarget} onChange={e => setDiscoveryBatchTarget(Number(e.target.value))}
                  min={10} max={500} className="w-20 px-2 py-1 text-sm border border-input rounded bg-background" />
              </label>
            )}
          </div>
          <button onClick={handleActivate} disabled={activate.isPending}
            className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium bg-emerald-600 text-white rounded-md hover:bg-emerald-700 disabled:opacity-50">
            <CheckCircle className="w-4 h-4" />
            {activate.isPending ? 'Activating...' : 'Activate config & ICP'}
          </button>
        </div>
      )}

      {isActive && (
        <div className="mt-6 p-4 bg-primary/10 border border-primary/30 rounded-lg">
          <p className="text-sm font-medium">This onboarding has been activated.</p>
          <p className="text-xs text-muted-foreground mt-1">Config and ICP are live. Activated {run.activated_at ? formatDate(run.activated_at) : ''}.</p>
        </div>
      )}
    </PageShell>
  );
}

// ============================================================================
// Overview Tab
// ============================================================================
function OverviewTab({ run, artifactCounts }: { run: any; artifactCounts: Record<string, number> }) {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <InfoCard label="Status" value={run.status.replace('_', ' ')} />
        <InfoCard label="Domain" value={run.input_domain} />
        <InfoCard label="LLM Model" value={run.llm_model ?? 'pending'} />
        <InfoCard label="Pages Crawled" value={String(artifactCounts['site_page_summary'] ?? 0)} />
      </div>

      {run.operator_note && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-1">Operator notes</h3>
          <p className="text-sm text-muted-foreground">{run.operator_note}</p>
        </div>
      )}

      {run.error && (
        <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
          <h3 className="text-sm font-medium text-red-800 dark:text-red-300 flex items-center gap-1">
            <AlertTriangle className="w-4 h-4" /> Error
          </h3>
          <p className="text-xs text-red-600 dark:text-red-400 mt-1">{run.error}</p>
        </div>
      )}

      {/* Quick preview of drafts */}
      {run.draft_config && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">Draft Config Preview</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
            <div><span className="text-muted-foreground">Brand:</span> {run.draft_config.brand_name}</div>
            <div><span className="text-muted-foreground">Niche:</span> {run.draft_config.niche}</div>
            <div className="md:col-span-2"><span className="text-muted-foreground">Offer:</span> {run.draft_config.offer}</div>
            <div><span className="text-muted-foreground">Tone:</span> {run.draft_config.tone}</div>
          </div>
          {run.draft_config.outreach_angles?.length > 0 && (
            <div className="mt-2">
              <span className="text-xs text-muted-foreground">Outreach angles:</span>
              <div className="flex flex-wrap gap-1 mt-1">
                {run.draft_config.outreach_angles.map((a: string, i: number) => (
                  <span key={i} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{a}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {run.draft_icp && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">Draft ICP Preview</h3>
          <div className="space-y-2 text-sm">
            {run.draft_icp.target_roles?.length > 0 && (
              <div><span className="text-muted-foreground">Target roles:</span> {run.draft_icp.target_roles.join(', ')}</div>
            )}
            {run.draft_icp.target_profile?.industries?.length > 0 && (
              <div><span className="text-muted-foreground">Industries:</span> {run.draft_icp.target_profile.industries.map((i: any) => i.name).join(', ')}</div>
            )}
            {run.draft_icp.disqualify_if?.length > 0 && (
              <div>
                <span className="text-muted-foreground">Disqualifiers:</span>
                <div className="flex flex-wrap gap-1 mt-1">
                  {run.draft_icp.disqualify_if.map((d: string, i: number) => (
                    <span key={i} className="px-1.5 py-0.5 text-xs bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 rounded">{d}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// JSON Editor Tab — editable textarea + formatted preview
// ============================================================================
function JsonEditorTab({ label, json, onChange, original }: {
  label: string; json: string; onChange: (v: string) => void; original: any;
}) {
  const [mode, setMode] = useState<'form' | 'json'>('json');
  const [parseError, setParseError] = useState('');

  const handleChange = (val: string) => {
    onChange(val);
    try {
      JSON.parse(val);
      setParseError('');
    } catch (e: any) {
      setParseError(e.message);
    }
  };

  const handleReset = () => {
    if (original) {
      onChange(JSON.stringify(original, null, 2));
      setParseError('');
    }
  };

  if (!json && !original) {
    return <p className="text-sm text-muted-foreground">No draft generated yet — waiting for AI generation to complete.</p>;
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium">{label}</h3>
        <div className="flex items-center gap-2">
          <button onClick={handleReset} className="text-xs text-muted-foreground hover:text-foreground">Reset to original</button>
        </div>
      </div>

      {parseError && (
        <div className="p-2 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded text-xs text-red-600">
          JSON error: {parseError}
        </div>
      )}

      <textarea
        value={json}
        onChange={e => handleChange(e.target.value)}
        rows={30}
        spellCheck={false}
        className="w-full px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y"
      />
    </div>
  );
}

// ============================================================================
// Review Notes Tab
// ============================================================================
function ReviewTab({ notes }: { notes: any }) {
  if (!notes) return <p className="text-sm text-muted-foreground">No review notes yet.</p>;

  return (
    <div className="space-y-4">
      {/* Confidence scores */}
      {notes.confidence_by_section && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">Confidence by Section</h3>
          <div className="grid grid-cols-3 gap-3">
            {Object.entries(notes.confidence_by_section).map(([key, val]) => (
              <div key={key} className="text-center p-2 bg-muted/30 rounded">
                <p className="text-lg font-bold tabular-nums">{((val as number) * 100).toFixed(0)}%</p>
                <p className="text-xs text-muted-foreground capitalize">{key}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {notes.icp_rationale && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-1">ICP Rationale</h3>
          <p className="text-sm text-muted-foreground">{notes.icp_rationale}</p>
        </div>
      )}

      <NotesList title="Assumptions" items={notes.assumptions} color="blue" />
      <NotesList title="Missing Information" items={notes.missing_information} color="amber" />
      <NotesList title="Uncertain Sections" items={notes.uncertain_sections} color="amber" />
      <NotesList title="Recommended Review Points" items={notes.recommended_review_points} color="emerald" />
    </div>
  );
}

function NotesList({ title, items, color }: { title: string; items?: string[]; color: string }) {
  if (!items?.length) return null;
  const bg = color === 'blue' ? 'bg-blue-50 dark:bg-blue-900/10 border-blue-200 dark:border-blue-800'
    : color === 'amber' ? 'bg-amber-50 dark:bg-amber-900/10 border-amber-200 dark:border-amber-800'
    : 'bg-emerald-50 dark:bg-emerald-900/10 border-emerald-200 dark:border-emerald-800';

  return (
    <div className={`p-4 border rounded-lg ${bg}`}>
      <h3 className="text-sm font-medium mb-2">{title}</h3>
      <ul className="space-y-1">
        {items.map((item, i) => <li key={i} className="text-xs text-muted-foreground">• {item}</li>)}
      </ul>
    </div>
  );
}

// ============================================================================
// Artifacts Tab
// ============================================================================
function ArtifactsTab({ counts, runId }: { counts: Record<string, number>; runId: string }) {
  return (
    <div className="space-y-2">
      <p className="text-sm text-muted-foreground mb-3">Raw artifacts stored during the onboarding process.</p>
      {Object.entries(counts).map(([type, count]) => (
        <div key={type} className="flex items-center justify-between p-3 bg-card border border-border rounded-lg">
          <span className="text-sm font-mono">{type}</span>
          <span className="text-sm tabular-nums">{count} item{count !== 1 ? 's' : ''}</span>
        </div>
      ))}
      {Object.keys(counts).length === 0 && (
        <p className="text-sm text-muted-foreground text-center py-6">No artifacts yet — pipeline is still running.</p>
      )}
    </div>
  );
}

// ============================================================================
// Helpers
// ============================================================================
function InfoCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="p-3 bg-card border border-border rounded-lg">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="text-sm font-medium mt-0.5 truncate">{value}</p>
    </div>
  );
}
