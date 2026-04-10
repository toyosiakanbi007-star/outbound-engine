'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { JsonViewer, CollapsibleSection } from '@/components/ui/json-viewer';
import { useCompanyDetail, useCompanyHypotheses, useCompanyNews, useCompanyFullPrequal, useRerunPrequal, useDeleteCompany } from '@/lib/api/client';
import { RefreshCw, Trash2 } from 'lucide-react';

const TABS = ['overview', 'hypotheses', 'news', 'raw'] as const;
type Tab = typeof TABS[number];

export default function CompanyDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const router = useRouter();
  const { data, isLoading, isError, error } = useCompanyDetail(id);
  const [tab, setTab] = useState<Tab>('overview');
  const rerunPrequal = useRerunPrequal(id);
  const deleteCompany = useDeleteCompany();

  // Lazy-loaded from dedicated table endpoints
  const { data: hypothesesData, isLoading: hypoLoading } = useCompanyHypotheses(id, tab === 'hypotheses');
  const { data: newsData, isLoading: newsLoading } = useCompanyNews(id, tab === 'news');
  const { data: fullPrequalData, isLoading: rawLoading } = useCompanyFullPrequal(id, tab === 'raw');

  const handleDelete = async () => {
    const name = data?.data?.company?.name ?? id;
    if (!confirm(`Delete "${name}" and all its prequal/hypothesis/news data? This cannot be undone.`)) return;
    await deleteCompany.mutateAsync(id);
    router.push('/companies');
  };

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading company...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load: {(error as any)?.message}</p></PageShell>;
  if (!data?.data) return <PageShell title="Not found"><p className="text-muted-foreground">Company not found</p></PageShell>;

  const { company, latest_candidate, latest_prequal, offer_fit, snapshot, run_history_count } = data.data;

  return (
    <PageShell
      title={company.name}
      description={company.domain ?? undefined}
      actions={
        <div className="flex items-center gap-2">
          <button onClick={() => rerunPrequal.mutate()} disabled={rerunPrequal.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${rerunPrequal.isPending ? 'animate-spin' : ''}`} /> Rerun prequal
          </button>
          <button onClick={handleDelete} disabled={deleteCompany.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-red-300 text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-900/20 disabled:opacity-50">
            <Trash2 className="w-3.5 h-3.5" /> Delete
          </button>
        </div>
      }
    >
      {/* Company header */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4 text-sm">
        <div><span className="text-muted-foreground">Industry:</span> {company.industry ?? '—'}</div>
        <div><span className="text-muted-foreground">Employees:</span> {company.employee_count ?? '—'}</div>
        <div><span className="text-muted-foreground">Location:</span> {[company.city, company.region, company.country].filter(Boolean).join(', ') || '—'}</div>
        <div><span className="text-muted-foreground">Source:</span> {company.source ?? '—'}</div>
      </div>
      <div className="flex items-center gap-4 mb-6">
        {latest_candidate && <StatusBadge status={latest_candidate.status} />}
        {latest_prequal && <ScoreBar score={latest_prequal.score} />}
        <span className="text-xs text-muted-foreground">{run_history_count} run(s)</span>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-border mb-4">
        {TABS.map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-2 text-sm font-medium border-b-2 transition-colors capitalize ${tab === t ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}>
            {t === 'raw' ? 'Raw JSON' : t}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab prequal={latest_prequal} offerFit={offer_fit} snapshot={snapshot} />}
      {tab === 'hypotheses' && <HypothesesTab data={hypothesesData?.data} loading={hypoLoading} />}
      {tab === 'news' && <NewsTab data={newsData?.data} loading={newsLoading} />}
      {tab === 'raw' && <RawTab data={fullPrequalData?.data} loading={rawLoading} />}
    </PageShell>
  );
}

// ============================================================================
// Overview — prequal summary + offer fit + snapshot
// ============================================================================
function OverviewTab({ prequal, offerFit, snapshot }: { prequal: any; offerFit: any; snapshot: any }) {
  return (
    <div className="space-y-4">
      {/* Prequal + Decision row */}
      {prequal ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="bg-card border border-border rounded-lg p-4 space-y-3">
            <h3 className="text-sm font-medium">Prequal result</h3>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <div><span className="text-muted-foreground">Score:</span> <span className="font-mono">{prequal.score?.toFixed(2)}</span></div>
              <div><span className="text-muted-foreground">Qualifies:</span> {prequal.qualifies ? <span className="text-emerald-600">Yes</span> : <span className="text-red-600">No</span>}</div>
              <div><span className="text-muted-foreground">Evidence:</span> {prequal.evidence_count ?? '—'}</div>
              <div><span className="text-muted-foreground">Sources:</span> {prequal.distinct_sources ?? '—'}</div>
            </div>
            <TagList label="Gates passed" tags={prequal.gates_passed} color="emerald" />
            <TagList label="Gates failed" tags={prequal.gates_failed} color="red" />
            <TagList label="Why now" tags={prequal.why_now_indicators} color="blue" />
          </div>
          <div className="bg-card border border-border rounded-lg p-4 space-y-3">
            <h3 className="text-sm font-medium">Decision</h3>
            {prequal.prequal_reasons?.summary && <p className="text-sm">{prequal.prequal_reasons.summary}</p>}
            {prequal.prequal_reasons?.reasons?.map((r: string, i: number) => (
              <p key={i} className="text-xs text-muted-foreground">• {r}</p>
            ))}
            {prequal.prequal_reasons?.gates_failed_explained?.map((g: string, i: number) => (
              <p key={i} className="text-xs text-red-600 dark:text-red-400 mt-1">{g}</p>
            ))}
            <TagList label="Offer fit" tags={prequal.offer_fit_tags} color="secondary" />
          </div>
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">No prequal results yet</p>
      )}

      {/* Offer fit detail */}
      {offerFit && (
        <div className="bg-card border border-border rounded-lg p-4 space-y-3">
          <h3 className="text-sm font-medium">Offer fit — {offerFit.icp_fit} ({offerFit.fit_strength})</h3>
          {offerFit.reasons?.map((r: string, i: number) => (
            <p key={i} className="text-xs text-muted-foreground">• {r}</p>
          ))}
          {offerFit.recommended_angles?.length > 0 && (
            <CollapsibleSection title={`Recommended angles (${offerFit.recommended_angles.length})`}>
              <ul className="space-y-1 mt-2">{offerFit.recommended_angles.map((a: string, i: number) => (
                <li key={i} className="text-xs text-muted-foreground">• {a}</li>
              ))}</ul>
            </CollapsibleSection>
          )}
          {offerFit.missing_info?.length > 0 && (
            <CollapsibleSection title="Missing info">
              <ul className="space-y-1 mt-2">{offerFit.missing_info.map((m: string, i: number) => (
                <li key={i} className="text-xs text-muted-foreground">• {m}</li>
              ))}</ul>
            </CollapsibleSection>
          )}
        </div>
      )}

      {/* Snapshot */}
      {snapshot && (
        <div className="bg-card border border-border rounded-lg p-4 space-y-2">
          <h3 className="text-sm font-medium">Company snapshot</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
            <div><span className="text-muted-foreground">Business model:</span> {snapshot.business_model_guess}</div>
            <div><span className="text-muted-foreground">Sells to:</span> {snapshot.who_they_sell_to}</div>
            <div><span className="text-muted-foreground">GTM:</span> {Array.isArray(snapshot.gtm_motion) ? snapshot.gtm_motion.join(', ') : snapshot.gtm_motion}</div>
            <div><span className="text-muted-foreground">Industry:</span> {Array.isArray(snapshot.industry_guess) ? snapshot.industry_guess.join(', ') : snapshot.industry_guess}</div>
          </div>
          {snapshot.what_they_sell && <p className="text-xs text-muted-foreground mt-1">{snapshot.what_they_sell}</p>}
          <TagList label="Keywords" tags={snapshot.keywords} color="secondary" />
        </div>
      )}
    </div>
  );
}

// ============================================================================
// Hypotheses — from v3_hypotheses table
// ============================================================================
function HypothesesTab({ data, loading }: { data: any; loading: boolean }) {
  if (loading) return <p className="text-sm text-muted-foreground">Loading hypotheses...</p>;
  const hypotheses = Array.isArray(data) ? data : [];
  if (hypotheses.length === 0) return <p className="text-sm text-muted-foreground">No pain hypotheses</p>;

  return (
    <div className="space-y-4">
      {hypotheses.map((h: any, idx: number) => (
        <div key={idx} className="bg-card border border-border rounded-lg overflow-hidden">
          <div className="px-4 py-3 border-b border-border bg-muted/30 flex items-center gap-2 flex-wrap">
            <StrengthBadge strength={h.strength} />
            <span className="px-2 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{h.pain_category}</span>
            <span className="text-xs text-muted-foreground">conf: {(h.confidence ?? h.final_confidence)?.toFixed?.(2) ?? '—'}</span>
            {h.do_not_outreach && <span className="px-2 py-0.5 text-xs bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 rounded">do not outreach</span>}
            {h.corroborated && <span className="px-2 py-0.5 text-xs bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300 rounded">corroborated</span>}
          </div>
          <div className="p-4 space-y-3">
            <p className="text-sm leading-relaxed">{h.hypothesis}</p>
            {h.why_now && (
              <div className="p-3 bg-blue-50 dark:bg-blue-900/10 border border-blue-200 dark:border-blue-800 rounded-md">
                <p className="text-xs font-medium text-blue-800 dark:text-blue-300 mb-1">Why now · {h.why_now_urgency ?? ''}</p>
                <p className="text-xs text-blue-700 dark:text-blue-400">{h.why_now}</p>
              </div>
            )}
            {h.suggested_offer_fit && (
              <div><p className="text-xs font-medium text-muted-foreground mb-1">Suggested offer fit</p><p className="text-xs">{h.suggested_offer_fit}</p></div>
            )}
            {h.recommended_personas?.length > 0 && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1">Target personas</p>
                <div className="flex flex-wrap gap-1">{h.recommended_personas.map((p: string) => (
                  <span key={p} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{p}</span>
                ))}</div>
              </div>
            )}
            {h.evidence?.length > 0 && (
              <CollapsibleSection title={`Evidence (${h.evidence.length})`}>
                <div className="space-y-2 mt-2">{h.evidence.map((ev: any, i: number) => (
                  <div key={i} className="p-2 bg-muted/30 rounded text-xs space-y-1">
                    <div className="flex gap-2 text-muted-foreground">
                      <span>Tier {ev.source_tier}</span>
                      {ev.date && <span>{ev.date}</span>}
                    </div>
                    <p className="font-mono text-muted-foreground break-all">{ev.url}</p>
                    {ev.verbatim_quote && <p className="italic text-muted-foreground line-clamp-3">{ev.verbatim_quote}</p>}
                  </div>
                ))}</div>
              </CollapsibleSection>
            )}
            {h.corroborated_by?.length > 0 && (
              <p className="text-xs"><span className="text-emerald-600">Corroborated by:</span> {h.corroborated_by.join(', ')}</p>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

// ============================================================================
// News — from company_news table
// ============================================================================
function NewsTab({ data, loading }: { data: any; loading: boolean }) {
  if (loading) return <p className="text-sm text-muted-foreground">Loading news...</p>;
  const news = Array.isArray(data) ? data : [];
  if (news.length === 0) return <p className="text-sm text-muted-foreground">No news items</p>;

  return (
    <div className="space-y-2">
      {news.map((n: any, idx: number) => (
        <div key={idx} className="bg-card border border-border rounded-lg p-3">
          <div className="flex items-start justify-between gap-4">
            <p className="text-sm font-medium flex-1">{n.title}</p>
            {n.confidence > 0.8 && <span className="text-xs px-1.5 py-0.5 bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300 rounded flex-shrink-0">{n.confidence.toFixed(1)}</span>}
          </div>
          {n.summary && <p className="text-xs text-muted-foreground mt-1">{n.summary}</p>}
          <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
            {n.source_name && <span>{n.source_name}</span>}
            {n.source_type && <span className="px-1.5 py-0.5 bg-secondary rounded">{n.source_type}</span>}
            {n.date && <span>{typeof n.date === 'string' ? n.date.slice(0, 10) : ''}</span>}
          </div>
          {n.tags?.length > 0 && (
            <div className="flex flex-wrap gap-1 mt-2">{n.tags.map((t: string) => (
              <span key={t} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{t}</span>
            ))}</div>
          )}
          {n.url && <a href={n.url} target="_blank" rel="noopener noreferrer" className="text-xs text-primary hover:underline mt-1 block truncate">{n.url}</a>}
        </div>
      ))}
    </div>
  );
}

// ============================================================================
// Raw JSON — full_result from company_prequal
// ============================================================================
function RawTab({ data, loading }: { data: any; loading: boolean }) {
  if (loading) return <p className="text-sm text-muted-foreground">Loading...</p>;
  if (!data) return <p className="text-sm text-muted-foreground">No prequal data</p>;
  return <JsonViewer data={data} />;
}

// ============================================================================
// Shared components
// ============================================================================
function TagList({ label, tags, color }: { label: string; tags?: string[]; color: string }) {
  if (!tags?.length) return null;
  const colorMap: Record<string, string> = {
    emerald: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
    red: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
    blue: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
    secondary: 'bg-secondary text-secondary-foreground',
  };
  const cls = colorMap[color] ?? colorMap.secondary;
  return (
    <div>
      <p className="text-xs text-muted-foreground mb-1">{label}</p>
      <div className="flex flex-wrap gap-1">{tags.map((t) => (
        <span key={t} className={`px-1.5 py-0.5 text-xs rounded ${cls}`}>{t}</span>
      ))}</div>
    </div>
  );
}

function StrengthBadge({ strength }: { strength?: string }) {
  const cls = strength === 'high' ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
    : strength === 'medium' ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
    : 'bg-secondary text-secondary-foreground';
  return <span className={`px-2 py-0.5 text-xs font-medium rounded ${cls}`}>{strength ?? '—'}</span>;
}
