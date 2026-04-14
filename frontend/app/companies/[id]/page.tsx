'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { JsonViewer, CollapsibleSection } from '@/components/ui/json-viewer';
import { useCompanyDetail, useCompanyHypotheses, useCompanyNews, useCompanyFullPrequal, useRerunPrequal, useDeleteCompany, useCompanyAggregate, useAggregateCompany } from '@/lib/api/client';
import { RefreshCw, Trash2, Layers } from 'lucide-react';

const TABS = ['overview', 'hypotheses', 'news', 'aggregate', 'raw'] as const;
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
  const { data: aggregateData, isLoading: aggLoading } = useCompanyAggregate(id, tab === 'aggregate' || tab === 'overview');
  const triggerAggregate = useAggregateCompany(id);

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
          <button onClick={() => triggerAggregate.mutate()} disabled={triggerAggregate.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-purple-300 text-purple-600 hover:bg-purple-50 dark:border-purple-800 dark:hover:bg-purple-900/20 disabled:opacity-50">
            <Layers className={`w-3.5 h-3.5 ${triggerAggregate.isPending ? 'animate-spin' : ''}`} /> Aggregate
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

      {tab === 'overview' && <OverviewTab prequal={latest_prequal} offerFit={offer_fit} snapshot={snapshot} aggregate={aggregateData?.data} />}
      {tab === 'hypotheses' && <HypothesesTab data={hypothesesData?.data} loading={hypoLoading} />}
      {tab === 'news' && <NewsTab data={newsData?.data} loading={newsLoading} />}
      {tab === 'aggregate' && <AggregateTab data={aggregateData?.data} loading={aggLoading} />}
      {tab === 'raw' && <RawTab data={fullPrequalData?.data} loading={rawLoading} />}
    </PageShell>
  );
}

// ============================================================================
// Overview — prequal summary + offer fit + snapshot
// ============================================================================
function OverviewTab({ prequal, offerFit, snapshot, aggregate }: { prequal: any; offerFit: any; snapshot: any; aggregate?: any }) {
  const TIER_COLORS: Record<string, string> = {
    A: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
    B: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
    C: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
    D: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
  };

  return (
    <div className="space-y-4">
      {/* Aggregate final result banner (if available) */}
      {aggregate && (
        <div className={`p-4 rounded-lg border ${aggregate.qualifies ? 'bg-emerald-50 dark:bg-emerald-900/10 border-emerald-200 dark:border-emerald-800' : 'bg-red-50 dark:bg-red-900/10 border-red-200 dark:border-red-800'}`}>
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium">Aggregated Result (Final)</h3>
            <div className="flex items-center gap-2">
              <span className={`px-2 py-0.5 text-xs font-bold rounded ${TIER_COLORS[aggregate.tier] ?? TIER_COLORS.D}`}>
                Tier {aggregate.tier}
              </span>
              <span className="text-sm font-mono font-bold">{aggregate.final_score?.toFixed(3)}</span>
              {aggregate.do_not_outreach && <span className="px-2 py-0.5 text-xs bg-red-600 text-white rounded">DO NOT OUTREACH</span>}
              {aggregate.needs_review && <span className="px-2 py-0.5 text-xs bg-amber-200 text-amber-800 rounded">NEEDS REVIEW</span>}
            </div>
          </div>
          {aggregate.decision_notes?.length > 0 && (
            <div className="space-y-1">
              {aggregate.decision_notes.map((n: string, i: number) => (
                <p key={i} className="text-xs text-muted-foreground">• {n}</p>
              ))}
            </div>
          )}
          {aggregate.recommended_personas?.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1">
              <span className="text-xs text-muted-foreground mr-1">Target:</span>
              {aggregate.recommended_personas.map((p: string, i: number) => (
                <span key={i} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{p}</span>
              ))}
            </div>
          )}
        </div>
      )}
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
// Aggregate — final unified result from aggregate_results table
// ============================================================================
function AggregateTab({ data, loading }: { data: any; loading: boolean }) {
  if (loading) return <p className="text-sm text-muted-foreground">Loading aggregate data...</p>;
  if (!data) return <p className="text-sm text-muted-foreground">No aggregate result yet. Run the aggregator first (requires prequal + Apollo enrichment).</p>;

  const TIER_COLORS: Record<string, string> = {
    A: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
    B: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
    C: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
    D: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
  };

  return (
    <div className="space-y-4">
      {/* Score header */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <div className="p-3 bg-card border border-border rounded-lg text-center">
          <p className="text-2xl font-bold tabular-nums">{data.final_score?.toFixed(3)}</p>
          <p className="text-xs text-muted-foreground">Final Score</p>
        </div>
        <div className="p-3 bg-card border border-border rounded-lg text-center">
          <p className={`text-2xl font-bold px-3 py-1 rounded inline-block ${TIER_COLORS[data.tier] ?? ''}`}>Tier {data.tier}</p>
          <p className="text-xs text-muted-foreground">Tier</p>
        </div>
        <div className="p-3 bg-card border border-border rounded-lg text-center">
          <p className={`text-2xl font-bold ${data.qualifies ? 'text-emerald-600' : 'text-red-600'}`}>{data.qualifies ? 'YES' : 'NO'}</p>
          <p className="text-xs text-muted-foreground">Qualifies</p>
        </div>
        <div className="p-3 bg-card border border-border rounded-lg text-center">
          <p className={`text-2xl font-bold ${data.do_not_outreach ? 'text-red-600' : 'text-emerald-600'}`}>{data.do_not_outreach ? 'YES' : 'NO'}</p>
          <p className="text-xs text-muted-foreground">Do Not Outreach</p>
        </div>
        <div className="p-3 bg-card border border-border rounded-lg text-center">
          <p className="text-2xl font-bold tabular-nums">{data.llm_calls ?? '—'}</p>
          <p className="text-xs text-muted-foreground">LLM Calls</p>
        </div>
      </div>

      {/* Score breakdown */}
      {data.score_breakdown && Object.keys(data.score_breakdown).length > 0 && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">Score Breakdown</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {Object.entries(data.score_breakdown).map(([key, val]) => (
              <div key={key} className="p-2 bg-muted/30 rounded text-center">
                <p className="text-lg font-bold tabular-nums">{typeof val === 'number' ? (val as number).toFixed(2) : String(val)}</p>
                <p className="text-xs text-muted-foreground">{key.replace(/_/g, ' ')}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Unified hypotheses */}
      {data.unified_hypotheses?.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-3">Unified Hypotheses ({data.unified_hypotheses.length})</h3>
          <div className="space-y-3">
            {data.unified_hypotheses.map((h: any, i: number) => (
              <div key={i} className="p-3 bg-muted/30 rounded-lg space-y-2">
                <p className="text-sm leading-relaxed">{h.hypothesis || h.unified_hypothesis || h.title || JSON.stringify(h).slice(0, 300)}</p>
                <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
                  {h.confidence != null && <span>Confidence: {typeof h.confidence === 'number' ? h.confidence.toFixed(2) : h.confidence}</span>}
                  {h.strength && <span className="px-1.5 py-0.5 bg-secondary rounded">{h.strength}</span>}
                  {h.pain_category && <span className="px-1.5 py-0.5 bg-secondary rounded">{h.pain_category}</span>}
                  {h.why_now && <span className="px-1.5 py-0.5 bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 rounded">why now</span>}
                </div>
                {h.suggested_offer_fit && <p className="text-xs text-muted-foreground">Offer fit: {h.suggested_offer_fit}</p>}
                {h.recommended_personas?.length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {h.recommended_personas.map((p: string, j: number) => (
                      <span key={j} className="px-1.5 py-0.5 text-xs bg-primary/10 text-primary rounded">{p}</span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Tech context */}
      {data.tech_context && Object.keys(data.tech_context).length > 0 && (
        <CollapsibleSection title="Tech Context & Fit">
          <JsonViewer data={data.tech_context} />
        </CollapsibleSection>
      )}

      {/* Final offer fit */}
      {data.final_offer_fit && Object.keys(data.final_offer_fit).length > 0 && (
        <CollapsibleSection title="Final Offer Fit">
          <JsonViewer data={data.final_offer_fit} />
        </CollapsibleSection>
      )}

      {/* Contact suggestions */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {data.recommended_personas?.length > 0 && (
          <div className="bg-card border border-border rounded-lg p-4">
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Recommended Personas</h4>
            <div className="flex flex-wrap gap-1">{data.recommended_personas.map((p: string, i: number) => (
              <span key={i} className="px-1.5 py-0.5 text-xs bg-primary/10 text-primary rounded">{p}</span>
            ))}</div>
          </div>
        )}
        {data.title_keywords?.length > 0 && (
          <div className="bg-card border border-border rounded-lg p-4">
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Title Keywords</h4>
            <div className="flex flex-wrap gap-1">{data.title_keywords.map((t: string, i: number) => (
              <span key={i} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{t}</span>
            ))}</div>
          </div>
        )}
        {data.title_exact?.length > 0 && (
          <div className="bg-card border border-border rounded-lg p-4">
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Exact Titles to Search</h4>
            <div className="flex flex-wrap gap-1">{data.title_exact.map((t: string, i: number) => (
              <span key={i} className="px-1.5 py-0.5 text-xs bg-secondary text-secondary-foreground rounded">{t}</span>
            ))}</div>
          </div>
        )}
      </div>

      {/* Decision notes */}
      {data.decision_notes?.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">Decision Notes</h3>
          <div className="space-y-1">
            {data.decision_notes.map((n: string, i: number) => (
              <p key={i} className="text-xs text-muted-foreground">• {n}</p>
            ))}
          </div>
        </div>
      )}

      {/* Meta */}
      {data.duration_ms && (
        <p className="text-xs text-muted-foreground">Aggregation took {(data.duration_ms / 1000).toFixed(1)}s with {data.llm_calls} LLM calls</p>
      )}
    </div>
  );
}
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
