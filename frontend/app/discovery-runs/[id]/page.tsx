'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatCard } from '@/components/ui/stat-card';
import { StatusBadge } from '@/components/ui/status-badge';
import { JsonViewer, CollapsibleSection } from '@/components/ui/json-viewer';
import { useDiscoveryRun, useDiscoveryRunCompanies } from '@/lib/api/client';
import { formatDate, formatDuration, formatNumber } from '@/lib/utils/format';
import { Database, FileText, Layers, Clock } from 'lucide-react';

export default function DiscoveryRunDetailPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = useDiscoveryRun(id);
  const [compPage, setCompPage] = useState(1);
  const { data: companiesData } = useDiscoveryRunCompanies(id, compPage);

  const run = data?.data;
  const candidates = companiesData?.data ?? [];

  if (isLoading) return <PageShell title="Loading..."><p className="text-muted-foreground">Loading run details...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load run: {(error as any)?.message ?? 'Unknown error'}</p><p className="text-xs text-muted-foreground mt-1">ID: {id}</p></PageShell>;
  if (!run) return <PageShell title="Not found"><p className="text-muted-foreground">Run not found</p></PageShell>;

  const elapsed = run.started_at && run.ended_at
    ? Math.round((new Date(run.ended_at).getTime() - new Date(run.started_at).getTime()) / 1000)
    : null;

  return (
    <PageShell title={`Discovery run ${run.id.slice(0, 12)}`} description={`Client: ${run.client_id.slice(0, 8)}`}>
      {/* Status + stats */}
      <div className="flex items-center gap-3 mb-4">
        <StatusBadge status={run.status} />
        <span className="text-xs text-muted-foreground">{formatDate(run.started_at ?? run.created_at)}</span>
        {elapsed != null && <span className="text-xs text-muted-foreground">({formatDuration(elapsed)})</span>}
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <StatCard label="Upserted" value={formatNumber(run.unique_upserted)} icon={Database} />
        <StatCard label="Duplicates" value={formatNumber(run.duplicates)} icon={Layers} />
        <StatCard label="Pages" value={formatNumber(run.pages_fetched)} icon={FileText} />
        <StatCard label="Credits" value={formatNumber(run.api_credits_used)} icon={Clock} />
      </div>

      {/* Industry summary */}
      {run.industry_summary && Object.keys(run.industry_summary).length > 0 && (
        <CollapsibleSection title="Industry summary" defaultOpen>
          <JsonViewer data={run.industry_summary} className="mt-2" />
        </CollapsibleSection>
      )}

      {/* Error */}
      {run.error && (
        <div className="mt-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
          <p className="text-sm font-medium text-red-800 dark:text-red-300 mb-1">Error</p>
          <JsonViewer data={run.error} />
        </div>
      )}

      {/* Candidates table */}
      <div className="mt-6 bg-card border border-border rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-border">
          <h2 className="text-sm font-medium">Companies discovered ({companiesData?.meta?.total ?? '...'})</h2>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Company</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Domain</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Industry</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Status</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Variant</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Dupe</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {candidates.map((c) => (
              <tr key={c.id} className="hover:bg-muted/30">
                <td className="px-4 py-2">
                  <Link href={`/companies/${c.company_id}`} className="text-primary hover:underline">{c.company_name}</Link>
                </td>
                <td className="px-4 py-2 text-xs text-muted-foreground font-mono">{c.company_domain ?? '—'}</td>
                <td className="px-4 py-2 text-xs">{c.industry ?? '—'}</td>
                <td className="px-4 py-2"><StatusBadge status={c.status} /></td>
                <td className="px-4 py-2 text-xs font-mono text-muted-foreground">{c.variant ?? '—'}</td>
                <td className="px-4 py-2 text-xs">{c.was_duplicate ? 'yes' : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {/* Pagination */}
        <div className="flex items-center justify-between px-4 py-3 border-t border-border">
          <button onClick={() => setCompPage(Math.max(1, compPage - 1))} disabled={compPage === 1}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Prev</button>
          <span className="text-xs text-muted-foreground">Page {compPage}</span>
          <button onClick={() => setCompPage(compPage + 1)} disabled={candidates.length < 50}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Next</button>
        </div>
      </div>

      {/* Raw JSON */}
      <CollapsibleSection title="Raw run JSON">
        <JsonViewer data={run} className="mt-2" />
      </CollapsibleSection>
    </PageShell>
  );
}
