'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { useDiscoveryRuns } from '@/lib/api/client';
import { timeAgo, formatDuration, formatNumber } from '@/lib/utils/format';

export default function DiscoveryRunsPage() {
  const [page, setPage] = useState(1);
  const { data, isLoading } = useDiscoveryRuns({ page, per_page: 30 });
  const runs = data?.data ?? [];
  const total = data?.meta?.total ?? 0;

  return (
    <PageShell title="Discovery runs" description={`${total} run(s)`}>
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Run ID</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Target</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Upserted</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Dupes</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Pages</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Credits</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Duration</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Started</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {isLoading ? (
              <tr><td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">Loading...</td></tr>
            ) : runs.length === 0 ? (
              <tr><td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">No runs yet</td></tr>
            ) : (
              runs.map((run) => {
                const elapsed = run.started_at && run.ended_at
                  ? Math.round((new Date(run.ended_at).getTime() - new Date(run.started_at).getTime()) / 1000)
                  : null;
                return (
                  <tr key={run.id} className="hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3">
                      <Link href={`/discovery-runs/${run.id}`} className="font-mono text-xs text-primary hover:underline">
                        {run.id.slice(0, 12)}
                      </Link>
                    </td>
                    <td className="px-4 py-3"><StatusBadge status={run.status} /></td>
                    <td className="px-4 py-3 text-right tabular-nums">{formatNumber(run.batch_target)}</td>
                    <td className="px-4 py-3 text-right tabular-nums">{formatNumber(run.unique_upserted)}</td>
                    <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{formatNumber(run.duplicates)}</td>
                    <td className="px-4 py-3 text-right tabular-nums">{formatNumber(run.pages_fetched)}</td>
                    <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{formatNumber(run.api_credits_used)}</td>
                    <td className="px-4 py-3 text-xs text-muted-foreground">{formatDuration(elapsed)}</td>
                    <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(run.started_at ?? run.created_at)}</td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {total > 30 && (
        <div className="flex items-center justify-between mt-4">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
            Previous
          </button>
          <span className="text-xs text-muted-foreground">Page {page}</span>
          <button onClick={() => setPage(page + 1)} disabled={runs.length < 30}
            className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">
            Next
          </button>
        </div>
      )}
    </PageShell>
  );
}
