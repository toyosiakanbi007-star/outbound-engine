'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { useCompanies, useDeleteCompany } from '@/lib/api/client';
import { formatNumber } from '@/lib/utils/format';
import { Search, Trash2 } from 'lucide-react';

function scoreTier(score: number | null): string {
  if (score == null) return '—';
  if (score >= 0.75) return 'T1';
  if (score >= 0.50) return 'T2';
  return 'T3';
}

const TIER_COLORS: Record<string, string> = {
  T1: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
  T2: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
  T3: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
};

export default function CompaniesPage() {
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('');
  const [tier, setTier] = useState('');
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const router = useRouter();

  const { data, isLoading } = useCompanies({
    search: search || undefined,
    status: status || undefined,
    tier: tier || undefined,
    page,
    per_page: 50,
  });
  const companies = data?.data ?? [];
  const deleteCompany = useDeleteCompany();

  const handleDelete = async (id: string, name: string) => {
    if (!confirm(`Delete "${name}" and all its prequal data? This cannot be undone.`)) return;
    await deleteCompany.mutateAsync(id);
  };

  const handleBulkDelete = async () => {
    if (selected.size === 0) return;
    if (!confirm(`Delete ${selected.size} companies and all their data? This cannot be undone.`)) return;
    for (const id of selected) {
      await deleteCompany.mutateAsync(id);
    }
    setSelected(new Set());
  };

  const toggleSelect = (id: string) => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id); else next.add(id);
    setSelected(next);
  };

  const toggleAll = () => {
    if (selected.size === companies.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(companies.map(c => c.id)));
    }
  };

  return (
    <PageShell
      title="Companies"
      actions={selected.size > 0 ? (
        <button onClick={handleBulkDelete} disabled={deleteCompany.isPending}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50">
          <Trash2 className="w-3.5 h-3.5" /> Delete {selected.size} selected
        </button>
      ) : undefined}
    >
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input type="text" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }}
            placeholder="Search name or domain..."
            className="w-full pl-9 pr-3 py-1.5 text-sm border border-input rounded-md bg-background" />
        </div>
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All statuses</option>
          <option value="qualified">Qualified</option>
          <option value="disqualified">Disqualified</option>
          <option value="new">New</option>
          <option value="prequal_queued">Prequal queued</option>
        </select>
        <select value={tier} onChange={(e) => { setTier(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All tiers</option>
          <option value="1">Tier 1 (≥ 0.75)</option>
          <option value="2">Tier 2 (0.50 – 0.74)</option>
          <option value="3">Tier 3 (&lt; 0.50)</option>
        </select>
        {(search || status || tier) && (
          <button onClick={() => { setSearch(''); setStatus(''); setTier(''); setPage(1); }}
            className="text-xs text-muted-foreground hover:text-foreground">Clear filters</button>
        )}
      </div>

      {/* Table */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="w-8 px-4 py-3">
                <input type="checkbox" checked={selected.size === companies.length && companies.length > 0}
                  onChange={toggleAll} className="rounded border-input" />
              </th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Company</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Domain</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Industry</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Employees</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Score</th>
              <th className="text-center px-4 py-3 font-medium text-muted-foreground">Tier</th>
              <th className="w-10 px-4 py-3"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {isLoading ? (
              <tr><td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">Loading...</td></tr>
            ) : companies.length === 0 ? (
              <tr><td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">No companies found</td></tr>
            ) : (
              companies.map((c) => {
                const t = scoreTier(c.latest_score);
                return (
                  <tr key={c.id} className="hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3">
                      <input type="checkbox" checked={selected.has(c.id)}
                        onChange={() => toggleSelect(c.id)} className="rounded border-input" />
                    </td>
                    <td className="px-4 py-3">
                      <Link href={`/companies/${c.id}`} className="font-medium text-primary hover:underline">{c.name}</Link>
                    </td>
                    <td className="px-4 py-3 text-xs font-mono text-muted-foreground">{c.domain ?? '—'}</td>
                    <td className="px-4 py-3 text-xs">{c.industry ?? '—'}</td>
                    <td className="px-4 py-3 text-right text-xs tabular-nums">{formatNumber(c.employee_count)}</td>
                    <td className="px-4 py-3">{c.latest_status ? <StatusBadge status={c.latest_status} /> : '—'}</td>
                    <td className="px-4 py-3"><ScoreBar score={c.latest_score} /></td>
                    <td className="px-4 py-3 text-center">
                      {t !== '—' && <span className={`px-2 py-0.5 text-xs font-medium rounded ${TIER_COLORS[t] ?? ''}`}>{t}</span>}
                    </td>
                    <td className="px-4 py-3">
                      <button onClick={() => handleDelete(c.id, c.name)} title="Delete company"
                        className="p-1 text-muted-foreground hover:text-red-600 transition-colors">
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between mt-4">
        <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
          className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Previous</button>
        <span className="text-xs text-muted-foreground">Page {page}</span>
        <button onClick={() => setPage(page + 1)} disabled={companies.length < 50}
          className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Next</button>
      </div>
    </PageShell>
  );
}
