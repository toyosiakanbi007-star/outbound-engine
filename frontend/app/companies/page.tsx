'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { ScoreBar } from '@/components/ui/score-bar';
import { useCompanies, useDeleteCompany, useClients, useFlushCompanies, useBulkPrequal, useBulkAggregate } from '@/lib/api/client';
import { formatNumber } from '@/lib/utils/format';
import { Search, Trash2, RotateCcw, ShieldCheck, Layers, X } from 'lucide-react';

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
  const [clientId, setClientId] = useState('');
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [actionMsg, setActionMsg] = useState('');
  const [showBulkPrequal, setShowBulkPrequal] = useState(false);

  const { data: clientsData } = useClients();
  const clients = clientsData?.data ?? [];
  const { data, isLoading } = useCompanies({
    search: search || undefined, status: status || undefined,
    tier: tier || undefined, client_id: clientId || undefined,
    page, per_page: 50,
  });
  const companies = data?.data ?? [];
  const deleteCompany = useDeleteCompany();
  const flushCompanies = useFlushCompanies();
  const bulkPrequal = useBulkPrequal();
  const bulkAggregate = useBulkAggregate();

  // Check if any selected companies already have prequal results
  const selectedHavePrequal = companies.filter(
    (c: any) => selected.has(c.id) && (c.latest_status === 'qualified' || c.latest_status === 'disqualified')
  ).length > 0;

  const handleBulkDelete = async () => {
    if (selected.size === 0) return;
    if (!confirm(`Delete ${selected.size} companies and all their data?`)) return;
    for (const id of selected) { await deleteCompany.mutateAsync(id); }
    setSelected(new Set());
  };

  const handleFlush = async () => {
    if (!clientId) { setActionMsg('Select a client first'); return; }
    const clientName = clients.find((c: any) => c.id === clientId)?.name ?? clientId;
    if (!confirm(`Delete ALL companies for "${clientName}", reset cursors, cancel jobs?`)) return;
    try {
      const r: any = await flushCompanies.mutateAsync({ clientId, reset_cursors: true, cancel_jobs: true });
      setActionMsg(`Flushed: ${r.data?.companies_deleted ?? 0} companies`);
      setSelected(new Set());
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const handleBulkAggregate = async () => {
    if (selected.size === 0) return;
    setActionMsg('');
    try {
      const r: any = await bulkAggregate.mutateAsync({ company_ids: Array.from(selected) });
      setActionMsg(`Aggregate: ${r.data?.jobs_created ?? 0} jobs enqueued`);
    } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
  };

  const toggleSelect = (id: string) => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id); else next.add(id);
    setSelected(next);
  };
  const toggleAll = () => {
    if (selected.size === companies.length) setSelected(new Set());
    else setSelected(new Set(companies.map((c: any) => c.id)));
  };

  return (
    <PageShell title="Companies">
      {/* Action messages */}
      {actionMsg && (
        <div className={`mb-4 px-4 py-2 rounded-md text-sm ${actionMsg.startsWith('Error') ? 'bg-red-50 text-red-700 dark:bg-red-900/20 dark:text-red-400' : 'bg-emerald-50 text-emerald-700 dark:bg-emerald-900/20 dark:text-emerald-400'}`}>
          {actionMsg}
          <button onClick={() => setActionMsg('')} className="ml-2 text-xs underline">dismiss</button>
        </div>
      )}

      {/* Bulk action bar */}
      {selected.size > 0 && (
        <div className="mb-4 p-3 bg-muted/50 border border-border rounded-lg flex items-center gap-2 flex-wrap">
          <span className="text-sm font-medium">{selected.size} selected</span>
          <div className="w-px h-5 bg-border" />
          <button onClick={() => setShowBulkPrequal(true)} disabled={bulkPrequal.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
            <ShieldCheck className="w-3.5 h-3.5" /> Prequal
          </button>
          <button onClick={handleBulkAggregate} disabled={bulkAggregate.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-purple-600 text-white rounded-md hover:bg-purple-700 disabled:opacity-50">
            <Layers className="w-3.5 h-3.5" /> Aggregate
          </button>
          <button onClick={handleBulkDelete} disabled={deleteCompany.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50">
            <Trash2 className="w-3.5 h-3.5" /> Delete
          </button>
          <button onClick={() => setSelected(new Set())} className="text-xs text-muted-foreground hover:text-foreground ml-auto">Clear selection</button>
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input type="text" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }}
            placeholder="Search name or domain..."
            className="w-full pl-9 pr-3 py-1.5 text-sm border border-input rounded-md bg-background" />
        </div>
        <select value={clientId} onChange={(e) => { setClientId(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All clients</option>
          {clients.map((c: any) => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All statuses</option>
          <option value="qualified">Qualified</option>
          <option value="disqualified">Disqualified</option>
          <option value="new">New</option>
          <option value="prequal_queued">Queued</option>
        </select>
        <select value={tier} onChange={(e) => { setTier(e.target.value); setPage(1); }}
          className="px-3 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All tiers</option>
          <option value="1">T1 (≥ 0.75)</option>
          <option value="2">T2 (0.50 – 0.74)</option>
          <option value="3">T3 (&lt; 0.50)</option>
        </select>
        {clientId && (
          <button onClick={handleFlush} disabled={flushCompanies.isPending}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-red-300 text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-900/20 disabled:opacity-50">
            <RotateCcw className="w-3.5 h-3.5" /> Flush + reset
          </button>
        )}
        {(search || status || tier || clientId) && (
          <button onClick={() => { setSearch(''); setStatus(''); setTier(''); setClientId(''); setPage(1); }}
            className="text-xs text-muted-foreground hover:text-foreground">Clear</button>
        )}
      </div>

      {/* Table */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="w-8 px-4 py-3"><input type="checkbox" checked={selected.size === companies.length && companies.length > 0} onChange={toggleAll} className="rounded border-input" /></th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Company</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Domain</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Industry</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Emp</th>
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
            ) : companies.map((c: any) => {
              const t = scoreTier(c.latest_score);
              return (
                <tr key={c.id} className={`hover:bg-muted/30 transition-colors ${selected.has(c.id) ? 'bg-primary/5' : ''}`}>
                  <td className="px-4 py-3"><input type="checkbox" checked={selected.has(c.id)} onChange={() => toggleSelect(c.id)} className="rounded border-input" /></td>
                  <td className="px-4 py-3"><Link href={`/companies/${c.id}`} className="font-medium text-primary hover:underline">{c.name}</Link></td>
                  <td className="px-4 py-3 text-xs font-mono text-muted-foreground">{c.domain ?? '—'}</td>
                  <td className="px-4 py-3 text-xs">{c.industry ?? '—'}</td>
                  <td className="px-4 py-3 text-right text-xs tabular-nums">{formatNumber(c.employee_count)}</td>
                  <td className="px-4 py-3">{c.latest_status ? <StatusBadge status={c.latest_status} /> : '—'}</td>
                  <td className="px-4 py-3"><ScoreBar score={c.latest_score} /></td>
                  <td className="px-4 py-3 text-center">{t !== '—' && <span className={`px-2 py-0.5 text-xs font-medium rounded ${TIER_COLORS[t] ?? ''}`}>{t}</span>}</td>
                  <td className="px-4 py-3">
                    <button onClick={() => { if (confirm(`Delete "${c.name}"?`)) deleteCompany.mutateAsync(c.id); }} title="Delete"
                      className="p-1 text-muted-foreground hover:text-red-600"><Trash2 className="w-3.5 h-3.5" /></button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between mt-4">
        <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1} className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Previous</button>
        <span className="text-xs text-muted-foreground">Page {page}</span>
        <button onClick={() => setPage(page + 1)} disabled={companies.length < 50} className="px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted disabled:opacity-50">Next</button>
      </div>

      {/* Bulk prequal config modal */}
      {showBulkPrequal && (
        <BulkPrequalModal
          selectedCount={selected.size}
          hasExisting={selectedHavePrequal}
          onClose={() => setShowBulkPrequal(false)}
          onSubmit={async (force, batchSize) => {
            setShowBulkPrequal(false);
            setActionMsg('');
            try {
              const r: any = await bulkPrequal.mutateAsync({
                company_ids: Array.from(selected), force, batch_size: batchSize,
              });
              setActionMsg(`Prequal: ${r.data?.candidates_queued ?? 0} queued in ${r.data?.jobs_created ?? 0} batches${force ? ' (force re-prequal)' : ''}`);
            } catch (e: any) { setActionMsg(`Error: ${e.message}`); }
          }}
          isPending={bulkPrequal.isPending}
        />
      )}
    </PageShell>
  );
}

// ============================================================================
// Bulk Prequal Config Modal
// ============================================================================
function BulkPrequalModal({ selectedCount, hasExisting, onClose, onSubmit, isPending }: {
  selectedCount: number; hasExisting: boolean;
  onClose: () => void; onSubmit: (force: boolean, batchSize: number) => void;
  isPending: boolean;
}) {
  const [force, setForce] = useState(false);
  const [batchSize, setBatchSize] = useState(10);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-sm mx-4">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">Bulk Prequal — {selectedCount} companies</h2>
          <button onClick={onClose} className="p-1 text-muted-foreground hover:text-foreground"><X className="w-4 h-4" /></button>
        </div>
        <div className="p-5 space-y-4">
          {hasExisting && (
            <div className="p-3 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-md">
              <label className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} className="rounded" />
                Force re-prequal for already processed companies
              </label>
              <p className="text-xs text-amber-700 dark:text-amber-400 mt-1">This will delete old results and re-run from scratch.</p>
            </div>
          )}
          <div className="flex items-center justify-between">
            <div>
              <label className="text-sm">Batch size</label>
              <p className="text-xs text-muted-foreground">Companies per prequal job</p>
            </div>
            <input type="number" value={batchSize} onChange={e => setBatchSize(Number(e.target.value))} min={1} max={50}
              className="w-20 px-2 py-1 text-sm text-right border border-input rounded bg-background" />
          </div>
          <p className="text-xs text-muted-foreground">
            This will create {Math.ceil(selectedCount / batchSize)} batch job(s), each processing {batchSize} companies.
          </p>
        </div>
        <div className="px-5 py-4 border-t border-border flex justify-end gap-2">
          <button onClick={onClose} className="px-3 py-1.5 text-sm rounded-md hover:bg-muted">Cancel</button>
          <button onClick={() => onSubmit(force, batchSize)} disabled={isPending}
            className="px-4 py-1.5 text-sm font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
            {isPending ? 'Queuing...' : `Run prequal (${selectedCount})`}
          </button>
        </div>
      </div>
    </div>
  );
}
