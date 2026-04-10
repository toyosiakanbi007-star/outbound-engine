'use client';

import { useState } from 'react';
import { PageShell } from '@/components/layout/page-shell';
import { JsonViewer } from '@/components/ui/json-viewer';
import { useLogs } from '@/lib/api/client';
import { formatDate } from '@/lib/utils/format';
import { cn } from '@/lib/utils/cn';
import { RefreshCw } from 'lucide-react';

const LEVEL_COLORS: Record<string, string> = {
  error: 'text-red-600 bg-red-50 dark:bg-red-900/20',
  warn: 'text-amber-600 bg-amber-50 dark:bg-amber-900/20',
  info: 'text-blue-600 bg-blue-50 dark:bg-blue-900/20',
  debug: 'text-muted-foreground bg-muted/50',
  trace: 'text-muted-foreground bg-muted/30',
};

export default function LogsPage() {
  const [level, setLevel] = useState('');
  const [search, setSearch] = useState('');
  const [runId, setRunId] = useState('');
  const [clientId, setClientId] = useState('');
  const [companyId, setCompanyId] = useState('');
  const [page, setPage] = useState(1);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const filters: Record<string, unknown> = { page, per_page: 100, order: 'desc' };
  if (level) filters.level = level;
  if (search) filters.search = search;
  if (runId) filters.run_id = runId;
  if (clientId) filters.client_id = clientId;
  if (companyId) filters.company_id = companyId;

  const { data, isLoading, refetch, isFetching } = useLogs(filters);
  const logs = data?.data ?? [];
  const total = data?.meta?.total ?? 0;

  return (
    <PageShell
      title="Logs"
      description={`${total} entries`}
      actions={
        <button onClick={() => refetch()}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted">
          <RefreshCw className={cn('w-3.5 h-3.5', isFetching && 'animate-spin')} /> Refresh
        </button>
      }
    >
      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <select value={level} onChange={(e) => { setLevel(e.target.value); setPage(1); }}
          className="px-2 py-1.5 text-sm border border-input rounded-md bg-background">
          <option value="">All levels</option>
          <option value="error">Error</option>
          <option value="warn">Warn</option>
          <option value="info">Info</option>
          <option value="debug">Debug</option>
        </select>
        <input type="text" value={search} onChange={(e) => { setSearch(e.target.value); setPage(1); }}
          placeholder="Search message..." className="px-3 py-1.5 text-sm border border-input rounded-md bg-background w-48" />
        <input type="text" value={runId} onChange={(e) => { setRunId(e.target.value); setPage(1); }}
          placeholder="Run ID" className="px-3 py-1.5 text-sm font-mono border border-input rounded-md bg-background w-36" />
        <input type="text" value={clientId} onChange={(e) => { setClientId(e.target.value); setPage(1); }}
          placeholder="Client ID" className="px-3 py-1.5 text-sm font-mono border border-input rounded-md bg-background w-36" />
        <input type="text" value={companyId} onChange={(e) => { setCompanyId(e.target.value); setPage(1); }}
          placeholder="Company ID" className="px-3 py-1.5 text-sm font-mono border border-input rounded-md bg-background w-36" />
      </div>

      {/* Log entries */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <div className="divide-y divide-border">
          {isLoading ? (
            <p className="px-4 py-8 text-sm text-muted-foreground text-center">Loading...</p>
          ) : logs.length === 0 ? (
            <p className="px-4 py-8 text-sm text-muted-foreground text-center">No log entries found</p>
          ) : (
            logs.map((log) => (
              <div key={log.id}>
                <button
                  onClick={() => setExpandedId(expandedId === log.id ? null : log.id)}
                  className="w-full text-left px-4 py-2 hover:bg-muted/30 transition-colors"
                >
                  <div className="flex items-start gap-2">
                    <span className={cn('inline-block px-1.5 py-0.5 text-xs font-mono font-medium rounded', LEVEL_COLORS[log.level] ?? LEVEL_COLORS.debug)}>
                      {log.level.toUpperCase().padEnd(5)}
                    </span>
                    <span className="text-xs text-muted-foreground font-mono flex-shrink-0 w-36">{formatDate(log.timestamp)}</span>
                    <span className="text-sm flex-1 truncate">{log.message}</span>
                    {log.module && <span className="text-xs text-muted-foreground font-mono flex-shrink-0 hidden lg:block">{log.module}</span>}
                  </div>
                  {/* Context IDs */}
                  {(log.run_id || log.client_id || log.company_id || log.job_id) && (
                    <div className="flex gap-3 mt-1 ml-[68px]">
                      {log.run_id && <span className="text-xs text-muted-foreground font-mono">run:{log.run_id.slice(0, 8)}</span>}
                      {log.client_id && <span className="text-xs text-muted-foreground font-mono">client:{log.client_id.slice(0, 8)}</span>}
                      {log.company_id && <span className="text-xs text-muted-foreground font-mono">co:{log.company_id.slice(0, 8)}</span>}
                      {log.job_id && <span className="text-xs text-muted-foreground font-mono">job:{log.job_id.slice(0, 8)}</span>}
                    </div>
                  )}
                </button>
                {/* Expanded detail */}
                {expandedId === log.id && (
                  <div className="px-4 pb-3 ml-[68px]">
                    <div className="space-y-1 text-xs">
                      <div><span className="text-muted-foreground">ID:</span> <span className="font-mono">{log.id}</span></div>
                      <div><span className="text-muted-foreground">Service:</span> {log.service}</div>
                      {log.module && <div><span className="text-muted-foreground">Module:</span> <span className="font-mono">{log.module}</span></div>}
                      {log.run_id && <div><span className="text-muted-foreground">Run ID:</span> <span className="font-mono">{log.run_id}</span></div>}
                      {log.client_id && <div><span className="text-muted-foreground">Client ID:</span> <span className="font-mono">{log.client_id}</span></div>}
                      {log.company_id && <div><span className="text-muted-foreground">Company ID:</span> <span className="font-mono">{log.company_id}</span></div>}
                      {log.job_id && <div><span className="text-muted-foreground">Job ID:</span> <span className="font-mono">{log.job_id}</span></div>}
                    </div>
                    {log.data_json && (
                      <div className="mt-2">
                        <JsonViewer data={log.data_json} />
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))
          )}
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between px-4 py-3 border-t border-border">
          <button onClick={() => setPage(Math.max(1, page - 1))} disabled={page === 1}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Prev</button>
          <span className="text-xs text-muted-foreground">Page {page} · {total} total</span>
          <button onClick={() => setPage(page + 1)} disabled={logs.length < 100}
            className="text-xs px-2 py-1 rounded border border-border hover:bg-muted disabled:opacity-50">Next</button>
        </div>
      </div>
    </PageShell>
  );
}
