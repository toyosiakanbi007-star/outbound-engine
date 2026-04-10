'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { useClients, useCreateClient } from '@/lib/api/client';
import { timeAgo, formatNumber } from '@/lib/utils/format';
import { Plus, X } from 'lucide-react';

export default function ClientsPage() {
  const { data, isLoading } = useClients();
  const [showCreate, setShowCreate] = useState(false);
  const clients = data?.data ?? [];

  return (
    <PageShell
      title="Clients"
      description={`${clients.length} client(s)`}
      actions={
        <button
          onClick={() => setShowCreate(true)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 transition-opacity"
        >
          <Plus className="w-4 h-4" /> New client
        </button>
      }
    >
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Name</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Companies</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Qualified</th>
              <th className="text-right px-4 py-3 font-medium text-muted-foreground">Pending</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Autopilot</th>
              <th className="text-left px-4 py-3 font-medium text-muted-foreground">Last run</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {isLoading ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">Loading...</td></tr>
            ) : clients.length === 0 ? (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">No clients yet</td></tr>
            ) : (
              clients.map((client) => (
                <tr key={client.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3">
                    <Link href={`/clients/${client.id}`} className="font-medium text-primary hover:underline">
                      {client.name}
                    </Link>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={client.is_active ? 'qualified' : 'disqualified'} />
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatNumber(client.stats?.total_companies)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatNumber(client.stats?.qualified_companies)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatNumber(client.stats?.pending_prequal)}</td>
                  <td className="px-4 py-3">
                    <span className={`text-xs ${client.stats?.autopilot_enabled ? 'text-emerald-600' : 'text-muted-foreground'}`}>
                      {client.stats?.autopilot_enabled ? 'ON' : 'OFF'}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(client.stats?.last_discovery_run)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {showCreate && <CreateClientModal onClose={() => setShowCreate(false)} />}
    </PageShell>
  );
}

function CreateClientModal({ onClose }: { onClose: () => void }) {
  const [name, setName] = useState('');
  const createClient = useCreateClient();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    await createClient.mutateAsync({ name: name.trim() });
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-md mx-4">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <h2 className="text-sm font-semibold">New client</h2>
          <button onClick={onClose} className="p-1 text-muted-foreground hover:text-foreground"><X className="w-4 h-4" /></button>
        </div>
        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1">Client name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Acme Corp"
              autoFocus
              className="w-full px-3 py-2 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          <div className="flex justify-end gap-2">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-sm rounded-md hover:bg-muted transition-colors">
              Cancel
            </button>
            <button
              type="submit"
              disabled={!name.trim() || createClient.isPending}
              className="px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50 transition-opacity"
            >
              {createClient.isPending ? 'Creating...' : 'Create'}
            </button>
          </div>
          {createClient.isError && (
            <p className="text-xs text-red-600">{(createClient.error as any)?.message}</p>
          )}
        </form>
      </div>
    </div>
  );
}
