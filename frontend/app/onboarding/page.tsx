'use client';

import { useState } from 'react';
import Link from 'next/link';
import { PageShell } from '@/components/layout/page-shell';
import { StatusBadge } from '@/components/ui/status-badge';
import { useOnboardingRuns, useStartOnboarding, useClients } from '@/lib/api/client';
import { timeAgo } from '@/lib/utils/format';
import { Plus, Sparkles, X } from 'lucide-react';

export default function OnboardingPage() {
  const { data } = useOnboardingRuns();
  const { data: clientsData } = useClients();
  const [showNew, setShowNew] = useState(false);
  const runs = data?.data ?? [];

  return (
    <PageShell
      title="Client Onboarding"
      description="AI-powered client setup — research, config generation, and ICP drafting"
      actions={
        <button onClick={() => setShowNew(true)}
          className="flex items-center gap-1.5 px-3 py-2 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90">
          <Plus className="w-3.5 h-3.5" /> New onboarding
        </button>
      }
    >
      {/* Runs list */}
      <div className="bg-card border border-border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Company</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Domain</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Status</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Model</th>
              <th className="text-left px-4 py-2 font-medium text-muted-foreground">Created</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {runs.length === 0 ? (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                No onboarding runs yet. Click "New onboarding" to get started.
              </td></tr>
            ) : (
              runs.map((run: any) => (
                <tr key={run.id} className="hover:bg-muted/30">
                  <td className="px-4 py-3">
                    <Link href={`/onboarding/${run.id}`} className="font-medium text-primary hover:underline">
                      {run.input_name}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-muted-foreground">{run.input_domain}</td>
                  <td className="px-4 py-3"><OnboardingStatusBadge status={run.status} /></td>
                  <td className="px-4 py-3 text-xs text-muted-foreground font-mono">{run.llm_model ?? '—'}</td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(run.created_at)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* New onboarding modal */}
      {showNew && (
        <NewOnboardingModal
          clients={clientsData?.data ?? []}
          onClose={() => setShowNew(false)}
        />
      )}
    </PageShell>
  );
}

// ============================================================================
// New Onboarding Modal
// ============================================================================
function NewOnboardingModal({ clients, onClose }: { clients: any[]; onClose: () => void }) {
  const [clientId, setClientId] = useState('');
  const [isNewClient, setIsNewClient] = useState(true);
  const [name, setName] = useState('');
  const [domain, setDomain] = useState('');
  const [note, setNote] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async () => {
    if (!name.trim() || !domain.trim()) {
      setError('Name and domain are required');
      return;
    }
    setSubmitting(true);
    setError('');

    try {
      let targetClientId = clientId;

      // Create new client if needed
      if (isNewClient || !clientId) {
        const resp = await fetch(`${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/api/clients`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: name.trim(), domain: domain.trim(), is_active: false }),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error?.message || 'Failed to create client');
        targetClientId = data.data?.id || data.id;
      }

      // Start onboarding
      const resp = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000'}/api/clients/${targetClientId}/onboarding-runs`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            client_name: name.trim(),
            client_domain: domain.trim(),
            operator_note: note.trim() || undefined,
          }),
        }
      );
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error?.message || 'Failed to start onboarding');

      // Redirect to run detail
      window.location.href = `/onboarding/${data.data.run_id}`;
    } catch (e: any) {
      setError(e.message);
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-md mx-4">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div className="flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-primary" />
            <h2 className="text-sm font-semibold">New Client Onboarding</h2>
          </div>
          <button onClick={onClose} className="p-1 text-muted-foreground hover:text-foreground"><X className="w-4 h-4" /></button>
        </div>

        <div className="p-5 space-y-4">
          {error && (
            <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-xs text-red-600 dark:text-red-400">{error}</p>
            </div>
          )}

          <div>
            <label className="text-sm font-medium">Company name</label>
            <input type="text" value={name} onChange={e => setName(e.target.value)}
              placeholder="e.g. PeerSpot" autoFocus
              className="mt-1 w-full px-3 py-2 text-sm border border-input rounded-md bg-background" />
          </div>

          <div>
            <label className="text-sm font-medium">Domain</label>
            <input type="text" value={domain} onChange={e => setDomain(e.target.value)}
              placeholder="e.g. peerspot.com"
              className="mt-1 w-full px-3 py-2 text-sm border border-input rounded-md bg-background" />
          </div>

          <div>
            <label className="text-sm font-medium">Operator notes <span className="text-muted-foreground">(optional)</span></label>
            <textarea value={note} onChange={e => setNote(e.target.value)} rows={3}
              placeholder="Any context about the client, their target market, or special requirements..."
              className="mt-1 w-full px-3 py-2 text-sm border border-input rounded-md bg-background resize-none" />
          </div>

          <div className="p-3 bg-muted/30 rounded-md">
            <p className="text-xs text-muted-foreground">
              The AI will research this company using Diffbot + their website, then generate draft config, ICP, and prequal settings. You'll review and approve before anything goes live.
            </p>
          </div>
        </div>

        <div className="px-5 py-4 border-t border-border flex justify-end gap-2">
          <button onClick={onClose} className="px-3 py-1.5 text-sm rounded-md hover:bg-muted">Cancel</button>
          <button onClick={handleSubmit} disabled={submitting}
            className="flex items-center gap-1.5 px-4 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50">
            <Sparkles className="w-3.5 h-3.5" />
            {submitting ? 'Starting...' : 'Start onboarding'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// Onboarding-specific status badge
// ============================================================================
function OnboardingStatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    pending: 'bg-secondary text-secondary-foreground',
    enriching: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
    generating: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
    review_ready: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
    activated: 'bg-primary/20 text-primary',
    failed: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
  };
  return (
    <span className={`px-2 py-0.5 text-xs font-medium rounded ${colors[status] ?? colors.pending}`}>
      {status.replace('_', ' ')}
    </span>
  );
}
