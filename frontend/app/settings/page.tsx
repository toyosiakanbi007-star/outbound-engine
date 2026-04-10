'use client';

import { PageShell } from '@/components/layout/page-shell';
import { useHealth } from '@/lib/api/client';

export default function SettingsPage() {
  const { data: health } = useHealth();

  return (
    <PageShell title="Settings" description="System configuration">
      <div className="max-w-lg space-y-6">
        <div className="bg-card border border-border rounded-lg p-4 space-y-3">
          <h2 className="text-sm font-medium">Backend</h2>
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Status</span>
            <span>{health?.status ?? '...'}</span>
          </div>
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Environment</span>
            <span className="font-mono text-xs">{health?.env ?? '...'}</span>
          </div>
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Version</span>
            <span className="font-mono text-xs">{health?.version ?? '...'}</span>
          </div>
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Database</span>
            <span>{health?.db ?? '...'}</span>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-4">
          <h2 className="text-sm font-medium mb-2">API endpoint</h2>
          <p className="text-xs font-mono text-muted-foreground">Proxied to http://localhost:3000/api/*</p>
          <p className="text-xs text-muted-foreground mt-1">Configure in next.config.mjs for production.</p>
        </div>
      </div>
    </PageShell>
  );
}
