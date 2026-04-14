'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { PageShell } from '@/components/layout/page-shell';
import { useManualSetup } from '@/lib/api/client';
import { Save, ArrowLeft } from 'lucide-react';
import Link from 'next/link';

const DEFAULT_CONFIG = JSON.stringify({
  brand_name: "",
  niche: "",
  offer: "",
  tone: "professional",
  calendar_link: "[PLACEHOLDER]",
  sender_name: "[PLACEHOLDER]",
  sender_email: "[PLACEHOLDER]",
  offer_capabilities: [],
  outreach_angles: [],
  signal_preferences: { strong_signals: [], weak_signals: [] },
}, null, 2);

const DEFAULT_ICP = JSON.stringify({
  target_roles: ["CTO", "VP Engineering", "Head of Security"],
  target_profile: {
    industries: [
      { name: "SaaS", priority: 1, diffbot_category: "Software", include_keywords_any: [], exclude_keywords_any: [] }
    ],
    company_sizes: ["51-200", "201-500"],
    locations: ["United States"],
  },
  must_have_any: [],
  strong_signals: [],
  weak_signals: [],
  disqualify_if: [],
  pain_categories_priority: [],
}, null, 2);

const DEFAULT_PREQUAL = JSON.stringify({
  autopilot_enabled: true,
  batch_size: 10,
  max_batches_per_dispatch: 5,
  dispatch_interval_minutes: 30,
  max_attempts_per_company: 3,
  azure_function_timeout_secs: 300,
}, null, 2);

export default function ManualSetupPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const router = useRouter();
  const setup = useManualSetup(id);

  const [configJson, setConfigJson] = useState(DEFAULT_CONFIG);
  const [icpJson, setIcpJson] = useState(DEFAULT_ICP);
  const [prequalJson, setPrequalJson] = useState(DEFAULT_PREQUAL);
  const [activate, setActivate] = useState(true);
  const [tab, setTab] = useState<'config' | 'icp' | 'prequal'>('config');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const validate = (json: string, label: string): any | null => {
    try { return JSON.parse(json); }
    catch (e: any) { setError(`Invalid JSON in ${label}: ${e.message}`); return null; }
  };

  const handleSave = async () => {
    setError(''); setSuccess('');
    const config = validate(configJson, 'Config');
    if (!config) return;
    const icp = validate(icpJson, 'ICP');
    if (!icp) return;
    const prequal = validate(prequalJson, 'Prequal Config');
    if (!prequal) return;

    try {
      await setup.mutateAsync({ config, icp, prequal_config: prequal, activate });
      setSuccess('Saved! Config, ICP, and prequal settings are now active.');
    } catch (e: any) { setError(e.message); }
  };

  const TABS = [
    { key: 'config' as const, label: 'Client Config' },
    { key: 'icp' as const, label: 'ICP Profile' },
    { key: 'prequal' as const, label: 'Prequal Config' },
  ];

  return (
    <PageShell
      title="Manual Setup"
      description="Set config, ICP, and prequal settings directly"
      actions={
        <div className="flex items-center gap-2">
          <Link href={`/clients/${id}`} className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-border hover:bg-muted">
            <ArrowLeft className="w-3.5 h-3.5" /> Back
          </Link>
          <button onClick={handleSave} disabled={setup.isPending}
            className="flex items-center gap-1.5 px-4 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50">
            <Save className="w-3.5 h-3.5" /> {setup.isPending ? 'Saving...' : 'Save all'}
          </button>
        </div>
      }
    >
      {error && <div className="mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md text-sm text-red-600">{error}</div>}
      {success && <div className="mb-4 p-3 bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 rounded-md text-sm text-emerald-600">{success}</div>}

      <div className="flex items-center gap-4 mb-4">
        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={activate} onChange={e => setActivate(e.target.checked)} className="rounded" />
          Activate client after saving
        </label>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-border mb-4">
        {TABS.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            className={`px-3 py-2 text-sm font-medium border-b-2 transition-colors ${tab === t.key ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}>
            {t.label}
          </button>
        ))}
      </div>

      {/* Editor */}
      {tab === 'config' && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Brand name, niche, offer, tone, outreach angles, signal preferences.</p>
          <textarea value={configJson} onChange={e => setConfigJson(e.target.value)} rows={25} spellCheck={false}
            className="w-full px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y" />
        </div>
      )}
      {tab === 'icp' && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Target roles, industries, company sizes, locations, signals, disqualifiers.</p>
          <textarea value={icpJson} onChange={e => setIcpJson(e.target.value)} rows={25} spellCheck={false}
            className="w-full px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y" />
        </div>
      )}
      {tab === 'prequal' && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Autopilot, batch size, dispatch interval, retry limits.</p>
          <textarea value={prequalJson} onChange={e => setPrequalJson(e.target.value)} rows={15} spellCheck={false}
            className="w-full px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y" />
        </div>
      )}
    </PageShell>
  );
}
