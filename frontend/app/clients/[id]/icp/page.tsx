'use client';

import { useState, useEffect } from 'react';
import { PageShell } from '@/components/layout/page-shell';
import { JsonViewer } from '@/components/ui/json-viewer';
import { useClientIcp, useUpdateIcp } from '@/lib/api/client';

export default function IcpEditorPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError } = useClientIcp(id);
  const updateIcp = useUpdateIcp(id);
  const [tab, setTab] = useState<'form' | 'json'>('form');
  const [msg, setMsg] = useState('');

  // Form fields for the most important ICP sections
  const [industries, setIndustries] = useState<any[]>([]);
  const [companySizes, setCompanySizes] = useState<string[]>([]);
  const [locations, setLocations] = useState<string[]>([]);
  const [targetRoles, setTargetRoles] = useState<string[]>([]);
  const [mustHaveAny, setMustHaveAny] = useState<string[]>([]);
  const [disqualifyIf, setDisqualifyIf] = useState<string[]>([]);
  const [strongSignals, setStrongSignals] = useState<string[]>([]);
  const [weakSignals, setWeakSignals] = useState<string[]>([]);
  const [rawJson, setRawJson] = useState('');

  useEffect(() => {
    if (!data?.data?.icp_json) return;
    const icp = data.data.icp_json as any;
    const tp = icp.target_profile || {};
    setIndustries(tp.industries || []);
    setCompanySizes(tp.company_sizes || []);
    setLocations(tp.locations || []);
    setTargetRoles(icp.target_roles || tp.roles || []);
    setMustHaveAny(icp.must_have_any || []);
    setDisqualifyIf(icp.disqualify_if || []);
    setStrongSignals(icp.strong_signals || []);
    setWeakSignals(icp.weak_signals || []);
    setRawJson(JSON.stringify(icp, null, 2));
  }, [data]);

  const handleSave = async () => {
    setMsg('');
    try {
      let icpJson: any;
      if (tab === 'json') {
        icpJson = JSON.parse(rawJson);
      } else {
        // Merge form edits into existing ICP structure
        const existing = (data?.data?.icp_json || {}) as any;
        icpJson = {
          ...existing,
          target_roles: targetRoles,
          must_have_any: mustHaveAny,
          disqualify_if: disqualifyIf,
          strong_signals: strongSignals,
          weak_signals: weakSignals,
          target_profile: {
            ...(existing.target_profile || {}),
            industries,
            company_sizes: companySizes,
            locations,
            roles: targetRoles,
          },
        };
      }
      await updateIcp.mutateAsync({ icp_json: icpJson });
      setMsg('Saved');
    } catch (e: any) {
      setMsg(`Error: ${e.message}`);
    }
  };

  if (isLoading) return <PageShell title="ICP Editor"><p className="text-muted-foreground">Loading...</p></PageShell>;
  if (isError) return <PageShell title="ICP Editor"><p className="text-red-600">Failed to load ICP profile. It may not exist yet — save to create one.</p></PageShell>;

  return (
    <PageShell
      title="ICP Profile"
      description="Ideal Customer Profile — targeting rules for discovery + prequal"
      actions={
        <div className="flex items-center gap-2">
          {msg && <span className={`text-xs ${msg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>{msg}</span>}
          <button onClick={handleSave} disabled={updateIcp.isPending}
            className="px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50">
            {updateIcp.isPending ? 'Saving...' : 'Save ICP'}
          </button>
        </div>
      }
    >
      {/* Tab switcher */}
      <div className="flex gap-1 mb-4 border-b border-border">
        {(['form', 'json'] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-2 text-sm font-medium border-b-2 transition-colors ${tab === t ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}>
            {t === 'form' ? 'Form Editor' : 'Raw JSON'}
          </button>
        ))}
      </div>

      {tab === 'json' ? (
        <textarea value={rawJson} onChange={(e) => setRawJson(e.target.value)}
          className="w-full h-[600px] px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y" />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left column */}
          <div className="space-y-6">
            {/* Industries */}
            <section className="bg-card border border-border rounded-lg p-4">
              <h3 className="text-sm font-medium mb-3">Industries ({industries.length})</h3>
              <div className="space-y-3">
                {industries.map((ind: any, idx: number) => (
                  <div key={idx} className="p-3 border border-border rounded-md space-y-2">
                    <div className="flex items-center justify-between">
                      <input value={ind.name || ''} onChange={(e) => {
                        const updated = [...industries]; updated[idx] = { ...ind, name: e.target.value }; setIndustries(updated);
                      }} className="flex-1 px-2 py-1 text-sm border border-input rounded bg-background" placeholder="Industry name" />
                      <button onClick={() => setIndustries(industries.filter((_, i) => i !== idx))}
                        className="ml-2 text-xs text-red-600 hover:text-red-800">Remove</button>
                    </div>
                    <input value={ind.diffbot_category || ''} onChange={(e) => {
                      const updated = [...industries]; updated[idx] = { ...ind, diffbot_category: e.target.value }; setIndustries(updated);
                    }} className="w-full px-2 py-1 text-xs border border-input rounded bg-background" placeholder="Diffbot category" />
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-muted-foreground">Priority:</span>
                      <input type="number" step="0.05" min="0" max="1" value={ind.priority ?? 1.0} onChange={(e) => {
                        const updated = [...industries]; updated[idx] = { ...ind, priority: parseFloat(e.target.value) }; setIndustries(updated);
                      }} className="w-20 px-2 py-1 text-xs border border-input rounded bg-background" />
                    </div>
                  </div>
                ))}
                <button onClick={() => setIndustries([...industries, { name: '', diffbot_category: '', priority: 0.8, include_keywords_any: [], exclude_keywords_any: [] }])}
                  className="text-xs text-primary hover:underline">+ Add industry</button>
              </div>
            </section>

            {/* Company sizes */}
            <ListEditor label="Company sizes" items={companySizes} onChange={setCompanySizes} placeholder="e.g. 51-200" />

            {/* Locations */}
            <ListEditor label="Locations" items={locations} onChange={setLocations} placeholder="e.g. United States" />
          </div>

          {/* Right column */}
          <div className="space-y-6">
            {/* Target roles */}
            <ListEditor label="Target roles" items={targetRoles} onChange={setTargetRoles} placeholder="e.g. General Counsel" />

            {/* Must have any */}
            <ListEditor label="Must have any (qualification signals)" items={mustHaveAny} onChange={setMustHaveAny}
              placeholder="e.g. High contract volume" />

            {/* Disqualify if */}
            <ListEditor label="Disqualify if" items={disqualifyIf} onChange={setDisqualifyIf}
              placeholder="e.g. Under 20 employees" />

            {/* Strong signals */}
            <ListEditor label="Strong signals" items={strongSignals} onChange={setStrongSignals}
              placeholder="e.g. M&A activity" />

            {/* Weak signals */}
            <ListEditor label="Weak signals" items={weakSignals} onChange={setWeakSignals}
              placeholder="e.g. Very low contract volume" />
          </div>
        </div>
      )}

      {/* Current stored ICP (read-only) */}
      <div className="mt-6">
        <h3 className="text-sm font-medium mb-2">Current stored ICP (read-only)</h3>
        <JsonViewer data={data?.data?.icp_json ?? {}} />
      </div>
    </PageShell>
  );
}

function ListEditor({ label, items, onChange, placeholder }: {
  label: string; items: string[]; onChange: (items: string[]) => void; placeholder?: string;
}) {
  return (
    <section className="bg-card border border-border rounded-lg p-4">
      <h3 className="text-sm font-medium mb-2">{label} ({items.length})</h3>
      <div className="space-y-1">
        {items.map((item, idx) => (
          <div key={idx} className="flex items-center gap-2">
            <input value={item} onChange={(e) => {
              const updated = [...items]; updated[idx] = e.target.value; onChange(updated);
            }} className="flex-1 px-2 py-1 text-sm border border-input rounded bg-background" placeholder={placeholder} />
            <button onClick={() => onChange(items.filter((_, i) => i !== idx))}
              className="text-xs text-red-600 hover:text-red-800 px-1">×</button>
          </div>
        ))}
        <button onClick={() => onChange([...items, ''])}
          className="text-xs text-primary hover:underline mt-1">+ Add</button>
      </div>
    </section>
  );
}
