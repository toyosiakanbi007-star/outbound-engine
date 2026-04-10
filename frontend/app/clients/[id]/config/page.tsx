'use client';

import { useState, useEffect } from 'react';
import { PageShell } from '@/components/layout/page-shell';
import { JsonViewer } from '@/components/ui/json-viewer';
import { useClientConfig, useUpdateClientConfig } from '@/lib/api/client';

export default function ClientConfigPage({ params }: { params: { id: string } }) {
  const { id } = params;
  const { data, isLoading, isError, error } = useClientConfig(id);
  const updateConfig = useUpdateClientConfig(id);
  const [tab, setTab] = useState<'form' | 'json'>('form');
  const [msg, setMsg] = useState('');

  // Form state for known config keys
  const [brandName, setBrandName] = useState('');
  const [niche, setNiche] = useState('');
  const [offer, setOffer] = useState('');
  const [tone, setTone] = useState('');
  const [calendarLink, setCalendarLink] = useState('');
  const [senderEmail, setSenderEmail] = useState('');
  const [senderName, setSenderName] = useState('');
  const [dailyMax, setDailyMax] = useState(100);

  // Prequal config state
  const [autopilot, setAutopilot] = useState(true);
  const [batchSize, setBatchSize] = useState(5);
  const [maxBatches, setMaxBatches] = useState(10);
  const [dispatchInterval, setDispatchInterval] = useState(15);
  const [maxAttempts, setMaxAttempts] = useState(4);
  const [azureTimeout, setAzureTimeout] = useState(120);

  // Raw JSON state
  const [rawJson, setRawJson] = useState('');

  useEffect(() => {
    if (!data?.data) return;
    const cfg = data.data.config as any;
    const pq = data.data.prequal_config as any;
    setBrandName(cfg?.brand_name ?? '');
    setNiche(cfg?.niche ?? '');
    setOffer(cfg?.offer ?? '');
    setTone(cfg?.tone ?? '');
    setCalendarLink(cfg?.calendar_link ?? '');
    setSenderEmail(cfg?.sender_email ?? '');
    setSenderName(cfg?.sender_name ?? '');
    setDailyMax(cfg?.sending_limits?.daily_max ?? 100);
    setAutopilot(pq?.autopilot_enabled ?? true);
    setBatchSize(pq?.batch_size ?? 5);
    setMaxBatches(pq?.max_batches_per_dispatch ?? 10);
    setDispatchInterval(pq?.dispatch_interval_minutes ?? 15);
    setMaxAttempts(pq?.max_attempts_per_company ?? 4);
    setAzureTimeout(pq?.azure_function_timeout_secs ?? 120);
    setRawJson(JSON.stringify(cfg, null, 2));
  }, [data]);

  const handleSave = async () => {
    setMsg('');
    try {
      let config: any;
      if (tab === 'json') {
        config = JSON.parse(rawJson);
      } else {
        config = {
          ...(data?.data?.config as any),
          brand_name: brandName, niche, offer, tone,
          calendar_link: calendarLink,
          sender_email: senderEmail, sender_name: senderName,
          sending_limits: { daily_max: dailyMax },
        };
      }
      const prequal_config = {
        autopilot_enabled: autopilot,
        batch_size: batchSize,
        max_batches_per_dispatch: maxBatches,
        dispatch_interval_minutes: dispatchInterval,
        max_attempts_per_company: maxAttempts,
        azure_function_timeout_secs: azureTimeout,
      };
      await updateConfig.mutateAsync({ config, prequal_config });
      setMsg('Saved');
    } catch (e: any) {
      setMsg(`Error: ${e.message}`);
    }
  };

  if (isLoading) return <PageShell title="Config"><p className="text-muted-foreground">Loading config...</p></PageShell>;
  if (isError) return <PageShell title="Error"><p className="text-red-600">Failed to load config: {(error as any)?.message ?? 'Unknown error'}</p></PageShell>;

  return (
    <PageShell
      title="Client config"
      description={`Version ${data?.data?.version ?? '?'}`}
      actions={
        <div className="flex items-center gap-2">
          {msg && <span className={`text-xs ${msg.startsWith('Error') ? 'text-red-600' : 'text-emerald-600'}`}>{msg}</span>}
          <button onClick={handleSave} disabled={updateConfig.isPending}
            className="px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50">
            {updateConfig.isPending ? 'Saving...' : 'Save'}
          </button>
        </div>
      }
    >
      {/* Tab switcher */}
      <div className="flex gap-1 mb-4 border-b border-border">
        {(['form', 'json'] as const).map((t) => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-2 text-sm font-medium border-b-2 transition-colors ${tab === t ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}>
            {t === 'form' ? 'Form' : 'Raw JSON'}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Left: main config */}
        <div className="space-y-6">
          <h3 className="text-sm font-medium">Client settings</h3>
          {tab === 'form' ? (
            <div className="space-y-3">
              <Field label="Brand name" value={brandName} onChange={setBrandName} />
              <Field label="Niche" value={niche} onChange={setNiche} />
              <Field label="Offer" value={offer} onChange={setOffer} multiline />
              <Field label="Tone" value={tone} onChange={setTone} placeholder="professional, casual, direct" />
              <Field label="Calendar link" value={calendarLink} onChange={setCalendarLink} />
              <Field label="Sender email" value={senderEmail} onChange={setSenderEmail} />
              <Field label="Sender name" value={senderName} onChange={setSenderName} />
              <div>
                <label className="block text-xs font-medium text-muted-foreground mb-1">Daily send limit</label>
                <input type="number" value={dailyMax} onChange={(e) => setDailyMax(Number(e.target.value))}
                  className="w-32 px-3 py-1.5 text-sm border border-input rounded-md bg-background" />
              </div>
            </div>
          ) : (
            <textarea value={rawJson} onChange={(e) => setRawJson(e.target.value)}
              className="w-full h-80 px-3 py-2 text-xs font-mono border border-input rounded-md bg-background resize-y" />
          )}
        </div>

        {/* Right: prequal config */}
        <div className="space-y-6">
          <h3 className="text-sm font-medium">Prequal settings</h3>
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm">Autopilot</label>
              <button onClick={() => setAutopilot(!autopilot)}
                className={`relative w-10 h-5 rounded-full transition-colors ${autopilot ? 'bg-primary' : 'bg-muted'}`}>
                <div className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${autopilot ? 'left-5' : 'left-0.5'}`} />
              </button>
            </div>
            <NumberField label="Batch size" value={batchSize} onChange={setBatchSize} min={1} max={50} />
            <NumberField label="Max batches per dispatch" value={maxBatches} onChange={setMaxBatches} min={1} max={100} />
            <NumberField label="Dispatch interval (min)" value={dispatchInterval} onChange={setDispatchInterval} min={5} max={120} />
            <NumberField label="Max attempts per company" value={maxAttempts} onChange={setMaxAttempts} min={1} max={10} />
            <NumberField label="Azure timeout (sec)" value={azureTimeout} onChange={setAzureTimeout} min={30} max={600} />
          </div>

          {/* Current raw prequal config */}
          <div className="mt-4">
            <h3 className="text-sm font-medium mb-2">Current prequal config (read-only)</h3>
            <JsonViewer data={data?.data?.prequal_config} />
          </div>
        </div>
      </div>
    </PageShell>
  );
}

function Field({ label, value, onChange, multiline, placeholder }: {
  label: string; value: string; onChange: (v: string) => void; multiline?: boolean; placeholder?: string;
}) {
  const cls = "w-full px-3 py-1.5 text-sm border border-input rounded-md bg-background focus:outline-none focus:ring-2 focus:ring-ring";
  return (
    <div>
      <label className="block text-xs font-medium text-muted-foreground mb-1">{label}</label>
      {multiline ? (
        <textarea value={value} onChange={(e) => onChange(e.target.value)} rows={3} placeholder={placeholder} className={cls} />
      ) : (
        <input type="text" value={value} onChange={(e) => onChange(e.target.value)} placeholder={placeholder} className={cls} />
      )}
    </div>
  );
}

function NumberField({ label, value, onChange, min, max }: {
  label: string; value: number; onChange: (v: number) => void; min: number; max: number;
}) {
  return (
    <div className="flex items-center justify-between">
      <label className="text-sm">{label}</label>
      <input type="number" value={value} onChange={(e) => onChange(Number(e.target.value))}
        min={min} max={max}
        className="w-24 px-3 py-1.5 text-sm text-right border border-input rounded-md bg-background" />
    </div>
  );
}
