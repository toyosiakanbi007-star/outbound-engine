'use client';

import { useState } from 'react';
import { cn } from '@/lib/utils/cn';
import { ChevronRight, Copy, Check } from 'lucide-react';

interface JsonViewerProps {
  data: unknown;
  defaultExpanded?: boolean;
  className?: string;
}

export function JsonViewer({ data, defaultExpanded = false, className }: JsonViewerProps) {
  const [copied, setCopied] = useState(false);
  const json = typeof data === 'string' ? data : JSON.stringify(data, null, 2);

  const handleCopy = () => {
    navigator.clipboard.writeText(json);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className={cn('relative', className)}>
      <button
        onClick={handleCopy}
        className="absolute top-2 right-2 p-1.5 rounded-md bg-secondary hover:bg-accent text-muted-foreground hover:text-foreground transition-colors z-10"
      >
        {copied ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
      </button>
      <pre className="bg-secondary/50 border border-border rounded-lg p-4 overflow-auto text-xs font-mono leading-relaxed max-h-[500px] custom-scrollbar">
        {json}
      </pre>
    </div>
  );
}

// Collapsible section for detail pages
export function CollapsibleSection({
  title,
  children,
  defaultOpen = false,
}: {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-2 px-4 py-3 text-sm font-medium hover:bg-muted transition-colors"
      >
        <ChevronRight className={cn('w-4 h-4 transition-transform', open && 'rotate-90')} />
        {title}
      </button>
      {open && <div className="px-4 pb-4 border-t border-border">{children}</div>}
    </div>
  );
}
