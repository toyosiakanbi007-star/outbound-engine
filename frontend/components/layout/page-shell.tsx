import { cn } from '@/lib/utils/cn';

interface PageShellProps {
  title: string;
  description?: string;
  actions?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}

export function PageShell({ title, description, actions, children, className }: PageShellProps) {
  return (
    <div className={cn('flex flex-col h-full', className)}>
      <header className="flex-shrink-0 h-14 flex items-center justify-between px-6 border-b border-border bg-card">
        <div>
          <h1 className="text-base font-semibold">{title}</h1>
          {description && <p className="text-xs text-muted-foreground">{description}</p>}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </header>
      <div className="flex-1 overflow-y-auto p-6 custom-scrollbar">
        {children}
      </div>
    </div>
  );
}
