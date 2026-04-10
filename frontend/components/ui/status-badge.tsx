// components/ui/status-badge.tsx
import { cn } from '@/lib/utils/cn';

const STATUS_STYLES: Record<string, string> = {
  new: 'bg-secondary text-secondary-foreground',
  pending: 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300',
  running: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300 animate-status-pulse',
  prequal_queued: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300',
  qualified: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
  disqualified: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300',
  done: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
  succeeded: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
  failed: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300',
  depleted: 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300',
  partial: 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300',
  skipped: 'bg-secondary text-secondary-foreground',
};

export function StatusBadge({ status, className }: { status: string; className?: string }) {
  const style = STATUS_STYLES[status] || STATUS_STYLES.new;
  return (
    <span className={cn('inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full', style, className)}>
      {status.replace(/_/g, ' ')}
    </span>
  );
}
