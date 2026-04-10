// components/ui/score-bar.tsx
import { cn } from '@/lib/utils/cn';

export function ScoreBar({ score, className }: { score: number | null; className?: string }) {
  if (score == null) return <span className="text-xs text-muted-foreground">—</span>;
  const pct = Math.round(score * 100);
  const color =
    score >= 0.7 ? 'bg-emerald-500' : score >= 0.4 ? 'bg-amber-500' : 'bg-red-500';
  return (
    <div className={cn('flex items-center gap-2', className)}>
      <div className="flex-1 h-2 bg-secondary rounded-full overflow-hidden max-w-[80px]">
        <div className={cn('h-full rounded-full transition-all', color)} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-mono tabular-nums">{score.toFixed(2)}</span>
    </div>
  );
}
