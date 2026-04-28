import type { AuthorSignal } from '@/api/client'
import { cn } from '@/lib/utils'

interface AuthorSignalBarProps {
  signal: AuthorSignal | null
  className?: string
  showCaption?: boolean
}

function signalFillClass(score: number): string {
  if (score >= 70) return 'bg-emerald-500'
  if (score >= 40) return 'bg-emerald-400'
  if (score >= 20) return 'bg-amber-400'
  if (score > 0) return 'bg-amber-300'
  return 'bg-slate-300'
}

/**
 * Single, shared signal meter used across suggestion cards, followed cards,
 * and the detail dialog. Gives the Authors page one visually coherent
 * language for "how much do we like this author".
 */
export function AuthorSignalBar({ signal, className, showCaption = true }: AuthorSignalBarProps) {
  if (!signal) {
    return (
      <div className={cn('inline-flex items-center gap-2 text-[11px] text-slate-400', className)}>
        <span className="inline-block h-1 w-14 rounded-full bg-slate-200" />
        <span>no signal yet</span>
      </div>
    )
  }
  const pct = Math.max(0, Math.min(100, signal.score))
  const fill = signalFillClass(pct)
  return (
    <div className={cn('space-y-1', className)}>
      <div className="flex items-center gap-2">
        <div className="relative h-1 w-full overflow-hidden rounded-full bg-slate-200">
          <div
            className={cn('absolute inset-y-0 left-0 rounded-full transition-all', fill)}
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="shrink-0 text-[11px] font-semibold tabular-nums text-slate-700">
          {Math.round(pct)}
        </span>
      </div>
      {showCaption ? (
        <p className="text-[11px] text-slate-500">
          {signal.library_papers} lib / {signal.total_papers} total
          {signal.avg_rating != null ? ` · ★${signal.avg_rating.toFixed(1)}` : null}
        </p>
      ) : null}
    </div>
  )
}
