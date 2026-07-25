import { Meter } from '@/components/ui/meter'
import { cn } from '@/lib/utils'

/**
 * ScoreMeter — a 0–100 relevance score as a bar plus its number.
 *
 * PaperCard and PaperHoverCard each carried a byte-identical private
 * `ScoreBar`, which is exactly how the two SIGNAL colour maps drifted apart
 * before `lib/palette.ts` existed. One component, one set of thresholds.
 */
const HIGH = 70
const MID = 40

/** Score → valence. Above 70 argues strongly for the paper, below 40 against. */
function scoreTone(pct: number): 'success' | 'warning' | 'critical' {
  if (pct >= HIGH) return 'success'
  if (pct >= MID) return 'warning'
  return 'critical'
}

export function ScoreMeter({ score, className }: { score: number; className?: string }) {
  const pct = Math.round(score)
  return (
    <div className={cn('flex items-center gap-2', className)}>
      {/* The number sits right beside the bar, so the bar itself is decorative
          to a screen reader rather than announcing the value twice. */}
      <Meter value={pct} tone={scoreTone(pct)} size="sm" className="w-16" decorative />
      <span className="text-xs font-semibold tabular-nums text-slate-600">{pct}</span>
    </div>
  )
}
