import { Meter } from '@/components/ui/meter'
import { cn } from '@/lib/utils'

/**
 * ScoreMeter — a 0–100 relevance score as a bar plus its number.
 *
 * PaperCard and PaperHoverCard each carried a byte-identical private
 * `ScoreBar`, which is exactly how the two SIGNAL colour maps drifted apart
 * before `lib/palette.ts` existed. One component, one set of thresholds, and
 * colour ONLY through the Meter's semantic tones — never a hand-mixed hue.
 *
 * The bands are absolute: the scale is 0–100 and the colour says where on it
 * the score sits. The backend also publishes what an all-average paper scores
 * under the current weights (`explanation.reference_score`); that is drawn as a
 * tick and named in the tooltip, so a reader can see both "how high" and
 * "compared with typical" without the colour trying to mean two things.
 */
const HIGH = 70
const MID = 40

/** Score → valence. Above 70 argues strongly for the paper, below 40 against. */
function scoreTone(pct: number): 'success' | 'warning' | 'critical' {
  if (pct >= HIGH) return 'success'
  if (pct >= MID) return 'warning'
  return 'critical'
}

export function ScoreMeter({
  score,
  reference,
  className,
}: {
  score: number
  /** `explanation.reference_score` — the all-average paper's score. */
  reference?: number | null
  className?: string
}) {
  const pct = Math.round(score)
  const ref = reference == null ? undefined : Math.round(reference)
  const title =
    ref == null
      ? `Score ${pct} of 100`
      : `Score ${pct} of 100. A paper that is average on every signal would score ${ref} under your current weights.`
  return (
    <div className={cn('flex items-center gap-2', className)} title={title}>
      <span className="relative inline-block w-16">
        {/* The number sits right beside the bar, so the bar itself is decorative
            to a screen reader rather than announcing the value twice. */}
        <Meter value={pct} tone={scoreTone(pct)} size="sm" className="w-16" decorative />
        {ref != null && (
          <span
            aria-hidden
            className="pointer-events-none absolute -top-0.5 -bottom-0.5 w-px bg-slate-500"
            style={{ left: `${Math.max(0, Math.min(100, ref))}%` }}
          />
        )}
      </span>
      <span className="text-xs font-semibold tabular-nums text-slate-600">{pct}</span>
    </div>
  )
}
