import type { ScoreBreakdown, ScoreSignalDetail } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Meter } from '@/components/ui/meter'
import { SubPanel } from '@/components/ui/sub-panel'
import { SIGNAL_COLORS, SIGNAL_FALLBACK_COLOR } from '@/lib/palette'
import {
  SIGNAL_LABELS,
  SIGNAL_ORDER,
  SOURCE_TYPE_LABELS,
  getSignalDescription,
  isSignalDegraded,
} from '@/lib/signals'
import { truncate } from '@/lib/utils'
import { cn } from '@/lib/utils'
import { formatPercent } from '@/lib/format'
import { byWeightedDesc } from '@/lib/sort'

interface ScoreBreakdownPanelProps {
  breakdown: ScoreBreakdown
}

export function ScoreBreakdownPanel({ breakdown }: ScoreBreakdownPanelProps) {
  const signals = SIGNAL_ORDER.map((key) => {
    const detail = breakdown[key] as ScoreSignalDetail | undefined
    return {
      key,
      label: SIGNAL_LABELS[key] ?? key,
      description: detail?.description ?? getSignalDescription(key, breakdown),
      value: detail?.value ?? 0,
      weight: detail?.weight ?? 0,
      weighted: detail?.weighted ?? 0,
      color: SIGNAL_COLORS[key] ?? SIGNAL_FALLBACK_COLOR,
      degraded: isSignalDegraded(key, breakdown),
    }
  })

  const totalWeighted = signals.reduce((sum, s) => sum + Math.max(0, s.weighted), 0)
  // Per-signal bars are normalized to the STRONGEST signal so the lead
  // contributor reads as full and the rest scale proportionally. (The old
  // `weighted * 100 * 10` multiplied a [0,1] contribution by 1000, so any
  // signal above ~0.1 clamped to a full bar — every row looked maxed out.)
  const maxWeighted = Math.max(...signals.map((s) => Math.max(0, s.weighted)), 1e-9)

  const sortedSignals = [...signals].sort(byWeightedDesc<typeof signals[number]>())
  const topSignalKey = sortedSignals[0]?.key

  return (
    <SubPanel padded={false} className="mt-3 space-y-3 p-4">
      <div className="flex items-center justify-between">
        <EyebrowLabel tone="muted">Score Breakdown</EyebrowLabel>
        {breakdown.source_type && (
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">
              {SOURCE_TYPE_LABELS[breakdown.source_type] ?? breakdown.source_type}
            </Badge>
            {breakdown.source_key && (
              <span className="text-xs text-slate-400" title={breakdown.source_key}>
                via {truncate(breakdown.source_key, 40)}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Stacked bar chart — slim, ribbon-like, sitting on a hairline rule
          so it reads as a printed band, not a chip. Squared off and taller
          than a standard Meter rail on purpose: this is the summary band,
          not one of the per-signal rails below it. */}
      <Meter
        segments={signals
          .filter((s) => (totalWeighted > 0 ? Math.max(0, s.weighted) / totalWeighted : 0) >= 0.005)
          .map((s) => ({
            value: Math.max(0, s.weighted),
            fillClassName: s.weighted > 0 ? s.color : SIGNAL_FALLBACK_COLOR,
          }))}
        className="h-4 rounded-sm ring-1 ring-control-edge"
        label={`Score composition: ${signals
          .filter((s) => s.weighted > 0)
          .map((s) => `${s.label} ${formatPercent(s.weighted, 1)}`)
          .join(', ')}`}
      />

      {/* Signal detail rows — each one a sub-cell. The TOP signal lifts
          onto a paper-tone sub-panel so it reads as the lead voice in
          the chorus, not just a bolded row. */}
      <div className="space-y-1.5">
        {signals.map((s) => {
          const isTop = s.key === topSignalKey && s.weighted > 0
          const rowContent = (
            <>
              <div
                className="h-2.5 w-2.5 shrink-0 rounded-sm ring-1 ring-black/5"
                style={{ backgroundColor: s.weighted > 0 ? s.color : '#CBD5E1' }}
              />
              <span
                className={cn(
                  'w-28 shrink-0 font-medium',
                  s.weighted > 0 ? 'text-slate-700' : 'text-slate-400',
                  isTop && 'font-semibold text-alma-900',
                )}
                title={s.description}
              >
                {s.label}
                {isTop && (
                  <span className="ml-1 font-mono text-[9px] uppercase tracking-wider text-gold-500">
                    top
                  </span>
                )}
                {s.degraded && (
                  <span className="ml-1 text-[10px] text-warning-600" title={s.description}>
                    keyword
                  </span>
                )}
              </span>
              <span className="w-12 shrink-0 text-right font-mono text-slate-500">
                {formatPercent(s.value, 0)}
              </span>
              <span className="w-10 shrink-0 text-center font-mono text-slate-400">
                ×{s.weight.toFixed(2)}
              </span>
              <div className="flex-1">
                <Meter
                  value={s.weighted > 0 ? (s.weighted / maxWeighted) * 100 : 0}
                  fillClassName={s.weighted > 0 ? s.color : SIGNAL_FALLBACK_COLOR}
                  size="xs"
                  decorative
                />
              </div>
              <span
                className={cn(
                  'w-10 shrink-0 text-right font-mono text-[10px]',
                  s.weighted > 0 ? 'text-slate-600' : 'text-slate-300',
                )}
              >
                {formatPercent(s.weighted, 1, { withSign: false })}
              </span>
            </>
          )

          return isTop ? (
            <SubPanel
              key={s.key}
              variant="flat"
              padded={false}
              className="flex items-center gap-2 px-2 py-1.5 text-xs"
            >
              {rowContent}
            </SubPanel>
          ) : (
            <div
              key={s.key}
              className="flex items-center gap-2 rounded-sm px-2 py-1.5 text-xs"
            >
              {rowContent}
            </div>
          )
        })}
      </div>

      {/* Final score — set on a hairline gold rule, the same editorial
          accent the wordmark uses. The score is the colophon of this
          breakdown card. */}
      {breakdown.final_score != null && (
        <div className="flex items-center justify-end gap-1.5 border-t border-gold-300/50 pt-2.5">
          <EyebrowLabel tone="muted">Final score</EyebrowLabel>
          <span className="font-brand text-base font-semibold text-alma-900 tabular-nums">
            {breakdown.final_score.toFixed(1)}
          </span>
        </div>
      )}
    </SubPanel>
  )
}
