import type { ScoreBreakdown, ScoreSignalDetail } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { SubPanel } from '@/components/ui/sub-panel'
import {
  SIGNAL_COLORS,
  SIGNAL_LABELS,
  SIGNAL_ORDER,
  SOURCE_TYPE_LABELS,
  getSignalDescription,
  isSignalDegraded,
} from '@/lib/signals'
import { truncate } from '@/lib/utils'
import { cn } from '@/lib/utils'

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
      color: SIGNAL_COLORS[key] ?? '#94A3B8',
      degraded: isSignalDegraded(key, breakdown),
    }
  })

  const totalWeighted = signals.reduce((sum, s) => sum + Math.max(0, s.weighted), 0)

  const sortedSignals = [...signals].sort((a, b) => b.weighted - a.weighted)
  const topSignalKey = sortedSignals[0]?.key

  return (
    <SubPanel tone="parchment" padded={false} className="mt-3 space-y-3 p-4">
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
          so it reads as a printed band, not a chip. */}
      <div className="flex h-4 w-full overflow-hidden rounded-sm bg-parchment-200/60 ring-1 ring-parchment-300/60">
        {signals.map((s) => {
          const pct = totalWeighted > 0 ? (Math.max(0, s.weighted) / totalWeighted) * 100 : 0
          if (pct < 0.5) return null
          return (
            <div
              key={s.key}
              className="transition-all duration-300"
              style={{
                width: `${pct}%`,
                backgroundColor: s.weighted > 0 ? s.color : '#CBD5E1',
                minWidth: pct > 0 ? '3px' : '0',
              }}
              title={`${s.label}: ${(s.weighted * 100).toFixed(1)}%`}
            />
          )
        })}
      </div>

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
                  <span className="ml-1 text-[10px] text-orange-500" title={s.description}>
                    keyword
                  </span>
                )}
              </span>
              <span className="w-12 shrink-0 text-right font-mono text-slate-500">
                {(s.value * 100).toFixed(0)}%
              </span>
              <span className="w-10 shrink-0 text-center font-mono text-slate-400">
                ×{s.weight.toFixed(2)}
              </span>
              <div className="flex-1">
                <div className="h-1 w-full rounded-full bg-parchment-200/70">
                  <div
                    className="h-1 rounded-full transition-all duration-300"
                    style={{
                      width: `${Math.min(100, s.weighted * 100 * 10)}%`,
                      backgroundColor: s.weighted > 0 ? s.color : '#CBD5E1',
                    }}
                  />
                </div>
              </div>
              <span
                className={cn(
                  'w-10 shrink-0 text-right font-mono text-[10px]',
                  s.weighted > 0 ? 'text-slate-600' : 'text-slate-300',
                )}
              >
                {(s.weighted * 100).toFixed(1)}
              </span>
            </>
          )

          return isTop ? (
            <SubPanel
              key={s.key}
              tone="paper"
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
