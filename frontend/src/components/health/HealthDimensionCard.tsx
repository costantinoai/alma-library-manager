/**
 * HealthDimensionCard — one canonical health dimension rendered as a
 * problem + why-it-matters + metric + fix actions. Lives on the cool
 * alma-50 ramp because it's a "potential to fix" (lessons.md surface
 * contrast); the severity StatusBadge is the only saturated element.
 */
import { History, Wrench } from 'lucide-react'

import { MetricTile } from '@/components/shared/MetricTile'
import { StatusBadge } from '@/components/ui/status-badge'
import { AsyncButton } from '@/components/settings/primitives'
import { cn, formatRelativeShort } from '@/lib/utils'
import type { HealthDimension } from '@/api/client'
import { dimensionBadgeTone, severityLabel, severityMetricTone } from './healthFormat'

interface HealthDimensionCardProps {
  dim: HealthDimension
  /** Trigger a maintenance run for the given operation key. */
  onRun: (operationKey: string) => void
  /** The operation key currently running (shows the spinner), if any. */
  runningKey: string | null
  /** task key → ISO timestamp of its last successful run, for the "last fixed" line. */
  lastSuccessByTask: Record<string, string | null>
  /** Open the drilldown for this dimension (which papers + per-issue fixes). */
  onOpen: () => void
}

export function HealthDimensionCard({
  dim,
  onRun,
  runningKey,
  lastSuccessByTask,
  onOpen,
}: HealthDimensionCardProps) {
  // The repair tasks this dimension can trigger, and when any of them last
  // completed successfully (ISO timestamps sort lexically → max = most recent).
  const runActions = dim.actions.filter((a) => a.kind === 'run_now')
  const successTimes = runActions
    .map((a) => lastSuccessByTask[a.operation_key])
    .filter((v): v is string => !!v)
    .sort()
  const lastFixed = successTimes.length ? successTimes[successTimes.length - 1] : undefined
  // Coverage dimensions read as a percentage; gap dimensions read as count/total.
  const isCoverage = dim.coverage_pct != null
  const metricValue = isCoverage ? `${Math.round(dim.coverage_pct ?? 0)}%` : dim.count
  const metricHint = isCoverage
    ? `of ${dim.total.toLocaleString()} papers`
    : dim.total > 0
      ? `of ${dim.total.toLocaleString()}`
      : undefined

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onOpen()
        }
      }}
      className="flex cursor-pointer flex-col gap-3 rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-4 shadow-paper-sm transition-colors hover:border-alma-300 hover:shadow-paper focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio"
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="min-w-0 font-medium text-alma-800">{dim.label}</h3>
        <StatusBadge tone={dimensionBadgeTone(dim.severity)} className="shrink-0 capitalize">
          {severityLabel(dim.severity)}
        </StatusBadge>
      </div>

      <div className="flex items-start gap-3">
        <MetricTile
          label={isCoverage ? 'coverage' : dim.entity === 'paper' ? 'papers' : 'items'}
          value={metricValue}
          hint={metricHint}
          tone={severityMetricTone(dim.severity)}
          className="w-32 shrink-0"
        />
        <div className="min-w-0 space-y-1 text-sm">
          <p className="text-slate-700">{dim.explanation}</p>
          {dim.impact ? <p className="text-xs text-slate-500">{dim.impact}</p> : null}
        </div>
      </div>

      {/* Coverage progress toward the 80% ready threshold. */}
      {isCoverage ? (
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-parchment-200" aria-hidden>
          <div
            className={cn(
              'h-full rounded-full',
              (dim.coverage_pct ?? 0) >= 80 ? 'bg-emerald-600' : 'bg-amber-500',
            )}
            style={{ width: `${Math.min(100, dim.coverage_pct ?? 0)}%` }}
          />
        </div>
      ) : null}

      {runActions.length > 0 ? (
        <div className="space-y-2">
          <p className="flex items-center gap-1.5 text-[11px] text-slate-400">
            <History className="h-3.5 w-3.5" aria-hidden />
            {lastFixed ? `Last fixed ${formatRelativeShort(lastFixed)}` : 'Not fixed yet'}
          </p>
          <div className="flex flex-wrap gap-2">
            {runActions.map((action) => (
              <AsyncButton
                key={action.operation_key}
                size="sm"
                variant="outline"
                icon={<Wrench className="h-4 w-4" />}
                pending={runningKey === action.operation_key}
                disabled={runningKey != null && runningKey !== action.operation_key}
                className="border-alma-200 text-alma-700 hover:bg-alma-50"
                onClick={(e) => {
                  e.stopPropagation()
                  onRun(action.operation_key)
                }}
              >
                {action.label}
              </AsyncButton>
            ))}
          </div>
        </div>
      ) : null}

      <p className="text-[11px] font-medium text-alma-folio">View affected papers →</p>
    </div>
  )
}
