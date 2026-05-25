/**
 * HealthDimensionCard — one canonical health dimension rendered as a
 * problem + why-it-matters + metric + fix actions. Lives on the cool
 * alma-50 ramp because it's a "potential to fix" (lessons.md surface
 * contrast); the severity StatusBadge is the only saturated element.
 */
import { Wrench } from 'lucide-react'

import { MetricTile } from '@/components/shared/MetricTile'
import { StatusBadge } from '@/components/ui/status-badge'
import { AsyncButton } from '@/components/settings/primitives'
import { cn } from '@/lib/utils'
import type { HealthDimension } from '@/api/client'
import { dimensionBadgeTone, severityLabel, severityMetricTone } from './healthFormat'

interface HealthDimensionCardProps {
  dim: HealthDimension
  /** Trigger a maintenance run for the given operation key. */
  onRun: (operationKey: string) => void
  /** The operation key currently running (shows the spinner), if any. */
  runningKey: string | null
}

export function HealthDimensionCard({ dim, onRun, runningKey }: HealthDimensionCardProps) {
  // Coverage dimensions read as a percentage; gap dimensions read as count/total.
  const isCoverage = dim.coverage_pct != null
  const metricValue = isCoverage ? `${Math.round(dim.coverage_pct ?? 0)}%` : dim.count
  const metricHint = isCoverage
    ? `of ${dim.total.toLocaleString()} papers`
    : dim.total > 0
      ? `of ${dim.total.toLocaleString()}`
      : undefined

  return (
    <div className="flex flex-col gap-3 rounded-sm border border-alma-100 bg-alma-50 p-4 shadow-paper-sm">
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
          className="w-32 shrink-0 bg-alma-content-elev"
        />
        <div className="min-w-0 space-y-1 text-sm">
          <p className="text-slate-700">{dim.explanation}</p>
          {dim.impact ? <p className="text-xs text-slate-500">{dim.impact}</p> : null}
        </div>
      </div>

      {/* Coverage progress toward the 80% ready threshold. */}
      {isCoverage ? (
        <div className="h-1.5 w-full overflow-hidden rounded-full bg-alma-100" aria-hidden>
          <div
            className={cn(
              'h-full rounded-full',
              (dim.coverage_pct ?? 0) >= 80 ? 'bg-emerald-600' : 'bg-amber-500',
            )}
            style={{ width: `${Math.min(100, dim.coverage_pct ?? 0)}%` }}
          />
        </div>
      ) : null}

      {dim.actions.length > 0 ? (
        <div className="flex flex-wrap gap-2">
          {dim.actions.map((action) => {
            if (action.kind !== 'run_now') return null
            return (
              <AsyncButton
                key={action.operation_key}
                size="sm"
                variant="outline"
                icon={<Wrench className="h-4 w-4" />}
                pending={runningKey === action.operation_key}
                disabled={runningKey != null && runningKey !== action.operation_key}
                className="border-alma-200 text-alma-700 hover:bg-alma-100"
                onClick={() => onRun(action.operation_key)}
              >
                {action.label}
              </AsyncButton>
            )
          })}
        </div>
      ) : null}
    </div>
  )
}
