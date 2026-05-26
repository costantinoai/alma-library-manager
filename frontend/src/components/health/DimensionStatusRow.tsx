/**
 * DimensionStatusRow — a `StatusRow` adapter for a health dimension. Builds the
 * metric (count/total, or a coverage % with a mini progress bar) and makes the
 * row clickable only when the dimension has a backed affected-papers drilldown
 * (author dimensions are fixed by running the op, not by drilling into rows).
 *
 * Used by `RepairCard` (the gaps an op repairs) and `DiagnosticsSection` (the
 * observed-only dimensions with no repair op).
 */
import { cn } from '@/lib/utils'
import type { HealthDimension } from '@/api/client'
import { canDrilldown } from './healthFormat'
import { StatusRow } from './StatusRow'

export function DimensionStatusRow({ dim, onOpen }: { dim: HealthDimension; onOpen: () => void }) {
  const isCoverage = dim.coverage_pct != null
  const pct = Math.round(dim.coverage_pct ?? 0)

  const metric = isCoverage ? (
    <span className="flex items-center gap-2">
      <span className="h-1.5 w-16 overflow-hidden rounded-full bg-parchment-200" aria-hidden>
        <span
          className={cn('block h-full rounded-full', pct >= 80 ? 'bg-emerald-600' : 'bg-amber-500')}
          style={{ width: `${Math.min(100, pct)}%` }}
        />
      </span>
      <span className="text-xs tabular-nums text-slate-600">{pct}%</span>
    </span>
  ) : (
    <span className="flex shrink-0 items-center gap-1.5 text-xs tabular-nums text-slate-600">
      <span>
        {dim.count.toLocaleString()}
        {dim.total > 0 ? <span className="text-slate-400"> / {dim.total.toLocaleString()}</span> : null}
      </span>
      {dim.exhausted ? (
        <span
          className="font-normal text-slate-400"
          title="Tried — no automatic fix available (e.g. Semantic Scholar has no vector for these). Only local compute can help."
        >
          · {dim.exhausted.toLocaleString()} no fix
        </span>
      ) : null}
    </span>
  )

  return (
    <StatusRow
      severity={dim.severity}
      label={dim.label}
      metric={metric}
      onOpen={canDrilldown(dim.key) ? onOpen : undefined}
    />
  )
}
