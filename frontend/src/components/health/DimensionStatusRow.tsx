/**
 * DimensionStatusRow — a `StatusRow` adapter for a health dimension. Builds the
 * metric (count/total, or a coverage % with a mini progress bar) and makes the
 * row clickable so you can drill into the affected items:
 *   - **paper** dimensions open the affected-papers drilldown modal (`onOpen`);
 *   - **author** dimensions (`authors.*`) jump to the Authors page, whose
 *     needs-attention section is the canonical place to repair/merge them —
 *     reusing that surface rather than duplicating author management here.
 *
 * Used by `RepairCard` (the gaps an op repairs) and `DiagnosticsSection` (the
 * observed-only dimensions with no repair op).
 */
import { cn } from '@/lib/utils'
import { navigateTo } from '@/lib/hashRoute'
import type { HealthDimension } from '@/api/client'
import { canDrilldown } from './healthFormat'
import { StatusRow } from './StatusRow'

export function DimensionStatusRow({ dim, onOpen }: { dim: HealthDimension; onOpen: () => void }) {
  const isAuthorDim = dim.key.startsWith('authors.')
  const isCoverage = dim.coverage_pct != null
  const pct = Math.round(dim.coverage_pct ?? 0)

  const metric = isCoverage ? (
    <span className="flex items-center gap-2">
      <span className="h-1.5 w-16 overflow-hidden rounded-full bg-parchment-200" aria-hidden>
        <span
          className={cn('block h-full rounded-full', pct >= 80 ? 'bg-success-600' : 'bg-warning-500')}
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

  // Paper dims → drilldown modal; author dims → the Authors page's
  // needs-attention section. We pass ?focus=needs-attention because that
  // section sits BELOW the corpus table: a bare navigation lands above the
  // fold and the gap reads as "nothing to fix". The param tells AuthorsPage to
  // scroll it into view and flash it, so the drilldown lands on the card that
  // actually resolves the gap (retry / pick affiliation / accept-unidentified).
  const handleOpen = canDrilldown(dim.key)
    ? onOpen
    : isAuthorDim
      ? () => navigateTo('authors', { focus: 'needs-attention' })
      : undefined

  return <StatusRow severity={dim.severity} label={dim.label} metric={metric} onOpen={handleOpen} />
}
