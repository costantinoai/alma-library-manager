/**
 * Health-specific formatting helpers.
 *
 * The SEVERITY vocabulary itself (`critical | warning | info | ok`, its rank,
 * badge tone and label) moved to `lib/severity.ts` when Home started speaking
 * it too — it was never Health-specific, and a second copy would have been
 * free to disagree. Re-exported here so existing Health call sites keep one
 * import, but `lib/severity` is the definition.
 */
import type { MetricTileTone } from '@/components/shared/MetricTile'
import type { StatusBadgeTone } from '@/components/ui/status-badge'
import type { HealthDimension, HealthSnapshot, MaintenanceOperation } from '@/api/client'

import { severityRank } from '@/lib/severity'

export { dimensionBadgeTone, severityLabel, severityRank } from '@/lib/severity'

type Severity = HealthDimension['severity']

/** MetricTile value tone for a severity (healthy "ok" → success/emerald). */
export function severityMetricTone(severity?: string | null): MetricTileTone {
  if (severity === 'critical') return 'critical'
  if (severity === 'warning') return 'warning'
  if (severity === 'ok') return 'success'
  return 'info'
}

/** StatusBadge tone for a maintenance job's last-run status. */
export function runStatusTone(status?: string | null): StatusBadgeTone {
  if (status === 'completed') return 'positive'
  if (status === 'partial') return 'warning'
  if (status === 'failed') return 'negative'
  if (status === 'running' || status === 'queued' || status === 'scheduled') return 'info'
  return 'neutral'
}

/** Cost-class label for a maintenance task. */
export const COST_LABEL: Record<string, string> = {
  cheap: 'local',
  network: 'network',
  compute: 'compute',
}

/** Worst-first ordering (critical → warning → info → ok), then larger count first. */
export function sortBySeverity(dims: HealthDimension[]): HealthDimension[] {
  return [...dims].sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity) || (b.count ?? 0) - (a.count ?? 0),
  )
}

/**
 * H-8: an honest one-line freshness note for the unified snapshot. Corpus and
 * author health are SEPARATE materialized views, so when only one is
 * rebuilding/stale we name WHICH — never a blanket "updating…" that hides a
 * fresh part or implies the whole page is stale. Returns null when both are
 * current. (Falls back to the flat `rebuilding` flag on an older payload with no
 * per-view metadata.)
 */
export function freshnessNote(snapshot: HealthSnapshot): string | null {
  const v = snapshot.views
  if (!v) return snapshot.rebuilding ? 'updating…' : null
  const label = (k: 'corpus' | 'authors') => (k === 'authors' ? 'author health' : 'corpus')
  const updating = (['corpus', 'authors'] as const).filter((k) => v[k].rebuilding || v[k].stale)
  if (updating.length === 2) return 'updating…'
  if (updating.length === 1) return `${label(updating[0])} updating…`
  return null
}

/**
 * Worst severity across the dimensions one operation repairs (lower rank =
 * worse). Returns `null` for a dimension-less cleanup op (gc / preprint dedup),
 * which carries no severity — only a pending count.
 */
export function opSeverity(dims: HealthDimension[]): Severity | null {
  if (dims.length === 0) return null
  return dims.reduce<Severity>(
    (worst, d) => (severityRank(d.severity) < severityRank(worst) ? d.severity : worst),
    'ok',
  )
}

/**
 * Backend owns whether pending work is a health defect or normal pipeline work.
 * This keeps the UI from turning routine freshness queues into maintenance alarms.
 */
export function isOpAttention(op: MaintenanceOperation, dims: HealthDimension[]): boolean {
  void dims
  return op.attention_required
}

/** Worst-first ordering for op cards: severity, then larger pending first. */
export function sortOpsByAttention(
  ops: MaintenanceOperation[],
  dimsOf: (op: MaintenanceOperation) => HealthDimension[],
): MaintenanceOperation[] {
  return [...ops].sort((a, b) => {
    const ra = severityRank(opSeverity(dimsOf(a)) ?? 'ok')
    const rb = severityRank(opSeverity(dimsOf(b)) ?? 'ok')
    return ra - rb || (b.candidates_pending ?? Number.POSITIVE_INFINITY) - (a.candidates_pending ?? Number.POSITIVE_INFINITY)
  })
}
