/**
 * Shared formatting + severity helpers for the Health page.
 *
 * Severity vocabulary is the canonical one from `alma.services.health`:
 * "ok" | "info" | "warning" | "critical". The shared `severityTone()` in
 * status-badge maps everything non-critical/non-warning to `info`, which is
 * wrong for the healthy "ok" state — so the Health surface uses the explicit
 * maps below (ok → positive / success).
 */
import type { MetricTileTone } from '@/components/shared/MetricTile'
import type { StatusBadgeTone } from '@/components/ui/status-badge'
import type { HealthDimension, HealthSnapshot, MaintenanceOperation } from '@/api/client'

type Severity = HealthDimension['severity']

const SEVERITY_RANK: Record<string, number> = {
  critical: 0,
  warning: 1,
  info: 2,
  ok: 3,
}

/** Lower = surface first. Unknown severities sort last. */
export function severityRank(severity?: string | null): number {
  return SEVERITY_RANK[severity ?? ''] ?? 9
}

/** MetricTile value tone for a severity (healthy "ok" → success/emerald). */
export function severityMetricTone(severity?: string | null): MetricTileTone {
  if (severity === 'critical') return 'critical'
  if (severity === 'warning') return 'warning'
  if (severity === 'ok') return 'success'
  return 'info'
}

/** StatusBadge tone for a dimension severity — maps "ok" → positive (unlike
 * the shared severityTone(), which would render "ok" as info). */
export function dimensionBadgeTone(severity?: string | null): StatusBadgeTone {
  if (severity === 'critical') return 'negative'
  if (severity === 'warning') return 'warning'
  if (severity === 'ok') return 'positive'
  return 'info'
}

export function severityLabel(severity?: string | null): string {
  if (severity === 'ok') return 'healthy'
  return severity ?? 'unknown'
}

/** StatusBadge tone for a maintenance job's last-run status. */
export function runStatusTone(status?: string | null): StatusBadgeTone {
  if (status === 'completed') return 'positive'
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

/**
 * Dimension keys whose affected-papers drilldown is backed by the API.
 * Mirrors `DIMENSION_ITEM_KEYS` in `src/alma/services/health.py` — keep in
 * sync. Author dimensions are deliberately absent: they're repaired by running
 * the op, not by drilling into individual paper rows, so their status rows
 * render read-only (no "view →").
 */
export const DRILLDOWN_DIM_KEYS: ReadonlySet<string> = new Set([
  'identity.unresolved',
  'papers.missing_abstract',
  'papers.missing_doi',
  'papers.missing_url',
  'papers.missing_publication_date',
  'papers.missing_authorships',
  'papers.missing_topics',
  'papers.missing_references',
  'embeddings.local_computable',
  'embeddings.s2_vector_missing',
  'embeddings.coverage',
  'ledger.retry_waiting',
])

/** Whether a dimension exposes the affected-papers drilldown. */
export function canDrilldown(key: string): boolean {
  return DRILLDOWN_DIM_KEYS.has(key)
}

/** Worst-first ordering (critical → warning → info → ok), then larger count first. */
export function sortBySeverity(dims: HealthDimension[]): HealthDimension[] {
  return [...dims].sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity) || (b.count ?? 0) - (a.count ?? 0),
  )
}

/** A dimension is "needs attention" when it is anything but healthy. */
export function isAttention(dim: HealthDimension): boolean {
  return dim.severity !== 'ok'
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
 * An op "needs attention" when any dimension it repairs is non-healthy, or —
 * for a dimension-less cleanup op — when it has items pending. Healthy + idle
 * ops collapse into the group's "All clear" strip.
 */
export function isOpAttention(op: MaintenanceOperation, dims: HealthDimension[]): boolean {
  const sev = opSeverity(dims)
  if (sev != null) return sev !== 'ok'
  return op.candidates_pending > 0
}

/** Worst-first ordering for op cards: severity, then larger pending first. */
export function sortOpsByAttention(
  ops: MaintenanceOperation[],
  dimsOf: (op: MaintenanceOperation) => HealthDimension[],
): MaintenanceOperation[] {
  return [...ops].sort((a, b) => {
    const ra = severityRank(opSeverity(dimsOf(a)) ?? 'ok')
    const rb = severityRank(opSeverity(dimsOf(b)) ?? 'ok')
    return ra - rb || b.candidates_pending - a.candidates_pending
  })
}
