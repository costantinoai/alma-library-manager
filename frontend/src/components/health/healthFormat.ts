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
import type { HealthDimension } from '@/api/client'

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

/** Cost-class label for a maintenance task. */
export const COST_LABEL: Record<string, string> = {
  cheap: 'local',
  network: 'network',
  compute: 'compute',
}

/** Worst-first ordering (critical → warning → info → ok), then larger count first. */
export function sortBySeverity(dims: HealthDimension[]): HealthDimension[] {
  return [...dims].sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity) || b.count - a.count,
  )
}

/** A dimension is "needs attention" when it is anything but healthy. */
export function isAttention(dim: HealthDimension): boolean {
  return dim.severity !== 'ok'
}
