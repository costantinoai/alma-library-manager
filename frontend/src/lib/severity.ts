import type { StatusBadgeTone } from '@/components/ui/status-badge'

/**
 * THE severity vocabulary: `critical | warning | info | ok`.
 *
 * Canonically produced by `alma.services.health`, but it is not a Health-page
 * idea — anything that grades "how urgent is this" speaks it (Home's needs-you
 * panel, diagnostics rows, subsystem cards). It lives in `lib/` rather than
 * inside the Health components so a second surface can use it without either
 * importing upward into a feature folder or writing a second map that is free
 * to disagree.
 *
 * Distinct from the shared `severityTone()` in `status-badge-tones.ts`, which
 * folds everything non-critical/non-warning into `info` and so renders the
 * healthy `ok` state as an informational blue.
 */
export type Severity = 'critical' | 'warning' | 'info' | 'ok'

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

/** `StatusBadge` tone for a severity — `ok` maps to positive, not info. */
export function dimensionBadgeTone(severity?: string | null): StatusBadgeTone {
  if (severity === 'critical') return 'negative'
  if (severity === 'warning') return 'warning'
  if (severity === 'ok') return 'positive'
  return 'info'
}

/** The word shown on the badge. `ok` reads "healthy" — a state, not an ack. */
export function severityLabel(severity?: string | null): string {
  if (severity === 'ok') return 'healthy'
  return severity ?? 'unknown'
}
