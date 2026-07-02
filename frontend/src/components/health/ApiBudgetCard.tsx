/**
 * ApiBudgetCard — the Health page's external-API budget surface (task 37 B/C).
 *
 * Shows the live remaining OpenAlex daily quota (with the reserve we keep for the
 * user's own manual operations), and — when a background sweep recently stopped to
 * protect that reserve — a truthful "last operation aborted due to credit limit"
 * line with the credit count captured at abort time. Fed from
 * GET /health/operations → `api_budget`.
 */
import { AlertTriangle } from 'lucide-react'

import type { HealthOperationsResponse } from '@/api/client'
import { MetricTile } from '@/components/shared/MetricTile'
import { Alert, AlertDescription } from '@/components/ui/alert'

type Tone = 'neutral' | 'info' | 'warning' | 'critical'

export function ApiBudgetCard({ budget }: { budget: HealthOperationsResponse['api_budget'] }) {
  if (!budget) return null
  const remaining = budget.openalex_credits_remaining
  const reserve = budget.reserved_user_calls
  const abort = budget.last_credit_abort
  const pause = budget.last_pause

  // Tone tracks headroom: critical at/below the reserve, warning within 2× of it.
  const tone: Tone =
    remaining == null
      ? 'neutral'
      : remaining <= reserve
        ? 'critical'
        : remaining <= reserve * 2
          ? 'warning'
          : 'info'

  return (
    <div className="space-y-2">
      <MetricTile
        tone={tone}
        label="OpenAlex API budget today"
        value={remaining == null ? '—' : remaining.toLocaleString()}
        hint={
          remaining == null
            ? 'Unknown until the first request this session'
            : `${reserve.toLocaleString()} reserved for your manual operations`
        }
      />
      {abort ? (
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            A background operation stopped to preserve your credit reserve
            {abort.openalex_credits_remaining != null
              ? ` (${abort.openalex_credits_remaining.toLocaleString()} credits left)`
              : ''}
            {abort.finished_at ? ` · ${new Date(abort.finished_at).toLocaleString()}` : ''}. It
            will resume automatically once the quota recovers.
          </AlertDescription>
        </Alert>
      ) : null}
      {/* 42.6: a background op that yielded to user activity — informational, so a
          paused system reads as "paused, resumes when idle", not stalled. */}
      {pause ? (
        <p className="text-xs text-slate-500">
          Background enrichment paused for your activity
          {pause.finished_at ? ` at ${new Date(pause.finished_at).toLocaleTimeString()}` : ''} — it
          resumes automatically when the app is idle.
        </p>
      ) : null}
    </div>
  )
}
