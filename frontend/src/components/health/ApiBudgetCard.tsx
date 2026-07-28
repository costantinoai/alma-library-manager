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
import { Switch } from '@/components/ui/switch'

type Tone = 'neutral' | 'info' | 'warning' | 'critical'

interface ApiBudgetCardProps {
  budget: HealthOperationsResponse['api_budget']
  networkChangePending?: boolean
  onNetworkChange?: (enabled: boolean) => void
}

export function ApiBudgetCard({
  budget,
  networkChangePending = false,
  onNetworkChange,
}: ApiBudgetCardProps) {
  if (!budget) return null
  const remaining = budget.openalex_credits_remaining
  const reserve = budget.reserved_user_calls
  const abort = budget.last_credit_abort
  const pause = budget.last_pause
  const policy = budget.network_policy

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
      {policy ? (
        <div className="flex items-center justify-between gap-4 rounded-sm border border-[var(--color-border)] bg-surface-1 px-3 py-2.5">
          <div className="min-w-0">
            <p className="text-sm font-medium text-slate-800">External network access</p>
            <p className="text-xs text-slate-500">
              {policy.forced_off_by_env
                ? 'Off by operations override'
                : policy.enabled
                  ? 'On — external APIs and integrations may connect'
                  : 'Off — all outbound transports are blocked'}
            </p>
          </div>
          <Switch
            checked={policy.enabled}
            disabled={networkChangePending || policy.forced_off_by_env || !onNetworkChange}
            onCheckedChange={onNetworkChange}
            aria-label="Allow external network access from Health"
          />
        </div>
      ) : null}
      {policy?.enabled === false ? (
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {policy.forced_off_by_env
              ? 'External network access is disabled by an operations override. Remove the ALMA_DISABLE_NETWORK override to use this switch.'
              : 'External network access is off. API, integration, and hosted-AI operations are blocked until enabled with the switch above.'}
          </AlertDescription>
        </Alert>
      ) : null}
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
