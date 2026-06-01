import { useQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'

import { api } from '@/api/client'
import { isBackgroundTriggerSource } from '@/lib/activity'

interface ActivityOp {
  job_id: string
  status: string
  operation_key?: string
  trigger_source?: string
  message?: string
  parent_job_id?: string
}

/**
 * U-1: a persistent in-page banner shown while a Discovery lens refresh or a
 * Feed inbox refresh is still running in the background.
 *
 * The refresh POST returns in ~100 ms (the work runs in the APS pool), so
 * without this the multi-minute job was invisible after the button settled —
 * users couldn't tell whether anything was happening. This rides the SAME
 * shared `['activity-operations']` poll that `useOperationToasts` /
 * `ActivityPanel` already use (React Query dedups all observers to one request),
 * so it adds no network cost and clears itself the moment the job goes terminal
 * — at which point `useOperationToasts` raises the single outcome toast and
 * invalidates the list query, swapping in the fresh results.
 */
export function RefreshRunningBanner({
  domain,
  label,
}: {
  domain: 'discovery' | 'feed'
  label: string
}) {
  const { data } = useQuery({
    queryKey: ['activity-operations'],
    queryFn: () => api.get<ActivityOp[]>('/activity'),
    refetchInterval: 12000,
  })

  const prefix = domain === 'discovery' ? 'discovery.' : 'feed.'
  const running = (data ?? []).find(
    (op) =>
      (op.operation_key ?? '').startsWith(prefix) &&
      !op.parent_job_id &&
      (op.status === 'running' || op.status === 'queued') &&
      !isBackgroundTriggerSource(op.trigger_source),
  )
  if (!running) return null

  const detail =
    running.message?.trim() ||
    'Running in the background — this can take about a minute. You’ll get a notification when it finishes.'

  return (
    <div
      role="status"
      aria-live="polite"
      className="flex items-start gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-3 text-sm"
    >
      <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-alma-folio" aria-hidden />
      <p className="min-w-0 flex-1 leading-snug">
        <span className="font-medium text-alma-900">{label}</span>{' '}
        <span className="text-slate-500">{detail}</span>
      </p>
    </div>
  )
}
