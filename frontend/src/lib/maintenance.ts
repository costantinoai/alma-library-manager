import type { RunMaintenanceResponse } from '@/api/client'

export interface MaintenanceLaunchMessage {
  title: string
  description: string
}

/**
 * How a `POST /health/operations/{key}/run` request ended, in words — the ONE
 * mapping of `RunMaintenanceResponse.status` for every button that launches a
 * maintenance task (Health's repair cards, Settings' Reconcile Paper Groups).
 *
 * It lived inline in HealthPage; the Settings button re-derived it from
 * `job_id` alone and would have called a network-blocked or quota-capped run
 * "Nothing to reconcile". `label` names the operation for the started /
 * already-running cases; the backend's `message`, when present, wins.
 */
export function describeMaintenanceLaunch(
  result: RunMaintenanceResponse,
  label: string,
): MaintenanceLaunchMessage {
  switch (result.status) {
    case 'blocked_external':
      return {
        title: 'External network access is off',
        description: result.message ?? 'Enable network access in Settings → Connections.',
      }
    case 'skipped_daily_cap':
      return {
        title: 'Daily API limit reached',
        description:
          result.message ?? 'The provider daily API quota is exhausted — try again after it resets.',
      }
    case 'already_running':
      return {
        title: `${label} already running`,
        description: result.job_id
          ? `Job ${result.job_id} is still going. Track it in Activity.`
          : 'Track it in Activity.',
      }
  }
  if (result.status === 'noop' || !result.job_id) {
    return { title: 'Nothing to run', description: result.message ?? 'No provider or no eligible items.' }
  }
  return {
    title: `${label} started`,
    description: `${result.key} queued (${result.job_id}). Track it in Activity.`,
  }
}
