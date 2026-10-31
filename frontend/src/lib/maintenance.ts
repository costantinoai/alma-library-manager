import type { RunMaintenanceResponse } from '@/api/client'
import { describeJobLaunch, type LaunchMessage } from '@/lib/activity'

/**
 * How a `POST /health/operations/{key}/run` request ended, in words — for every
 * button that launches a maintenance task (Health's repair cards, the
 * drilldown's Fix selected, Settings' Reconcile Paper Groups).
 *
 * Only the outcomes particular to maintenance live here — the network switch,
 * a missing dependency and the provider daily cap, where nothing was launched
 * and the backend explains why. Started / already running / nothing to do are the
 * generic job-launch wording (`describeJobLaunch`), so a maintenance run and
 * any other background job read the same.
 */
export function describeMaintenanceLaunch(
  result: RunMaintenanceResponse,
  label: string,
): LaunchMessage {
  if (result.status === 'blocked_external') {
    return {
      title: 'External network access is off',
      description: result.message ?? 'Enable network access in Settings → Connections.',
    }
  }
  if (result.status === 'blocked_unavailable') {
    return {
      title: 'A required dependency is unavailable',
      description: result.message ?? 'Check the task\'s requirements in Settings.',
    }
  }
  if (result.status === 'skipped_daily_cap') {
    return {
      title: 'Daily API limit reached',
      description:
        result.message ?? 'The provider daily API quota is exhausted — try again after it resets.',
    }
  }
  if (!result.job_id && result.status !== 'already_running') {
    return { title: 'Nothing to run', description: result.message ?? 'No provider or no eligible items.' }
  }
  return describeJobLaunch(result, label)
}
