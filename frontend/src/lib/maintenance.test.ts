import { describe, expect, it } from 'vitest'

import type { RunMaintenanceResponse } from '@/api/client'
import { describeMaintenanceLaunch } from '@/lib/maintenance'

const run = (patch: Partial<RunMaintenanceResponse>): RunMaintenanceResponse => ({
  key: 'paper_group_reconcile',
  status: 'queued',
  job_id: 'job_1',
  message: null,
  plan: {} as RunMaintenanceResponse['plan'],
  ...patch,
})

describe('describeMaintenanceLaunch', () => {
  it('names a started run and points at Activity', () => {
    const m = describeMaintenanceLaunch(run({}), 'Paper-group reconcile')
    expect(m.title).toBe('Paper-group reconcile started')
    expect(m.description).toContain('job_1')
  })

  it('says an in-flight run is already running, not "started"', () => {
    expect(describeMaintenanceLaunch(run({ status: 'already_running' }), 'Reconcile').title).toBe(
      'Reconcile already running',
    )
  })

  it('never calls a blocked run "nothing to do"', () => {
    const blocked = describeMaintenanceLaunch(
      run({ status: 'blocked_external', job_id: null, message: 'External network access is disabled in Settings.' }),
      'Reconcile',
    )
    expect(blocked.title).toBe('External network access is off')
    expect(blocked.description).toBe('External network access is disabled in Settings.')

    const capped = describeMaintenanceLaunch(run({ status: 'skipped_daily_cap', job_id: null }), 'Reconcile')
    expect(capped.title).toBe('Daily API limit reached')
  })

  it('reports a noop as nothing to run', () => {
    expect(describeMaintenanceLaunch(run({ status: 'noop', job_id: null }), 'Reconcile').title).toBe(
      'Nothing to run',
    )
  })
})
