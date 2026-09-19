import { describe, expect, it } from 'vitest'

import { describeJobLaunch } from '@/lib/activity'

describe('describeJobLaunch', () => {
  it('names a started job and points at Activity', () => {
    const m = describeJobLaunch({ status: 'queued', job_id: 'j1' }, 'Smart tagging')
    expect(m.title).toBe('Smart tagging started')
    expect(m.description).toContain('j1')
  })

  it('prefers the backend message, then the surface detail', () => {
    expect(describeJobLaunch({ status: 'queued', job_id: 'j1', message: 'Queued 3 papers' }, 'X').description)
      .toBe('Queued 3 papers')
    expect(describeJobLaunch({ status: 'queued', job_id: 'j1' }, 'X', { startedDetail: 'Soon.' }).description)
      .toBe('Soon.')
  })

  it('says already running, never "started", for an in-flight job', () => {
    expect(describeJobLaunch({ status: 'already_running', job_id: 'j2' }, 'Feed refresh').title)
      .toBe('Feed refresh already running')
  })

  it('reports a noop as nothing to run, with the backend reason', () => {
    const m = describeJobLaunch({ status: 'noop', message: 'Everything is resolved.' }, 'OpenAlex resolution')
    expect(m).toEqual({ title: 'Nothing to run', description: 'Everything is resolved.' })
  })
})
