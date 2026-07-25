/**
 * The `#/insights` shim. Insights split in two — analytics into Library ›
 * Analytics, operational Activity into Health — and old deep links live in
 * Alerts digests, Health popups and user bookmarks. Every one of them must
 * still land somewhere correct, which is exactly what this locks down.
 */
import { render, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it } from 'vitest'

import { InsightsRedirect } from './InsightsRedirect'

function goTo(hash: string) {
  window.location.hash = hash
  window.dispatchEvent(new HashChangeEvent('hashchange'))
}

describe('InsightsRedirect', () => {
  beforeEach(() => {
    window.location.hash = ''
  })

  it.each([
    ['#/insights?tab=stats', '#/library?tab=analytics&section=overview'],
    ['#/insights?tab=graph', '#/library?tab=analytics&section=map'],
    ['#/insights?tab=reports', '#/library?tab=analytics&section=reports'],
    // No tab at all → the default analytics section, never a dead end.
    ['#/insights', '#/library?tab=analytics&section=overview'],
    // An unknown tab must still land somewhere real.
    ['#/insights?tab=bogus', '#/library?tab=analytics&section=overview'],
  ])('sends %s to %s', async (from, to) => {
    goTo(from)
    render(<InsightsRedirect />)
    await waitFor(() => expect(window.location.hash).toBe(to))
  })

  it('sends the operational Activity tab to Health, preserving focus', async () => {
    // The Health "Background jobs" popup deep-links with ?focus=failed; losing
    // that param would drop the reader on the tab but not on the failure.
    goTo('#/insights?tab=activity&focus=failed')
    render(<InsightsRedirect />)
    await waitFor(() =>
      expect(window.location.hash).toBe('#/health?tab=activity&focus=failed'),
    )
  })

  it('carries focus through to the analytics sections too', async () => {
    goTo('#/insights?tab=stats&focus=coverage')
    render(<InsightsRedirect />)
    await waitFor(() =>
      expect(window.location.hash).toBe(
        '#/library?tab=analytics&section=overview&focus=coverage',
      ),
    )
  })
})
