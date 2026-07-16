import { describe, expect, it } from 'vitest'

import { getNavBadgeCount } from './navBadgeCounts'

describe('getNavBadgeCount', () => {
  const bootstrap = {
    library: { papers: 10, candidates: 20, authors: 3, followed_authors: 2, collections: 4, tags: 5 },
    feed: { unread: 2 },
    discovery: { active_lenses: 2, pending_recommendations: 99, new_recommendations: 3 },
    alerts: { active: 1 },
    app: { version: 'test' },
    onboarding: { completed: true, has_owner: true },
  }

  it('uses only latest-refresh New counts for Feed and Discovery', () => {
    expect(getNavBadgeCount('feed', bootstrap)).toBe(2)
    expect(getNavBadgeCount('discovery', bootstrap)).toBe(3)
  })
})
