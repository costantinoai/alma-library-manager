import { useEffect } from 'react'

import { navigateTo, useHashRoute } from '@/lib/hashRoute'

// The Insights page retired into Library › Analytics (task 47 Phase 4, 47-C)
// and its operational Activity tab into Health (Phase 5). This shim keeps every
// old `#/insights?tab=…` deep link (Alerts digests, Health status popups,
// bookmarks) landing where the content actually lives now.
const SECTION_MAP: Record<string, string> = {
  stats: 'overview',
  reports: 'reports',
}

export function InsightsRedirect() {
  const route = useHashRoute()
  useEffect(() => {
    const tab = route.params.get('tab')?.trim() ?? 'stats'
    const focus = route.params.get('focus')?.trim()
    // Operational telemetry moved to Health, analytics to Library (Phase 5).
    if (tab === 'activity') {
      const params: Record<string, string> = { tab: 'activity' }
      if (focus) params.focus = focus
      navigateTo('health', params)
      return
    }
    // Task 50 M3 (50-A): the graph moved again — to the top-level Map page.
    if (tab === 'graph') {
      navigateTo('map')
      return
    }
    const params: Record<string, string> = {
      tab: 'analytics',
      section: SECTION_MAP[tab] ?? 'overview',
    }
    if (focus) params.focus = focus
    navigateTo('library', params)
  }, [route.params])

  return (
    <div className="py-16 text-center text-sm text-slate-500">Redirecting to Library › Analytics…</div>
  )
}
