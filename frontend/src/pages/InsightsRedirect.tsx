import { useEffect } from 'react'

import { navigateTo, useHashRoute } from '@/lib/hashRoute'

// The Insights page retired into Library › Analytics (task 47 Phase 4, 47-C).
// This shim keeps every old `#/insights?tab=…` deep link (Alerts digests,
// Health status popups, bookmarks) landing on the right Analytics section.
// Phase 5 will re-point the `activity` subtab at the Health page.
const SECTION_MAP: Record<string, string> = {
  stats: 'overview',
  graph: 'map',
  activity: 'activity',
  reports: 'reports',
}

export function InsightsRedirect() {
  const route = useHashRoute()
  useEffect(() => {
    const tab = route.params.get('tab')?.trim() ?? 'stats'
    const focus = route.params.get('focus')?.trim()
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
