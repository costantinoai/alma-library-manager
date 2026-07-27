import { useEffect, useState } from 'react'

import type { Page } from '@/components/layout/AppShell'

// Record<Page, true> so tsc fails when a new Page is added but not routed —
// a missing entry here made "#/map" silently parse as "home" (nav needed two
// clicks to land on the Map page).
const PAGE_ROUTES: Record<Page, true> = {
  home: true,
  feed: true,
  discovery: true,
  map: true,
  authors: true,
  library: true,
  insights: true,
  health: true,
  alerts: true,
  settings: true,
}

const VALID_PAGES = Object.keys(PAGE_ROUTES) as Page[]

export interface HashRoute {
  page: Page
  found: boolean
  params: URLSearchParams
  raw: string
}

export function parseHashRoute(rawHash?: string): HashRoute {
  const raw = typeof rawHash === 'string' ? rawHash : window.location.hash
  const withoutHash = raw.startsWith('#') ? raw.slice(1) : raw
  const normalized = withoutHash.startsWith('/') ? withoutHash.slice(1) : withoutHash
  const [pagePart, queryPart = ''] = normalized.split('?', 2)
  // An empty hash lands on Home (task 47 Phase 6); Feed stays one click away.
  const pageCandidate = pagePart || 'home'
  const found = VALID_PAGES.includes(pageCandidate as Page)
  const page = found ? (pageCandidate as Page) : 'home'
  return {
    page,
    found,
    params: new URLSearchParams(queryPart),
    raw,
  }
}

export function buildHashRoute(
  page: Page,
  params?: Record<string, string | number | boolean | null | undefined>,
): string {
  const qs = new URLSearchParams()
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value === null || value === undefined || value === '') continue
    qs.set(key, String(value))
  }
  const query = qs.toString()
  return `#/${page}${query ? `?${query}` : ''}`
}

/**
 * Navigate to a hash route imperatively. Collapses the repeated
 * `window.location.hash = buildHashRoute(page, params)` pattern used in
 * click handlers throughout the app.
 *
 * In SSR / test contexts where `window` is undefined, the call is a no-op
 * (matches how the surrounding onClick handlers already behave).
 */
export function navigateTo(
  page: Page,
  params?: Record<string, string | number | boolean | null | undefined>,
): void {
  if (typeof window === 'undefined') return
  window.location.hash = buildHashRoute(page, params)
}

export function useHashRoute(): HashRoute {
  const [route, setRoute] = useState<HashRoute>(() => parseHashRoute())

  useEffect(() => {
    const onHashChange = () => setRoute(parseHashRoute())
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  return route
}
