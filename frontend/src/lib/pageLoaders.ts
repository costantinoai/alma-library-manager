import type { ComponentType } from 'react'

import type { Page } from '@/components/layout/AppShell'

type PageModule = { default: ComponentType }
type PageLoader = () => Promise<PageModule>

/** One chunk-loader registry shared by React.lazy and navigation prefetch. */
export const pageLoaders = {
  home: () => import('@/pages/HomePage').then((m) => ({ default: m.HomePage })),
  feed: () => import('@/pages/FeedPage').then((m) => ({ default: m.FeedPage })),
  discovery: () =>
    import('@/pages/DiscoveryPage').then((m) => ({ default: m.DiscoveryPage })),
  authors: () =>
    import('@/pages/AuthorsPage').then((m) => ({ default: m.AuthorsPage })),
  map: () => import('@/pages/MapPage').then((m) => ({ default: m.MapPage })),
  library: () =>
    import('@/pages/LibraryPage').then((m) => ({ default: m.LibraryPage })),
  insights: () =>
    import('@/pages/InsightsRedirect').then((m) => ({
      default: m.InsightsRedirect,
    })),
  health: () =>
    import('@/pages/HealthPage').then((m) => ({ default: m.HealthPage })),
  alerts: () =>
    import('@/pages/AlertsPage').then((m) => ({ default: m.AlertsPage })),
  settings: () =>
    import('@/pages/SettingsPage').then((m) => ({ default: m.SettingsPage })),
} satisfies Record<Page, PageLoader>

export function preloadPage(page: Page): Promise<PageModule> {
  return pageLoaders[page]()
}
