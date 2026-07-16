import type { BootstrapData } from '@/api/client'
import type { Page } from './Sidebar'

export function getNavBadgeCount(pageId: Page, data: BootstrapData): number {
  switch (pageId) {
    case 'feed':
      return data.feed.unread
    case 'discovery':
      return data.discovery.new_recommendations
    case 'alerts':
      return data.alerts.active
    default:
      return 0
  }
}
