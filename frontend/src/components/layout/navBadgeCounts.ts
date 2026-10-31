import type { BootstrapData } from '@/api/client'
import type { Page } from './Sidebar'

export function getNavBadgeCount(pageId: Page | string, data: BootstrapData): number {
  switch (pageId) {
    case 'feed':
      return data.feed.unread
    case 'discovery':
      return data.discovery.new_recommendations
    default:
      return data.addons.find((addon) => addon.id === pageId)?.badge ?? 0
  }
}
