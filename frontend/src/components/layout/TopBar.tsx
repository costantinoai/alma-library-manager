import { Menu, RefreshCw, Search } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { BrandRule } from '@/components/ui/brand-rule'
import { cn } from '@/lib/utils'
import type { Page } from './Sidebar'

const pageTitles: Record<Page, string> = {
  feed: 'Feed',
  discovery: 'Discovery',
  authors: 'Authors',
  library: 'Library',
  insights: 'Insights',
  alerts: 'Alerts',
  settings: 'Settings',
}

function isMacPlatform(): boolean {
  if (typeof navigator === 'undefined') return false
  const platform = (navigator as Navigator & { userAgentData?: { platform?: string } })
  const ua = platform.userAgentData?.platform || navigator.platform || navigator.userAgent || ''
  return /Mac|iPhone|iPad|iPod/i.test(ua)
}

interface TopBarProps {
  currentPage: Page
  onMenuClick: () => void
  onRefresh?: () => void
  isRefreshing?: boolean
  onOpenCommandPalette?: () => void
}

export function TopBar({
  currentPage,
  onMenuClick,
  onRefresh,
  isRefreshing,
  onOpenCommandPalette,
}: TopBarProps) {
  const shortcutKey = isMacPlatform() ? '⌘K' : 'Ctrl K'

  return (
    <header className="sticky top-0 z-30 flex h-16 items-center justify-between border-b border-[var(--color-border)] bg-alma-chrome px-4 lg:px-6 shadow-paper-sm">
      <div className="flex items-center gap-4">
        <button
          onClick={onMenuClick}
          className="rounded-md p-2 text-alma-700 hover:bg-parchment-100 hover:text-alma-900 lg:hidden"
          aria-label="Open navigation"
        >
          <Menu className="h-5 w-5" />
        </button>
        {/* Page title with editorial gold rule + diamond underline,
            mirroring the wordmark's separator pattern. Width-capped to
            the title text via inline-block so the rule reads as
            decoration on this title only, not a bar across the bar. */}
        <div className="flex flex-col items-start gap-1">
          <h1 className="font-brand text-xl font-semibold tracking-wide text-alma-800">
            {pageTitles[currentPage]}
          </h1>
          <BrandRule center="diamond" className="w-full max-w-[140px]" />
        </div>
      </div>

      <div className="flex items-center gap-2">
        {onOpenCommandPalette && (
          <button
            type="button"
            onClick={onOpenCommandPalette}
            className="hidden items-center gap-2 rounded-md border border-[var(--color-border)] bg-alma-chrome px-2.5 py-1.5 text-xs text-alma-700 transition-colors hover:border-parchment-400 hover:text-alma-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio sm:flex"
            aria-label="Open command palette"
            title="Open command palette"
          >
            <Search className="h-3.5 w-3.5" />
            <span>Search</span>
            <kbd className="ml-1 rounded border border-[var(--color-border)] bg-parchment-100 px-1.5 font-sans text-[10px] font-medium text-alma-700">
              {shortcutKey}
            </kbd>
          </button>
        )}
        {onRefresh && (
          <Button
            variant="ghost"
            size="icon"
            onClick={onRefresh}
            disabled={isRefreshing}
            aria-label="Refresh"
          >
            <RefreshCw className={cn('h-4 w-4', isRefreshing && 'animate-spin')} />
          </Button>
        )}
      </div>
    </header>
  )
}
