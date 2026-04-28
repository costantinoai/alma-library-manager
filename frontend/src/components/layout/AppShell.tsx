import { useState, useEffect, type ReactNode } from 'react'
import { Sidebar, type Page } from './Sidebar'
import { TopBar } from './TopBar'
import { ActivityPanel } from '@/components/ActivityPanel'
import { CommandPalette } from '@/components/CommandPalette'
import { useOperationToasts } from '@/hooks/useOperationToasts'

interface AppShellProps {
  currentPage: Page
  onNavigate: (page: Page) => void
  onRefresh?: () => void
  isRefreshing?: boolean
  children: ReactNode
}

const SIDEBAR_COLLAPSED_KEY = 'alma.sidebar.collapsed'

function readInitialCollapsed(): boolean {
  if (typeof window === 'undefined') return false
  try {
    return window.localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1'
  } catch {
    return false
  }
}

export function AppShell({
  currentPage,
  onNavigate,
  onRefresh,
  isRefreshing,
  children,
}: AppShellProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => readInitialCollapsed())
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false)

  // Persist sidebar collapse preference. localStorage write is cheap
  // and synchronous; doing it on every change keeps the next reload
  // honest without needing a debounce.
  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      window.localStorage.setItem(SIDEBAR_COLLAPSED_KEY, sidebarCollapsed ? '1' : '0')
    } catch {
      // Storage unavailable (private mode, quota, etc.) — fail silently;
      // the in-memory state still works for the current session.
    }
  }, [sidebarCollapsed])

  // Monitor operations and show toast notifications for completions/failures
  useOperationToasts()

  // Global keyboard shortcut: Ctrl+K or Cmd+K
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault()
        setCommandPaletteOpen(true)
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [])

  const handleCommandPaletteNavigate = (url: string) => {
    const normalized = url.startsWith('#') ? url : `#/${url.replace(/^\/+/, '')}`
    window.location.hash = normalized
  }

  return (
    // No bg here on purpose — body in index.css sets the paper-warm
    // background plus the SVG fiber-grain tile. Putting bg-alma-paper
    // on this wrapper would hide the texture (solid color over the
    // tiled bg). Surfaces above (sidebar nav area, cards, top bar)
    // cover the texture explicitly via their own bg.
    <div className="min-h-screen">
      <Sidebar
        currentPage={currentPage}
        onNavigate={onNavigate}
        isOpen={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
        collapsed={sidebarCollapsed}
        onToggleCollapsed={() => setSidebarCollapsed((prev) => !prev)}
      />

      {/* Main content area shifts to clear the fixed sidebar. The
          left padding tracks the sidebar's actual desktop width
          (260px expanded, 72px collapsed) so they stay flush. The
          transition runs on the same 200ms curve as the sidebar
          width change for a unified motion. */}
      <div
        className={`transition-[padding] duration-200 ${
          sidebarCollapsed ? 'lg:pl-[72px]' : 'lg:pl-[260px]'
        }`}
      >
        <TopBar
          currentPage={currentPage}
          onMenuClick={() => setSidebarOpen(true)}
          onRefresh={onRefresh}
          isRefreshing={isRefreshing}
          onOpenCommandPalette={() => setCommandPaletteOpen(true)}
        />

        <main className="p-4 pb-16 lg:p-6 lg:pb-16">{children}</main>
      </div>

      <ActivityPanel />

      <CommandPalette
        isOpen={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        onNavigate={handleCommandPaletteNavigate}
      />
    </div>
  )
}

export type { Page }
