import { useState, useEffect, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { WifiOff } from 'lucide-react'
import { api } from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { PageThemeProvider } from '@/components/ui/page-theme'
import { PAGE_THEMES } from '@/lib/palette'
import { Sidebar, type Page } from './Sidebar'
import { SIDEBAR_CONTENT_INSET, sidebarInset } from './sidebarMetrics'
import { TopBar } from './TopBar'
import { ActivityPanel } from '@/components/ActivityPanel'
import { CommandPalette } from '@/components/CommandPalette'
import { useOperationToasts } from '@/hooks/useOperationToasts'
import { useConnectorStatus } from '@/hooks/useConnectorStatus'

interface AppShellProps {
  currentPage: Page
  onNavigate: (page: Page) => void
  onPrefetch?: (page: Page) => void
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
  onPrefetch,
  onRefresh,
  isRefreshing,
  children,
}: AppShellProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => readInitialCollapsed())
  const [commandPaletteOpen, setCommandPaletteOpen] = useState(false)
  const networkPolicyQuery = useQuery({
    queryKey: ['network-policy'],
    queryFn: () =>
      api.get<{ enabled: boolean; settings_enabled: boolean; forced_off_by_env: boolean }>(
        '/settings/network-policy',
      ),
    staleTime: 30_000,
    retry: 1,
  })

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

  // Detect the browser connector at startup; toast only on an update/problem
  // (incompatible save-contract), never when it's installed and healthy.
  useConnectorStatus()

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
    // background plus the SVG fiber-grain tile. Putting a solid surface
    // on this wrapper would hide the texture (solid color over the
    // tiled bg). Surfaces above (sidebar nav area, cards, top bar)
    // cover the texture explicitly via their own bg.
    <div className="min-h-screen">
      <Sidebar
        currentPage={currentPage}
        onNavigate={onNavigate}
        onPrefetch={onPrefetch}
        isOpen={sidebarOpen}
        onClose={() => setSidebarOpen(false)}
        collapsed={sidebarCollapsed}
        onToggleCollapsed={() => setSidebarCollapsed((prev) => !prev)}
      />

      {/* Main content area shifts to clear the fixed sidebar. The left
          padding tracks the rail's actual desktop width, read from
          `sidebarMetrics` so the rail, this column and the Activity dock
          cannot drift apart. The transition runs on the same 200ms curve as
          the sidebar width change for a unified motion. */}
      <div
        className={`transition-[padding] duration-200 ${sidebarInset(
          SIDEBAR_CONTENT_INSET,
          sidebarCollapsed,
        )}`}
      >
        <TopBar
          currentPage={currentPage}
          onMenuClick={() => setSidebarOpen(true)}
          onRefresh={onRefresh}
          isRefreshing={isRefreshing}
          onOpenCommandPalette={() => setCommandPaletteOpen(true)}
        />
        {networkPolicyQuery.data?.enabled === false ? (
          <div className="px-4 pt-4 lg:px-6">
            <Alert variant="warning" className="pr-36">
              <WifiOff className="h-4 w-4" />
              <AlertDescription>
                External network access is off
                {networkPolicyQuery.data.forced_off_by_env ? ' by an operations override' : ''}.
                Local Library, maps, and search still work; API, Slack, email, and hosted-AI calls
                are blocked.
              </AlertDescription>
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="absolute right-3 top-2.5"
                onClick={() => onNavigate('settings')}
              >
                Open Settings
              </Button>
            </Alert>
          </div>
        ) : null}

        {/* One assignment for the whole app: the routed page decides the
            identity hue, and every piece of structural chrome under `main`
            reads it from context. No page passes a colour prop. */}
        <PageThemeProvider theme={PAGE_THEMES[currentPage] ?? null}>
          <main className="p-4 pb-16 lg:p-6 lg:pb-16">{children}</main>
        </PageThemeProvider>
      </div>

      {/* The dock is `fixed`, so it can't inherit the column's padding — it
          needs the rail's width told to it directly. */}
      <ActivityPanel sidebarCollapsed={sidebarCollapsed} />

      <CommandPalette
        isOpen={commandPaletteOpen}
        onClose={() => setCommandPaletteOpen(false)}
        onNavigate={handleCommandPaletteNavigate}
      />
    </div>
  )
}

export type { Page }
