import { lazy, Suspense, useState, useEffect, useCallback } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AppShell, type Page } from '@/components/layout/AppShell'
import { Toaster } from '@/components/ui/sonner'
import { TooltipProvider } from '@/components/ui/tooltip'
import { buildHashRoute, parseHashRoute, navigateTo } from '@/lib/hashRoute'

const FeedPage = lazy(() => import('@/pages/FeedPage').then((m) => ({ default: m.FeedPage })))
const DiscoveryPage = lazy(() => import('@/pages/DiscoveryPage').then((m) => ({ default: m.DiscoveryPage })))
const AuthorsPage = lazy(() => import('@/pages/AuthorsPage').then((m) => ({ default: m.AuthorsPage })))
const LibraryPage = lazy(() => import('@/pages/LibraryPage').then((m) => ({ default: m.LibraryPage })))
const InsightsPage = lazy(() => import('@/pages/InsightsPage').then((m) => ({ default: m.InsightsPage })))
const AlertsPage = lazy(() => import('@/pages/AlertsPage').then((m) => ({ default: m.AlertsPage })))
const SettingsPage = lazy(() => import('@/pages/SettingsPage').then((m) => ({ default: m.SettingsPage })))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Longer default cache to avoid refetches when the user navigates between pages.
      // Page-specific mutations still invalidate narrowly to keep their data fresh.
      staleTime: 60_000,
      gcTime: 5 * 60_000,
      refetchOnWindowFocus: false,
      retry: 2,
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 8000),
    },
  },
})

function getPageFromHash(): Page {
  return parseHashRoute().page
}

function PageLoader() {
  return (
    <div className="flex items-center justify-center py-24">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-[var(--color-border)] border-t-alma-500" />
    </div>
  )
}

function AppContent() {
  const [currentPage, setCurrentPage] = useState<Page>(getPageFromHash)

  useEffect(() => {
    const onHashChange = () => {
      setCurrentPage(getPageFromHash())
    }
    window.addEventListener('hashchange', onHashChange)
    return () => window.removeEventListener('hashchange', onHashChange)
  }, [])

  const navigate = useCallback((page: Page) => {
    navigateTo(page)
    setCurrentPage(page)
  }, [])

  const handleRefresh = useCallback(() => {
    // Invalidate all caches but only refetch queries that are currently rendered.
    // Prevents a nuclear refetch of unrelated pages' data when the user taps the
    // TopBar refresh affordance.
    queryClient.invalidateQueries({ refetchType: 'active' })
  }, [])

  const renderPage = () => {
    switch (currentPage) {
      case 'feed':
        return <FeedPage />
      case 'discovery':
        return <DiscoveryPage />
      case 'authors':
        return <AuthorsPage />
      case 'library':
        return <LibraryPage />
      case 'insights':
        return <InsightsPage />
      case 'alerts':
        return <AlertsPage />
      case 'settings':
        return <SettingsPage />
      default:
        return <FeedPage />
    }
  }

  return (
    <AppShell
      currentPage={currentPage}
      onNavigate={navigate}
      onRefresh={handleRefresh}
    >
      <Suspense fallback={<PageLoader />}>
        {renderPage()}
      </Suspense>
      <Toaster />
    </AppShell>
  )
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={200}>
        <AppContent />
      </TooltipProvider>
    </QueryClientProvider>
  )
}

export default App
