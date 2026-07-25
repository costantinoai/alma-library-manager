import { lazy, Suspense, useState, useEffect, useCallback } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AppShell, type Page } from '@/components/layout/AppShell'
import { Toaster } from '@/components/ui/sonner'
import { TooltipProvider } from '@/components/ui/tooltip'
import { PageReveal } from '@/components/ui/reveal'
import { OnboardingGate } from '@/components/onboarding'
import { prefetchMapPage } from '@/components/map/mapQueries'
import { parseHashRoute, navigateTo } from '@/lib/hashRoute'
import { pageLoaders, preloadPage } from '@/lib/pageLoaders'

const HomePage = lazy(pageLoaders.home)
const FeedPage = lazy(pageLoaders.feed)
const DiscoveryPage = lazy(pageLoaders.discovery)
const AuthorsPage = lazy(pageLoaders.authors)
const MapPage = lazy(pageLoaders.map)
const LibraryPage = lazy(pageLoaders.library)
// Insights retired into Library › Analytics (task 47 Phase 4); the page is now
// a redirect shim so old #/insights?tab=… deep links still land correctly.
const InsightsRedirect = lazy(pageLoaders.insights)
const HealthPage = lazy(pageLoaders.health)
const AlertsPage = lazy(pageLoaders.alerts)
const SettingsPage = lazy(pageLoaders.settings)

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // General API rows stay fresh for one minute. Durable semantic-map
      // layouts override this centrally in mapQueries.ts.
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

  const prefetch = useCallback((page: Page) => {
    void preloadPage(page)
    if (page === 'map' || page === 'authors') {
      void prefetchMapPage(queryClient, page)
    }
  }, [])

  const renderPage = () => {
    switch (currentPage) {
      case 'home':
        return <HomePage />
      case 'feed':
        return <FeedPage />
      case 'discovery':
        return <DiscoveryPage />
      case 'map':
        return <MapPage />
      case 'authors':
        return <AuthorsPage />
      case 'library':
        return <LibraryPage />
      case 'insights':
        return <InsightsRedirect />
      case 'health':
        return <HealthPage />
      case 'alerts':
        return <AlertsPage />
      case 'settings':
        return <SettingsPage />
      default:
        return <HomePage />
    }
  }

  return (
    <AppShell
      currentPage={currentPage}
      onNavigate={navigate}
      onPrefetch={prefetch}
      onRefresh={handleRefresh}
    >
      <Suspense fallback={<PageLoader />}>
        <PageReveal
          key={currentPage}
          animate={!['discovery', 'map', 'authors'].includes(currentPage)}
        >
          {renderPage()}
        </PageReveal>
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
        <OnboardingGate />
      </TooltipProvider>
    </QueryClientProvider>
  )
}

export default App
