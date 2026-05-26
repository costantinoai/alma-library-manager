import { Suspense, lazy } from 'react'
import { useQuery } from '@tanstack/react-query'
import { motion } from 'framer-motion'
import { Loader2, Network, Sparkles } from 'lucide-react'

import { getAuthorNeighbourhood } from '@/api/client'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorBoundary } from '@/components/ui/ErrorBoundary'
import { Kbd } from '@/components/ui/kbd'
import { LoadingState } from '@/components/ui/LoadingState'

// Lazy so the graph renderer is code-split: the chunk only downloads the first
// time this dialog opens, never in the main bundle.
const AuthorNeighbourhoodGraph = lazy(
  () => import('@/components/authors/AuthorNeighbourhoodGraph'),
)

interface NeighbourhoodDialogProps {
  authorId: string | null
  authorName: string
  /** Drives the empty-state action: build data vs. resolve identity first. */
  hasOpenAlexId: boolean
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Fire the existing per-author deep-refresh (fetch works → co-authors +
   *  centroid). Bounded, background, Activity-enveloped. */
  onBuildData: () => void
  isBuilding?: boolean
  /** Jump to the Identifiers tab so the user can resolve the OpenAlex ID. */
  onResolveIdentity: () => void
}

export function NeighbourhoodDialog({
  authorId,
  authorName,
  hasOpenAlexId,
  open,
  onOpenChange,
  onBuildData,
  isBuilding = false,
  onResolveIdentity,
}: NeighbourhoodDialogProps) {
  // Fetch ONLY while open — the backend computes the ego-network on demand.
  const query = useQuery({
    queryKey: ['author-neighbourhood', authorId],
    queryFn: () => getAuthorNeighbourhood(authorId as string),
    enabled: open && !!authorId,
    staleTime: 5 * 60 * 1000,
  })

  const data = query.data
  const counts = data?.counts
  const showGraph = !!data && !data.empty && !query.isError

  // Legend doubles as a tally — dots inherit the same semantic tokens the
  // graph reads at runtime, so colours match exactly.
  const legend: Array<{ key: string; label: string; dotClass: string; count: number | null }> = [
    { key: 'center', label: 'This author', dotClass: 'bg-accent', count: null },
    { key: 'coauthor', label: 'Co-authors', dotClass: 'bg-success-500', count: counts?.coauthor ?? 0 },
    { key: 'citation', label: 'Citations', dotClass: 'bg-info-500', count: counts?.citation ?? 0 },
    { key: 'similar', label: 'Similar', dotClass: 'bg-gold-500', count: counts?.similar ?? 0 },
  ]

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="h-[82vh] w-[min(92vw,1100px)] max-w-none gap-0 overflow-hidden p-0">
        <motion.div
          initial={{ opacity: 0, scale: 0.94, y: 24 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          transition={{ type: 'spring', stiffness: 260, damping: 24 }}
          className="flex h-full flex-col"
        >
          <DialogHeader className="border-b border-edge-1 px-6 py-4 text-left">
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-400">
              Intellectual neighbourhood
            </p>
            <DialogTitle className="flex items-center gap-2 text-lg">
              <Network className="h-[18px] w-[18px] text-accent" aria-hidden />
              {authorName}
            </DialogTitle>
            <DialogDescription className="sr-only">
              Interactive graph of {authorName}&apos;s co-authors, citation neighbours, and
              similar researchers.
            </DialogDescription>
            <div className="mt-0.5 flex flex-wrap items-center gap-1.5 text-xs text-slate-500">
              <Kbd>drag</Kbd> orbit
              <span className="text-slate-300">·</span>
              <Kbd>scroll</Kbd> zoom
              <span className="text-slate-300">·</span>
              <Kbd>click</Kbd> focus a node
            </div>
          </DialogHeader>

          <div className="relative flex-1 bg-info-50">
            {query.isLoading ? (
              <LoadingState message="Charting the neighbourhood…" />
            ) : query.isError ? (
              <div className="flex h-full items-center justify-center p-6">
                <EmptyState
                  icon={Network}
                  title="Couldn't load the neighbourhood"
                  description="Something went wrong building this author's graph."
                  action={
                    <Button variant="outline" size="sm" onClick={() => void query.refetch()}>
                      Try again
                    </Button>
                  }
                />
              </div>
            ) : !data || data.empty ? (
              <div className="flex h-full items-center justify-center p-6">
                {hasOpenAlexId ? (
                  <EmptyState
                    icon={Network}
                    title="No neighbourhood data yet"
                    description="Fetch this author's works to map their co-authors, citations, and similar researchers. Runs in the background — reopen this view once it finishes."
                    action={
                      <Button size="sm" onClick={onBuildData} disabled={isBuilding} className="gap-2">
                        {isBuilding ? (
                          <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                        ) : (
                          <Sparkles className="h-4 w-4" aria-hidden />
                        )}
                        {isBuilding ? 'Building…' : 'Build neighbourhood data'}
                      </Button>
                    }
                  />
                ) : (
                  <EmptyState
                    icon={Network}
                    title="No identity resolved yet"
                    description="This author needs a resolved OpenAlex ID before we can map their neighbourhood."
                    action={
                      <Button variant="outline" size="sm" onClick={onResolveIdentity}>
                        Resolve identity
                      </Button>
                    }
                  />
                )}
              </div>
            ) : (
              <ErrorBoundary
                fallback={
                  <div className="flex h-full items-center justify-center p-6">
                    <EmptyState
                      icon={Network}
                      title="Graph unavailable"
                      description="Something went wrong rendering this author's graph. Try reopening."
                    />
                  </div>
                }
              >
                <Suspense fallback={<LoadingState message="Loading graph…" />}>
                  <AuthorNeighbourhoodGraph data={data} />
                </Suspense>
              </ErrorBoundary>
            )}

            {/* Tally-legend — a quiet card keyed to the constellation. */}
            {showGraph ? (
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.3, duration: 0.45, ease: [0.22, 0.61, 0.36, 1] }}
                className="pointer-events-none absolute bottom-4 left-4 w-44 rounded-sm border border-edge-3 bg-surface-3 px-3 py-2.5 shadow-paper-sm"
              >
                <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  Legend
                </p>
                <ul className="space-y-1">
                  {legend.map((item) => (
                    <li
                      key={item.key}
                      className="flex items-center justify-between gap-2 text-[11px] text-slate-600"
                    >
                      <span className="flex items-center gap-2">
                        <span className={`inline-block h-2 w-2 rounded-full ${item.dotClass}`} />
                        {item.label}
                      </span>
                      {item.count != null ? (
                        <span className="font-medium tabular-nums text-slate-800">{item.count}</span>
                      ) : null}
                    </li>
                  ))}
                </ul>
              </motion.div>
            ) : null}
          </div>
        </motion.div>
      </DialogContent>
    </Dialog>
  )
}
