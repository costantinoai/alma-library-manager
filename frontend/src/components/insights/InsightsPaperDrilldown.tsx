import { useInfiniteQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'

import {
  getInsightsPapers,
  type InsightsDrilldownFilter,
  type Publication,
} from '@/api/client'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { StatusBadge } from '@/components/ui/status-badge'
import { truncate } from '@/lib/utils'

const PAGE = 30

/**
 * The figure a drilldown was opened from. `filterType`/`filterValue` are passed
 * straight to the parameterized `/insights/papers` route; `title` is the human
 * label ("Papers in cluster: …").
 */
export interface DrilldownTarget {
  filterType: InsightsDrilldownFilter
  filterValue: string
  scope?: string
  title: string
}

interface InsightsPaperDrilldownProps {
  target: DrilldownTarget | null
  onClose: () => void
}

/**
 * I-19: the ONE reusable paper-list drilldown for Insights — a graph cluster, or
 * a topic / journal / institution / year bar all open this same read-only
 * dialog. It mirrors the Health page's `HealthDimensionDrilldown` pattern
 * (Dialog + `useInfiniteQuery` paging) minus the repair/remove actions, since
 * Insights is descriptive (D7). Rendered open whenever `target` is non-null.
 */
export function InsightsPaperDrilldown({ target, onClose }: InsightsPaperDrilldownProps) {
  const open = target !== null
  const query = useInfiniteQuery({
    queryKey: ['insights-papers', target?.filterType, target?.filterValue, target?.scope],
    initialPageParam: 0,
    queryFn: async ({ pageParam }) => {
      const offset = typeof pageParam === 'number' ? pageParam : 0
      const res = await getInsightsPapers(target!.filterType, target!.filterValue, {
        scope: target?.scope,
        limit: PAGE,
        offset,
      })
      // Page on item count: another page exists only if this one was full.
      return { ...res, nextOffset: res.items.length === PAGE ? offset + PAGE : undefined }
    },
    getNextPageParam: (last) => last.nextOffset,
    enabled: open && !!target,
  })

  const pages = query.data?.pages ?? []
  const items: Publication[] = pages.flatMap((p) => p.items)
  const total = pages[0]?.total ?? 0

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onClose()
      }}
    >
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{target?.title ?? 'Papers'}</DialogTitle>
          <DialogDescription>
            {total} paper{total === 1 ? '' : 's'} · saved Library
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[60vh] space-y-2 overflow-y-auto pr-1">
          {query.isLoading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-5 w-5 animate-spin text-slate-400" />
            </div>
          ) : items.length === 0 ? (
            <p className="py-8 text-center text-sm text-slate-400">No papers found.</p>
          ) : (
            items.map((paper) => <PaperRow key={paper.id} paper={paper} />)
          )}
          {query.hasNextPage && (
            <div className="pt-2 text-center">
              <Button
                size="sm"
                variant="outline"
                disabled={query.isFetchingNextPage}
                onClick={() => query.fetchNextPage()}
              >
                {query.isFetchingNextPage ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  `Load more (${items.length} of ${total})`
                )}
              </Button>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function PaperRow({ paper }: { paper: Publication }) {
  return (
    <div className="rounded-sm border border-[var(--color-border)] p-3">
      <p className="text-sm font-medium text-alma-800">{paper.title}</p>
      <p className="mt-1 text-xs text-slate-500">
        {paper.authors ? truncate(paper.authors, 90) : 'Unknown authors'}
      </p>
      <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-slate-400">
        {paper.year != null && <span className="tabular-nums">{paper.year}</span>}
        {paper.journal ? <span className="truncate">· {paper.journal}</span> : null}
        <span className="tabular-nums">· {paper.cited_by_count} cites</span>
        {typeof paper.rating === 'number' && paper.rating > 0 ? (
          <StatusBadge tone="neutral" size="sm">
            ★ {paper.rating}
          </StatusBadge>
        ) : null}
      </div>
    </div>
  )
}
