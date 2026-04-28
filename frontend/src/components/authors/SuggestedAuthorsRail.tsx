import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { ChevronDown, ChevronUp, Sparkles, UserSearch } from 'lucide-react'

import {
  api,
  followAuthor,
  listAuthorSuggestions,
  refreshAuthorSuggestionNetwork,
  rejectAuthorSuggestion,
  type Author,
  type AuthorSuggestion,
} from '@/api/client'
import { SuggestedAuthorCard } from '@/components/authors/SuggestedAuthorCard'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Skeleton } from '@/components/ui/skeleton'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

const COLLAPSED_COUNT = 5
// Expanded view = 5 columns × 5 rows = 25 cards.
const EXPANDED_COUNT = 25
// Fetch enough to cover the expanded view + a buffer for already-acted
// rows. Server route enforces its own ceiling (limit ≤ 30).
const FETCH_COUNT = EXPANDED_COUNT + 5

interface SuggestedAuthorsRailProps {
  onOpenDetail?: (suggestion: AuthorSuggestion) => void
}

export function SuggestedAuthorsRail({ onOpenDetail }: SuggestedAuthorsRailProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const reducedMotion = useReducedMotion()

  // ── Acted-on set + sequential mutation queue ─────────────────────
  // The acted-on set is the primary defense against the "card bounces
  // back" bug. Symptoms (verified 2026-04-26):
  //   1. Click Follow A → optimistic remove → API A starts → invalidate → refetch starts.
  //   2. Click Follow B before refetch returns → optimistic remove → API B in flight.
  //   3. Refetch reads followed_authors snapshot taken *before* API B committed
  //      → returns B (with A excluded) → cache overwrites optimistic remove
  //      → B re-appears in the rail seconds after the user dismissed it.
  // The acted-on set lives outside React Query's cache; the visible memo
  // filters by it on every render. The server is allowed to return a
  // bounced-back row — we just refuse to render it.
  // `useState(new Set)` (not useRef) so toggling triggers a re-render
  // when an action completes — `useReducer` would also work, but a
  // shallow setState is cheaper. We DO mutate the underlying set in
  // place and re-wrap with `new Set(...)` to keep React's reference
  // identity check honest.
  const [actedOn, setActedOn] = useState<Set<string>>(() => new Set())
  // See-more toggle — collapsed shows the top 5; expanded fills a 5×5
  // grid (25 cards). The fetched payload covers the expanded view, so
  // toggling is a pure visual change with no extra network round-trip.
  const [expanded, setExpanded] = useState(false)
  const markActed = useCallback((openalexId: string | null | undefined) => {
    const id = (openalexId ?? '').trim().toLowerCase()
    if (!id) return
    setActedOn((prev) => {
      if (prev.has(id)) return prev
      const next = new Set(prev)
      next.add(id)
      return next
    })
  }, [])

  // Sequential follow-mutation queue. The acted-on set fixes the
  // visual bounce; the queue fixes the *backend* race — running two
  // follow flows in parallel can hit the author-create + monitor-sync
  // path concurrently, which has occasional UNIQUE-constraint hiccups
  // on `feed_monitors` under load. One in-flight at a time is cheap
  // and removes the failure mode entirely. Reject (sync, single
  // INSERT) doesn't need queuing.
  const followQueueRef = useRef<Promise<unknown>>(Promise.resolve())

  const suggestionsQuery = useQuery({
    queryKey: ['author-suggestions', FETCH_COUNT],
    queryFn: () => listAuthorSuggestions(FETCH_COUNT),
    retry: 1,
  })

  // D12 AUTH-SUG-3/4: on mount, fire-and-forget the refresh-network
  // call so the OpenAlex co-author expansion + S2 paper-recommendation
  // buckets warm their caches in the background. Stale/missing caches
  // enqueue an Activity job; fresh caches no-op. `useOperationToasts`
  // auto-invalidates `author-suggestions` on `authors.*` completion,
  // so the new rows will appear here without a manual refetch.
  const refreshTriggeredRef = useRef(false)
  useEffect(() => {
    if (refreshTriggeredRef.current) return
    refreshTriggeredRef.current = true
    refreshAuthorSuggestionNetwork().catch(() => {
      // silent — the rail always has the local buckets to fall back on
    })
  }, [])

  const rejectMutation = useMutation({
    mutationFn: (openalexId: string) => rejectAuthorSuggestion(openalexId),
    onMutate: async (openalexId) => {
      // Optimistic removal keeps the animation snappy — no spinner gap.
      // Persist the openalex_id in the acted-on set so a subsequent
      // refetch (from this mutation OR from any unrelated invalidation)
      // can NOT bring the dismissed card back. The server's
      // `missing_author_feedback` write also suppresses it long-term,
      // but that round-trip races our refetch under rapid clicks.
      markActed(openalexId)
      await queryClient.cancelQueries({ queryKey: ['author-suggestions', FETCH_COUNT] })
      const prev = queryClient.getQueryData<AuthorSuggestion[]>([
        'author-suggestions',
        FETCH_COUNT,
      ])
      queryClient.setQueryData<AuthorSuggestion[]>(
        ['author-suggestions', FETCH_COUNT],
        (old) => (old ?? []).filter((s) => s.openalex_id !== openalexId),
      )
      return { prev }
    },
    onError: (_err, _vars, ctx) => {
      if (ctx?.prev) {
        queryClient.setQueryData(['author-suggestions', FETCH_COUNT], ctx.prev)
      }
      errorToast('Error', 'Failed to dismiss suggestion.')
    },
    onSettled: () => {
      void invalidateQueries(queryClient, ['author-suggestions'])
    },
  })

  const followMutation = useMutation({
    mutationFn: async (suggestion: AuthorSuggestion) => {
      // Sequential queue — chain onto the previous in-flight follow so
      // two near-simultaneous clicks never race the author-create +
      // monitor-sync write path. Tail of the queue resolves with this
      // call's result so React Query still sees a normal Promise.
      const tail = followQueueRef.current
      const next = tail.then(async () => {
        if (suggestion.existing_author_id) {
          return followAuthor(suggestion.existing_author_id)
        }
        if (suggestion.openalex_id) {
          const created = await api.post<Author>('/authors', {
            openalex_id: suggestion.openalex_id,
            name: suggestion.name,
          })
          return followAuthor(created.id)
        }
        throw new Error('Suggestion is missing an actionable identifier')
      })
      // Replace the tail with a swallowing copy so an error in this
      // call doesn't poison the queue for the next click.
      followQueueRef.current = next.catch(() => undefined)
      return next
    },
    onMutate: async (suggestion) => {
      // Same defensive pattern as reject: persist in the acted-on set
      // BEFORE the API call, so any refetch that snapshots the server
      // state mid-flight (followed_authors not yet committed) still
      // can't render the just-followed card.
      markActed(suggestion.openalex_id)
      await queryClient.cancelQueries({ queryKey: ['author-suggestions', FETCH_COUNT] })
      const prev = queryClient.getQueryData<AuthorSuggestion[]>([
        'author-suggestions',
        FETCH_COUNT,
      ])
      queryClient.setQueryData<AuthorSuggestion[]>(
        ['author-suggestions', FETCH_COUNT],
        (old) => (old ?? []).filter((s) => s.openalex_id !== suggestion.openalex_id),
      )
      return { prev }
    },
    onSuccess: (_data, suggestion) => {
      toast({ title: 'Followed', description: `${suggestion.name} is now followed.` })
    },
    onError: (_err, _suggestion, ctx) => {
      if (ctx?.prev) {
        queryClient.setQueryData(['author-suggestions', FETCH_COUNT], ctx.prev)
      }
      errorToast('Error', 'Failed to follow author.')
    },
    onSettled: () => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-suggestions'],
      )
    },
  })

  const filtered = useMemo(() => {
    // Filter through the acted-on set FIRST so an acted-on row never
    // reserves a visible slot (fewer cards would render than expected).
    const all = suggestionsQuery.data ?? []
    if (actedOn.size === 0) return all
    return all.filter((s) => {
      const oid = (s.openalex_id || '').trim().toLowerCase()
      return oid && !actedOn.has(oid)
    })
  }, [suggestionsQuery.data, actedOn])

  const visibleCap = expanded ? EXPANDED_COUNT : COLLAPSED_COUNT
  const visible = useMemo(() => filtered.slice(0, visibleCap), [filtered, visibleCap])
  // Show the toggle when the filtered pool actually has more rows than
  // the current cap — collapsing always works, expanding only matters
  // when there's something extra to reveal.
  const canToggle = expanded ? visible.length > COLLAPSED_COUNT : filtered.length > COLLAPSED_COUNT

  const isLoading = suggestionsQuery.isLoading
  const hasError = suggestionsQuery.isError
  const empty = !isLoading && !hasError && visible.length === 0

  const animationDuration = reducedMotion ? 0 : 0.25

  return (
    <section className="space-y-3">
      <header className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-alma-600" />
          <h2 className="text-sm font-semibold text-alma-800">Suggested authors</h2>
          <span className="text-xs text-slate-500">
            Ranked from your Library. Dismissed authors stop appearing here.
          </span>
        </div>
      </header>

      {isLoading ? (
        <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-5">
          {Array.from({ length: COLLAPSED_COUNT }).map((_, i) => (
            <Skeleton key={i} className="h-52 rounded-lg" />
          ))}
        </div>
      ) : hasError ? (
        <Alert variant="negative">
          <AlertDescription>Could not load author suggestions. Try reloading.</AlertDescription>
        </Alert>
      ) : empty ? (
        <EmptyState
          icon={UserSearch}
          title="No suggestions right now."
          description="Save more papers to your Library and their authors will surface here."
        />
      ) : (
        <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-5">
          <AnimatePresence mode="popLayout" initial={false}>
            {visible.map((s) => {
              const keyId = s.openalex_id || s.key
              return (
                <motion.div
                  key={keyId}
                  layout
                  layoutId={`suggested-${keyId}`}
                  initial={{ opacity: 0, y: 12, scale: 0.96 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, y: -12, scale: 0.92 }}
                  transition={{ duration: animationDuration, ease: 'easeOut' }}
                  className="h-full"
                >
                  <SuggestedAuthorCard
                    suggestion={s}
                    onClick={() => onOpenDetail?.(s)}
                    onFollow={() => followMutation.mutate(s)}
                    onReject={() => {
                      if (!s.openalex_id) {
                        errorToast('Error', 'Cannot dismiss: missing OpenAlex ID.')
                        return
                      }
                      rejectMutation.mutate(s.openalex_id)
                    }}
                    followPending={
                      followMutation.isPending &&
                      followMutation.variables?.openalex_id === s.openalex_id
                    }
                    rejectPending={
                      rejectMutation.isPending && rejectMutation.variables === s.openalex_id
                    }
                  />
                </motion.div>
              )
            })}
          </AnimatePresence>
        </div>
      )}

      {/* See-more toggle — flips between top-5 row and full 5×5 grid.
          Hidden when the filtered pool has nothing extra to reveal
          (e.g. fewer than 6 suggestions in the corpus, or every
          extra row already acted-on). */}
      {canToggle ? (
        <div className="flex justify-center pt-1">
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={() => setExpanded((v) => !v)}
            className="text-xs text-alma-700 hover:text-alma-800"
          >
            {expanded ? (
              <>
                <ChevronUp className="h-3.5 w-3.5" />
                Show top {COLLAPSED_COUNT}
              </>
            ) : (
              <>
                <ChevronDown className="h-3.5 w-3.5" />
                See more ({Math.min(filtered.length, EXPANDED_COUNT) - COLLAPSED_COUNT})
              </>
            )}
          </Button>
        </div>
      ) : null}
    </section>
  )
}
