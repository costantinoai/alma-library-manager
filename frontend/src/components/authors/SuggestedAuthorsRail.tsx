import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { RevealList, RevealItem } from '@/components/ui/reveal'
import { ChevronDown, ChevronUp, Sparkles, UserSearch } from 'lucide-react'

import {
  followAuthor,
  getApiErrorMessage,
  isRetryableApiError,
  listAuthorSuggestions,
  refreshAuthorSuggestionNetwork,
  rejectAuthorSuggestion,
  retryDelayMs,
  trackFollowedAuthorSuggestion,
  type AuthorSuggestion,
} from '@/api/client'
import { SuggestedAuthorCard } from '@/components/authors/SuggestedAuthorCard'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Skeleton } from '@/components/ui/skeleton'
import { useElementWidth } from '@/hooks/useElementWidth'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast, errorToast } from '@/hooks/useToast'

// Expanded view shows up to this many rows of the measured grid.
const EXPANDED_ROWS = 5
// Server route enforces its own ceiling (limit ≤ 30) — fetch right up to it
// so the expanded view (max 6 columns × 5 rows) is always covered.
const FETCH_COUNT = 30

// ── Container-measured grid ──────────────────────────────────────────
// The card count is DYNAMIC: we measure the rail's container width and fit
// as many ≥240px columns as possible (viewport breakpoints can't see a
// fixed-width modal or a sidebar-squeezed panel; a ResizeObserver can).
// 240px keeps each card near its natural ~1:1 footprint — narrower and the
// chip rows wrap, stretching every card in the row into a tall sliver.
const MIN_CARD_WIDTH = 240
// Matches the `gap-3` (0.75rem) used between cards.
const GRID_GAP = 12
const MAX_COLUMNS = 6
// Render width before the first ResizeObserver tick lands (one frame).
const FALLBACK_COLUMNS = 3

// ── Durable follow-intent journal ────────────────────────────────────
// "Queued" is a real author status (see tasks/AUTHORS_COMPONENT.md): once
// the user clicks Follow, that author must NEVER be re-suggested — even if
// the page reloads before the API call commits. Each intent is journalled
// to localStorage on click and removed on success / permanent failure; on
// mount the journal seeds the acted-on set (queued authors don't render)
// and replays outstanding intents. Replay is safe because the backend
// follow endpoint is idempotent (re-following is a success no-op).
const PENDING_FOLLOWS_KEY = 'alma.pending-author-follows.v1'

interface FollowIntent {
  openalexId: string
  name: string
  existingAuthorId: string | null
  suggestionType: string | null
}

function readPendingFollows(): FollowIntent[] {
  try {
    const raw = localStorage.getItem(PENDING_FOLLOWS_KEY)
    if (!raw) return []
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(
      (item): item is FollowIntent =>
        !!item &&
        typeof item === 'object' &&
        typeof (item as FollowIntent).openalexId === 'string' &&
        typeof (item as FollowIntent).name === 'string',
    )
  } catch {
    return []
  }
}

function writePendingFollows(intents: FollowIntent[]): void {
  try {
    if (intents.length === 0) {
      localStorage.removeItem(PENDING_FOLLOWS_KEY)
    } else {
      localStorage.setItem(PENDING_FOLLOWS_KEY, JSON.stringify(intents))
    }
  } catch {
    // localStorage unavailable (private mode quota etc.) — the in-memory
    // queue still works for this session; only refresh-survival degrades.
  }
}

function addPendingFollow(intent: FollowIntent): void {
  const current = readPendingFollows()
  if (current.some((item) => item.openalexId === intent.openalexId)) return
  writePendingFollows([...current, intent])
}

function removePendingFollow(openalexId: string): void {
  writePendingFollows(readPendingFollows().filter((item) => item.openalexId !== openalexId))
}

interface SuggestedAuthorsRailProps {
  onOpenDetail?: (suggestion: AuthorSuggestion) => void
  /** Rows shown before the "see more" toggle (each row holds however many
   *  ≥240px columns fit the container). Default 1; the onboarding modal
   *  passes 2 so enough cards are visible to hit its follow-5 goal. */
  collapsedRows?: number
}

export function SuggestedAuthorsRail({
  onOpenDetail,
  collapsedRows = 1,
}: SuggestedAuthorsRailProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  // Measured container → column count → visible caps. The inline
  // `gridTemplateColumns` below renders exactly `columns` tracks, so the
  // measurement and the layout can never drift apart.
  const [sectionRef, sectionWidth] = useElementWidth<HTMLElement>()
  const columns = useMemo(() => {
    if (sectionWidth == null || sectionWidth <= 0) return FALLBACK_COLUMNS
    return Math.max(
      1,
      Math.min(MAX_COLUMNS, Math.floor((sectionWidth + GRID_GAP) / (MIN_CARD_WIDTH + GRID_GAP))),
    )
  }, [sectionWidth])
  const gridStyle = useMemo(
    () => ({ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }),
    [columns],
  )

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
  // See-more toggle — collapsed shows whole rows of the measured grid;
  // expanded fills up to EXPANDED_ROWS of it. The fetched payload covers
  // the expanded view, so toggling is a pure visual change with no extra
  // network round-trip.
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
  // Permanent failures hand the card back: the action did NOT happen, so
  // hiding it would be untruthful (and the journal entry is dropped too).
  const unmarkActed = useCallback((openalexId: string | null | undefined) => {
    const id = (openalexId ?? '').trim().toLowerCase()
    if (!id) return
    setActedOn((prev) => {
      if (!prev.has(id)) return prev
      const next = new Set(prev)
      next.delete(id)
      return next
    })
  }, [])

  // Sequential follow-mutation queue. One in-flight follow at a time keeps
  // the optimistic cache writes ordered; the backend write itself is
  // serialized + idempotent, so this is purely a client-ordering concern.
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
    mutationFn: (suggestion: AuthorSuggestion) =>
      rejectAuthorSuggestion(
        suggestion.openalex_id ?? '',
        suggestion.suggestion_type ?? null,
      ),
    // Transient backend lock blips (503 + Retry-After) retry quietly
    // instead of surfacing a fatal toast for a click that will succeed.
    retry: (failureCount, err) => isRetryableApiError(err) && failureCount < 3,
    retryDelay: retryDelayMs,
    onMutate: async (suggestion) => {
      const openalexId = suggestion.openalex_id ?? ''
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
    onError: (err, suggestion, ctx) => {
      if (ctx?.prev) {
        queryClient.setQueryData(['author-suggestions', FETCH_COUNT], ctx.prev)
      }
      unmarkActed(suggestion.openalex_id)
      errorToast(`Could not dismiss ${suggestion.name}`, getApiErrorMessage(err))
    },
    onSettled: () => {
      void invalidateQueries(queryClient, ['author-suggestions'])
    },
  })

  const followMutation = useMutation({
    mutationFn: async (intent: FollowIntent) => {
      // ONE canonical call: the follow endpoint resolves/creates the author
      // row server-side (with the human name) and is idempotent. Never
      // pre-create via POST /authors here — that route auto-follows, and
      // chaining create + follow produced spurious "already following"
      // failures (root cause of the 2026-06 "could not add authors" bug).
      const authorRef = intent.existingAuthorId ?? intent.openalexId
      if (!authorRef) {
        throw new Error('Suggestion is missing an actionable identifier')
      }
      // Sequential queue — chain onto the previous in-flight follow so the
      // optimistic cache updates stay ordered under rapid clicks. Tail of
      // the queue resolves with this call's result so React Query still
      // sees a normal Promise.
      const tail = followQueueRef.current
      const next = tail.then(() => followAuthor(authorRef, true, intent.name))
      // Replace the tail with a swallowing copy so an error in this
      // call doesn't poison the queue for the next click. Errors are
      // already surfaced via `onError` (toast); we log to console here
      // so the queue's silent failure mode is visible in DevTools.
      followQueueRef.current = next.catch((err: unknown) => {
        console.warn('[SuggestedAuthorsRail] follow queue tail rejected', err)
      })
      return next
    },
    // 503 = transient write contention; retry with backoff before treating
    // the follow as failed. Each retry re-chains through the queue.
    retry: (failureCount, err) => isRetryableApiError(err) && failureCount < 3,
    retryDelay: retryDelayMs,
    onMutate: async (intent) => {
      // Same defensive pattern as reject: persist in the acted-on set AND
      // the durable journal BEFORE the API call, so neither a mid-flight
      // refetch nor a full page reload can resurface the queued author.
      markActed(intent.openalexId)
      addPendingFollow(intent)
      await queryClient.cancelQueries({ queryKey: ['author-suggestions', FETCH_COUNT] })
      const prev = queryClient.getQueryData<AuthorSuggestion[]>([
        'author-suggestions',
        FETCH_COUNT,
      ])
      queryClient.setQueryData<AuthorSuggestion[]>(
        ['author-suggestions', FETCH_COUNT],
        (old) => (old ?? []).filter((s) => s.openalex_id !== intent.openalexId),
      )
      return { prev }
    },
    onSuccess: (_data, intent) => {
      removePendingFollow(intent.openalexId)
      toast({ title: 'Followed', description: `${intent.name} is now followed.` })
      // Fire-and-forget bucket-attribution log for outcome calibration.
      // Backend computes per-bucket follow rates from this; failures are
      // ignored because the actual follow has already succeeded.
      if (intent.openalexId) {
        trackFollowedAuthorSuggestion(intent.openalexId, intent.suggestionType).catch(
          (err: unknown) => {
            // Outcome-calibration tracking is best-effort but devs still
            // need to see when the attribution endpoint is down — silent
            // .catch used to drop these without any breadcrumb.
            console.warn('[SuggestedAuthorsRail] follow attribution failed', err)
          },
        )
      }
    },
    onError: (err, intent, ctx) => {
      // Permanent failure (retries exhausted or a real 4xx/5xx): drop the
      // journal entry so replay doesn't loop forever, hand the card back,
      // and say WHO failed and WHY.
      removePendingFollow(intent.openalexId)
      if (ctx?.prev) {
        queryClient.setQueryData(['author-suggestions', FETCH_COUNT], ctx.prev)
      }
      unmarkActed(intent.openalexId)
      errorToast(`Could not follow ${intent.name}`, getApiErrorMessage(err))
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

  // Replay outstanding follow intents from the journal once per mount —
  // these are clicks from a previous page load that never got to commit
  // (refresh mid-burst). Seeding the acted-on set happens inside
  // followMutation.onMutate, so the authors stay hidden from first paint
  // of the data; idempotent backend follow makes re-running safe.
  const replayTriggeredRef = useRef(false)
  const replayFollow = followMutation.mutate
  useEffect(() => {
    if (replayTriggeredRef.current) return
    replayTriggeredRef.current = true
    for (const intent of readPendingFollows()) {
      replayFollow(intent)
    }
  }, [replayFollow])

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

  // Caps are whole rows of the measured grid: collapsed = `collapsedRows`
  // rows, expanded = up to EXPANDED_ROWS (bounded by what we fetched).
  const collapsedCap = columns * Math.max(1, collapsedRows)
  const expandedCap = Math.min(columns * EXPANDED_ROWS, FETCH_COUNT)
  const visibleCap = expanded ? expandedCap : collapsedCap
  const visible = useMemo(() => filtered.slice(0, visibleCap), [filtered, visibleCap])
  // Show the toggle when the filtered pool actually has more rows than
  // the current cap — collapsing always works, expanding only matters
  // when there's something extra to reveal.
  const canToggle = expanded ? visible.length > collapsedCap : filtered.length > collapsedCap

  const isLoading = suggestionsQuery.isLoading
  const hasError = suggestionsQuery.isError
  const empty = !isLoading && !hasError && visible.length === 0

  return (
    <section ref={sectionRef} className="space-y-3">
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
        <div className="grid gap-3" style={gridStyle}>
          {Array.from({ length: collapsedCap }).map((_, i) => (
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
        <RevealList className="grid gap-3" style={gridStyle}>
          {visible.map((s, i) => {
            const keyId = s.openalex_id || s.key
            return (
              <RevealItem key={keyId} index={i} className="h-full">
                <SuggestedAuthorCard
                  suggestion={s}
                  onClick={() => onOpenDetail?.(s)}
                  onFollow={() => {
                    if (!s.openalex_id && !s.existing_author_id) {
                      errorToast('Error', 'Cannot follow: missing OpenAlex ID.')
                      return
                    }
                    followMutation.mutate({
                      openalexId: s.openalex_id ?? '',
                      name: s.name,
                      existingAuthorId: s.existing_author_id ?? null,
                      suggestionType: s.suggestion_type ?? null,
                    })
                  }}
                  onReject={() => {
                    if (!s.openalex_id) {
                      errorToast('Error', 'Cannot dismiss: missing OpenAlex ID.')
                      return
                    }
                    rejectMutation.mutate(s)
                  }}
                  followPending={
                    followMutation.isPending &&
                    followMutation.variables?.openalexId === s.openalex_id
                  }
                  rejectPending={
                    rejectMutation.isPending &&
                    rejectMutation.variables?.openalex_id === s.openalex_id
                  }
                />
              </RevealItem>
            )
          })}
        </RevealList>
      )}

      {/* See-more toggle — flips between the collapsed row(s) and the full
          grid (up to EXPANDED_ROWS). Hidden when the filtered pool has
          nothing extra to reveal (few suggestions in the corpus, or every
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
                Show fewer
              </>
            ) : (
              <>
                <ChevronDown className="h-3.5 w-3.5" />
                See more ({Math.min(filtered.length, expandedCap) - collapsedCap})
              </>
            )}
          </Button>
        </div>
      ) : null}
    </section>
  )
}
