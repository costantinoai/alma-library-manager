import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { RevealList, RevealItem } from '@/components/ui/reveal'
import { Plus, Users } from 'lucide-react'

import {
  api,
  getApiErrorMessage,
  getFollowedAuthorSignals,
  isRetryableApiError,
  listAuthorsNeedsAttention,
  listFollowedAuthors,
  retryDelayMs,
  type Author,
  type AuthorNeedsAttentionRow,
  type AuthorSignal,
  type AuthorSuggestion,
} from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Skeleton } from '@/components/ui/skeleton'
import { AuthorDetailPanel } from '@/components/AuthorDetailPanel'
import { PageTour, AUTHORS_TOUR } from '@/components/onboarding'
import { AddAuthorDialog, type AddAuthorPayload } from '@/components/authors/AddAuthorDialog'
import { CorpusAuthorsTable } from '@/components/authors/CorpusAuthorsTable'
import { FollowedAuthorCard } from '@/components/authors/FollowedAuthorCard'
import { SuggestedAuthorsRail } from '@/components/authors/SuggestedAuthorsRail'
import {
  AuthorsNeedsAttentionSection,
} from '@/components/authors/AuthorsNeedsAttentionSection'
import { useAuthorAttentionRouter } from '@/components/authors/useAuthorAttentionRouter'
import { invalidateQueries } from '@/lib/queryHelpers'
import { buildHashRoute, useHashRoute } from '@/lib/hashRoute'
import { cn } from '@/lib/utils'
import { useToast, errorToast } from '@/hooks/useToast'

/**
 * Authors page — three-section product model (2026-04-23):
 *
 *   1. Suggested (top)   — 5-card rail with enter/exit animations. Reject
 *                          writes a negative signal so the author is never
 *                          re-suggested; Follow promotes into section 2.
 *   2. Followed (middle) — grid of followed-author cards with monitor
 *                          health and the shared AuthorSignalBar.
 *   3. Corpus (bottom)   — compact table of every author in the DB. Row
 *                          click opens the same detail dialog.
 *
 * Every card / row opens the shared AuthorDetailPanel dialog, which
 * bundles overview + publications + identifier-resolution into one
 * controlled popup (replaces the old inline-expansion panel).
 */
export function AuthorsPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const [selectedAuthor, setSelectedAuthor] = useState<Author | null>(null)
  const [selectedSuggestion, setSelectedSuggestion] = useState<AuthorSuggestion | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [addAuthorOpen, setAddAuthorOpen] = useState(false)

  const authorsQuery = useQuery({
    queryKey: ['authors'],
    queryFn: () => api.get<Author[]>('/authors'),
    retry: 1,
  })

  const followedAuthorsQuery = useQuery({
    queryKey: ['library-followed-authors'],
    queryFn: listFollowedAuthors,
    retry: 1,
  })

  // Hoisted from AuthorsNeedsAttentionSection so the followed-author
  // grid can mark cards whose authors are in the needs-attention list.
  // React Query dedups the cache key, so the section stays in sync
  // with no second network request.
  const needsAttentionQuery = useQuery({
    queryKey: ['authors-needs-attention'],
    queryFn: () => listAuthorsNeedsAttention(50),
    staleTime: 60_000,
  })

  // Health-drilldown landing. The Health page routes author-dimension
  // drilldowns here with ?focus=needs-attention (DimensionStatusRow). That
  // section renders below the corpus table, so without this it lands above the
  // fold and the conflict the user clicked looks absent. Once the list has
  // loaded (so the layout is settled), scroll it into view and flash an accent
  // ring. Guarded to fire once per arrival so a manual scroll-up never re-snaps.
  const route = useHashRoute()
  const focusNeedsAttention = route.params.get('focus') === 'needs-attention'
  const needsAttentionRef = useRef<HTMLDivElement>(null)
  const [highlightAttention, setHighlightAttention] = useState(false)
  const didFocusAttentionRef = useRef(false)
  useEffect(() => {
    if (!focusNeedsAttention || needsAttentionQuery.isLoading || didFocusAttentionRef.current) {
      return
    }
    didFocusAttentionRef.current = true
    needsAttentionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    setHighlightAttention(true)
    const timer = setTimeout(() => setHighlightAttention(false), 2200)
    return () => clearTimeout(timer)
  }, [focusNeedsAttention, needsAttentionQuery.isLoading])

  const addAuthorMutation = useMutation({
    mutationFn: (payload: AddAuthorPayload) => api.post<Author>('/authors', payload),
    // Transient backend lock blips (503 + Retry-After) retry quietly.
    retry: (failureCount, err) => isRetryableApiError(err) && failureCount < 3,
    retryDelay: retryDelayMs,
    onSuccess: () => {
      void invalidateQueries(queryClient, ['authors'], ['library-followed-authors'])
      setAddAuthorOpen(false)
      toast({ title: 'Author added', description: 'They will contribute to Feed on the next refresh.' })
    },
    onError: (err, payload) => {
      // Name WHO failed and WHY — the backend detail carries the specific
      // reason (already followed / identifier unresolvable / upstream down).
      const label =
        payload.name || payload.openalex_id || payload.orcid || payload.scholar_id || 'author'
      errorToast(`Could not add ${label}`, getApiErrorMessage(err))
    },
  })

  // Bulk identifier resolution lives in Settings → Corpus maintenance
  // (2026-04-24). One-off user flows hit the per-author resolve inside
  // AuthorDetailPanel; the old header "Resolve IDs" button was removed
  // to keep the Authors page focused on exploration + triage.

  const authors = useMemo(() => authorsQuery.data ?? [], [authorsQuery.data])
  const followedIds = useMemo(
    () => new Set((followedAuthorsQuery.data ?? []).map((item) => item.author_id)),
    [followedAuthorsQuery.data],
  )
  // The single owner row (set during onboarding) → "This is you" badge.
  const ownerId = useMemo(
    () => (followedAuthorsQuery.data ?? []).find((item) => item.is_owner)?.author_id ?? null,
    [followedAuthorsQuery.data],
  )

  // Canonical author signals come from a SEPARATE, non-blocking query so the
  // (slow) signal context build never gates the (fast) followed list that
  // drives the grid — dismissing an author updates the grid instantly while
  // signals refetch in the background. Keyed by the followed-id set so it
  // auto-syncs when membership changes; keepPreviousData keeps the bars filled
  // (no "no signal yet" flash) during that refetch.
  const followedSignalsQuery = useQuery({
    queryKey: ['followed-author-signals', [...followedIds].sort().join(',')],
    queryFn: getFollowedAuthorSignals,
    enabled: followedIds.size > 0,
    staleTime: 60_000,
    placeholderData: keepPreviousData,
  })
  const signalByAuthorId = useMemo(() => {
    const map = new Map<string, AuthorSignal | null>()
    for (const [id, signal] of Object.entries(followedSignalsQuery.data ?? {})) {
      map.set(id, signal)
    }
    return map
  }, [followedSignalsQuery.data])
  const followedAuthors = useMemo(
    () =>
      authors
        .filter((a) => followedIds.has(a.id))
        .sort((a, b) => a.name.localeCompare(b.name)),
    [authors, followedIds],
  )
  const authorsById = useMemo(() => {
    const map = new Map<string, Author>()
    for (const a of authors) map.set(a.id, a)
    return map
  }, [authors])

  const attentionRows = useMemo(
    () => needsAttentionQuery.data?.items ?? [],
    [needsAttentionQuery.data?.items],
  )
  // Map keyed by `authors.id` so each followed-author card can render
  // its own warning triangle in O(1). Background-author rows from the
  // needs-attention list still appear in the dedicated section below
  // — they just don't have a card to decorate.
  const attentionByAuthor = useMemo(() => {
    const map = new Map<string, AuthorNeedsAttentionRow>()
    for (const row of attentionRows) map.set(row.author_id, row)
    return map
  }, [attentionRows])

  const openDetail = (author: Author) => {
    setSelectedSuggestion(null)
    setSelectedAuthor(author)
    setDetailOpen(true)
  }

  // Deep-link landing from the global command-palette search. An author result
  // routes here as `#/authors?author=<authors.id>` (api/routes/search.py). The
  // page previously only read `?focus`, so the param was ignored and clicking an
  // author in search did nothing. Once the author list has loaded, open the
  // shared detail dialog for that id. The ref guards against reopening after the
  // user closes the dialog; a NEW id — or the same id re-arriving after the
  // param is cleared on close — re-triggers.
  const requestedAuthorId = route.params.get('author')
  const handledAuthorParamRef = useRef<string | null>(null)
  // Drop the ?author deep-link param while preserving the rest, so the same
  // author can be reopened from search later. Shared by the not-found error
  // path (below) and the dialog-close handler (44.6).
  const clearAuthorDeepLinkParam = useCallback(() => {
    if (!route.params.get('author')) return
    const nextParams = new URLSearchParams(route.params)
    nextParams.delete('author')
    window.location.hash = buildHashRoute('authors', Object.fromEntries(nextParams))
  }, [route.params])
  useEffect(() => {
    if (!requestedAuthorId) {
      handledAuthorParamRef.current = null
      return
    }
    if (handledAuthorParamRef.current === requestedAuthorId) return
    const author = authorsById.get(requestedAuthorId)
    if (author) {
      handledAuthorParamRef.current = requestedAuthorId
      openDetail(author)
      return
    }
    // Not in the map. Wait while the list is still loading; once it has
    // SUCCESSFULLY loaded and the id is still absent, that's a bad deep-link —
    // surface it loudly and drop the param instead of silently hanging (44.6).
    if (authorsQuery.isSuccess && !authorsQuery.isFetching) {
      handledAuthorParamRef.current = requestedAuthorId
      errorToast('Author not found', 'That author is no longer in your list.')
      clearAuthorDeepLinkParam()
    }
  }, [requestedAuthorId, authorsById, authorsQuery.isSuccess, authorsQuery.isFetching, clearAuthorDeepLinkParam])

  // Single shared router for the needs-attention sub-dialogs. The
  // section's row buttons AND each followed-author card's warning
  // triangle dispatch through `router.openForRow`, so dialog state
  // never duplicates and `review_candidates` / `manual_search` action
  // codes route into this page's `openDetail`.
  const attentionRouter = useAuthorAttentionRouter({
    authorsById,
    onOpenDetail: openDetail,
  })

  const openSuggestionDetail = (s: AuthorSuggestion) => {
    // If the suggestion is already backed by a local author row, open that
    // directly — full detail / publications / identifiers work.
    if (s.existing_author_id) {
      const existing = authors.find((a) => a.id === s.existing_author_id)
      if (existing) {
        openDetail(existing)
        return
      }
    }
    // Otherwise synthesize a minimal Author so the dialog header renders,
    // and pass the suggestion so the dialog can populate Overview from its
    // payload instead of trying (and failing) to fetch detail.
    const synth: Author = {
      id: s.existing_author_id ?? s.openalex_id ?? s.key,
      name: s.name,
      openalex_id: s.openalex_id ?? undefined,
      author_type: 'background',
    }
    setSelectedSuggestion(s)
    setSelectedAuthor(synth)
    setDetailOpen(true)
  }

  const isLoading = authorsQuery.isLoading || followedAuthorsQuery.isLoading
  const hasError = authorsQuery.isError || followedAuthorsQuery.isError

  return (
    <div className="space-y-8 p-6">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-alma-800">Authors</h1>
          <p className="text-sm text-slate-500">
            Suggestions drawn from your Library, followed authors that own their Feed monitor, and
            the full corpus view.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" onClick={() => setAddAuthorOpen(true)}>
            <Plus className="h-4 w-4" />
            Add author
          </Button>
          <PageTour pageKey="authors" steps={AUTHORS_TOUR} />
        </div>
      </header>

      {hasError ? (
        <Alert variant="negative">
          <AlertDescription>Could not load authors. Try reloading.</AlertDescription>
        </Alert>
      ) : null}

      <div data-tour="authors-suggestions">
        <SuggestedAuthorsRail onOpenDetail={openSuggestionDetail} />
      </div>

      <section className="space-y-3" data-tour="authors-followed">
        <header className="flex items-center gap-2">
          <Users className="h-4 w-4 text-alma-600" />
          <h2 className="text-sm font-semibold text-alma-800">Followed authors</h2>
          <span className="text-xs text-slate-500">
            {followedAuthors.length} followed · monitors run on Feed refresh
          </span>
        </header>

        {isLoading ? (
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-40 rounded-lg" />
            ))}
          </div>
        ) : followedAuthors.length === 0 ? (
          <EmptyState
            icon={Users}
            title="No followed authors yet."
            description="Follow a suggestion above or add an author by OpenAlex / ORCID."
          />
        ) : (
          <RevealList className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {followedAuthors.map((author, i) => (
              <RevealItem
                key={author.id}
                index={i}

              >
                <FollowedAuthorCard
                  author={author}
                  signal={signalByAuthorId.get(author.id) ?? null}
                  isOwner={author.id === ownerId}
                  onClick={() => openDetail(author)}
                  attentionRow={attentionByAuthor.get(author.id) ?? null}
                  onAttentionClick={() => {
                    const row = attentionByAuthor.get(author.id)
                    if (row) attentionRouter.openForRow(row)
                  }}
                />
              </RevealItem>
            ))}
          </RevealList>
        )}
      </section>

      <CorpusAuthorsTable authors={authors} followedIds={followedIds} onSelect={openDetail} />

      <div
        ref={needsAttentionRef}
        id="authors-needs-attention"
        data-tour="authors-attention"
        className={cn(
          'scroll-mt-6 rounded-lg transition-shadow',
          // Transient accent ring on arrival from a Health drilldown, so the
          // just-scrolled section is unmistakable. Brand accent = folio.
          highlightAttention && 'ring-2 ring-alma-folio ring-offset-2 ring-offset-surface-1',
        )}
      >
        <AuthorsNeedsAttentionSection
          rows={attentionRows}
          isLoading={needsAttentionQuery.isLoading}
          isError={needsAttentionQuery.isError}
          router={attentionRouter}
        />
      </div>

      {attentionRouter.dialogs}

      <AuthorDetailPanel
        author={selectedAuthor}
        suggestion={selectedSuggestion}
        isOwner={!!selectedAuthor && selectedAuthor.id === ownerId}
        open={detailOpen}
        onOpenChange={(next) => {
          setDetailOpen(next)
          if (!next) {
            setSelectedSuggestion(null)
            // Drop the ?author deep-link param on close so the SAME author can
            // be reopened from search later (a repeat click re-sets the param,
            // which the effect above then acts on). Other params are preserved.
            clearAuthorDeepLinkParam()
          }
        }}
        onDeleted={() => {
          void invalidateQueries(queryClient, ['authors'], ['library-followed-authors'])
        }}
      />

      <AddAuthorDialog
        open={addAuthorOpen}
        onOpenChange={setAddAuthorOpen}
        onSubmit={(payload) => addAuthorMutation.mutate(payload)}
        isPending={addAuthorMutation.isPending}
        isError={addAuthorMutation.isError}
        errorMessage={addAuthorMutation.error ? getApiErrorMessage(addAuthorMutation.error) : null}
      />
    </div>
  )
}
