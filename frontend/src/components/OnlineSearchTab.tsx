/**
 * Find & add — the canonical multi-source search + triage panel.
 *
 * Used by the Import dialog ("Online" tab) and the Discovery surface
 * ("Find & add"). User types a query (title / DOI / author:<name> /
 * OpenAlex URL or Semantic Scholar id). The backend fans out across
 * OpenAlex, Semantic Scholar, Crossref, arXiv, and bioRxiv via the
 * shared `search_across_sources` stack, deduplicates results across
 * sources, and returns them decorated with `in_library`, `paper_id`,
 * `sources` (provenance), and a personal `like_score`.
 *
 * Each result can be triaged with the shared Add / Like / Love /
 * Dislike contract (3/4/5/1). Save lands the paper in Library with
 * added_from='online_search'; Dislike writes a negative feedback
 * signal and dismisses the paper (unless it's already saved, in which
 * case only the signal is recorded). When OpenAlex can't resolve the
 * paper the full candidate is passed to the backend so Semantic
 * Scholar / Crossref / arXiv / bioRxiv-only results still land.
 *
 * Design notes:
 * - Uses InputGroup + Kbd so the search bar reads as a real command
 *   affordance, not a generic text field.
 * - Filters are hidden behind a ghost toggle so the idle state stays
 *   typographically quiet; opening them reveals two small year inputs.
 * - Results reuse PaperCard (size="default" — same as Feed normal mode), keeping reaction state
 *   and saved state driven by the backend response — no optimistic
 *   divergence.
 * - Empty / loading / error states all flow through the shared
 *   primitives so the dialog stays cohesive with the rest of ALMa.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Search, SearchX, SlidersHorizontal, Users } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { EmptyState } from '@/components/ui/empty-state'
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from '@/components/ui/input-group'
import { Kbd } from '@/components/ui/kbd'
import { PaperCard, type PaperCardPaper, SkeletonPaperCard } from '@/components/shared'
import type { PaperReaction } from '@/components/discovery/PaperActionBar'
import { toast, errorToast} from '@/hooks/useToast'
import { usePaperUndo } from '@/hooks/usePaperUndo'
import { invalidateQueries } from '@/lib/queryHelpers'
import {
  fetchAuthorTopCitedWorks,
  followAuthor,
  onlineAuthorSearch,
  onlineImportSave,
  onlineImportSearchStream,
  rejectAuthorSuggestion,
  type Author,
  type AuthorSuggestion,
  type OnlineAuthorSearchResult,
  type OnlineSearchItem,
  type ScoreBreakdown,
} from '@/api/client'
import { StatusBadge } from '@/components/ui/status-badge'
import { SuggestedAuthorCard } from '@/components/authors/SuggestedAuthorCard'
import { AuthorDetailPanel } from '@/components/AuthorDetailPanel'
import { Loader2, CheckCircle2, AlertCircle, Clock } from 'lucide-react'

// ---------------------------------------------------------------------------
// Per-source progress, used to render the "openalex ✓ · arxiv ⟳ ..." strip
// while the streaming endpoint is in flight.
// ---------------------------------------------------------------------------

type SourceStatus = 'pending' | 'partial' | 'timeout' | 'error'

interface SourceProgress {
  status: SourceStatus
  count: number
  ms?: number
  error?: string
}

// ---------------------------------------------------------------------------
// Per-row UI state derived from the backend's authoritative response.
// ---------------------------------------------------------------------------

interface RowState {
  reaction: PaperReaction
  isSaved: boolean
  pending: boolean
  /** Resolved local paper id after a save — needed to undo. */
  paperId?: string
}

const IDLE: RowState = { reaction: null, isSaved: false, pending: false }

/**
 * Translate the backend save response into the card's visible state. We
 * read ``status`` + ``action`` together so dislike-on-saved papers
 * correctly stay as ``isSaved`` while still showing the dislike reaction.
 */
function rowStateFromResponse(
  status: string,
  action: 'add' | 'like' | 'love' | 'dislike',
  paperId?: string,
): RowState {
  const isSaved = status === 'library'
  const base = { isSaved, pending: false, paperId }
  if (action === 'add') return { reaction: null, ...base }
  if (action === 'like') return { reaction: 'like', ...base }
  if (action === 'love') return { reaction: 'love', ...base }
  return { reaction: 'dislike', ...base }
}

function initialRowState(item: OnlineSearchItem): RowState {
  return {
    reaction: null,
    isSaved: !!item.in_library,
    pending: false,
  }
}

function rowKey(item: OnlineSearchItem): string {
  return item.openalex_id || item.doi || item.title
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

const DEFAULT_RESULT_PREVIEW_LIMIT = 5

interface OnlineSearchTabProps {
  onImportComplete?: () => void
  /** Seed the search input with an initial query string (e.g. from a
   *  deep link or a seed paper's title). */
  initialQuery?: string
  /** When true (and `initialQuery` is non-empty), run the search
   *  automatically on mount. Re-runs whenever the value of
   *  `initialQuery` changes. */
  autoRun?: boolean
  /**
   * When the search returns more than this many items, only the top
   * N render initially with a "Show all M results" footer that
   * expands the list. `null` disables the cap. Default: 5 (used by
   * the Discovery top-of-page Find & add slot so the section stays
   * compact); pass `null` from full-page surfaces that already give
   * the search results a column of their own.
   */
  resultPreviewLimit?: number | null
}

export function OnlineSearchTab({
  onImportComplete,
  initialQuery = '',
  autoRun = false,
  resultPreviewLimit = DEFAULT_RESULT_PREVIEW_LIMIT,
}: OnlineSearchTabProps) {
  const [query, setQuery] = useState(initialQuery)
  const [yearMin, setYearMin] = useState('')
  const [yearMax, setYearMax] = useState('')
  const [showFilters, setShowFilters] = useState(false)
  const PAGE_SIZE = 10
  const INITIAL_VISIBLE = resultPreviewLimit ?? 5
  const [visibleCount, setVisibleCount] = useState<number>(INITIAL_VISIBLE)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<OnlineSearchItem[] | null>(null)
  const [resolvedQuery, setResolvedQuery] = useState<string | null>(null)
  const [rowStates, setRowStates] = useState<Record<string, RowState>>({})
  const [sourceProgress, setSourceProgress] = useState<Record<string, SourceProgress>>({})
  const [streamFinalised, setStreamFinalised] = useState(false)
  // Author scope — populated when the query starts with `author:` so the
  // result list shows actionable author cards (Follow) instead of paper
  // cards. Mutually exclusive with `items` for a given query.
  const [authorResults, setAuthorResults] = useState<OnlineAuthorSearchResult[] | null>(null)
  const [authorPending, setAuthorPending] = useState<Record<string, boolean>>({})
  const [authorRejectPending, setAuthorRejectPending] = useState<Record<string, boolean>>({})
  // The two most-cited papers per author load in a SECOND (non-blocking)
  // request so they never gate the author list's time-to-display.
  const [authorTitlesLoading, setAuthorTitlesLoading] = useState(false)
  // Author detail popup — clicking a search card opens the SAME
  // AuthorDetailPanel the Authors page uses. When the author already has a
  // local row we open full detail; otherwise we pass a synthetic suggestion
  // so the dialog shows the OpenAlex-only view (Overview + live bibliography)
  // without forcing a follow first.
  const [selectedAuthor, setSelectedAuthor] = useState<Author | null>(null)
  const [selectedAuthorSuggestion, setSelectedAuthorSuggestion] =
    useState<AuthorSuggestion | null>(null)
  const [authorDetailOpen, setAuthorDetailOpen] = useState(false)
  const queryClient = useQueryClient()
  const abortRef = useRef<AbortController | null>(null)

  const canSearch = query.trim().length > 0 && !loading
  const lastAutoRef = useRef<string>('')

  const handleSearch = useCallback(async (overrideQuery?: string) => {
    const q = (overrideQuery ?? query).trim()
    if (!q) return
    // Cancel any in-flight stream from the previous query so its late
    // events don't bleed into the new search.
    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller
    setLoading(true)
    setError(null)
    setItems(null)
    setAuthorResults(null)
    setAuthorPending({})
    setAuthorRejectPending({})
    setAuthorTitlesLoading(false)
    setVisibleCount(INITIAL_VISIBLE)
    setRowStates({})
    setSourceProgress({})
    setStreamFinalised(false)
    setResolvedQuery(q)

    // Author scope branch — `author:foo` → propose authors to follow,
    // not papers. Skips the multi-source paper stream entirely.
    if (q.toLowerCase().startsWith('author:')) {
      try {
        const results = await onlineAuthorSearch({ query: q, limit: 15 })
        if (controller.signal.aborted) return
        setAuthorResults(results)
        // Non-blocking: fetch each author's two most-cited papers in a
        // second round-trip and merge them in once they land. The cards are
        // already on screen, so this never extends time-to-display.
        const ids = results.map((r) => r.openalex_id).filter(Boolean)
        if (ids.length > 0) {
          setAuthorTitlesLoading(true)
          fetchAuthorTopCitedWorks(ids)
            .then((titlesByOid) => {
              if (controller.signal.aborted) return
              setAuthorResults((prev) =>
                prev
                  ? prev.map((a) => ({
                      ...a,
                      top_cited_titles:
                        titlesByOid[a.openalex_id.toLowerCase()] ?? a.top_cited_titles ?? [],
                    }))
                  : prev,
              )
            })
            .catch(() => {
              // Best-effort enrichment — leave the title lists empty on failure.
            })
            .finally(() => {
              if (!controller.signal.aborted) setAuthorTitlesLoading(false)
            })
        }
      } catch (err) {
        if ((err as { name?: string })?.name !== 'AbortError') {
          setError(err instanceof Error ? err.message : 'Author search failed')
          setAuthorResults([])
        }
      } finally {
        setLoading(false)
      }
      return
    }

    // Accumulate per-source raw items so the UI can show partials as
    // they stream in. The backend's `final` event delivers the
    // personal-fit ranked + dedup'd union which replaces this buffer.
    const partialBySource: Record<string, OnlineSearchItem[]> = {}
    const updateUnion = () => {
      const seen = new Set<string>()
      const merged: OnlineSearchItem[] = []
      Object.values(partialBySource).forEach((arr) => {
        arr.forEach((item) => {
          const key = rowKey(item)
          if (!key || seen.has(key)) return
          seen.add(key)
          merged.push(item)
        })
      })
      setItems(merged)
      setRowStates((prev) => {
        const next = { ...prev }
        merged.forEach((it) => {
          const k = rowKey(it)
          if (k && !(k in next)) next[k] = initialRowState(it)
        })
        return next
      })
    }

    try {
      const stream = onlineImportSearchStream(
        {
          query: q,
          limit: 20,
          year_min: yearMin ? Number(yearMin) : undefined,
          year_max: yearMax ? Number(yearMax) : undefined,
        },
        controller.signal,
      )
      for await (const event of stream) {
        if (controller.signal.aborted) break
        if (event.type === 'source_pending') {
          setSourceProgress((prev) => ({
            ...prev,
            [event.source]: { status: 'pending', count: 0 },
          }))
        } else if (event.type === 'source_partial') {
          partialBySource[event.source] = event.items || []
          setSourceProgress((prev) => ({
            ...prev,
            [event.source]: {
              status: 'partial',
              count: (event.items || []).length,
              ms: event.ms,
            },
          }))
          updateUnion()
        } else if (event.type === 'source_timeout') {
          setSourceProgress((prev) => ({
            ...prev,
            [event.source]: { status: 'timeout', count: 0, ms: event.ms },
          }))
        } else if (event.type === 'source_error') {
          setSourceProgress((prev) => ({
            ...prev,
            [event.source]: {
              status: 'error',
              count: 0,
              ms: event.ms,
              error: event.error,
            },
          }))
        } else if (event.type === 'final') {
          setItems(event.items || [])
          setRowStates(
            Object.fromEntries(
              (event.items || []).map((it) => [rowKey(it), initialRowState(it)]),
            ),
          )
          setStreamFinalised(true)
        } else if (event.type === 'error') {
          throw new Error(event.error)
        }
      }
    } catch (err) {
      if ((err as { name?: string })?.name === 'AbortError') {
        // User started a new query — silent abort is expected.
      } else {
        const message = err instanceof Error ? err.message : 'Search failed'
        setError(message)
        setItems((prev) => (prev === null ? [] : prev))
      }
    } finally {
      setLoading(false)
    }
  }, [query, yearMin, yearMax])

  // Cancel any in-flight stream when the component unmounts.
  useEffect(() => () => abortRef.current?.abort(), [])

  // Auto-run on mount / whenever `initialQuery` changes when the caller
  // requested it. Guarded by `lastAutoRef` so the effect doesn't loop
  // when the query is already loaded.
  useEffect(() => {
    if (!autoRun) return
    const seed = (initialQuery || '').trim()
    if (!seed) return
    if (lastAutoRef.current === seed) return
    lastAutoRef.current = seed
    setQuery(seed)
    void handleSearch(seed)
  }, [autoRun, initialQuery, handleSearch])

  const handleAction = useCallback(
    async (item: OnlineSearchItem, action: 'add' | 'like' | 'love' | 'dislike') => {
      const key = rowKey(item)
      if (!key) return
      setRowStates((prev) => ({
        ...prev,
        [key]: { ...(prev[key] ?? IDLE), pending: true },
      }))
      try {
        const resp = await onlineImportSave({
          openalex_id: item.openalex_id || undefined,
          doi: item.doi || undefined,
          title: item.title || undefined,
          // Pass the full candidate so non-OpenAlex results (Semantic
          // Scholar / Crossref / arXiv / bioRxiv only) can still land
          // when OpenAlex has no match for the paper.
          candidate: item,
          action,
        })
        setRowStates((prev) => ({
          ...prev,
          [key]: rowStateFromResponse(resp.status, action, resp.paper_id),
        }))
        toast({
          title:
            action === 'dislike'
              ? resp.status === 'library'
                ? 'Signal recorded'
                : 'Dismissed'
              : 'Saved to Library',
          description: resp.title || item.title,
        })
        await invalidateQueries(
          queryClient,
          ['library-papers'],
          ['library-saved'],
          ['library-workflow-summary'],
          ['reading-queue'],
          ['papers'],
        )
        onImportComplete?.()
      } catch (err) {
        setRowStates((prev) => ({
          ...prev,
          [key]: { ...(prev[key] ?? IDLE), pending: false },
        }))
        errorToast('Action failed')
      }
    },
    [onImportComplete, queryClient],
  )

  const undoMutation = usePaperUndo()

  const handleUndo = useCallback(
    (item: OnlineSearchItem, aspect: 'membership' | 'rating' | 'reading') => {
      const key = rowKey(item)
      if (!key) return
      const paperId = rowStates[key]?.paperId || item.paper_id
      if (!paperId) return
      undoMutation.mutate({ paperId, aspect })
      setRowStates((prev) => {
        const cur = prev[key] ?? IDLE
        const next =
          aspect === 'membership'
            ? { ...cur, isSaved: false, reaction: null }
            : aspect === 'rating'
              ? { ...cur, reaction: null }
              : cur
        return { ...prev, [key]: next }
      })
    },
    [rowStates, undoMutation],
  )

  const handleFollowAuthor = useCallback(
    async (author: OnlineAuthorSearchResult) => {
      if (author.already_followed) return
      setAuthorPending((prev) => ({ ...prev, [author.openalex_id]: true }))
      try {
        // Follow goes through the canonical pipeline (resolve_canonical_author_id
        // + apply_follow_state → dedup + hydration + monitor sync). The returned
        // author_id is the local row we can now link the card to.
        const followed = await followAuthor(author.openalex_id, true)
        setAuthorResults((prev) =>
          prev
            ? prev.map((a) =>
                a.openalex_id === author.openalex_id
                  ? {
                      ...a,
                      already_followed: true,
                      existing_author_id: followed.author_id,
                      existing_author_type: 'followed',
                    }
                  : a,
              )
            : prev,
        )
        toast({ title: 'Following', description: author.name })
        await invalidateQueries(
          queryClient,
          ['followed-authors'],
          ['authors'],
          ['author-suggestions'],
          ['feed-monitors'],
        )
      } catch (err) {
        errorToast(err instanceof Error ? err.message : 'Could not follow author')
      } finally {
        setAuthorPending((prev) => ({ ...prev, [author.openalex_id]: false }))
      }
    },
    [queryClient],
  )

  // Dismiss = reject the author (writes a negative signal so they stop being
  // suggested), mirroring the suggestion rail's Dismiss. The card is removed
  // optimistically.
  const handleRejectAuthor = useCallback(
    async (author: OnlineAuthorSearchResult) => {
      // Never write a negative signal against an author you follow (the card
      // disables Dismiss for them; this guards programmatic callers too).
      if (author.already_followed || author.existing_author_type === 'followed') return
      setAuthorRejectPending((prev) => ({ ...prev, [author.openalex_id]: true }))
      try {
        await rejectAuthorSuggestion(author.openalex_id, 'online_search')
        setAuthorResults((prev) =>
          prev ? prev.filter((a) => a.openalex_id !== author.openalex_id) : prev,
        )
        toast({ title: 'Dismissed', description: `${author.name} won't be suggested.` })
        await invalidateQueries(queryClient, ['author-suggestions'])
      } catch (err) {
        errorToast(err instanceof Error ? err.message : 'Could not dismiss author')
      } finally {
        setAuthorRejectPending((prev) => ({ ...prev, [author.openalex_id]: false }))
      }
    },
    [queryClient],
  )

  // Open the shared AuthorDetailPanel for a search result. Mirrors the
  // Authors page `openSuggestionDetail`: when the human already has a local
  // row (followed OR background — resolved by the backend's dedup union) we
  // open full detail by its id; otherwise we pass the mapped suggestion so the
  // dialog renders the OpenAlex-only view (topics + live bibliography) without
  // 404ing on the local detail endpoints.
  const openAuthorDetail = useCallback((author: OnlineAuthorSearchResult) => {
    if (author.existing_author_id) {
      setSelectedAuthorSuggestion(null)
      setSelectedAuthor({
        id: author.existing_author_id,
        name: author.name,
        openalex_id: author.openalex_id,
        author_type: author.existing_author_type ?? 'background',
      })
      setAuthorDetailOpen(true)
      return
    }
    setSelectedAuthorSuggestion(searchResultToSuggestion(author))
    setSelectedAuthor({
      id: author.openalex_id,
      name: author.name,
      openalex_id: author.openalex_id,
      orcid: author.orcid ?? undefined,
      affiliation: author.institution ?? undefined,
      h_index: author.h_index || undefined,
      citedby: author.cited_by_count || undefined,
      works_count: author.works_count || undefined,
      author_type: 'background',
    })
    setAuthorDetailOpen(true)
  }, [])

  // ── Result count + filter echo ──
  const resultHeader = useMemo(() => {
    if (!items || !resolvedQuery || error) return null
    const count = items.length
    const parts: string[] = []
    if (yearMin) parts.push(`from ${yearMin}`)
    if (yearMax) parts.push(`to ${yearMax}`)
    const status = loading
      ? streamFinalised
        ? null
        : ' · ranking…'
      : null
    return (
      <p className="text-xs text-slate-500">
        <span className="font-semibold text-slate-700">{count}</span>{' '}
        {count === 1 ? 'result' : 'results'} for{' '}
        <span className="font-medium text-slate-700">“{resolvedQuery}”</span>
        {parts.length > 0 && <span className="text-slate-400"> · {parts.join(', ')}</span>}
        {status && <span className="text-slate-400">{status}</span>}
      </p>
    )
  }, [items, resolvedQuery, yearMin, yearMax, loading, streamFinalised, error])

  const filtersActive = !!(yearMin || yearMax)

  return (
    <div className="space-y-4">
      {/* ── Search bar ── */}
      <div className="space-y-2">
        <InputGroup className="h-11">
          <InputGroupAddon align="inline-start">
            <Search className="size-4 text-slate-400" aria-hidden />
          </InputGroupAddon>
          <InputGroupInput
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && canSearch) {
                e.preventDefault()
                void handleSearch()
              }
            }}
            placeholder="Title, DOI, author:name, or OpenAlex URL"
            aria-label="Search online sources"
            autoFocus
            disabled={loading}
          />
          <InputGroupAddon align="inline-end">
            <Kbd className="text-[10px]">↵</Kbd>
          </InputGroupAddon>
        </InputGroup>

        <div className="flex items-center justify-between gap-2">
          <p className="text-xs leading-relaxed text-slate-500">
            Prefix with{' '}
            <code className="rounded bg-surface-2 px-1 py-0.5 text-[11px] text-slate-600">
              author:
            </code>{' '}
            or{' '}
            <code className="rounded bg-surface-2 px-1 py-0.5 text-[11px] text-slate-600">
              title:
            </code>{' '}
            to scope the search. DOIs and OpenAlex URLs are detected automatically.
          </p>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => setShowFilters((s) => !s)}
            aria-expanded={showFilters}
            aria-controls="online-search-filters"
            className={filtersActive ? 'text-alma-700' : ''}
          >
            <SlidersHorizontal className="size-3.5" />
            Filters{filtersActive ? ` · ${[yearMin, yearMax].filter(Boolean).join('–')}` : ''}
          </Button>
        </div>

        {showFilters && (
          <div
            id="online-search-filters"
            className="flex flex-wrap items-end gap-3 rounded-md border border-[var(--color-border)] bg-surface-2/60 p-3"
          >
            <div className="space-y-1">
              <Label htmlFor="online-year-min" className="text-[11px] font-medium text-slate-600">
                From year
              </Label>
              <Input
                id="online-year-min"
                type="number"
                inputMode="numeric"
                min={1800}
                max={2100}
                value={yearMin}
                onChange={(e) => setYearMin(e.target.value)}
                placeholder="1900"
                className="h-8 w-24 text-sm"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="online-year-max" className="text-[11px] font-medium text-slate-600">
                To year
              </Label>
              <Input
                id="online-year-max"
                type="number"
                inputMode="numeric"
                min={1800}
                max={2100}
                value={yearMax}
                onChange={(e) => setYearMax(e.target.value)}
                placeholder="2026"
                className="h-8 w-24 text-sm"
              />
            </div>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => {
                setYearMin('')
                setYearMax('')
              }}
              className="ml-auto"
              disabled={!filtersActive}
            >
              Clear filters
            </Button>
          </div>
        )}
      </div>

      {/* ── Per-source progress strip ── */}
      {(loading || Object.keys(sourceProgress).length > 0) && (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500" data-testid="online-search-progress">
          {Object.entries(sourceProgress).map(([source, progress]) => {
            const Icon = progress.status === 'pending' ? Loader2
              : progress.status === 'partial' ? CheckCircle2
              : progress.status === 'timeout' ? Clock
              : AlertCircle
            const tone = progress.status === 'pending' ? 'text-slate-400'
              : progress.status === 'partial' ? 'text-success-600'
              : progress.status === 'timeout' ? 'text-warning-600'
              : 'text-critical-600'
            return (
              <span key={source} className="inline-flex items-center gap-1">
                <Icon className={`size-3 ${tone} ${progress.status === 'pending' ? 'animate-spin' : ''}`} />
                <span className="text-slate-600">{source}</span>
                {progress.status === 'partial' && (
                  <span className="text-slate-400">· {progress.count}{progress.ms != null ? ` · ${(progress.ms / 1000).toFixed(1)}s` : ''}</span>
                )}
                {progress.status === 'timeout' && <span className="text-warning-500">· timeout</span>}
                {progress.status === 'error' && <span className="text-critical-500">· error</span>}
              </span>
            )
          })}
          {loading && !streamFinalised && (
            <span className="ml-auto text-[10px] text-slate-400">ranking…</span>
          )}
        </div>
      )}

      {/* ── Results area ──
          Rendered only when there's something to show. With no active
          search the section is gone entirely (no Globe placeholder). */}
      {error && (
        <EmptyState
          icon={SearchX}
          title="Search failed"
          description={error}
          action={
            <Button type="button" size="sm" onClick={() => void handleSearch()}>
              Try again
            </Button>
          }
        />
      )}

      {!error && loading && items === null && authorResults === null && (
        <div className="space-y-3" data-testid="online-search-loading">
          <SkeletonPaperCard compact />
          <SkeletonPaperCard compact />
          <SkeletonPaperCard compact />
        </div>
      )}

      {/* Author scope results — rendered when query starts with `author:` */}
      {!error && authorResults !== null && (
        <div className="space-y-3" data-testid="online-search-author-results">
          {resolvedQuery && (
            <p className="text-xs text-slate-500">
              <span className="font-semibold text-slate-700">{authorResults.length}</span>{' '}
              {authorResults.length === 1 ? 'author' : 'authors'} for{' '}
              <span className="font-medium text-slate-700">“{resolvedQuery}”</span>
            </p>
          )}
          {authorResults.length === 0 ? (
            <EmptyState
              icon={Users}
              title="No author matches"
              description="Try a different spelling, or drop the author: prefix to search papers."
            />
          ) : (
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              {authorResults.map((author) => (
                <SuggestedAuthorCard
                  key={author.openalex_id}
                  suggestion={searchResultToSuggestion(author)}
                  showScore={false}
                  institution={author.institution}
                  titlesLoading={authorTitlesLoading}
                  alreadyFollowed={
                    author.already_followed || author.existing_author_type === 'followed'
                  }
                  onClick={() => openAuthorDetail(author)}
                  onFollow={() => void handleFollowAuthor(author)}
                  onReject={() => void handleRejectAuthor(author)}
                  followPending={!!authorPending[author.openalex_id]}
                  rejectPending={!!authorRejectPending[author.openalex_id]}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {!error && authorResults === null && !loading && items && items.length === 0 && (
        <EmptyState
          icon={SearchX}
          title="No matches"
          description="Try a DOI, an OpenAlex URL, or prefix your query with author:<name>."
        />
      )}

      {!error && authorResults === null && items && items.length > 0 && (
        <div className="space-y-3" data-testid="online-search-results">
          {resultHeader}
          <div className="space-y-2.5">
            {items.slice(0, visibleCount).map((item) => {
              const key = rowKey(item)
              const state = rowStates[key] ?? initialRowState(item)
              const paper: PaperCardPaper = {
                id: item.paper_id || item.openalex_id || key || item.title,
                title: item.title,
                authors: item.authors,
                year: item.year ?? undefined,
                journal: item.journal,
                doi: item.doi,
                publication_date: item.publication_date ?? undefined,
                cited_by_count: item.cited_by_count,
                abstract: item.abstract,
                url: item.url,
              }
              return (
                <div key={key} className="space-y-1">
                  <PaperCard
                    paper={paper}
                    size="default"
                    sources={item.sources}
                    reaction={state.reaction}
                    isSaved={state.isSaved}
                    actionDisabled={state.pending}
                    onAdd={() => void handleAction(item, 'add')}
                    onLike={() => void handleAction(item, 'like')}
                    onLove={() => void handleAction(item, 'love')}
                    onDislike={() => void handleAction(item, 'dislike')}
                    onUndo={(aspect) => handleUndo(item, aspect)}
                  />
                  <WhyChips score={item.like_score} breakdown={item.score_breakdown} />
                </div>
              )
            })}
          </div>
          {/* Reveal in PAGE_SIZE chunks. Footer disappears once every
              ranked result is on screen. */}
          {visibleCount < items.length && (
            <div className="flex justify-center pt-1">
              <Button
                type="button"
                size="sm"
                variant="ghost"
                onClick={() =>
                  setVisibleCount((prev) =>
                    Math.min(prev + PAGE_SIZE, items.length),
                  )
                }
                className="text-xs text-alma-700 hover:text-alma-800"
              >
                + {Math.min(PAGE_SIZE, items.length - visibleCount)} more
              </Button>
            </div>
          )}
        </div>
      )}

      {/* Shared author detail popup — the SAME component the Authors page
          opens on a card click. Works for not-yet-followed authors via the
          suggestion-only fallback, and for authors already in the library
          (followed/background) via full detail. */}
      <AuthorDetailPanel
        author={selectedAuthor}
        suggestion={selectedAuthorSuggestion}
        open={authorDetailOpen}
        onOpenChange={(next) => {
          setAuthorDetailOpen(next)
          if (!next) setSelectedAuthorSuggestion(null)
        }}
        onDeleted={() => {
          void invalidateQueries(
            queryClient,
            ['authors'],
            ['library-followed-authors'],
            ['author-suggestions'],
          )
        }}
      />
    </div>
  )
}

/**
 * Per-result "why" chip row rendered under each PaperCard in the Find
 * & Add results list. Replaces the planned-but-dropped sort dropdown
 * (we always rank by personal-fit; chips explain the rank). Shows the
 * top 3 contributing signals from `score_breakdown` ranked by
 * |weighted| magnitude — keeps the card dense while making the rank
 * legible. Hidden when no breakdown is available (older cached
 * candidates pre-T4).
 */
const SIGNAL_LABELS: Record<string, string> = {
  text_similarity: 'Text',
  topic_score: 'Topics',
  author_affinity: 'Authors',
  journal_affinity: 'Journal',
  recency_boost: 'Recent',
  citation_quality: 'Cited',
  feedback_adj: 'Signal',
  preference_affinity: 'Preference',
  usefulness_boost: 'Useful',
  source_relevance: 'Rank',
}

interface WhyChipsProps {
  score?: number
  breakdown?: ScoreBreakdown | null
}

function WhyChips({ score, breakdown }: WhyChipsProps) {
  if (!breakdown) return null
  const entries = Object.entries(breakdown).flatMap(([key, raw]) => {
    const label = SIGNAL_LABELS[key]
    if (!label || !raw || typeof raw !== 'object') return []
    const detail = raw as { value?: number; weighted?: number }
    const weighted = typeof detail.weighted === 'number' ? detail.weighted : 0
    const value = typeof detail.value === 'number' ? detail.value : 0
    if (Math.abs(weighted) < 0.005) return []
    return [{ key, label, value, weighted }]
  })
  if (entries.length === 0) return null
  entries.sort((a, b) => Math.abs(b.weighted) - Math.abs(a.weighted))
  const top = entries.slice(0, 3)
  return (
    <div className="flex flex-wrap items-center gap-1.5 pl-1 text-[10px] text-slate-500">
      <span className="font-medium uppercase tracking-[0.08em] text-slate-400">Why</span>
      {typeof score === 'number' && score > 0 && (
        <StatusBadge tone="neutral" size="sm" title="Personal-fit score (0–100)">
          Fit {Math.round(score)}
        </StatusBadge>
      )}
      {top.map((entry) => (
        <StatusBadge
          key={entry.key}
          tone={entry.weighted < 0 ? 'warning' : 'neutral'}
          size="sm"
          title={`${entry.label}: ${entry.value.toFixed(2)} · weighted ${entry.weighted.toFixed(2)}`}
        >
          {entry.label} {entry.value.toFixed(2)}
        </StatusBadge>
      ))}
    </div>
  )
}

/**
 * Map an OpenAlex author search result into the `AuthorSuggestion` shape the
 * Authors page uses, so the Discovery author search renders through the SAME
 * primitives: `SuggestedAuthorCard` (Follow / Dismiss, topic chips, click to
 * open) and, on click, the SAME `AuthorDetailPanel` popup. Search-specific
 * tweaks live on the card props (`showScore={false}`, `institution`): the
 * card hides the ranking bar and surfaces the author's two most-cited papers
 * (carried here as `sample_titles`) instead.
 */
function searchResultToSuggestion(a: OnlineAuthorSearchResult): AuthorSuggestion {
  return {
    key: a.openalex_id,
    name: a.name,
    suggestion_type: 'online_search',
    score: 0,
    openalex_id: a.openalex_id,
    existing_author_id: a.existing_author_id ?? null,
    known_author_type: a.existing_author_type ?? null,
    shared_paper_count: 0,
    shared_followed_count: 0,
    local_paper_count: 0,
    recent_paper_count: 0,
    shared_followed_authors: [],
    shared_topics: a.top_topics ?? [],
    shared_venues: [],
    sample_titles: a.top_cited_titles ?? [],
  }
}
