import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { RevealList, RevealItem } from '@/components/ui/reveal'
import { Plus, Share2, Users, X } from 'lucide-react'

import {
  api,
  getApiErrorMessage,
  getFollowedAuthorSignals,
  followAuthor,
  isRetryableApiError,
  listAuthorsNeedsAttention,
  listFollowedAuthors,
  retryDelayMs,
  unfollowAuthor,
  type Author,
  type AuthorNeedsAttentionRow,
  type AuthorSignal,
  type AuthorSuggestion,
  type GraphData,
  type GraphNode,
} from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { Skeleton } from '@/components/ui/skeleton'
import { AuthorDetailPanel } from '@/components/AuthorDetailPanel'
import { PageTour, AUTHORS_TOUR } from '@/components/onboarding'
import { AddAuthorDialog, type AddAuthorPayload } from '@/components/authors/AddAuthorDialog'
import {
  authorSuggestionReasons,
  authorSuggestionSourceLabel,
} from '@/components/authors/authorSuggestionEvidence'
import { CorpusAuthorsTable } from '@/components/authors/CorpusAuthorsTable'
import { GraphMapView } from '@/components/map/GraphMapView'
import { MapAuthorPopup } from '@/components/map/MapAuthorPopup'
import type { MapNodeKind } from '@/components/map/mapNodeStyle'
import {
  AUTHOR_MAP_DEFAULTS,
  useMapSessionState,
} from '@/components/map/mapSessionState'
import { useAuthorField } from '@/components/map/useAuthorField'
import { MetricTile } from '@/components/shared/MetricTile'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { SignalChip } from '@/components/shared/SignalChip'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { StatusBadge } from '@/components/ui/status-badge'
import {
  MapDisplayTuningRows,
  MapModeSwitch,
  MapTuningPopover,
  SliderRow,
} from '@/components/map/MapChrome'
import { FollowedAuthorCard } from '@/components/authors/FollowedAuthorCard'
import { SuggestedAuthorsRail } from '@/components/authors/SuggestedAuthorsRail'
import { authorSuggestionsQueryOptions } from '@/components/authors/authorSuggestionQueries'
import {
  AuthorsNeedsAttentionSection,
} from '@/components/authors/AuthorsNeedsAttentionSection'
import { useAuthorAttentionRouter } from '@/components/authors/useAuthorAttentionRouter'
import { invalidateQueries } from '@/lib/queryHelpers'
import { buildHashRoute, useHashRoute } from '@/lib/hashRoute'
import { cn } from '@/lib/utils'
import { useToast, errorToast } from '@/hooks/useToast'

/**
 * Authors page — map-first product model (2026-07-25; sections 2026-04-23):
 *
 *   0. Author map (top)  — a FIRST-CLASS citizen like the Discovery frontier:
 *                          always visible, never behind a collapse, and drawn
 *                          on the SAME common space as the paper maps — filled
 *                          = yours (dashed halo = followed), hollow = currently
 *                          suggested, faint = the rest of the corpus. Every dot
 *                          opens a compact action card; full detail is one
 *                          action away.
 *   1. Suggested         — 5-card rail with enter/exit animations. Reject
 *                          writes a negative signal so the author is never
 *                          re-suggested; Follow promotes into section 2.
 *   2. Followed          — grid of followed-author cards with monitor
 *                          health and the shared AuthorSignalBar.
 *   3. Corpus (bottom)   — compact table of every author in the DB. Row
 *                          click opens the same detail dialog.
 *
 * Every card / row opens the shared AuthorDetailPanel dialog, which
 * bundles overview + publications + identifier-resolution into one
 * controlled popup (replaces the old inline-expansion panel).
 */
/**
 * Fold any author reference (map node id, `authors.id`, `openalex_id`) to one
 * comparable key. OpenAlex author ids are case-insensitive identifiers stored
 * with inconsistent case across our tables — see `authorsByKey` below.
 */
function authorKey(value: string | null | undefined): string {
  return (value ?? '').trim().toLowerCase()
}

interface AuthorClusterMeta {
  id: number
  label: string
  size: number
  description?: string
  word_cloud?: Array<{ term: string; weight: number }>
}

export function AuthorsPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const [selectedAuthor, setSelectedAuthor] = useState<Author | null>(null)
  const [selectedSuggestion, setSelectedSuggestion] = useState<AuthorSuggestion | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [addAuthorOpen, setAddAuthorOpen] = useState(false)
  // Network map scope: your library's authors, or the full tracked corpus
  // (which includes the authors of suggested papers).
  const [networkScope, setNetworkScope] = useMapSessionState<
    'library' | 'corpus'
  >('author-map', 'scope', AUTHOR_MAP_DEFAULTS.scope)
  const [networkResolution, setNetworkResolution] = useMapSessionState(
    'author-map',
    'resolution',
    AUTHOR_MAP_DEFAULTS.resolution,
  )
  const [networkSizeScale, setNetworkSizeScale] = useMapSessionState(
    'author-map',
    'sizeScale',
    AUTHOR_MAP_DEFAULTS.sizeScale,
  )
  const [networkDotOpacity, setNetworkDotOpacity] = useMapSessionState(
    'author-map',
    'dotOpacity',
    AUTHOR_MAP_DEFAULTS.dotOpacity,
  )
  const [networkWordScale, setNetworkWordScale] = useMapSessionState(
    'author-map',
    'wordScale',
    AUTHOR_MAP_DEFAULTS.wordScale,
  )
  const [networkWordCount, setNetworkWordCount] = useMapSessionState(
    'author-map',
    'wordCount',
    AUTHOR_MAP_DEFAULTS.wordCount,
  )
  const [networkSelected, setNetworkSelected] = useState<GraphNode | null>(null)
  const [networkPayload, setNetworkPayload] = useState<GraphData | null>(null)
  // Do not build the expensive author field in parallel with a missing map.
  // Once a payload exists it stays live for popup/drilldown scores, and the map
  // itself shares this exact React Query cache when Score/Terrain is active.
  const networkAuthorField = useAuthorField(
    networkScope,
    networkPayload !== null || networkSelected !== null,
  )

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
  const routeAction = route.params.get('action')?.trim() ?? ''
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

  useEffect(() => {
    if (routeAction !== 'follow') return
    setAddAuthorOpen(true)
    const nextParams = new URLSearchParams(route.params)
    nextParams.delete('action')
    window.history.replaceState(
      null,
      '',
      buildHashRoute('authors', Object.fromEntries(nextParams)),
    )
  }, [route.params, routeAction])

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

  const networkFollowMutation = useMutation({
    mutationFn: ({
      author,
      isFollowed,
    }: {
      author: { id: string; name: string }
      isFollowed: boolean
    }) => {
      if (isFollowed) return unfollowAuthor(author.id)
      return followAuthor(author.id, true, author.name).then(() => undefined)
    },
    onSuccess: async (_result, { author, isFollowed }) => {
      await invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['author-detail', author.id],
      )
      toast({
        title: isFollowed ? 'Unfollowed' : 'Followed',
        description: isFollowed
          ? `${author.name} is no longer followed.`
          : `${author.name} will contribute to Feed on the next refresh.`,
      })
    },
    onError: (_error, { isFollowed }) =>
      errorToast(
        isFollowed ? 'Could not unfollow author' : 'Could not follow author',
        'Try again in a moment.',
      ),
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

  // ── Map identity: one folded key per human ──────────────────────────────
  // The map's node ids are `publication_authors.openalex_id`, stored UPPER-case;
  // `authors.id` / `followed_authors.author_id` are the same OpenAlex ids stored
  // LOWER-case. A raw === comparison therefore matched NOTHING: the followed
  // halo never drew, and every popup lookup fell through to the payload's
  // fallback. OpenAlex ids are case-insensitive identifiers, so every id
  // crossing this boundary is folded through `authorKey` (2026-07-26).
  const authorsByKey = useMemo(() => {
    const map = new Map<string, Author>()
    for (const a of authors) {
      map.set(authorKey(a.id), a)
      // A local row may carry its OpenAlex id in the dedicated column instead
      // of (or as well as) in `id` — index both so either reaches the person.
      if (a.openalex_id) map.set(authorKey(a.openalex_id), a)
    }
    return map
  }, [authors])

  const followedKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const item of followedAuthorsQuery.data ?? []) {
      keys.add(authorKey(item.author_id))
      const local = authorsByKey.get(authorKey(item.author_id))
      if (local?.openalex_id) keys.add(authorKey(local.openalex_id))
    }
    return keys
  }, [followedAuthorsQuery.data, authorsByKey])

  // Authors currently offered in the suggestions rail — the map's hollow tier.
  // Same cached query the rail itself uses, so the two surfaces can never
  // disagree about who is being suggested right now.
  const suggestionsQuery = useQuery(authorSuggestionsQueryOptions())
  const suggestionsByKey = useMemo(() => {
    const map = new Map<string, AuthorSuggestion>()
    for (const s of suggestionsQuery.data ?? []) {
      for (const value of [s.key, s.openalex_id, s.existing_author_id]) {
        const key = authorKey(value)
        if (key) map.set(key, s)
      }
      // Same-human dedup collapses split OpenAlex profiles onto one row; the
      // dropped ids still name the same person on the map.
      for (const alt of s.alt_openalex_ids ?? []) map.set(authorKey(alt), s)
    }
    return map
  }, [suggestionsQuery.data])
  const suggestionForNode = useCallback(
    (node: GraphNode): AuthorSuggestion | null => {
      const direct =
        suggestionsByKey.get(authorKey(node.id)) ??
        suggestionsByKey.get(
          authorKey(
            typeof node.metadata?.openalex_id === 'string'
              ? node.metadata.openalex_id
              : undefined,
          ),
        )
      if (direct) return direct
      const local = authorsByKey.get(authorKey(node.id))
      return local
        ? suggestionsByKey.get(authorKey(local.id)) ??
            suggestionsByKey.get(authorKey(local.openalex_id)) ??
            null
        : null
    },
    [authorsByKey, suggestionsByKey],
  )

  /** Map membership tier — the SAME common space the paper maps draw: what is
   *  yours (filled), what is being offered (hollow), and the corpus context
   *  behind both (faint). Followed and library-co-author read as one "yours"
   *  tier because both are people you have already committed to; the dashed
   *  halo separates followed within it. */
  const networkNodeKind = useCallback(
    (n: GraphNode): MapNodeKind => {
      const key = authorKey(n.id)
      if (followedKeys.has(key) || n.in_library) return 'author_library'
      if (suggestionForNode(n)) return 'author_suggested'
      return 'author_corpus'
    },
    [followedKeys, suggestionForNode],
  )
  const networkNodeHalo = useCallback(
    (n: GraphNode) => followedKeys.has(authorKey(n.id)),
    [followedKeys],
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

  const networkClusters = useMemo(
    () =>
      ((((networkPayload?.metadata ?? {}) as Record<string, unknown>).clusters ??
        []) as AuthorClusterMeta[]),
    [networkPayload],
  )
  const networkSelectedCluster = useMemo(
    () =>
      networkSelected && typeof networkSelected.cluster_id === 'number'
        ? networkClusters.find((cluster) => cluster.id === networkSelected.cluster_id) ?? null
        : null,
    [networkClusters, networkSelected],
  )
  const networkSelectedSuggestion = networkSelected
    ? suggestionForNode(networkSelected)
    : null
  const networkSelectedLocal = networkSelected
    ? authorsByKey.get(authorKey(networkSelected.id)) ?? null
    : null
  const networkSelectedField = networkSelected
    ? networkAuthorField.entriesById.get(authorKey(networkSelected.id))
    : undefined
  const networkSelectedScore =
    networkSelectedField?.score ??
    (typeof networkSelected?.metadata?.score === 'number'
      ? networkSelected.metadata.score
      : null)
  const networkSelectedFollowTargetId = networkSelected
    ? networkSelectedLocal?.id ?? authorKey(networkSelected.id)
    : ''
  const networkSelectedIsFollowed = networkSelected
    ? followedKeys.has(authorKey(networkSelected.id))
    : false
  const networkSelectedInterests = networkSelected
    ? networkSelectedLocal?.interests ??
      (Array.isArray(networkSelected.metadata?.interests)
        ? networkSelected.metadata.interests.filter(
            (value): value is string => typeof value === 'string',
          )
        : [])
    : []

  // Author maps deliberately draw no links: semantic proximity already is the
  // relationship. The drilldown names the nearest people rather than reviving
  // a hairball of co-author lines.
  const networkNearbyAuthors = useMemo(() => {
    if (!networkSelected || !networkPayload) return []
    return networkPayload.nodes
      .filter((node) => node.id !== networkSelected.id)
      .map((node) => ({
        node,
        distance: Math.hypot(node.x - networkSelected.x, node.y - networkSelected.y),
      }))
      .sort((a, b) => a.distance - b.distance)
      .slice(0, 6)
  }, [networkPayload, networkSelected])
  const networkCommunityMembers = useMemo(() => {
    if (!networkSelected || !networkPayload || typeof networkSelected.cluster_id !== 'number') {
      return []
    }
    return networkPayload.nodes
      .filter(
        (node) =>
          node.cluster_id === networkSelected.cluster_id && node.id !== networkSelected.id,
      )
      .sort(
        (a, b) =>
          Number(b.metadata?.pub_count ?? 0) - Number(a.metadata?.pub_count ?? 0),
      )
      .slice(0, 8)
  }, [networkPayload, networkSelected])
  const networkCommunitySuggestionCount = useMemo(
    () =>
      networkSelected && networkPayload && typeof networkSelected.cluster_id === 'number'
        ? networkPayload.nodes.filter(
            (node) =>
              node.cluster_id === networkSelected.cluster_id && suggestionForNode(node) != null,
          ).length
        : 0,
    [networkPayload, networkSelected, suggestionForNode],
  )

  // Suggestions that have no dot on the plate. Reported as ONE honest line, not
  // as a second card grid — the rail below already renders every suggestion, so
  // duplicating them inside the map card said the same thing twice.
  //
  // Only meaningful once a payload has ARRIVED: while the corpus view is still
  // building, `networkPayload` is null, nothing is "placed", and the count
  // reads as "all 30 suggestions are unplaceable" — which is false. Measured
  // 2026-07-26: 23 of 30 had ≥2 tracked papers AND an embedded paper, i.e. they
  // were simply waiting on the layout rebuild.
  const unplacedSuggestions = useMemo(() => {
    if (networkScope !== 'corpus' || !networkPayload) return []
    const placed = new Set<string>()
    for (const node of networkPayload?.nodes ?? []) {
      placed.add(authorKey(node.id))
      if (typeof node.metadata?.openalex_id === 'string') {
        placed.add(authorKey(node.metadata.openalex_id))
      }
      const local = authorsByKey.get(authorKey(node.id))
      if (local) {
        placed.add(authorKey(local.id))
        placed.add(authorKey(local.openalex_id))
      }
    }
    return (suggestionsQuery.data ?? []).filter((suggestion) => {
      const identities = [
        suggestion.key,
        suggestion.openalex_id,
        suggestion.existing_author_id,
        ...(suggestion.alt_openalex_ids ?? []),
      ]
        .map(authorKey)
        .filter(Boolean)
      return !identities.some((identity) => placed.has(identity))
    })
  }, [authorsByKey, networkPayload, networkScope, suggestionsQuery.data])
  const placedSuggestionCount =
    networkScope === 'corpus'
      ? Math.max(0, (suggestionsQuery.data?.length ?? 0) - unplacedSuggestions.length)
      : 0

  // WHY each one is off the plate — the two reasons are not the same promise.
  // The map needs an author with ≥2 in-scope papers (`_MIN_AUTHOR_PUBS`) AND a
  // vector. Someone sitting on one paper does NOT arrive "after embedding": they
  // need a SECOND tracked paper first, which may never come. Saying "they join
  // automatically after enrichment" to all of them was a promise we can't keep.
  const unplacedNeedingPapers = useMemo(
    () => unplacedSuggestions.filter((s) => (s.local_paper_count ?? 0) < 2).length,
    [unplacedSuggestions],
  )
  const unplacedAwaitingLayout = unplacedSuggestions.length - unplacedNeedingPapers

  const openNetworkNodeDetail = (node: GraphNode) => {
    const local = authorsByKey.get(authorKey(node.id))
    const suggestion = suggestionForNode(node)
    if (suggestion && !local) {
      openSuggestionDetail(suggestion)
      return
    }
    openDetail(
      local ?? {
        id: authorKey(node.id),
        name: node.name,
        openalex_id:
          typeof node.metadata?.openalex_id === 'string'
            ? node.metadata.openalex_id
            : undefined,
        author_type: followedKeys.has(authorKey(node.id)) ? 'followed' : 'background',
      },
    )
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

      {/* Task 50 (user call 2026-07-25): the co-authorship map is a
          FIRST-CLASS citizen — top of the page, always visible, like the
          Discovery frontier map. Membership reads on the SAME channels as the
          paper maps (filled = yours, hollow = suggested, faint = context). */}
      <section data-tour="authors-network">
      {/* Proper section box in the Branch Studio idiom (user call
          2026-07-25): Card with a tinted header band, brand-face title,
          subtitle — the map is a first-class section, never a floating
          plate. */}
      <Card className="overflow-hidden">
        <CardHeader className="border-b border-[var(--color-border)] bg-surface-2">
          <CardTitle className="flex items-center gap-2 font-brand text-xl text-alma-800">
            <Share2 className="h-5 w-5 text-alma-folio" />
            Author Map
          </CardTitle>
          <p className="mt-1 max-w-3xl text-sm text-slate-600">
            One shared space for the people behind your corpus — the authors you already follow,
            the ones being suggested, and everyone else, placed by what they write about.
          </p>
        </CardHeader>
      <CardContent className="space-y-3 p-4">
        <ConceptCallout
          eyebrow="How to read this map"
          summary="Every author in scope on one plate — filled dots are yours, gold outlines are current suggestions, faint dots are context."
        >
          <p>
            Each dot is one author, placed by what they write about, so authors working on similar
            things sit together and the map shows the research communities behind your corpus.
          </p>
          <p className="mt-2">
            <strong>Who is who</strong> reads the same way as the paper maps — one common space,
            three tiers. A <strong>filled dot</strong> is yours: an author you follow, or a
            co-author of a paper you saved (a <strong>dashed ring</strong> marks the followed ones
            specifically). A <strong>gold outline</strong> marks a current author suggestion — the
            same suggestions as the rail below, shown where they actually sit relative to your
            people. Everyone else is <strong>faint</strong>: context, not a claim.
          </p>
          <p className="mt-2">
            <strong>Colour modes:</strong> Clusters shows the communities;{' '}
            <strong>Score and the Terrain overlay use the engine&apos;s internal criteria</strong>.
            Score is the mean of an author&apos;s papers&apos; latest relevance scores (0–100, green
            strong / red weak) — the same scoring Discovery uses. Terrain is the mean{' '}
            <em>signal</em> of the papers of theirs you have an opinion on (saved, rated, dismissed,
            or scored), so a green region is a community your own behaviour keeps endorsing. Authors
            you have no signal on leave the paper bare rather than washing the terrain flat.
            Terrain composes with any colouring.
          </p>
          <p className="mt-2">
            <strong>Scope:</strong> Library shows only authors of papers you saved; Corpus widens
            to every tracked paper, including the authors behind current suggestions — useful for
            spotting who anchors an area you haven&apos;t saved into yet.
          </p>
        </ConceptCallout>
        <GraphMapView
          endpoint="author-network"
          params={{
            scope: networkScope,
            cluster_resolution: networkResolution.toFixed(1),
          }}
          // Year is meaningless for an author; Score/Heat reflect the mean
          // internal score of the author's papers (same criteria as
          // Discovery) — user call 2026-07-25.
          colourModes={['clusters', 'score']}
          toolbarExtras={
            <>
              <MapModeSwitch
                value={networkScope}
                onChange={(next) => {
                  setNetworkScope(next)
                  setNetworkSelected(null)
                  setNetworkPayload(null)
                }}
                options={[
                  { value: 'library', label: 'Library', title: 'Authors of papers you saved' },
                  { value: 'corpus', label: 'Corpus', title: 'Authors across every tracked paper — including suggestions' },
                ]}
              />
              <MapTuningPopover title="Fine tuning — cluster detail, dot size, dot opacity, words">
                <SliderRow
                  label="Cluster detail"
                  value={networkResolution}
                  min={0.5}
                  max={3}
                  step={0.1}
                  format={(v) => `${v.toFixed(1)}×`}
                  onCommit={(v) => setNetworkResolution(Number(v.toFixed(1)))}
                />
                <MapDisplayTuningRows
                  sizeScale={networkSizeScale}
                  onSizeScale={setNetworkSizeScale}
                  dotOpacity={networkDotOpacity}
                  onDotOpacity={setNetworkDotOpacity}
                  wordScale={networkWordScale}
                  onWordScale={setNetworkWordScale}
                  wordCount={networkWordCount}
                  onWordCount={setNetworkWordCount}
                />
              </MapTuningPopover>
            </>
          }
          sizeScale={networkSizeScale}
          dotOpacity={networkDotOpacity}
          toponymScale={networkWordScale}
          toponymWordCount={networkWordCount}
          onPayload={setNetworkPayload}
          selectedNodeId={networkSelected?.id ?? null}
          focusClusterId={
            networkSelected &&
            typeof networkSelected.cluster_id === 'number' &&
            networkSelected.cluster_id >= 0
              ? networkSelected.cluster_id
              : null
          }
          // Membership tiers — the shared common space (see networkNodeKind).
          nodeKind={networkNodeKind}
          nodeSuggestionOutline={(node) =>
            networkScope === 'corpus' && suggestionForNode(node) != null
          }
          // The author map draws no link layer: adjacency here already IS
          // collaboration (co-authorship is a layout input), so lines would
          // re-state position as topology and bury the dots.
          showLinks={false}
          // Popup-primary: selection + cluster focus are the secondary map
          // effect; full detail is an explicit action inside the card.
          onOpenNode={setNetworkSelected}
          onBackgroundClick={() => setNetworkSelected(null)}
          // Dashed halo = an author you follow (host-documented meaning).
          nodeHalo={networkNodeHalo}
          hoverCard={(n) => {
            const key = authorKey(n.id)
            const suggestion = suggestionForNode(n)
            const score =
              networkAuthorField.scoresById.get(key) ??
              (typeof n.metadata?.score === 'number' ? n.metadata.score : null)
            return (
              <>
                <p className="line-clamp-2 font-medium text-alma-800">{n.name}</p>
                <p className="mt-0.5 text-slate-500">
                  {typeof n.metadata?.pub_count === 'number' ? `${n.metadata.pub_count} papers` : ''}
                  {typeof n.metadata?.h_index === 'number' ? ` · h-index ${n.metadata.h_index}` : ''}
                  {followedKeys.has(key)
                    ? ' · followed'
                    : n.in_library
                      ? ' · in your library'
                      : suggestion
                        ? ' · suggested'
                        : ''}
                </p>
                {score != null && (
                  <p className="mt-0.5 font-medium text-alma-800">
                    Score {Math.round(score)}/100 · mean of their papers
                  </p>
                )}
                {suggestion && (
                  <p className="mt-0.5 text-gold-700">
                    {authorSuggestionSourceLabel(suggestion.suggestion_type)}
                  </p>
                )}
              </>
            )
          }}
          renderClickCard={(n, close) => {
            const local = authorsByKey.get(authorKey(n.id))
            const isFollowed = followedKeys.has(authorKey(n.id))
            const suggestion = suggestionForNode(n)
            const liveScore =
              networkAuthorField.scoresById.get(authorKey(n.id)) ??
              (typeof n.metadata?.score === 'number' ? n.metadata.score : undefined)
            // Follow/unfollow must target the LOCAL author row when we have
            // one. Posting the payload's upper-cased OpenAlex id would not
            // match `authors.id` and would mint a duplicate author under the
            // other casing; the folded id matches the stored convention.
            const followTargetId = local?.id ?? authorKey(n.id)
            const openNetworkDetail = () => {
              close()
              openNetworkNodeDetail(n)
            }
            return (
              <MapAuthorPopup
                author={{
                  id: followTargetId,
                  name: local?.name || n.name,
                  affiliation:
                    local?.affiliation ||
                    (typeof n.metadata?.affiliation === 'string'
                      ? n.metadata.affiliation
                      : undefined),
                  publicationCount:
                    typeof n.metadata?.pub_count === 'number'
                      ? n.metadata.pub_count
                      : local?.publication_count,
                  hIndex:
                    typeof n.metadata?.h_index === 'number'
                      ? n.metadata.h_index
                      : local?.h_index,
                  score: liveScore,
                  clusterLabel:
                    typeof n.metadata?.cluster_label === 'string'
                      ? n.metadata.cluster_label
                      : undefined,
                  interests: local?.interests,
                  suggestion: suggestion
                    ? {
                        source: authorSuggestionSourceLabel(suggestion.suggestion_type),
                        reasons: authorSuggestionReasons(suggestion),
                        score: suggestion.score,
                      }
                    : null,
                }}
                isFollowed={isFollowed}
                isOwner={ownerId != null && authorKey(n.id) === authorKey(ownerId)}
                pending={
                  networkFollowMutation.isPending &&
                  networkFollowMutation.variables?.author.id === followTargetId
                }
                onFollow={() =>
                  networkFollowMutation.mutate({
                    author: { id: followTargetId, name: local?.name || n.name },
                    isFollowed: false,
                  })
                }
                onUnfollow={() =>
                  networkFollowMutation.mutate({
                    author: { id: followTargetId, name: local?.name || n.name },
                    isFollowed: true,
                  })
                }
                onOpenDetails={openNetworkDetail}
                onClose={close}
              />
            )
          }}
          legendExtras={
            <>
              <span className="inline-flex items-center gap-1.5 text-gold-700">
                <span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-gold-400" />
                gold outline = current suggestion
              </span>
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span className="inline-block h-2.5 w-2.5 rounded-full border border-dashed border-slate-500" />
                dashed ring = followed
              </span>
              {networkScope === 'corpus' && (
                <span className="text-slate-400">
                  {placedSuggestionCount}/{suggestionsQuery.data?.length ?? 0} suggestions on plate
                </span>
              )}
            </>
          }
          height={480}
        />

        {networkSelected && (
          <div className="grid items-start gap-4 border-t border-[var(--color-border)] pt-4 lg:grid-cols-2">
            <Card>
              <CardContent className="space-y-4 p-4 text-xs">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                      Author drilldown
                    </p>
                    <h2 className="mt-1 text-base font-semibold text-alma-800">
                      {networkSelectedLocal?.name || networkSelected.name}
                    </h2>
                    {(networkSelectedLocal?.affiliation ||
                      (typeof networkSelected.metadata?.affiliation === 'string'
                        ? networkSelected.metadata.affiliation
                        : '')) && (
                      <p className="mt-1 text-slate-500">
                        {networkSelectedLocal?.affiliation ||
                          String(networkSelected.metadata?.affiliation)}
                      </p>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={() => setNetworkSelected(null)}
                    className="shrink-0 rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                    aria-label="Clear author selection"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>

                <div className="flex flex-wrap gap-1">
                  {ownerId != null &&
                    authorKey(networkSelected.id) === authorKey(ownerId) && (
                      <StatusBadge tone="accent" size="sm">This is you</StatusBadge>
                    )}
                  {networkSelectedIsFollowed && (
                    <StatusBadge tone="positive" size="sm">Following</StatusBadge>
                  )}
                  {networkSelected.in_library && (
                    <StatusBadge tone="accent" size="sm">In your library</StatusBadge>
                  )}
                  {networkSelectedSuggestion && (
                    <StatusBadge tone="warning" size="sm">Suggested</StatusBadge>
                  )}
                </div>

                <div className="grid grid-cols-3 gap-2">
                  <MetricTile
                    label="Papers"
                    value={Number(
                      networkSelected.metadata?.pub_count ??
                        networkSelectedLocal?.publication_count ??
                        0,
                    )}
                    align="center"
                  />
                  <MetricTile
                    label="h-index"
                    value={Number(
                      networkSelected.metadata?.h_index ??
                        networkSelectedLocal?.h_index ??
                        0,
                    )}
                    align="center"
                  />
                  <MetricTile
                    label="Citations"
                    value={Number(
                      networkSelected.metadata?.author_citedby ??
                        networkSelectedLocal?.citedby ??
                        networkSelected.metadata?.citation_count ??
                        0,
                    )}
                    align="center"
                  />
                </div>

                {networkSelectedScore != null && (
                  <div className="flex items-center justify-between rounded-sm border border-edge-2 px-3 py-2">
                    <div>
                      <p className="font-medium text-alma-800">Internal score</p>
                      <p className="text-[10px] text-slate-400">
                        Mean relevance of scored papers
                      </p>
                    </div>
                    <ScoreMeter score={networkSelectedScore} />
                  </div>
                )}

                {networkSelectedField && networkSelectedField.signal_papers > 0 && (
                  <p className="text-slate-500">
                    Preference terrain here rests on {networkSelectedField.signal_papers} of{' '}
                    {networkSelectedField.papers} in-scope papers carrying a signal.
                  </p>
                )}

                {networkSelectedInterests.length > 0 && (
                  <div>
                    <p className="mb-1.5 font-medium text-alma-800">Research interests</p>
                    <div className="flex flex-wrap gap-1">
                      {networkSelectedInterests.slice(0, 8).map((interest) => (
                        <SignalChip key={interest} kind="topic">{interest}</SignalChip>
                      ))}
                    </div>
                  </div>
                )}

                {networkSelectedSuggestion && (
                  <div className="space-y-2 rounded-sm border border-gold-300/70 bg-gold-50/50 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div>
                        <p className="font-medium text-gold-700">Why this author is suggested</p>
                        <p className="text-[11px] text-slate-500">
                          Source: {authorSuggestionSourceLabel(
                            networkSelectedSuggestion.suggestion_type,
                          )}
                        </p>
                      </div>
                      <ScoreMeter score={networkSelectedSuggestion.score} />
                    </div>
                    <ul className="space-y-1 text-slate-600">
                      {authorSuggestionReasons(networkSelectedSuggestion).map((reason) => (
                        <li key={reason}>• {reason}</li>
                      ))}
                    </ul>
                  </div>
                )}

                <div className="flex flex-wrap gap-2 border-t border-[var(--color-border)] pt-3">
                  {!(ownerId != null &&
                    authorKey(networkSelected.id) === authorKey(ownerId)) && (
                    <Button
                      size="sm"
                      variant={networkSelectedIsFollowed ? 'outline' : 'default'}
                      disabled={networkFollowMutation.isPending}
                      onClick={() =>
                        networkFollowMutation.mutate({
                          author: {
                            id: networkSelectedFollowTargetId,
                            name: networkSelectedLocal?.name || networkSelected.name,
                          },
                          isFollowed: networkSelectedIsFollowed,
                        })
                      }
                    >
                      {networkSelectedIsFollowed ? 'Unfollow' : 'Follow'}
                    </Button>
                  )}
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => openNetworkNodeDetail(networkSelected)}
                  >
                    Open full profile
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="space-y-4 p-4 text-xs">
                <div>
                  <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                    Community &amp; relationships
                  </p>
                  <h2 className="mt-1 text-base font-semibold text-alma-800">
                    {networkSelectedCluster?.label ?? 'Unclustered'}
                  </h2>
                  {networkSelectedCluster?.description && (
                    <p className="mt-1 text-slate-500">
                      {networkSelectedCluster.description}
                    </p>
                  )}
                </div>

                <div className="grid grid-cols-3 gap-2">
                  <MetricTile
                    label="Authors"
                    value={networkSelectedCluster?.size ?? 1}
                    align="center"
                  />
                  <MetricTile
                    label="Suggestions"
                    value={networkCommunitySuggestionCount}
                    align="center"
                    tone={networkCommunitySuggestionCount > 0 ? 'warning' : 'neutral'}
                  />
                  <MetricTile
                    label="Nearby shown"
                    value={networkNearbyAuthors.length}
                    align="center"
                  />
                </div>

                {(networkSelectedCluster?.word_cloud?.length ?? 0) > 0 && (
                  <div>
                    <p className="mb-1.5 font-medium text-alma-800">Community vocabulary</p>
                    <div className="flex flex-wrap gap-1">
                      {networkSelectedCluster?.word_cloud?.slice(0, 8).map((item) => (
                        <SignalChip key={item.term} kind="topic">{item.term}</SignalChip>
                      ))}
                    </div>
                  </div>
                )}

                <div>
                  <p className="mb-2 font-medium text-alma-800">Nearest authors on the map</p>
                  {networkNearbyAuthors.length > 0 ? (
                    <ul className="grid gap-1.5 sm:grid-cols-2">
                      {networkNearbyAuthors.map(({ node, distance }) => {
                        const suggestion = suggestionForNode(node)
                        return (
                          <li key={node.id}>
                            <button
                              type="button"
                              className="flex w-full items-center gap-2 rounded-sm px-1 py-0.5 text-left hover:bg-control-quiet"
                              onClick={() => setNetworkSelected(node)}
                            >
                              <span className="min-w-0 flex-1 truncate font-medium text-slate-700">
                                {node.name}
                              </span>
                              {suggestion && (
                                <span
                                  className="h-2 w-2 shrink-0 rounded-full border-2 border-gold-400"
                                  title="Current suggestion"
                                />
                              )}
                              <span className="shrink-0 tabular-nums text-slate-400">
                                {Math.round(distance * 100)}
                              </span>
                            </button>
                          </li>
                        )
                      })}
                    </ul>
                  ) : (
                    <p className="text-slate-500">No other placed authors in this scope.</p>
                  )}
                  <p className="mt-2 text-[10px] text-slate-400">
                    Lower distance means more similar paper embeddings; it is not a quality score.
                  </p>
                </div>

                {networkCommunityMembers.length > 0 && (
                  <div className="border-t border-[var(--color-border)] pt-3">
                    <p className="mb-2 font-medium text-alma-800">Anchors in this community</p>
                    <ul className="grid gap-1.5 sm:grid-cols-2">
                      {networkCommunityMembers.map((node) => (
                        <li key={node.id}>
                          <button
                            type="button"
                            className="flex w-full items-center justify-between gap-2 text-left text-slate-600 hover:text-alma-800 hover:underline"
                            onClick={() => setNetworkSelected(node)}
                          >
                            <span className="truncate">{node.name}</span>
                            <span className="shrink-0 tabular-nums text-slate-400">
                              {Number(node.metadata?.pub_count ?? 0)} papers
                            </span>
                          </button>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* ONE quiet footnote, not a second suggestions list. The rail directly
            below this card already renders every suggestion as a full card, so
            a grid here said the same thing twice — and it gave ONE reason for a
            set that has two, promising a dot to authors who can never get one
            from enrichment alone. */}
        {networkScope === 'corpus' && unplacedSuggestions.length > 0 && (
          <p className="border-t border-[var(--color-border)] pt-3 text-xs text-slate-500">
            {unplacedSuggestions.length} of {suggestionsQuery.data?.length ?? 0} current
            suggestions have no dot yet
            {unplacedAwaitingLayout > 0 && (
              <> — {unplacedAwaitingLayout} join at the next layout rebuild</>
            )}
            {unplacedNeedingPapers > 0 && (
              <>
                {unplacedAwaitingLayout > 0 ? ', ' : ' — '}
                {unplacedNeedingPapers} need a second tracked paper first (the map places an
                author from two or more papers that carry vectors)
              </>
            )}
            . All of them stay in the rail below.
          </p>
        )}
      </CardContent>
      </Card>
      </section>

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
