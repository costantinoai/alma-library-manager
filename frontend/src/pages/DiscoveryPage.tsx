import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ArrowDownRight,
  ArrowUpRight,
  ExternalLink,
  Eye,
  EyeOff,
  Gauge,
  Globe,
  LayoutGrid,
  LayoutList,
  Loader2,
  Map as MapIcon,
  RefreshCw,
  Rows3,
} from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'

import { DataTable } from '@/components/ui/data-table'

import {
  createLens,
  discoverSimilar,
  deleteLens,
  reorderLenses,
  dislikeRecommendation,
  dismissRecommendation,
  explainRecommendation,
  getDiscoveryStatus,
  getDiscoverySettings,
  updateDiscoverySettings,
  likeRecommendation,
  getPaperById,
  listLensRecommendations,
  listLenses,
  markLensSeen,
  readRecommendation,
  saveRecommendation,
  refreshLens,
  updateLens,
  type CustomDirection,
  type Lens,
  type LensRecommendation,
  type Publication,
  type SimilarityResultItem,
} from '@/api/client'
import { JargonHint, MetricTile, SignalChip, type SignalKind } from '@/components/shared'
import { DiscoverIcon } from '@/components/ui/brand-icons'
import { ConceptCallout } from '@/components/ui/concept-callout'
import {
  BranchExplorerPanel,
  LensManager,
  LensWeightsPanel,
  PaperDetailPanel,
} from '@/components/discovery'
import { FrontierMap } from '@/components/discovery/FrontierMap'
import { RecommendationEngagement } from '@/components/discovery/RecommendationEngagement'
import { OnlineSearchTab } from '@/components/OnlineSearchTab'
import { PageTour, DISCOVERY_TOUR } from '@/components/onboarding'
import { RecommendationProvenance } from '@/components/discovery/RecommendationProvenance'
import type { PaperReaction } from '@/components/discovery/PaperActionBar'
import { ListControlBar, PaperCard, RefreshRunningBanner, SkeletonList } from '@/components/shared'
import { SubPanel } from '@/components/ui/sub-panel'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/ui/status-badge'
import { Switch } from '@/components/ui/switch'
import { Card, CardContent } from '@/components/ui/card'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { errorToast, useToast } from '@/hooks/useToast'
import { usePaperUndo } from '@/hooks/usePaperUndo'
import { buildHashRoute, navigateTo, useHashRoute } from '@/lib/hashRoute'
import {
  invalidateAfterPaperMutation,
  invalidateQueries,
} from '@/lib/queryHelpers'
import { cn, formatPublicationDate, formatRelativeShort, formatTimestamp } from '@/lib/utils'

// List view state — mirrors the Feed page so Discovery and Feed feel
// like the same product. `relevance` keeps the lens's ranked order;
// `recent` re-sorts by publication date desc so the user can scan
// what's new in the lens without losing the underlying scoring.
type DiscoverySort = 'relevance' | 'recent'
// Task 50 M4: `map` is no longer a view mode — the frontier map is a panel
// above the list (50-B), so the list density and the map are independent.
type DiscoveryViewMode = 'compact' | 'normal' | 'extended'
// Per-refresh target — number of recommendations actually staged on
// the Discovery page after dedup, diversity, lifecycle filters, and
// truncation. The backend oversamples internally so the post-filter
// landing reliably hits this number.
const LENS_REFRESH_LIMIT = 50

function deriveDiscoveryReaction(rec: LensRecommendation): PaperReaction {
  if (rec.user_action === 'like' || rec.user_action === 'love' || rec.user_action === 'dislike') {
    return rec.user_action
  }
  const rating = Number(rec.paper?.rating ?? 0)
  if (rating >= 5) return 'love'
  if (rating >= 4) return 'like'
  if (rating > 0 && rating <= 2) return 'dislike'
  return null
}

// How many recs are visible by default in the Discovery card list
// before the user clicks "Show all". Keeps the initial scroll
// economical while letting the curious dig into the full 50.
const DEFAULT_VISIBLE_RECS = 20

export function DiscoveryPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const route = useHashRoute()
  const seedPaperId = route.params.get('seed')?.trim() ?? ''
  // Optional title carried through the URL by the pivot deep-link so the
  // Discovery page can show "Anchored on: *Title*" without a second fetch.
  // Falls back to the hash ID when the caller didn't supply it (e.g. the
  // user pasted the URL directly).
  const seedPaperTitle = route.params.get('seedTitle')?.trim() ?? ''
  const routeQuery = route.params.get('query')?.trim() ?? ''
  // T8 — `?lens=<id>` pre-selects a specific lens when landing from a
  // deep-link (e.g. the "Turn this Collection into a Discovery feed"
  // button in Library). Ignored if the lens doesn't exist.
  const routeLensId = route.params.get('lens')?.trim() ?? ''
  const routePaperId = route.params.get('paper')?.trim() ?? ''
  const routeAction = route.params.get('action')?.trim() ?? ''
  const [selectedLensId, setSelectedLensId] = useState<string | null>(null)
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  // Find & add is the manual entry point at the top of the page — open by
  // default (but collapsible; state keeps re-renders from fighting the user).
  const [findAddOpen, setFindAddOpen] = useState(true)
  const findAddRef = useRef<HTMLDetailsElement>(null)
  // Discovery already excludes Library papers when it BUILDS a deck, but keeps
  // a card visible the moment you save it so it doesn't vanish under the
  // cursor. This is the opt-in "clear them out" view; persisted, since it's a
  // stable preference rather than a transient filter.
  const [hideLibrary, setHideLibrary] = useState(
    () => window.localStorage.getItem('alma.discovery.hideLibrary') === '1',
  )
  useEffect(() => {
    window.localStorage.setItem('alma.discovery.hideLibrary', hideLibrary ? '1' : '0')
  }, [hideLibrary])
  const [detailOpen, setDetailOpen] = useState(false)
  // Track dismissed rec IDs locally for instant removal. Dismiss is the ONLY
  // action that removes a card from Discovery; save / read / like / love / add-
  // to-collection all keep the card visible (they just flip its button state).
  const [dismissedIds, setDismissedIds] = useState<Set<string>>(new Set())
  // Lazily fetched explanations keyed by rec ID
  const [explanations, setExplanations] = useState<Record<string, string | null>>({})
  // List view state — mirrors Feed: sort (relevance vs publication date),
  // density (compact / normal / extended), and bulk-selection set.
  const [sort, setSort] = useState<DiscoverySort>('relevance')
  const [viewMode, setViewMode] = useState<DiscoveryViewMode>('normal')
  // Task 50 M4 (50-B): the frontier map is a PANEL above the rec list — both
  // visible at once, selection flows down. Open state persists per user; a
  // lasso on the map can filter the list below (cleared on lens switch).
  const [mapOpen, setMapOpen] = useState(
    () => localStorage.getItem('alma.discovery.mapOpen') !== 'false',
  )
  const [mapFilterIds, setMapFilterIds] = useState<Set<string> | null>(null)
  // Clicking a suggestion dot on the map jumps to its row: selected + a
  // transient accent pulse (same idiom as the Health→Authors drilldown ring).
  const [pulsePaperId, setPulsePaperId] = useState<string | null>(null)
  const [selectedRecIds, setSelectedRecIds] = useState<Set<string>>(new Set())
  // "Show all" toggle for the rec list. False -> only the first
  // DEFAULT_VISIBLE_RECS are rendered; true -> full list.
  const [showAllRecs, setShowAllRecs] = useState(false)

  const deepLinkPaperQuery = useQuery({
    queryKey: ['discovery-deeplink-paper', routePaperId],
    queryFn: () => getPaperById(routePaperId),
    enabled: Boolean(routePaperId),
    staleTime: 30_000,
    retry: 1,
  })
  const handledPaperParam = useRef<string | null>(null)
  useEffect(() => {
    if (!routePaperId) {
      handledPaperParam.current = null
      return
    }
    if (handledPaperParam.current === routePaperId) return
    if (deepLinkPaperQuery.isError) {
      handledPaperParam.current = routePaperId
      errorToast('Paper not found', 'The linked Discovery paper could not be loaded.')
      return
    }
    if (!deepLinkPaperQuery.data) return
    handledPaperParam.current = routePaperId
    setSelectedPaper(deepLinkPaperQuery.data)
    setDetailOpen(true)
  }, [deepLinkPaperQuery.data, deepLinkPaperQuery.isError, routePaperId])

  useEffect(() => {
    if (routeAction !== 'find') return
    setFindAddOpen(true)
    window.setTimeout(() => {
      findAddRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
    const next = new URLSearchParams(route.params)
    next.delete('action')
    window.history.replaceState(
      null,
      '',
      buildHashRoute('discovery', Object.fromEntries(next)),
    )
  }, [route.params, routeAction])

  const lensesQuery = useQuery({
    queryKey: ['lenses'],
    queryFn: listLenses,
  })

  useEffect(() => {
    if (!lensesQuery.data || lensesQuery.data.length === 0) return
    // T8 — honour `?lens=<id>` deep-links first, falling back to the
    // first lens when no valid route param is present. We do this even
    // when `selectedLensId` is already set so deep-links land cleanly
    // after a cold navigation (user types the URL or clicks "Turn into
    // Discovery feed" from Library).
    if (routeLensId && lensesQuery.data.some((l) => l.id === routeLensId)) {
      if (selectedLensId !== routeLensId) setSelectedLensId(routeLensId)
      return
    }
    if (!selectedLensId) {
      setSelectedLensId(lensesQuery.data[0].id)
    }
  }, [lensesQuery.data, selectedLensId, routeLensId])

  // Reset dismissed IDs and the show-all toggle when the lens changes —
  // a fresh lens always opens to the curated first DEFAULT_VISIBLE_RECS
  // so users land on a focused page, not a 50-card scroll.
  useEffect(() => {
    setDismissedIds(new Set())
    setShowAllRecs(false)
  }, [selectedLensId])

  const lensRecommendationsQuery = useQuery({
    queryKey: ['lens-recommendations', selectedLensId, hideLibrary],
    queryFn: () =>
      listLensRecommendations(selectedLensId as string, {
        limit: 200,
        offset: 0,
        hide_library: hideLibrary,
      }),
    enabled: Boolean(selectedLensId),
    // Recommendations only change when the user refreshes the lens (an explicit
    // mutation that invalidates this key). Without a staleTime every lens switch
    // + window refocus refetched the full 200-rec list for no new data.
    staleTime: 60_000,
  })

  // Review state belongs to the lens that was actually rendered. Home only
  // reads these per-lens stamps, so visiting Home can never clear Discovery
  // carryover and opening one lens cannot silently consume another.
  const reviewedDecks = useRef<Map<string, string>>(new Map())
  useEffect(() => {
    if (!selectedLensId || !lensRecommendationsQuery.data) return
    const firstRecommendation = lensRecommendationsQuery.data[0]
    const deckKey =
      firstRecommendation?.suggestion_set_id
      ?? `${lensRecommendationsQuery.data.length}:${firstRecommendation?.created_at ?? 'empty'}`
    if (reviewedDecks.current.get(selectedLensId) === deckKey) return
    reviewedDecks.current.set(selectedLensId, deckKey)
    void markLensSeen(selectedLensId)
      .then(() => invalidateQueries(queryClient, ['home-brief']))
      .catch(() => {
        if (reviewedDecks.current.get(selectedLensId) === deckKey) {
          reviewedDecks.current.delete(selectedLensId)
        }
      })
  }, [lensRecommendationsQuery.data, queryClient, selectedLensId])

  const seededSimilarityQuery = useQuery({
    queryKey: ['discovery-seeded-similar', seedPaperId],
    queryFn: () => discoverSimilar([seedPaperId], 8),
    enabled: Boolean(seedPaperId),
    staleTime: 30_000,
  })

  const selectedLens = useMemo(() => {
    const lenses = lensesQuery.data ?? []
    return lenses.find((lens) => lens.id === selectedLensId) ?? null
  }, [lensesQuery.data, selectedLensId])

  // A collection lens is tied to a collection: saving a recommendation files
  // it into that collection (and its discovery surfaces Library papers from
  // other collections so they can be pulled in — they arrive with
  // `rec.in_library === true`).
  const selectedLensCollectionId = useMemo(() => {
    if (!selectedLens || selectedLens.context_type !== 'collection') return null
    const cfg = (selectedLens.context_config ?? {}) as Record<string, unknown>
    const id = typeof cfg.collection_id === 'string' ? cfg.collection_id.trim() : ''
    return id || null
  }, [selectedLens])

  const upsertLensCache = (lens: Lens) => {
    queryClient.setQueryData<Lens[]>(['lenses'], (prev) => {
      const current = prev ?? []
      const existingIndex = current.findIndex((item) => item.id === lens.id)
      if (existingIndex === -1) {
        return [lens, ...current]
      }
      return current.map((item) => (item.id === lens.id ? lens : item))
    })
  }

  const createLensMutation = useMutation({
    mutationFn: createLens,
    onSuccess: (lens) => {
      upsertLensCache(lens)
      setSelectedLensId(lens.id)
      toast({ title: 'Lens created', description: lens.name })
    },
    onError: () => errorToast('Create failed', 'Could not create lens.'),
  })

  const deleteLensMutation = useMutation({
    mutationFn: deleteLens,
    onSuccess: (_result, lensId) => {
      queryClient.setQueryData<Lens[]>(['lenses'], (prev) => (prev ?? []).filter((lens) => lens.id !== lensId))
      setSelectedLensId(null)
      toast({ title: 'Lens deleted', description: 'The lens was removed.' })
    },
    onError: () => errorToast('Delete failed', 'Could not delete lens.'),
  })

  const reorderLensesMutation = useMutation({
    mutationFn: reorderLenses,
    // Optimistic: reflect the new order immediately, persist in the background.
    onMutate: (orderedIds: string[]) => {
      queryClient.setQueryData<Lens[]>(['lenses'], (prev) => {
        if (!prev) return prev
        const byId = new Map(prev.map((lens) => [lens.id, lens]))
        const reordered = orderedIds
          .map((id) => byId.get(id))
          .filter((lens): lens is Lens => Boolean(lens))
        const rest = prev.filter((lens) => !orderedIds.includes(lens.id))
        return [...reordered, ...rest]
      })
    },
    onError: () => errorToast('Reorder failed', 'Could not save the lens order.'),
  })

  const updateLensMutation = useMutation({
    mutationFn: ({ lensId, weights }: { lensId: string; weights: Record<string, number> }) =>
      updateLens(lensId, { weights }),
    onSuccess: async (lens) => {
      upsertLensCache(lens)
      await invalidateQueries(queryClient,
        ['lenses'], ['lens-branches', lens.id],
      )
      toast({ title: 'Weights saved', description: 'Channel weights updated.' })
    },
    onError: () => errorToast('Update failed', 'Could not save weights.'),
  })

  const refreshLensMutation = useMutation({
    mutationFn: ({ lensId, limit }: { lensId: string; limit: number }) => refreshLens(lensId, limit),
    onSuccess: (envelope) => {
      // The refresh runs in the APS pool. useOperationToasts owns the single
      // outcome toast ("Discovery refresh complete/failed" with the result
      // counts) and auto-invalidates `lens-recommendations` etc. on
      // completion — so we deliberately don't toast a redundant "queued" here.
      // Clear the actioned-id overlay now so the incoming rec set isn't masked
      // by stale state.
      setDismissedIds(new Set())
      if (envelope.status === 'already_running') {
        toast({
          title: 'Refresh already running',
          description: 'Track progress in Activity — you’ll get a notification when it finishes.',
        })
      }
    },
    onError: () => errorToast('Refresh failed', 'Could not queue lens refresh.'),
  })

  // Adopt a selected map region as a custom direction on the current lens
  // (task 47 §8): merge it into branch_controls.custom_directions and refresh so
  // retrieval deepens toward it. Member IDS are stored (the backend recomputes
  // the centroid live), so the direction never goes stale.
  const adoptDirectionMutation = useMutation({
    mutationFn: async (dir: { label: string; terms: string[]; member_paper_ids: string[] }) => {
      if (!selectedLensId || !selectedLens) throw new Error('No lens selected')
      const controls = (selectedLens.branch_controls ?? {}) as NonNullable<Lens['branch_controls']>
      const existing = controls.custom_directions ?? []
      const direction: CustomDirection = {
        id: crypto.randomUUID?.() ?? `dir-${Date.now()}`,
        label: dir.label,
        terms: dir.terms,
        member_paper_ids: dir.member_paper_ids,
        mode: 'boost',
        created_at: new Date().toISOString(),
      }
      await updateLens(selectedLensId, {
        branch_controls: { ...controls, custom_directions: [...existing, direction] },
      })
      return refreshLens(selectedLensId, 50)
    },
    onSuccess: async () => {
      setDismissedIds(new Set())
      await invalidateQueries(queryClient, ['lenses'], ['lens-branches', selectedLensId])
      toast({
        title: 'Exploring direction',
        description: 'Refreshing suggestions toward the area you selected…',
      })
    },
    onError: () => errorToast('Could not adopt direction', 'Please try again.'),
  })

  const discoveryStatusQuery = useQuery({
    queryKey: ['discovery-status'],
    // Timer-driven poll → background so an open Discovery tab doesn't pin the
    // app "active" (41.1).
    queryFn: () => getDiscoveryStatus({ background: true }),
    retry: 1,
    refetchInterval: 60_000,
  })

  // Auto-refresh opt-in. The page toggle and Settings drive the same KV-backed
  // flag; flipping it here just persists the setting (the backend scheduler is
  // the executor), so the page never blocks on a refresh.
  const discoverySettingsQuery = useQuery({
    queryKey: ['discovery-settings'],
    queryFn: getDiscoverySettings,
    retry: 1,
    staleTime: 30_000,
  })
  const autoRefresh = discoverySettingsQuery.data?.schedule
  const autoRefreshMutation = useMutation({
    mutationFn: (next: boolean) => {
      const settings = discoverySettingsQuery.data
      if (!settings) throw new Error('settings not loaded')
      // Enabling with an unset/zero interval would register no job — coerce to a
      // sane default (6h) so the page toggle always produces a working schedule.
      const interval =
        next && settings.schedule.refresh_interval_hours <= 0
          ? 6
          : settings.schedule.refresh_interval_hours
      return updateDiscoverySettings({
        ...settings,
        schedule: { ...settings.schedule, refresh_enabled: next, refresh_interval_hours: interval },
      })
    },
    onSuccess: async (saved) => {
      await invalidateQueries(queryClient, ['discovery-settings'])
      toast({
        title: saved.schedule.refresh_enabled ? 'Auto-refresh on' : 'Auto-refresh off',
        description: saved.schedule.refresh_enabled
          ? `Discovery will refresh in the background every ${saved.schedule.refresh_interval_hours}h.`
          : 'Discovery will only refresh when you click Refresh Lens.',
      })
    },
    onError: () => errorToast('Could not update auto-refresh'),
  })

  // Optimistic removal for the ONE action that hides a card: Dismiss.
  const markDismissed = (recId: string) => {
    setDismissedIds((prev) => new Set([...prev, recId]))
  }

  const undoMutation = usePaperUndo(selectedLensId)

  const dismissMutation = useMutation({
    mutationFn: dismissRecommendation,
    onSuccess: async (_data, recId) => {
      markDismissed(recId)
      toast({ title: 'Dismissed', description: 'Paper hidden from Discovery.' })
      await invalidateQueries(queryClient,
        ['lens-recommendations', selectedLensId], ['lens-signals', selectedLensId],
      )
    },
  })

  const likeMutation = useMutation({
    mutationFn: (recId: string) => likeRecommendation(recId, 4),
    onSuccess: async () => {
      toast({ title: 'Rated', description: 'Paper rated 4 stars.' })
      await invalidateQueries(queryClient,
        ['lens-recommendations', selectedLensId], ['lens-signals', selectedLensId], ['papers'],
      )
    },
  })

  const addMutation = useMutation({
    mutationFn: (recId: string) =>
      saveRecommendation(
        recId,
        undefined,
        selectedLensCollectionId ? [selectedLensCollectionId] : undefined,
      ),
    onSuccess: async () => {
      // Card stays visible (it flips to a checked "Saved" state); only Dismiss
      // removes a card from Discovery. The refetch below re-reads the rec with
      // its now-'library' paper status so the button reflects the save.
      toast({
        title: 'Added',
        description: selectedLensCollectionId
          ? "Paper added to this lens's collection."
          : 'Paper saved to library.',
      })
      await invalidateAfterPaperMutation(queryClient, selectedLensId)
      await invalidateQueries(queryClient, ['lens-recommendations', selectedLensId])
    },
  })

  // Feature A: add a recommendation to Library AND file it into one or more
  // chosen collections in a single action (the AddToCollectionMenu on the card).
  const addToCollectionsMutation = useMutation({
    mutationFn: ({ recId, collectionIds }: { recId: string; collectionIds: string[] }) =>
      saveRecommendation(recId, undefined, collectionIds),
    onSuccess: async () => {
      // Card stays visible (flips to a checked "Saved" state); only Dismiss removes.
      toast({ title: 'Added', description: 'Paper saved and filed into collection(s).' })
      await invalidateAfterPaperMutation(queryClient, selectedLensId)
      await invalidateQueries(
        queryClient,
        ['lens-recommendations', selectedLensId],
        ['library-collections'],
      )
    },
  })

  const loveMutation = useMutation({
    mutationFn: (recId: string) => likeRecommendation(recId, 5),
    onSuccess: async () => {
      toast({ title: 'Rated', description: 'Paper rated 5 stars.' })
      await invalidateQueries(queryClient,
        ['lens-recommendations', selectedLensId], ['lens-signals', selectedLensId], ['papers'],
      )
    },
  })

  // Dislike is a rating/signal only. It does not hide the card; Dismiss is
  // the explicit "hide this suggestion" action.
  const dislikeMutation = useMutation({
    mutationFn: dislikeRecommendation,
    onSuccess: async () => {
      toast({ title: 'Rated', description: 'Paper rated 1 star.' })
      await invalidateQueries(queryClient,
        ['lens-recommendations', selectedLensId], ['lens-signals', selectedLensId], ['papers'],
      )
    },
  })

  // Add to Reading List (papers.reading_status = 'reading'). D2 v3:
  // reading-list membership IS the reading state — there's no separate
  // queued step. Orthogonal to Library membership. The card STAYS visible
  // and flips to a checked "Queued" state; only Dismiss removes a card
  // from Discovery. The refetch re-reads the rec's reading_status.
  const queueMutation = useMutation({
    mutationFn: (recId: string) => readRecommendation(recId),
    onSuccess: async () => {
      toast({ title: 'Added to reading list', description: 'Marked as Reading.' })
      await invalidateQueries(queryClient,
        ['lens-recommendations', selectedLensId], ['library-workflow-summary'], ['reading-queue'], ['library-saved'],
      )
    },
    onError: () => errorToast('Queue failed', 'Could not add to reading list.'),
  })

  const allRecommendations = useMemo<LensRecommendation[]>(
    () => lensRecommendationsQuery.data ?? [],
    [lensRecommendationsQuery.data],
  )
  // Filter out ONLY dismissed papers (instantly via `dismissedIds`, and via the
  // persisted `user_action==='dismiss'` on refetch) + apply the user's sort
  // choice. Saved / read / liked cards stay in the deck showing their new state;
  // the backend scopes persisted saves/reads to the current suggestion set, so a
  // Refresh Lens still yields a clean deck. `relevance` sorts by raw score DESC
  // so visible ordering tracks the score bars on each card; the diversity-aware
  // `rank` value stays as a tie-breaker. `recent` re-sorts by publication date
  // DESC, falling back to `year` when no full date is set.
  const recommendations = useMemo(() => {
    const visible = allRecommendations.filter(
      (rec) =>
        !dismissedIds.has(rec.id) &&
        rec.user_action !== 'dismiss' &&
        // 50-B map→list sync: an active map-region filter narrows the list.
        (!mapFilterIds || mapFilterIds.has(rec.paper_id)),
    )
    if (sort === 'relevance') {
      return [...visible].sort((a, b) => {
        const sa = typeof a.score === 'number' ? a.score : 0
        const sb = typeof b.score === 'number' ? b.score : 0
        if (sb !== sa) return sb - sa
        const ra = a.rank ?? Number.POSITIVE_INFINITY
        const rb = b.rank ?? Number.POSITIVE_INFINITY
        return ra - rb
      })
    }
    const dateKey = (rec: LensRecommendation): string => {
      const direct = rec.paper?.publication_date ?? ''
      if (direct) return direct
      // Fallback so older rows without a full ISO date still sort
      // sensibly relative to one another. Year-only papers land on
      // YYYY-01-01 — better than sinking the whole batch to the end.
      const year = rec.paper?.year
      return year != null ? `${String(year).padStart(4, '0')}-01-01` : ''
    }
    return [...visible].sort((a, b) => {
      const dateA = dateKey(a)
      const dateB = dateKey(b)
      if (!dateA && !dateB) return 0
      if (!dateA) return 1
      if (!dateB) return -1
      return dateB.localeCompare(dateA)
    })
  }, [allRecommendations, dismissedIds, sort, mapFilterIds])

  // A map-region filter is lens-local — switching lens clears it.
  useEffect(() => {
    setMapFilterIds(null)
  }, [selectedLensId])

  // Bulk-selection helpers — mirror the Feed page so the affordance
  // feels identical between the two surfaces.
  const allVisibleSelected =
    recommendations.length > 0 &&
    recommendations.every((rec) => selectedRecIds.has(rec.id))
  const toggleRecSelection = (recId: string) => {
    setSelectedRecIds((prev) => {
      const next = new Set(prev)
      if (next.has(recId)) next.delete(recId)
      else next.add(recId)
      return next
    })
  }
  const toggleSelectAllVisible = () => {
    setSelectedRecIds((prev) => {
      const next = new Set(prev)
      if (allVisibleSelected) {
        for (const rec of recommendations) next.delete(rec.id)
      } else {
        for (const rec of recommendations) next.add(rec.id)
      }
      return next
    })
  }

  const fetchExplanation = (recId: string) => {
    if (recId in explanations) return // already fetched or in-flight
    setExplanations((prev) => ({ ...prev, [recId]: null })) // mark in-flight
    explainRecommendation(recId)
      .then((res) => {
        setExplanations((prev) => ({ ...prev, [recId]: res.explanation ?? null }))
      })
      .catch((err: unknown) => {
        // The score bars stay visible without an explanation, so this
        // is best-effort UX. But devs need to see when the explanation
        // endpoint is silently 5xx-ing — the in-flight `null` would
        // otherwise persist forever (no retry).
        console.warn('[DiscoveryPage] explainRecommendation failed', recId, err)
      })
  }

  // U-6: only the card whose action is in-flight should disable, not all 50.
  // Every rec mutation takes the rec id as its sole argument, so the in-flight
  // mutation's `.variables` is exactly that id.
  const pendingRecId =
    (dismissMutation.isPending && (dismissMutation.variables as string)) ||
    (likeMutation.isPending && (likeMutation.variables as string)) ||
    (addMutation.isPending && (addMutation.variables as string)) ||
    (loveMutation.isPending && (loveMutation.variables as string)) ||
    (dislikeMutation.isPending && (dislikeMutation.variables as string)) ||
    (queueMutation.isPending && (queueMutation.variables as string)) ||
    (addToCollectionsMutation.isPending && addToCollectionsMutation.variables?.recId) ||
    null
  const selectedLensSummary = (selectedLens?.last_retrieval_summary as Record<string, unknown> | null) ?? null

  /** Pull `[{label, value}]` out of one taste/negative-profile bucket. */
  const profileItems = (
    profile: unknown,
    bucket: string,
    labelKey: 'term' | 'name' | 'query',
    valueKey: 'weight' | 'strength' = 'weight',
  ): Array<{ label: string; value?: number | null }> =>
    (((profile as Record<string, unknown> | null)?.[bucket] as
      | Array<Record<string, unknown>>
      | undefined) ?? [])
      .map((item) => ({
        label: String(item[labelKey] ?? ''),
        value: Number(item[valueKey] ?? 0),
      }))
      .filter((item) => item.label)

  /**
   * One labelled row inside the taste ledger: "Topics", "Authors", … followed
   * by its chips. Strongest weight first, so the row reads as a ranking rather
   * than an arbitrary bag, and the weight rides *inside* the chip in dimmed
   * tabular figures instead of as loose text after it.
   */
  const renderTasteGroup = (
    title: string,
    kind: SignalKind,
    items: Array<{ label: string; value?: number | null }>,
  ) => {
    if (items.length === 0) return null
    const sorted = [...items].sort((a, b) => (b.value ?? 0) - (a.value ?? 0))
    return (
      <div key={title} className="space-y-1">
        <p className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">
          {title}
        </p>
        <div className="flex flex-wrap gap-1">
          {sorted.map((item, i) => (
            <SignalChip
              key={`${title}-${item.label}`}
              kind={kind}
              hideIcon={i > 0}
              title={
                item.value != null
                  ? `${item.label} · learned weight ${Math.round(item.value * 100) / 100}`
                  : item.label
              }
            >
              {item.label}
              {item.value != null && (
                <span className="ml-0.5 tabular-nums opacity-60">
                  {Math.round(item.value * 100) / 100}
                </span>
              )}
            </SignalChip>
          ))}
        </div>
      </div>
    )
  }

  // The taste ledger's two columns, built symmetrically so each side can
  // report its own emptiness honestly ("nothing suppressed yet" is a real,
  // meaningful state — not a reason to hide the column).
  // Lens id → display name, so engagement rows read as lenses not UUIDs.
  const lensNameById = useMemo(
    () => new Map((lensesQuery.data ?? []).map((l) => [l.id, l.name])),
    [lensesQuery.data],
  )

  const tasteProfile = selectedLensSummary?.taste_profile
  const negativeProfile = selectedLensSummary?.negative_profile
  const pullGroups = [
    renderTasteGroup('Topics', 'pref-topic', profileItems(tasteProfile, 'topics', 'term')),
    renderTasteGroup('Authors', 'pref-author', profileItems(tasteProfile, 'authors', 'name')),
    renderTasteGroup('Venues', 'pref-venue', profileItems(tasteProfile, 'venues', 'name')),
    renderTasteGroup(
      'Recent wins',
      'pref-query',
      profileItems(tasteProfile, 'recent_wins', 'query', 'strength'),
    ),
  ]
  const pushGroups = [
    renderTasteGroup('Topics', 'suppressed-topic', profileItems(negativeProfile, 'topics', 'term')),
    renderTasteGroup('Authors', 'suppressed-author', profileItems(negativeProfile, 'authors', 'name')),
    renderTasteGroup('Venues', 'suppressed-venue', profileItems(negativeProfile, 'venues', 'name')),
  ]

  const renderProvenance = (
    rec: LensRecommendation,
    options: { variant?: 'panel' | 'inline' } = {},
  ) => {
    const breakdown = (rec.score_breakdown ?? null) as Record<string, unknown> | null
    const readStringArray = (value: unknown): string[] =>
      Array.isArray(value)
        ? (value as unknown[]).filter(
            (t): t is string => typeof t === 'string' && t.trim().length > 0,
          )
        : []
    const readNumber = (value: unknown): number | null =>
      typeof value === 'number' && Number.isFinite(value) ? value : null
    const readString = (value: unknown): string | null =>
      typeof value === 'string' && value.trim().length > 0 ? value : null
    // T4: numeric provenance lives under `breakdown.provenance` when the
    // row was scored by the post-2026-04-24 refresh. Legacy rows have
    // these fields scattered at the top level (semantic_similarity_raw,
    // etc.) — fall back to those so nothing looks newly empty.
    const provenance = (breakdown?.provenance ?? null) as
      | Record<string, unknown>
      | null
    const specterCosine =
      readNumber(provenance?.specter_cosine) ??
      readNumber(breakdown?.semantic_similarity_raw)
    const lexicalSimilarity =
      readNumber(provenance?.lexical_similarity) ??
      readNumber(breakdown?.lexical_similarity_raw)
    return (
      <RecommendationProvenance
        variant={options.variant}
        signals={{
          branchLabel: rec.branch_label ?? null,
          branchMode: rec.branch_mode ?? null,
          sourceType: rec.source_type ?? null,
          sourceApi: rec.source_api ?? null,
          matchedQuery:
            typeof breakdown?.matched_query === 'string'
              ? (breakdown.matched_query as string)
              : null,
          branchCoreTopics: readStringArray(breakdown?.branch_core_topics),
          branchExploreTopics: readStringArray(breakdown?.branch_explore_topics),
          specterCosine,
          lexicalSimilarity,
          sharedAuthorsCount: readNumber(provenance?.shared_authors_count),
          sharedAuthorsSample: readString(provenance?.shared_authors_sample),
          negativeHit: readNumber(provenance?.negative_hit),
          scorePct: readNumber(provenance?.score_pct),
          consensusCount: readNumber(breakdown?.consensus_count),
          projectedFeedbackRaw: readNumber(breakdown?.projected_feedback_raw),
          couplingCount: readNumber(breakdown?.coupling_count),
          couplingPartnerTitle: readString(breakdown?.coupling_partner_title),
          cocitationCount: readNumber(breakdown?.cocitation_count),
          cocitationPartnerTitle: readString(breakdown?.cocitation_partner_title),
        }}
      />
    )
  }

  const renderSeededSimilarityCard = (item: SimilarityResultItem) => {
    // `paper_id` is populated for dense-fallback matches (real papers.id)
    // and mirrored in network-sourced rows when the T2 response pipeline
    // sees a merge key. Falls back to `source_key` (the lane's
    // correlation key) and finally the title so legacy cached rows still
    // render without crashing.
    const cardId = item.paper_id ?? item.source_key ?? item.title
    const sourceType = item.source_type ?? 'similar'
    return (
      <PaperCard
        key={`${cardId}:${item.title}`}
        paper={{
          id: cardId,
          title: item.title,
          authors: item.authors ?? '',
          year: item.year ?? null,
          journal: undefined,
          url: item.url ?? undefined,
          doi: item.doi ?? undefined,
        }}
        score={item.score}
        scoreBreakdown={item.score_breakdown}
        compact
        onPivot={item.paper_id ? () => navigateTo('discovery', {
          seed: item.paper_id!,
          seedTitle: item.title,
        }) : undefined}
      >
        <div className="mt-2 flex flex-wrap gap-1.5">
          <Badge variant="outline" size="sm">
            {sourceType.replace(/_/g, ' ')}
          </Badge>
        </div>
      </PaperCard>
    )
  }

  return (
    <div className="space-y-4">
      {/* ── Hero strip ─────────────────────────────────────────────────────
          Mirrors the Feed page hero so Discovery and Feed feel like the
          same product. The TopBar already shows the "Discovery" page
          title in font-brand, so this surface doesn't repeat it — it
          carries the description, a live lens-status pulse, and the
          primary Refresh action.
      ──────────────────────────────────────────────────────────────────── */}
      <section className="relative overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet">
        <div className="relative flex flex-col gap-4 p-5 md:flex-row md:items-center md:justify-between md:gap-8">
          <div className="min-w-0 flex-1 space-y-2">
            <p className="max-w-xl text-sm leading-relaxed text-slate-600">
              Context-aware recommendations across lexical, vector, graph, and
              external channels — driven by the selected lens.
            </p>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span className="inline-flex items-center gap-2">
                <span className="relative flex h-2 w-2" aria-hidden>
                  <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success-500 opacity-60" />
                  <span className="relative inline-flex h-2 w-2 rounded-full bg-success-500" />
                </span>
                <span>
                  <span className="font-semibold tabular-nums text-slate-800">
                    {(lensesQuery.data ?? []).length}
                  </span>
                  <span className="ml-1 text-slate-500">lenses</span>
                </span>
              </span>
              {selectedLens && (
                <>
                  <span className="text-slate-300" aria-hidden>·</span>
                  <span className="truncate text-alma-700">
                    Active: <span className="font-medium">{selectedLens.name}</span>
                  </span>
                </>
              )}
              <span className="text-slate-300" aria-hidden>·</span>
              <span className="tabular-nums">
                <span className="font-medium text-slate-700">{recommendations.length}</span>
                {' '}in view
              </span>
            </div>
          </div>
          <div className="flex shrink-0 flex-col items-end gap-1">
            <div className="flex items-center gap-1 self-end">
              <PageTour pageKey="discovery" steps={DISCOVERY_TOUR} />
            </div>
            <Button
              type="button"
              variant="default"
              onClick={() => selectedLensId && refreshLensMutation.mutate({ lensId: selectedLensId, limit: LENS_REFRESH_LIMIT })}
              disabled={!selectedLensId || refreshLensMutation.isPending}
              className="h-10 px-5"
            >
              {refreshLensMutation.isPending ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="mr-2 h-4 w-4" />
              )}
              Refresh Lens
            </Button>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="cursor-default text-xs text-slate-500">
                  {discoveryStatusQuery.data?.last_refresh_at
                    ? `Last refresh ${formatRelativeShort(discoveryStatusQuery.data.last_refresh_at)}`
                    : 'No refresh on record yet'}
                </span>
              </TooltipTrigger>
              <TooltipContent side="bottom">
                {discoveryStatusQuery.data?.last_refresh_at
                  ? formatTimestamp(discoveryStatusQuery.data.last_refresh_at)
                  : 'Run Refresh Lens to generate recommendations.'}
              </TooltipContent>
            </Tooltip>
            <label className="flex cursor-pointer items-center gap-2 self-end text-xs text-slate-500">
              <Switch
                checked={!!autoRefresh?.refresh_enabled}
                disabled={!discoverySettingsQuery.data || autoRefreshMutation.isPending}
                onCheckedChange={(next) => autoRefreshMutation.mutate(next)}
                aria-label="Toggle Discovery auto-refresh"
              />
              <span>
                {autoRefresh?.refresh_enabled
                  ? `Auto-refresh every ${autoRefresh.refresh_interval_hours}h`
                  : 'Auto-refresh off'}
              </span>
              <JargonHint
                title="Auto-refresh"
                description="Opt-in background refresh of Discovery on a schedule (set the interval in Settings). It runs without blocking the page — new recommendations appear automatically. Off by default."
              />
            </label>
          </div>
        </div>
      </section>

      {/* Page vocabulary — lens → branches → signals, once at the top. */}
      <ConceptCallout
        eyebrow="How Discovery works"
        summary="A lens sets what to recommend; branches are the sub-themes it pursues; signals show whether it's working."
      >
        <p className="mb-2">
          A <span className="font-medium text-alma-900">lens</span> is a saved point of view —
          "recommend from my whole library", or from a collection / topic / tag. It drives every
          recommendation below.
        </p>
        <p className="mb-2">
          Within a lens, Discovery clusters your taste into{' '}
          <span className="font-medium text-alma-900">branches</span> — the distinct sub-themes it's
          exploring. You steer them in <span className="font-medium">Tune this lens</span> (pin,
          boost, mute) and see them coloured on the frontier <span className="font-medium">Map</span>.
        </p>
        <p>
          <span className="font-medium text-alma-900">Signals</span> are the feedback loop: what you
          save, like, and dismiss reshapes the next refresh. <span className="font-medium">Lens
          performance</span> shows how your signals are landing.
        </p>
      </ConceptCallout>

      {/* Find & add — the manual entry point, first among the tools: search
          any source and add a paper by hand before (or instead of) drilling
          into a lens. Open by default; collapsible (state-controlled). */}
      <details
        ref={findAddRef}
        open={findAddOpen}
        onToggle={(e) => setFindAddOpen((e.currentTarget as HTMLDetailsElement).open)}
        className="group rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet"
      >
        <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
          <div className="flex items-center gap-2">
            <Globe className="h-4 w-4 text-alma-folio" />
            <span className="font-brand text-sm font-semibold text-alma-800">Find &amp; add a paper</span>
            <span className="text-xs text-slate-500">search any source and add it by hand</span>
          </div>
          <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
          <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
        </summary>
        <div className="border-t border-[var(--color-border)] p-4">
          <OnlineSearchTab initialQuery={routeQuery} autoRun={!!routeQuery} resultPreviewLimit={5} />
        </div>
      </details>

      {/* Anchor card — only when ?seed=<paperId>. Shows immediately
          after the hero so the user knows what they're looking at
          before they hit the lens controls. */}
      {seedPaperId && (
        <Card className="border-accent-edge bg-accent-soft">
          <CardContent className="p-4">
            <div className="mb-3 flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <StatusBadge tone="info">Anchored</StatusBadge>
                  <h3 className="truncate text-sm font-semibold text-alma-800">
                    {seedPaperTitle || seedPaperId}
                  </h3>
                </div>
                <p className="mt-1 text-xs text-slate-500">
                  Showing papers similar to this anchor. Re-root anytime with
                  the "Discover similar" action on any card below.
                  {seededSimilarityQuery.data?.dense_fallback_used ? (
                    <span className="ml-1 text-slate-600">
                      Network channels returned no new candidates — falling back to
                      SPECTER2 nearest neighbours from your corpus.
                    </span>
                  ) : null}
                </p>
              </div>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => navigateTo('discovery')}
                title="Clear the anchor and return to the lens default"
              >
                Clear anchor
              </Button>
            </div>
            {seededSimilarityQuery.isLoading ? (
              <SkeletonList count={3} compact />
            ) : seededSimilarityQuery.isError ? (
              <ErrorState message="Could not load seeded similarity for this paper." />
            ) : (seededSimilarityQuery.data?.results?.length ?? 0) === 0 ? (
              <EmptyState
                icon={DiscoverIcon}
                title="No similar papers found for this seed yet."
              />
            ) : (
              <div className="space-y-3">
                {(seededSimilarityQuery.data?.results ?? []).map(renderSeededSimilarityCard)}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Lens manager — combined select + create + delete. Always
          visible above the recommendations so the relationship
          between "selected lens" and "everything below" is obvious.
          Switching lenses respawns the recommendations + branch
          settings + lens diagnostics queries via their lens-keyed
          React Query keys. */}
      <div data-tour="discovery-lenses">
        <LensManager
          lenses={lensesQuery.data ?? []}
          selectedLensId={selectedLensId}
          onSelectLens={setSelectedLensId}
          onCreate={(payload) => createLensMutation.mutate(payload)}
          onDelete={(lensId) => deleteLensMutation.mutate(lensId)}
          onReorder={(orderedIds) => reorderLensesMutation.mutate(orderedIds)}
        />
      </div>

      <div className="space-y-4">
        {/* Branch Studio — collapsed by default. Sits above the
            recommendations list (between the lens picker and the
            results) so the affordance to tune branches is visible
            in the same vertical scan as "which lens am I on".
            Summary line carries the at-a-glance counts. */}
        <details
          data-tour="discovery-branches"
          className="group rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet"
        >
          <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
            <div className="flex flex-col gap-0.5">
              <span className="font-brand text-sm font-semibold text-alma-800">Tune this lens</span>
              <span className="text-xs text-slate-500">
                Branch Studio + weights — pin, boost, or mute the sub-themes this
                lens pursues before the next refresh.
              </span>
            </div>
            <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
            <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
          </summary>
          <div className="border-t border-[var(--color-border)]">
            <BranchExplorerPanel lens={selectedLens} />
          </div>
        </details>

        {/* Lens diagnostics — taste retrieval profile + scoring weights.
            Collapsed by default. Sits above the recommendations list so
            the lens-context surfaces (Branch Studio + this) cluster
            together right after the lens picker. */}
        <details className="group rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet">
          <summary className="flex cursor-pointer select-none items-center justify-between gap-4 px-4 py-3 text-left">
            <div className="flex min-w-0 items-start gap-3">
              <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-accent-soft text-alma-folio">
                <Gauge className="h-4 w-4" />
              </span>
              <div className="flex min-w-0 flex-col gap-1.5">
                <div className="flex flex-col gap-0.5">
                  <span className="font-brand text-sm font-semibold text-alma-800">Lens performance</span>
                  <span className="text-xs text-slate-500">
                    What this lens learned from your signals, and how the last refresh
                    composed its lanes.
                  </span>
                </div>
                {selectedLensSummary ? (
                  <div className="flex flex-wrap items-center gap-1.5">
                    {(
                      [
                        ['Mode', String(selectedLensSummary.recommendation_mode ?? '—')],
                        ['Seeds', String(Number(selectedLensSummary.seed_count ?? 0))],
                        ['Temp', Number(selectedLensSummary.temperature ?? 0).toFixed(2)],
                        [
                          'Lanes',
                          String(
                            Object.keys(
                              (selectedLensSummary.external_lanes as Record<string, unknown> | null) ?? {},
                            ).length,
                          ),
                        ],
                      ] as const
                    ).map(([label, value]) => (
                      <StatusBadge key={label} tone="neutral" className="items-baseline">
                        {label}
                        <strong className="font-mono tabular-nums text-alma-800">{value}</strong>
                      </StatusBadge>
                    ))}
                  </div>
                ) : (
                  <span className="text-xs text-slate-500">
                    Refresh the lens to capture taste-driven lane composition.
                  </span>
                )}
              </div>
            </div>
            <span className="shrink-0 text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
            <span className="hidden shrink-0 text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
          </summary>
          <div className="space-y-4 border-t border-[var(--color-border)] p-4">
          <Card>
            <CardContent className="space-y-4 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="flex items-center gap-1.5">
                    <h3 className="text-sm font-semibold text-alma-800">Taste Retrieval Profile</h3>
                    <JargonHint
                      title="Taste Retrieval Profile"
                      description={
                        <>
                          A snapshot of what this lens has learned to pull toward. Built from the
                          papers you've saved, liked, and dismissed, plus followed authors and
                          topics you've engaged with. Discovery uses it to <em>pre-filter</em>
                          candidates <strong>before</strong> ranking — so if a lens keeps surfacing
                          irrelevant papers, adjusting this profile (e.g. refining topics) is usually
                          more effective than tweaking channel weights.
                        </>
                      }
                    />
                  </div>
                  <p className="mt-0.5 text-xs text-slate-500">
                    Last refresh snapshot for the selected lens. These preferences drive candidate generation before ranking.
                  </p>
                </div>
                {selectedLensSummary?.recommendation_mode ? (
                  <Badge variant="outline" size="sm">
                    {String(selectedLensSummary.recommendation_mode)}
                  </Badge>
                ) : null}
              </div>

              {!selectedLensSummary ? (
                <EmptyState title="Refresh the lens to capture taste-driven lane composition and suppression state." />
              ) : (
                <>
                  <div className="grid gap-2 sm:grid-cols-3">
                    <MetricTile
                      label="Seeds"
                      value={Number(selectedLensSummary.seed_count ?? 0)}
                    />
                    <MetricTile
                      label="Temperature"
                      value={Number(selectedLensSummary.temperature ?? 0).toFixed(2)}
                      labelSuffix={
                        <JargonHint
                          title="Exploration Temperature"
                          description="How far afield this lens roams. 0 = tight, conservative, focused on continuity with what you've already saved. 1 = broad, speculative, more lateral / exploratory queries. Defaults around 0.28 for most lenses."
                          side="bottom"
                        />
                      }
                    />
                    <MetricTile
                      label="External Lanes"
                      value={Object.keys((selectedLensSummary.external_lanes as Record<string, unknown> | null) ?? {}).length}
                      labelSuffix={
                        <JargonHint
                          title="External Lanes"
                          description="Non-library retrieval sources this lens is pulling from — OpenAlex recent works, Semantic Scholar similar papers, arXiv preprints, etc. Each lane contributes its own slice of candidate papers before ranking combines them."
                          side="bottom"
                        />
                      }
                    />
                  </div>

                  {/* The taste ledger. This section answers one question —
                      what has this lens learned to chase, and what to steer
                      away from — so the two halves sit side by side and the
                      answer is spatial before it is textual. It used to be
                      seven identical flat chip rows, which buried the
                      pull/push duality that IS the data's meaning. */}
                  <div className="grid gap-3 md:grid-cols-2">
                    <SubPanel className="space-y-3 p-3">
                      <div className="flex items-center gap-1.5">
                        <ArrowUpRight className="h-3.5 w-3.5 text-success-700" />
                        <span className="text-xs font-semibold text-alma-800">Pulling toward</span>
                      </div>
                      {pullGroups.some(Boolean) ? (
                        pullGroups
                      ) : (
                        <p className="text-[11px] text-slate-500">
                          Nothing learned yet — save or rate a few papers and refresh.
                        </p>
                      )}
                    </SubPanel>

                    <SubPanel className="space-y-3 p-3">
                      <div className="flex items-center gap-1.5">
                        <ArrowDownRight className="h-3.5 w-3.5 text-warning-700" />
                        <span className="text-xs font-semibold text-alma-800">Steering away</span>
                      </div>
                      {pushGroups.some(Boolean) ? (
                        pushGroups
                      ) : (
                        <p className="text-[11px] text-slate-500">
                          Nothing suppressed yet. Dismissing a paper teaches this side.
                        </p>
                      )}
                    </SubPanel>
                  </div>
                </>
              )}
            </CardContent>
          </Card>
          {/* How Discovery is performing overall — moved here from the
              Analytics Overview, which described the LIBRARY. "Are these
              suggestions any good?" is a Discovery question, so it lives on
              the surface that answers it. */}
          <RecommendationEngagement lensNames={lensNameById} />

          {/* Lens scoring weights — power-user control. Hidden behind
              a disclosure so the everyday Discovery view stays quiet;
              expand only when you need to tune how signals combine. */}
          <details className="group rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet">
            <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
              <div className="flex flex-col gap-0.5">
                <span className="font-brand text-sm font-semibold text-alma-800">Advanced — scoring weights</span>
                <span className="text-xs text-slate-500">Tune how signals combine for this lens. Defaults are fine for most users.</span>
              </div>
              <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
              <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
            </summary>
            <div className="border-t border-[var(--color-border)] px-2 pb-2 pt-3">
              <LensWeightsPanel
                lens={selectedLens as Lens | null}
                onSave={(weights) => {
                  if (!selectedLensId) return
                  updateLensMutation.mutate({ lensId: selectedLensId, weights })
                }}
              />
            </div>
          </details>
          </div>
        </details>

        {/* U-1: visible while a lens refresh is still running in the APS pool
            (the POST returns instantly). Self-clears when the job goes
            terminal — useOperationToasts then raises the outcome toast +
            swaps in the fresh recs. */}
        <RefreshRunningBanner domain="discovery" label="Refreshing recommendations…" />

        {/* ── Control bar ──────────────────────────────────────────────
            Mirrors the Feed control bar so the two surfaces feel like
            the same product. Three zones: [sort] · [counter +
            select-all] · [view mode]. Nothing here mutates data — all
            controls are local view state.
        ─────────────────────────────────────────────────────────────── */}
        {/* Task 50 M4 (50-B): frontier map panel ABOVE the list — the map is a
            control surface for the deck, not an alternative to it. Collapsible,
            persisted; lasso → adopt a Direction or filter the list below. */}
        {selectedLensId && (
          <section className="space-y-2">
            <header className="flex items-center gap-2">
              <MapIcon className="h-4 w-4 text-alma-600" />
              <h3 className="text-sm font-semibold text-alma-800">Frontier map</h3>
              <span className="text-xs text-slate-500">
                your library, this lens&apos;s suggestions, and the space between
              </span>
              {mapFilterIds && (
                <button
                  type="button"
                  onClick={() => setMapFilterIds(null)}
                  className="inline-flex items-center gap-1 rounded-full border border-accent-edge bg-accent-soft px-2 py-0.5 text-xs font-medium text-alma-folio hover:opacity-80"
                  title="Clear the map-region filter"
                >
                  Map region · {recommendations.length} shown ×
                </button>
              )}
              <button
                type="button"
                onClick={() =>
                  setMapOpen((open) => {
                    localStorage.setItem('alma.discovery.mapOpen', String(!open))
                    return !open
                  })
                }
                className="ml-auto inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet"
              >
                {mapOpen ? 'Hide map' : 'Show map'}
              </button>
            </header>
            {mapOpen && (
              <ConceptCallout
                eyebrow="How to read this map"
                summary="Every paper in your corpus, placed by meaning — filled dots are yours, hollow dots are this lens's suggestions."
              >
                <p>
                  Position comes from the shared corpus layout: papers that talk about the same
                  things sit together, so a suggestion's neighbourhood tells you what it is before
                  you read it. <strong>Filled dots</strong> are papers in your library,{' '}
                  <strong>hollow dots</strong> are the lens's current suggestions (coloured by the
                  branch that found them), and <strong>faint dots</strong> are papers surfaced in
                  earlier refreshes you never acted on — your unworked frontier.
                </p>
                <p className="mt-2">
                  <strong>Colour modes:</strong> Branches shows who found what; Clusters shows the
                  corpus topics; Year is a recency ramp; <strong>Heat is the preference
                  terrain</strong> — a field built from ALL your signals (ratings, saves,
                  dismissals, engine scores) over the whole space. It belongs to the space, not the
                  view: hiding a layer never changes the terrain, so a red valley stays red even
                  when its dots are hidden.
                </p>
                <p className="mt-2">
                  <strong>Do with it:</strong> click a suggestion to jump to its row below; click
                  any paper to spotlight its cluster (background click clears); drag with{' '}
                  <em>Select a direction</em> to name a region and explore it as a Direction.
                </p>
              </ConceptCallout>
            )}
            {mapOpen && (
              <FrontierMap
                lensId={selectedLensId}
                lens={selectedLens as Lens | null}
                onSelectPaper={async (paperId) => {
                  try {
                    const paper = await getPaperById(paperId)
                    setSelectedPaper(paper)
                    setDetailOpen(true)
                  } catch {
                    /* deep-link 404s are handled elsewhere; ignore here */
                  }
                }}
                onAdoptDirection={(dir) => adoptDirectionMutation.mutate(dir)}
                onFilterList={(ids) => setMapFilterIds(new Set(ids))}
                onSelectRec={(paperId) => {
                  // Make sure the row can be on screen: unhide the long tail
                  // and drop a region filter that would exclude it.
                  setShowAllRecs(true)
                  setMapFilterIds((f) => (f && !f.has(paperId) ? null : f))
                  const rec = recommendations.find((r) => r.paper_id === paperId)
                  if (rec) {
                    setSelectedRecIds((prev) => new Set(prev).add(rec.id))
                  }
                  setPulsePaperId(paperId)
                  window.setTimeout(() => setPulsePaperId(null), 2200)
                  // Scroll after the list re-renders with the row present.
                  window.setTimeout(() => {
                    document
                      .getElementById(`rec-card-${paperId}`)
                      ?.scrollIntoView({ behavior: 'smooth', block: 'center' })
                  }, 80)
                }}
              />
            )}
          </section>
        )}

        <ListControlBar
          leading={
            <>
              <button
                type="button"
                onClick={() => setHideLibrary((v) => !v)}
                aria-pressed={hideLibrary}
                title={
                  hideLibrary
                    ? 'Showing only papers you have not saved or queued'
                    : 'Hide papers already in your Library or reading list'
                }
                className={cn(
                  'inline-flex h-7 items-center gap-1.5 rounded-sm px-2.5 text-xs font-medium transition-colors',
                  hideLibrary
                    ? 'bg-accent-soft text-alma-folio'
                    : 'text-slate-600 hover:bg-control-quiet-hover hover:text-alma-800',
                )}
              >
                {hideLibrary ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
                Unsaved only
              </button>
              <div className="h-5 w-px bg-control-edge" aria-hidden />
            </>
          }
          sort={{
            label: sort === 'relevance' ? 'Ranking' : 'Recent',
            title:
              sort === 'relevance'
                ? 'Currently sorted by lens ranking — switch to recent'
                : 'Currently sorted by recent — switch to lens ranking',
            ariaLabel: `Sort by ${sort === 'relevance' ? 'recent' : 'relevance'}`,
            onToggle: () => setSort(sort === 'relevance' ? 'recent' : 'relevance'),
          }}
          count={recommendations.length}
          selectAll={{
            allSelected: allVisibleSelected,
            onToggle: toggleSelectAllVisible,
            show: recommendations.length > 0,
          }}
          view={{
            value: viewMode,
            ariaLabel: 'Discovery view mode',
            onChange: (value) => setViewMode(value as DiscoveryViewMode),
            // Task 50 M4: `map` left the view modes — the frontier map is the
            // panel above, visible ALONGSIDE whichever list density is active.
            options: [
              { value: 'compact', label: 'Compact', icon: Rows3, title: 'Compact dense rows' },
              { value: 'normal', label: 'Normal', icon: LayoutGrid, title: 'Normal card view' },
              { value: 'extended', label: 'Extended', icon: LayoutList, title: 'Extended view — includes abstracts' },
            ],
          }}
        />

        <div className="space-y-3" data-tour="discovery-card">
          {lensRecommendationsQuery.isLoading ? (
            <SkeletonList count={5} />
          ) : recommendations.length === 0 ? (
            <EmptyState
              title={allRecommendations.length > 0 ? 'All recommendations reviewed' : 'No recommendations yet'}
              description={allRecommendations.length > 0
                ? 'Every suggestion in this lens has been saved, liked, or dismissed. Refresh to generate a fresh batch.'
                : 'Refresh the selected lens to generate a context-specific stream.'}
              action={selectedLensId ? (
                <Button
                  type="button"
                  size="sm"
                  onClick={() => refreshLensMutation.mutate({ lensId: selectedLensId, limit: LENS_REFRESH_LIMIT })}
                  disabled={refreshLensMutation.isPending}
                >
                  {refreshLensMutation.isPending ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <RefreshCw className="mr-2 h-4 w-4" />
                  )}
                  Refresh lens
                </Button>
              ) : undefined}
            />
          ) : viewMode === 'compact' ? (
            <DiscoveryCompactTable
              recommendations={recommendations}
              selectedIds={selectedRecIds}
              onSelectionChange={setSelectedRecIds}
              onOpenDetails={(paper) => {
                setSelectedPaper(paper)
                setDetailOpen(true)
              }}
            />
          ) : (
            (showAllRecs ? recommendations : recommendations.slice(0, DEFAULT_VISIBLE_RECS)).map((rec, recIdx) => {
              const paper = rec.paper ?? null
              const cardPaper = {
                id: rec.paper_id,
                title: paper?.title || rec.paper_id,
                authors: paper?.authors || '',
                year: paper?.year,
                journal: paper?.journal,
                url: paper?.url,
                doi: paper?.doi,
                publication_date: paper?.publication_date,
                cited_by_count: paper?.cited_by_count,
                rating: paper?.rating,
                status: paper?.status,
                reading_status: paper?.reading_status ?? null,
                abstract: paper?.abstract,
                // T5 — pass S2 tldr + influential count through to the
                // card so the TLDR line shows and the eventual
                // influential-citations badge can render.
                tldr: paper?.tldr ?? null,
                influential_citation_count: paper?.influential_citation_count ?? 0,
                // T15 — surface the paper_signal ranking on Discovery
                // cards too, so the "Rank N" chip is consistent with
                // Library sort. 0 hides the chip (sparse-field policy).
                global_signal_score: paper?.global_signal_score ?? 0,
              }

              return (
                <div
                  key={rec.id}
                  id={`rec-card-${rec.paper_id}`}
                  className={cn(
                    'rounded-lg transition-shadow',
                    // Transient landing ring for a map→list jump. Accent =
                    // selected, as everywhere.
                    pulsePaperId === rec.paper_id &&
                      'ring-2 ring-alma-folio ring-offset-2 ring-offset-surface-0',
                  )}
                >
                <PaperCard
                  // The `compact` viewMode is handled in the earlier
                  // branch above (line 959); by the time we reach here
                  // viewMode is narrowed to `'normal' | 'extended'`, so
                  // the size is always default.
                  size="default"
                  forceShowAbstract={viewMode === 'extended'}
                  // Normal view = dense scan: no TLDR, no abstract toggle,
                  // smaller triage buttons. Extended view keeps the full
                  // surfaces. Compact view already collapses through `size`.
                  suppressSummaries={viewMode === 'normal'}
                  compactActions={viewMode === 'normal'}
                  showActionLabels={viewMode === 'extended'}
                  selection={{
                    checked: selectedRecIds.has(rec.id),
                    onCheckedChange: () => toggleRecSelection(rec.id),
                    ariaLabel: 'Select recommendation',
                  }}
                  paper={cardPaper}
                  score={rec.score}
                  rank={recIdx + 1}
                  scoreBreakdown={rec.score_breakdown}
                  explanation={explanations[rec.id]}
                  onExpandBreakdown={() => fetchExplanation(rec.id)}
                  onDetails={() => {
                    setSelectedPaper(paper)
                    setDetailOpen(true)
                  }}
                  onDismiss={() => dismissMutation.mutate(rec.id)}
                  onAdd={() => addMutation.mutate(rec.id)}
                  onLike={() => likeMutation.mutate(rec.id)}
                  onLove={() => loveMutation.mutate(rec.id)}
                  onDislike={() => dislikeMutation.mutate(rec.id)}
                  onQueue={() => queueMutation.mutate(rec.id)}
                  onUndo={(aspect) => rec.paper_id && undoMutation.mutate({ paperId: rec.paper_id, aspect })}
                  onPivot={() => navigateTo('discovery', {
                    seed: cardPaper.id,
                    seedTitle: cardPaper.title,
                  })}
                  actionDisabled={pendingRecId === rec.id}
                  onAddToCollections={async (collectionIds) => {
                    await addToCollectionsMutation.mutateAsync({ recId: rec.id, collectionIds })
                  }}
                  defaultCollectionIds={selectedLensCollectionId ? [selectedLensCollectionId] : undefined}
                  reaction={deriveDiscoveryReaction(rec)}
                  // Gold ribbon + checked "In library" for a paper already in the
                  // Library. On a collection lens these are papers from OTHER
                  // collections (rec.in_library) — surfaced so they can be pulled
                  // in; the saved state is passive (savedReadOnly) since removing
                  // from the Library belongs in the Library, not the feed.
                  // Membership is read from the live `paper.status` join (the
                  // source of truth) — NOT `rec.user_action`, which stays stamped
                  // 'save' even after an undo and would falsely read "Saved".
                  isSaved={selectedLensCollectionId ? !!rec.in_library : paper?.status === 'library'}
                  // Reading-list membership. Reflects the "Queued" state on a card
                  // that stays visible after Add to reading list.
                  isQueued={paper?.reading_status === 'reading' || rec.user_action === 'read'}
                  savedReadOnly={!!selectedLensCollectionId && !!rec.in_library}
                  savedLabel={selectedLensCollectionId && rec.in_library ? 'In library' : undefined}
                  trailingHeader={rec.is_new ? <StatusBadge tone="positive" size="sm">New</StatusBadge> : undefined}
                >
                  {/* Normal view: provenance is folded into the card body
                      as a single chip row (no standalone "Why this surfaced"
                      section). Extended/compact still get the full panel
                      since they have room for it. */}
                  {renderProvenance(rec, {
                    variant: viewMode === 'normal' ? 'inline' : 'panel',
                  })}
                </PaperCard>
                </div>
              )
            })
          )}

          {/* Show-all toggle. Hidden in compact mode (the table already
              paginates differently) and only visible when the truncation
              actually omitted recs. Click reveals the rest of the
              already-fetched batch — no additional network request. */}
          {viewMode !== 'compact' && recommendations.length > DEFAULT_VISIBLE_RECS && (
            <div className="flex justify-center pt-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setShowAllRecs((prev) => !prev)}
              >
                {showAllRecs
                  ? `Show fewer (back to first ${DEFAULT_VISIBLE_RECS})`
                  : `Show all ${recommendations.length} recommendations`}
              </Button>
            </div>
          )}
        </div>

      </div>

      <PaperDetailPanel paper={selectedPaper} open={detailOpen} onOpenChange={setDetailOpen} />
    </div>
  )
}

// ──────────────────────────────────────────────────────────────────────
// DiscoveryCompactTable — compact-mode table for the recommendations
// list. Mirrors FeedPage's `FeedCompactTable` (same shared `DataTable`
// primitive, same column-pattern: Title / Authors / Published /
// Journal / one Discovery-specific column at the end). Discovery's
// last column is "Score" instead of Feed's "Source" since the row's
// raison d'être here is the lens-ranked relevance.
//
// Column visibility / order / sort state persists per-user via
// `storageKey="discovery.compact"`. Selection + row highlight come
// from DataTable itself via the selectedIds / onSelectionChange props.
// ──────────────────────────────────────────────────────────────────────

interface DiscoveryCompactRow {
  id: string
  rec: LensRecommendation
  paper: Publication | null
  title: string
  authors: string
  publishedSortKey: string
  publishedLabel: string
  journal: string
  scoreLabel: string
  scoreValue: number
  isNew: boolean
}

interface DiscoveryCompactTableProps {
  recommendations: LensRecommendation[]
  selectedIds: Set<string>
  onSelectionChange: (next: Set<string>) => void
  onOpenDetails: (paper: Publication | null) => void
}

function DiscoveryCompactTable({
  recommendations,
  selectedIds,
  onSelectionChange,
  onOpenDetails,
}: DiscoveryCompactTableProps) {
  const rows: DiscoveryCompactRow[] = useMemo(
    () =>
      recommendations.map((rec) => {
        const paper = rec.paper ?? null
        const score = typeof rec.score === 'number' ? rec.score : 0
        return {
          id: rec.id,
          rec,
          paper,
          title: paper?.title || rec.paper_id,
          authors: paper?.authors ?? '',
          publishedSortKey:
            paper?.publication_date ??
            (paper?.year != null ? `${paper.year}-01-01` : ''),
          publishedLabel: formatPublicationDate(paper),
          journal: paper?.journal ?? '',
          // Score is normalised in the engine to ~[0, 1]; render as a
          // 2-decimal label so the column stays narrow + tabular-aligns.
          scoreLabel: score.toFixed(2),
          scoreValue: score,
          isNew: Boolean(rec.is_new),
        }
      }),
    [recommendations],
  )

  const columns: ColumnDef<DiscoveryCompactRow>[] = useMemo(
    () => [
      {
        id: 'title',
        accessorKey: 'title',
        header: 'Title',
        size: 420,
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <div className="flex min-w-0 items-center gap-1.5">
            {row.original.isNew && <StatusBadge tone="positive">New</StatusBadge>}
            <span
              className="min-w-0 flex-1 truncate font-medium text-alma-800"
              title={row.original.title}
            >
              {row.original.title}
            </span>
            {row.original.paper?.url && (
              <a
                href={row.original.paper.url}
                target="_blank"
                rel="noopener noreferrer"
                className="shrink-0 text-slate-400 hover:text-alma-folio"
                title="Open source"
                onClick={(e) => e.stopPropagation()}
              >
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
        ),
      },
      {
        id: 'authors',
        accessorKey: 'authors',
        header: 'Authors',
        size: 200,
        cell: ({ row }) => (
          <span className="text-slate-600" title={row.original.authors}>
            {row.original.authors}
          </span>
        ),
      },
      {
        id: 'published',
        accessorKey: 'publishedSortKey',
        header: 'Published',
        size: 120,
        sortingFn: 'alphanumeric',
        cell: ({ row }) => (
          <span className="whitespace-nowrap text-slate-600">
            {row.original.publishedLabel}
          </span>
        ),
      },
      {
        id: 'journal',
        accessorKey: 'journal',
        header: 'Journal',
        size: 180,
        cell: ({ row }) => (
          <span className="text-slate-500" title={row.original.journal}>
            {row.original.journal}
          </span>
        ),
      },
      {
        id: 'score',
        accessorKey: 'scoreValue',
        header: 'Score',
        size: 80,
        sortingFn: 'basic',
        cell: ({ row }) => (
          <span className="font-brand tabular-nums text-alma-800">
            {row.original.scoreLabel}
          </span>
        ),
      },
    ],
    [],
  )

  return (
    <DataTable
      data={rows}
      columns={columns}
      storageKey="discovery.compact"
      getRowId={(row) => row.id}
      onRowClick={(row) => onOpenDetails(row.paper)}
      selectedIds={selectedIds}
      onSelectionChange={onSelectionChange}
    />
  )
}
