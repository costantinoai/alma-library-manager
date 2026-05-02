import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ArrowDownWideNarrow,
  ExternalLink,
  Globe,
  LayoutGrid,
  LayoutList,
  Loader2,
  RefreshCw,
  Rows3,
} from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'

import { DataTable } from '@/components/ui/data-table'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'

import {
  createLens,
  discoverSimilar,
  deleteLens,
  dislikeRecommendation,
  dismissRecommendation,
  explainRecommendation,
  getDiscoveryStatus,
  likeRecommendation,
  listLensRecommendations,
  listLenses,
  saveRecommendation,
  refreshLens,
  updateLens,
  updateReadingStatus,
  type Lens,
  type LensRecommendation,
  type Publication,
  type SimilarityResultItem,
} from '@/api/client'
import { JargonHint, MetricTile } from '@/components/shared'
import { DiscoverIcon } from '@/components/ui/brand-icons'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import {
  BranchExplorerPanel,
  LensManager,
  LensWeightsPanel,
  PaperDetailPanel,
} from '@/components/discovery'
import { OnlineSearchTab } from '@/components/OnlineSearchTab'
import { RecommendationProvenance } from '@/components/discovery/RecommendationProvenance'
import { PaperCard, SkeletonList } from '@/components/shared'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/ui/status-badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { errorToast, useToast } from '@/hooks/useToast'
import { navigateTo, useHashRoute } from '@/lib/hashRoute'
import {
  invalidateAfterPaperMutation,
  invalidateQueries,
} from '@/lib/queryHelpers'
import { formatPublicationDate, formatRelativeShort, formatTimestamp } from '@/lib/utils'

// List view state — mirrors the Feed page so Discovery and Feed feel
// like the same product. `relevance` keeps the lens's ranked order;
// `recent` re-sorts by publication date desc so the user can scan
// what's new in the lens without losing the underlying scoring.
type DiscoverySort = 'relevance' | 'recent'
type DiscoveryViewMode = 'compact' | 'normal' | 'extended'
const LENS_REFRESH_LIMIT = 30

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
  const [selectedLensId, setSelectedLensId] = useState<string | null>(null)
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  // Track actioned rec IDs locally for instant removal
  const [actionedIds, setActionedIds] = useState<Set<string>>(new Set())
  // Lazily fetched explanations keyed by rec ID
  const [explanations, setExplanations] = useState<Record<string, string | null>>({})
  // List view state — mirrors Feed: sort (relevance vs publication date),
  // density (compact / normal / extended), and bulk-selection set.
  const [sort, setSort] = useState<DiscoverySort>('relevance')
  const [viewMode, setViewMode] = useState<DiscoveryViewMode>('normal')
  const [selectedRecIds, setSelectedRecIds] = useState<Set<string>>(new Set())

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

  // Reset actioned IDs when lens changes
  useEffect(() => {
    setActionedIds(new Set())
  }, [selectedLensId])

  const lensRecommendationsQuery = useQuery({
    queryKey: ['lens-recommendations', selectedLensId],
    queryFn: () => listLensRecommendations(selectedLensId as string, { limit: 200, offset: 0 }),
    enabled: Boolean(selectedLensId),
  })

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
      // Refresh runs in the APS pool — useOperationToasts auto-invalidates
      // `lens-recommendations` etc. on `discovery.*` completion. Clear the
      // actioned-id overlay now so the new rec set isn't masked by stale state.
      setActionedIds(new Set())
      const queued = envelope.status === 'already_running'
        ? 'Refresh already running. Track progress in Activity.'
        : 'Refresh queued. Recommendations will appear when complete.'
      toast({ title: 'Lens refresh queued', description: queued })
    },
    onError: () => errorToast('Refresh failed', 'Could not queue lens refresh.'),
  })

  const discoveryStatusQuery = useQuery({
    queryKey: ['discovery-status'],
    queryFn: getDiscoveryStatus,
    retry: 1,
    refetchInterval: 60_000,
  })

  const markActioned = (recId: string) => {
    setActionedIds((prev) => new Set([...prev, recId]))
  }

  const dismissMutation = useMutation({
    mutationFn: dismissRecommendation,
    onSuccess: async (_data, recId) => {
      markActioned(recId)
      toast({ title: 'Dismissed', description: 'Paper hidden from discovery.' })
      await invalidateQueries(queryClient,
        ['feed-inbox'], ['library-workflow-summary'], ['lens-signals', selectedLensId],
      )
    },
  })

  const likeMutation = useMutation({
    mutationFn: (recId: string) => likeRecommendation(recId, 4),
    onSuccess: async (_data, recId) => {
      markActioned(recId)
      toast({ title: 'Liked', description: 'Paper added to library with a 4-star rating.' })
      await invalidateAfterPaperMutation(queryClient, selectedLensId)
    },
  })

  const addMutation = useMutation({
    mutationFn: (recId: string) => saveRecommendation(recId),
    onSuccess: async (_data, recId) => {
      markActioned(recId)
      toast({ title: 'Added', description: 'Paper saved to library.' })
      await invalidateAfterPaperMutation(queryClient, selectedLensId)
    },
  })

  const loveMutation = useMutation({
    mutationFn: (recId: string) => likeRecommendation(recId, 5),
    onSuccess: async (_data, recId) => {
      markActioned(recId)
      toast({ title: 'Loved', description: 'Paper added to library with 5-star rating.' })
      await invalidateAfterPaperMutation(queryClient, selectedLensId)
    },
  })

  // Dislike — negative signal, paper stays findable system-wide (per D6).
  // Card disappears from the active list locally via `markActioned`; the
  // backend still marks `user_action='dislike'` so polling won't re-surface
  // it either.
  const dislikeMutation = useMutation({
    mutationFn: dislikeRecommendation,
    onSuccess: async (_data, recId) => {
      markActioned(recId)
      toast({ title: 'Disliked', description: 'Negative signal recorded. Paper is not hidden.' })
      await invalidateQueries(queryClient,
        ['library-workflow-summary'], ['lens-signals', selectedLensId],
      )
    },
  })

  // Add to Reading List (papers.reading_status = 'reading'). D2 v3:
  // reading-list membership IS the reading state — there's no separate
  // queued step. Orthogonal to Library membership. Card disappears
  // from the active Discovery list after adding so the user sees the
  // progress.
  const queueMutation = useMutation({
    mutationFn: (args: { recId: string; paperId: string }) =>
      updateReadingStatus(args.paperId, 'reading'),
    onSuccess: async (_data, args) => {
      markActioned(args.recId)
      toast({ title: 'Added to reading list', description: 'Marked as Reading.' })
      await invalidateQueries(queryClient,
        ['library-workflow-summary'], ['reading-queue'], ['library-saved'],
      )
    },
    onError: () => errorToast('Queue failed', 'Could not add to reading list.'),
  })

  const allRecommendations: LensRecommendation[] = lensRecommendationsQuery.data ?? []
  // Filter out actioned papers instantly + apply the user's sort
  // choice. Default sort is `relevance` (the ranked order returned
  // by the API); `recent` re-sorts by paper publication date desc.
  const recommendations = useMemo(() => {
    const visible = allRecommendations.filter(
      (rec) => !actionedIds.has(rec.id) && !rec.user_action,
    )
    if (sort === 'relevance') return visible
    return [...visible].sort((a, b) => {
      const dateA = a.paper?.publication_date ?? ''
      const dateB = b.paper?.publication_date ?? ''
      // Empty dates sink to the end; otherwise lexical compare on
      // the ISO date string is correct (descending).
      if (!dateA && !dateB) return 0
      if (!dateA) return 1
      if (!dateB) return -1
      return dateB.localeCompare(dateA)
    })
  }, [allRecommendations, actionedIds, sort])

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
      .catch(() => {
        // silently fail — score bars still show
      })
  }

  const anyActionPending =
    dismissMutation.isPending ||
    likeMutation.isPending ||
    addMutation.isPending ||
    loveMutation.isPending ||
    dislikeMutation.isPending ||
    queueMutation.isPending
  const selectedLensSummary = (selectedLens?.last_retrieval_summary as Record<string, any> | null) ?? null

  const renderProfileList = (
    title: string,
    items: Array<{ label: string; value?: number | null }>,
    tone: 'positive' | 'negative' = 'positive',
  ) => {
    if (items.length === 0) return null
    return (
      <div className="space-y-2">
        <EyebrowLabel tone={tone === 'negative' ? 'muted' : 'accent'}>{title}</EyebrowLabel>
        <div className="flex flex-wrap gap-1.5">
          {items.map((item) => (
            <StatusBadge
              key={`${title}-${item.label}`}
              tone={tone === 'negative' ? 'negative' : 'neutral'}
              size="sm"
            >
              {item.label}
              {item.value != null ? ` · ${Math.round(item.value * 100) / 100}` : ''}
            </StatusBadge>
          ))}
        </div>
      </div>
    )
  }

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
        scoreBreakdown={item.score_breakdown as Record<string, any> | null}
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
      <section className="relative overflow-hidden rounded-sm border border-[var(--color-border)] bg-alma-chrome shadow-paper-sheet">
        <div className="relative flex flex-col gap-4 p-5 md:flex-row md:items-center md:justify-between md:gap-8">
          <div className="min-w-0 flex-1 space-y-2">
            <p className="max-w-xl text-sm leading-relaxed text-slate-600">
              Context-aware recommendations across lexical, vector, graph, and
              external channels — driven by the selected lens.
            </p>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span className="inline-flex items-center gap-2">
                <span className="relative flex h-2 w-2" aria-hidden>
                  <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-60" />
                  <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500" />
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
          </div>
        </div>
      </section>

      {/* Find & add — compact at the top. Just a search input until
          results land. The result section is rendered by
          OnlineSearchTab itself (only appears when items > 0); we
          deliberately don't carry a verbose description here — the
          input placeholder + the prefix-hint inside the tab are
          enough. */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Globe className="h-4 w-4 text-alma-folio" />
            Find &amp; add
          </CardTitle>
        </CardHeader>
        <CardContent>
          <OnlineSearchTab initialQuery={routeQuery} autoRun={!!routeQuery} resultPreviewLimit={5} />
        </CardContent>
      </Card>

      {/* Anchor card — only when ?seed=<paperId>. Shows immediately
          after the hero so the user knows what they're looking at
          before they hit the lens controls. */}
      {seedPaperId && (
        <Card className="border-alma-200 bg-alma-50/50">
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
      <LensManager
        lenses={lensesQuery.data ?? []}
        selectedLensId={selectedLensId}
        onSelectLens={setSelectedLensId}
        onCreate={(payload) => createLensMutation.mutate(payload)}
        onDelete={(lensId) => deleteLensMutation.mutate(lensId)}
      />

      <div className="space-y-4">
        {/* Branch Studio — collapsed by default. Sits above the
            recommendations list (between the lens picker and the
            results) so the affordance to tune branches is visible
            in the same vertical scan as "which lens am I on".
            Summary line carries the at-a-glance counts. */}
        <details className="group rounded-sm border border-[var(--color-border)] bg-alma-chrome shadow-paper-sheet">
          <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
            <div className="flex flex-col gap-0.5">
              <span className="font-brand text-sm font-semibold text-alma-800">Branch Studio</span>
              <span className="text-xs text-slate-500">
                Tune which clusters this lens pursues — pin, boost, mute, and
                review smart suggestions before the next refresh.
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
        <details className="group rounded-sm border border-[var(--color-border)] bg-alma-chrome shadow-paper-sheet">
          <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
            <div className="flex flex-col gap-0.5">
              <span className="font-brand text-sm font-semibold text-alma-800">Lens diagnostics</span>
              <span className="text-xs text-slate-500">
                {selectedLensSummary ? (
                  <>
                    Mode <strong className="text-alma-800">{String(selectedLensSummary.recommendation_mode ?? '—')}</strong>
                    {' · '}
                    Seeds <strong className="text-alma-800">{Number(selectedLensSummary.seed_count ?? 0)}</strong>
                    {' · '}
                    Temp <strong className="text-alma-800">{Number(selectedLensSummary.temperature ?? 0).toFixed(2)}</strong>
                    {' · '}
                    External lanes <strong className="text-alma-800">{Object.keys((selectedLensSummary.external_lanes as Record<string, unknown> | null) ?? {}).length}</strong>
                  </>
                ) : (
                  'Refresh the lens to capture taste-driven lane composition.'
                )}
              </span>
            </div>
            <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
            <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
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

                  {renderProfileList(
                    'Favorite Topics',
                    (((selectedLensSummary.taste_profile as Record<string, any> | null)?.topics as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.term ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                  )}

                  {renderProfileList(
                    'Favorite Authors',
                    (((selectedLensSummary.taste_profile as Record<string, any> | null)?.authors as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.name ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                  )}

                  {renderProfileList(
                    'Favorite Venues',
                    (((selectedLensSummary.taste_profile as Record<string, any> | null)?.venues as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.name ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                  )}

                  {renderProfileList(
                    'Recent Wins',
                    (((selectedLensSummary.taste_profile as Record<string, any> | null)?.recent_wins as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.query ?? ''),
                      value: Number(item.strength ?? 0),
                    })).filter((item) => item.label),
                  )}

                  {renderProfileList(
                    'Suppressed Topics',
                    (((selectedLensSummary.negative_profile as Record<string, any> | null)?.topics as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.term ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                    'negative',
                  )}

                  {renderProfileList(
                    'Suppressed Authors',
                    (((selectedLensSummary.negative_profile as Record<string, any> | null)?.authors as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.name ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                    'negative',
                  )}

                  {renderProfileList(
                    'Suppressed Venues',
                    (((selectedLensSummary.negative_profile as Record<string, any> | null)?.venues as Array<Record<string, any>> | undefined) ?? []).map((item) => ({
                      label: String(item.name ?? ''),
                      value: Number(item.weight ?? 0),
                    })).filter((item) => item.label),
                    'negative',
                  )}
                </>
              )}
            </CardContent>
          </Card>
          {/* Lens scoring weights — power-user control. Hidden behind
              a disclosure so the everyday Discovery view stays quiet;
              expand only when you need to tune how signals combine. */}
          <details className="group rounded-sm border border-[var(--color-border)] bg-alma-chrome shadow-paper-sheet">
            <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
              <div className="flex flex-col gap-0.5">
                <span className="font-brand text-sm font-semibold text-alma-800">Advanced — scoring weights</span>
                <span className="text-xs text-slate-500">Tune how signals combine for this lens. Defaults are fine for most users.</span>
              </div>
              <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
              <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
            </summary>
            <div className="border-t border-parchment-300/50 px-2 pb-2 pt-3">
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

        {/* ── Control bar ──────────────────────────────────────────────
            Mirrors the Feed control bar so the two surfaces feel like
            the same product. Three zones: [sort] · [counter +
            select-all] · [view mode]. Nothing here mutates data — all
            controls are local view state.
        ─────────────────────────────────────────────────────────────── */}
        <div className="flex flex-wrap items-center gap-3 rounded-sm border border-[var(--color-border)] bg-alma-chrome px-3 py-2 shadow-sm">
          {/* Sort toggle — pill button, binary state. */}
          <button
            type="button"
            onClick={() => setSort(sort === 'relevance' ? 'recent' : 'relevance')}
            title={
              sort === 'relevance'
                ? 'Currently sorted by lens ranking — switch to recent'
                : 'Currently sorted by recent — switch to lens ranking'
            }
            aria-label={`Sort by ${sort === 'relevance' ? 'recent' : 'relevance'}`}
            className="inline-flex h-7 items-center gap-1.5 rounded-sm border border-[var(--color-border)] bg-alma-chrome px-3 text-xs font-medium text-alma-800 transition-colors hover:bg-parchment-50"
          >
            <ArrowDownWideNarrow className="h-3.5 w-3.5 text-slate-500" />
            {sort === 'relevance' ? 'Ranking' : 'Recent'}
          </button>

          {/* Right cluster: counter with inline select-all, then view mode. */}
          <div className="ml-auto flex items-center gap-3">
            <div className="hidden items-center gap-1.5 text-xs text-slate-500 sm:inline-flex">
              <span className="tabular-nums font-medium text-slate-700">
                {recommendations.length}
              </span>
              <span>in view</span>
              {recommendations.length > 0 && (
                <>
                  <span className="text-slate-300" aria-hidden>·</span>
                  <button
                    type="button"
                    onClick={toggleSelectAllVisible}
                    className="text-alma-700 underline-offset-2 transition-colors hover:text-alma-800 hover:underline"
                  >
                    {allVisibleSelected ? 'Clear selection' : 'Select all'}
                  </button>
                </>
              )}
            </div>

            <ToggleGroup
              type="single"
              value={viewMode}
              onValueChange={(value) => {
                // Radix lets the user deselect the active item; we
                // require one always-active so the list always renders.
                if (value) setViewMode(value as DiscoveryViewMode)
              }}
              aria-label="Discovery view mode"
              className="gap-0 rounded-sm bg-parchment-100/80 p-0.5"
            >
              {[
                { value: 'compact' as DiscoveryViewMode, label: 'Compact', icon: Rows3, title: 'Compact dense rows' },
                { value: 'normal' as DiscoveryViewMode, label: 'Normal', icon: LayoutGrid, title: 'Normal card view' },
                { value: 'extended' as DiscoveryViewMode, label: 'Extended', icon: LayoutList, title: 'Extended view — includes abstracts' },
              ].map(({ value, label, icon: Icon, title }) => (
                <ToggleGroupItem
                  key={value}
                  value={value}
                  title={title}
                  className="h-7 min-w-0 gap-1 rounded-sm px-2.5 text-xs font-medium text-slate-600 hover:bg-transparent hover:text-alma-800 data-[state=on]:bg-alma-chrome data-[state=on]:text-alma-800 data-[state=on]:shadow-paper-sm data-[state=on]:ring-1 data-[state=on]:ring-[var(--color-border)]"
                >
                  <Icon className="h-3.5 w-3.5" />
                  <span className="hidden md:inline">{label}</span>
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
          </div>
        </div>

        <div className="space-y-3">
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
            recommendations.map((rec) => {
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
                <PaperCard
                  key={rec.id}
                  size={viewMode === 'compact' ? 'compact' : 'default'}
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
                  rank={rec.rank ?? undefined}
                  scoreBreakdown={rec.score_breakdown as Record<string, any> | null}
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
                  onQueue={() => queueMutation.mutate({ recId: rec.id, paperId: rec.paper_id })}
                  onPivot={() => navigateTo('discovery', {
                    seed: cardPaper.id,
                    seedTitle: cardPaper.title,
                  })}
                  actionDisabled={anyActionPending}
                  reaction={
                    rec.user_action === 'like' || rec.user_action === 'love' || rec.user_action === 'dislike'
                      ? rec.user_action
                      : null
                  }
                  isSaved={paper?.status === 'library' || rec.user_action === 'add' || rec.user_action === 'like' || rec.user_action === 'love'}
                >
                  {/* Normal view: provenance is folded into the card body
                      as a single chip row (no standalone "Why this surfaced"
                      section). Extended/compact still get the full panel
                      since they have room for it. */}
                  {renderProvenance(rec, {
                    variant: viewMode === 'normal' ? 'inline' : 'panel',
                  })}
                </PaperCard>
              )
            })
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
