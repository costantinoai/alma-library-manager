/**
 * FrontierMap — the Discovery host of the shared `<SemanticMap>` primitive
 * (task 50 M2; layers/decisions from task 47 P3/P8).
 *
 * This file owns Discovery-specific meaning ONLY: the frontier query, layer
 * toggles, the branches/clusters grouping switch (47-H: one grouping at a
 * time), branch legend chips that steer `branch_controls` through the shared
 * hook, and the lasso → describe → adopt-a-Direction loop. All rendering,
 * hit-testing, zoom/pan, toponym placement, and node semantics live in
 * `components/map/SemanticMap` — the same instrument every other map host
 * uses, so visuals and knobs cannot fork per surface (50-E/50-F).
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronUp,
  Eye,
  EyeOff,
  LassoSelect,
  Loader2,
  Mountain,
  Share2,
  Sparkles,
  Type,
} from 'lucide-react'

import { type FrontierNode, type Lens } from '@/api/client'
import { CreateSelectionLensButton } from '@/components/map/CreateSelectionLensButton'
import { MapRegionCard } from '@/components/map/MapRegionCard'
import { useRegionSelection } from '@/components/map/useRegionSelection'
import { useBranchControls } from '@/hooks/useBranchControls'
import { CorpusMapPaperPopup } from '@/components/map/CorpusMapPaperPopup'
import type { MapPaperNeighbour } from '@/components/map/MapPaperPopup'
import { SemanticMap, type SemanticMapNode } from '@/components/map/SemanticMap'
import { EDGE_LAYER_COLORS, EDGE_LAYER_FALLBACK_COLOR, MAP_INK, RAMP_GRADIENTS, summarizeValues, yearRampColor, yearRampLimits } from '@/components/map/mapNodeStyle'
import {
  ColourBarLegend,
  MapDataStatus,
  MapDisplayTuningRows,
  MapTuningPopover,
} from '@/components/map/MapChrome'
import { frontierQueryOptions } from '@/components/map/mapQueries'
import {
  MAP_TERRAIN_OPACITY_DEFAULT,
  useMapSessionSet,
  useMapSessionState,
} from '@/components/map/mapSessionState'
import { buildTerrainField } from '@/components/map/terrainField'
import { useSignalField } from '@/components/map/useSignalField'
import { StatusBadge } from '@/components/ui/status-badge'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'
import { ErrorState } from '@/components/ui/ErrorState'

interface FrontierMapProps {
  lensId: string | null
  /** The lens itself — needed to read/write branch controls from the legend
   *  chips. Optional so the map still renders read-only without it. */
  lens?: Lens | null
  /** Open the shared PaperDetailPanel for a paper (same as the list views). */
  onSelectPaper: (paperId: string) => void
  /** Adopt a selected region as a custom direction on the current lens
   *  (writes branch_controls.custom_directions + refreshes). */
  onAdoptDirection?: (direction: {
    label: string
    terms: string[]
    member_paper_ids: string[]
  }) => void
  /** 50-B map→list sync: filter the rec list below to a lassoed region. */
  onFilterList?: (paperIds: string[]) => void
  /** Recommendation card supplied by DiscoveryPage so it can reuse the
   *  already-mounted recommendation mutations rather than duplicating them. */
  renderRecommendationPopup: (
    node: FrontierNode,
    close: () => void,
    neighbours: MapPaperNeighbour[],
  ) => React.ReactNode
}


/** Map distinct branch ids → {color, label, index} in first-seen order. */
function useBranchColors(nodes: FrontierNode[]) {
  return useMemo(() => {
    const map = new Map<string, { color: string; label: string; index: number; count: number }>()
    for (const n of nodes) {
      if (n.layer !== 'rec' || !n.branch_id) continue
      const existing = map.get(n.branch_id)
      if (existing) {
        existing.count += 1
      } else {
        const index = map.size
        map.set(n.branch_id, {
          color: branchMapColor(index),
          label: (n.branch_label || 'branch').trim(),
          index,
          count: 1,
        })
      }
    }
    return map
  }, [nodes])
}

function frontierNeighbours(
  node: FrontierNode,
  nodes: FrontierNode[],
  edges: ReadonlyArray<{ source: string; target: string; edge_type: string; weight: number }>,
): MapPaperNeighbour[] {
  const byId = new Map(nodes.map((item) => [item.paper_id, item]))
  const connected = edges
    .filter((edge) => edge.source === node.paper_id || edge.target === node.paper_id)
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 4)
    .flatMap((edge) => {
      const id = edge.source === node.paper_id ? edge.target : edge.source
      const other = byId.get(id)
      if (!other) return []
      return [{
        id,
        title: other.title || id,
        relation:
          edge.edge_type === 'bibliographic_coupling'
            ? 'Shares references'
            : 'Cited together',
      }]
    })
  if (connected.length > 0) return connected

  // The edge payload is opt-in. When links are off, nearest substrate points
  // are still honest semantic neighbours: position is the map's meaning.
  const nearest: Array<{ node: FrontierNode; distance: number }> = []
  for (const candidate of nodes) {
    if (candidate.paper_id === node.paper_id) continue
    const distance = (candidate.x - node.x) ** 2 + (candidate.y - node.y) ** 2
    const insertAt = nearest.findIndex((item) => distance < item.distance)
    if (insertAt < 0) nearest.push({ node: candidate, distance })
    else nearest.splice(insertAt, 0, { node: candidate, distance })
    if (nearest.length > 4) nearest.pop()
  }
  return nearest.map(({ node: candidate }) => ({
    id: candidate.paper_id,
    title: candidate.title || candidate.paper_id,
    relation:
      candidate.cluster_id === node.cluster_id && node.cluster_id != null
        ? 'Nearby · same cluster'
        : 'Nearby in semantic space',
  }))
}

export function FrontierMap({
  lensId,
  lens,
  onSelectPaper,
  onAdoptDirection,
  onFilterList,
  renderRecommendationPopup,
}: FrontierMapProps) {
  const [showSeen, setShowSeen] = useMapSessionState('frontier', 'showSeen', false)
  const [showEdges, setShowEdges] = useMapSessionState('frontier', 'showEdges', false)
  // Words on/off is the user's call, not the grouping's side effect.
  const [showNames, setShowNames] = useMapSessionState('frontier', 'showNames', true)
  const [highlightBranch, setHighlightBranch] = useState<string | null>(null)
  // 47-H: ONE grouping at a time. Branch colouring is the frontier's default
  // (the recs are its hero layer); corpus clusters are the alternative lens on
  // the same points. Never both — two colourings on one scatter is a lie about
  // which structure you're looking at.
  const [groupBy, setGroupBy] = useMapSessionState<'branches' | 'clusters' | 'year'>(
    'frontier',
    'groupBy',
    'branches',
  )
  // Terrain (formerly "Heat") is an OVERLAY — the preference field composes
  // with ANY grouping (user call 2026-07-25), it never competes with them.
  const [showTerrain, setShowTerrain] = useMapSessionState('frontier', 'showTerrain', false)
  const [terrainOpacity, setTerrainOpacity] = useMapSessionState(
    'frontier',
    'terrainOpacity',
    MAP_TERRAIN_OPACITY_DEFAULT,
  )
  // Legend chips as toggles: a dimmed cluster recedes (never disappears —
  // the territory stays honest), so you can mute the mega-cluster and read
  // the rest. Reset on grouping switch.
  const [dimmedClusters, setDimmedClusters] = useMapSessionSet<number>(
    'frontier',
    'dimmedClusters',
  )
  const [hiddenEdgeTypes, setHiddenEdgeTypes] = useMapSessionSet<string>(
    'frontier',
    'hiddenEdgeTypes',
  )
  const [selectMode, setSelectMode] = useState(false)
  // Clicking a paper HIGHLIGHTS its cluster (everything else dims);
  // clicking the background clears it (user call 2026-07-25, all maps).
  const [focusClusterId, setFocusClusterId] = useState<number | null>(null)
  const [sizeScale, setSizeScale] = useMapSessionState('frontier', 'sizeScale', 1)
  const [dotOpacity, setDotOpacity] = useMapSessionState('frontier', 'dotOpacity', 1)
  const [wordScale, setWordScale] = useMapSessionState('frontier', 'wordScale', 1)
  const [wordCount, setWordCount] = useMapSessionState('frontier', 'wordCount', 3)

  const queryClient = useQueryClient()
  const query = useQuery(
    frontierQueryOptions(queryClient, lensId ?? '', showSeen, showEdges),
  )
  const data = query.data?.payload
  const building = query.data?.build
  const nodes = useMemo(() => data?.nodes ?? [], [data])
  const edges = useMemo(() => data?.edges ?? [], [data])
  const counts = data?.counts
  const branchColors = useBranchColors(nodes)
  const branchControls = useBranchControls(lens)
  const nodesById = useMemo(() => new Map(nodes.map((n) => [n.paper_id, n])), [nodes])
  const visibleMapIds = useMemo(
    () => new Set(nodes.map((node) => node.paper_id)),
    [nodes],
  )
  // Selection can never outlive the exact layer/filter payload that drew it.
  const region = useRegionSelection({ visibleIds: visibleMapIds })

  // Corpus clusters present on the map, largest first, each with a stable hue.
  // Unclustered (-1) is deliberately excluded from the legend: it is the
  // absence of a group, not a group.
  //
  // The HUE comes from the space (`cluster_hues`, ranked over the whole
  // substrate), never from this deck: Discovery renders a different subset of
  // the same corpus layout than the Map page does, and ranking locally gave one
  // cluster a different colour on each surface.
  const clusterColors = useMemo(() => {
    const spaceHue = new Map<number, number>()
    for (const [id, index] of Object.entries(data?.cluster_hues ?? {})) {
      spaceHue.set(Number(id), Number(index))
    }
    const tally = new Map<number, { label: string; count: number }>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      const row = tally.get(n.cluster_id)
      if (row) row.count += 1
      else tally.set(n.cluster_id, { label: n.cluster_label || `Cluster ${n.cluster_id}`, count: 1 })
    }
    const ordered = [...tally.entries()].sort((a, b) => b[1].count - a[1].count)
    return new Map(
      ordered.map(([id, v], i) => [
        id,
        { ...v, color: branchMapColor(spaceHue.get(id) ?? i), index: i },
      ]),
    )
  }, [nodes, data])

  // Recs that were NOT in the previous payload — the visible end of the
  // adopt-a-direction loop. Tracked across refetches for this session only;
  // an empty previous set (first load) marks nothing, so a cold open is calm.
  const prevRecIds = useRef<Set<string> | null>(null)
  const [newRecIds, setNewRecIds] = useState<Set<string>>(new Set())
  useEffect(() => {
    if (data?.status !== 'ready') return
    const current = new Set(nodes.filter((n) => n.layer === 'rec').map((n) => n.paper_id))
    const previous = prevRecIds.current
    if (previous && previous.size > 0) {
      const fresh = new Set([...current].filter((id) => !previous.has(id)))
      if (fresh.size > 0) setNewRecIds(fresh)
    }
    prevRecIds.current = current
  }, [nodes, data?.status])

  const yearRange = useMemo(
    () => yearRampLimits(nodes.map((n) => Number(n.year))),
    [nodes],
  )

  // Terrain (50-J): the SPACE-OWNED preference field — one valence per
  // corpus paper at its substrate coordinates, fetched from
  // /graphs/signal-field. NOT derived from the rendered dots: toggling
  // "show seen" (or any layer) never changes the terrain, and a
  // library-only view still shows the red of dismissed / weak-scored
  // territory whose dots are hidden (user call 2026-07-25).
  // The frontier is a pure read of the durable substrate — no tuning knob here
  // re-fits it — so this host is permanently in the substrate frame and gets
  // the whole space-owned field, off-view papers included. It still routes
  // through the shared builder so its terrain and the Map page's are the same
  // object with the same stats (`terrainField.ts`).
  const signalField = useSignalField(showTerrain)
  const terrain = useMemo(
    () =>
      buildTerrainField({
        frame: 'substrate',
        fallbackIsSubstrate: true,
        nodes: [],
        spacePoints: signalField.points,
        valenceById: signalField.valenceById,
        confidenceById: signalField.confidenceById,
      }),
    [signalField.points, signalField.valenceById, signalField.confidenceById],
  )

  const yearStats = useMemo(
    () => summarizeValues(nodes.map((n) => Number(n.year)).filter((y) => y > 1800)),
    [nodes],
  )
  const terrainStats = terrain.stats

  // ── FrontierNode → SemanticMapNode: meaning mapping only (50-E) ──────────
  const mapNodes = useMemo<SemanticMapNode[]>(() => {
    return nodes.map((n): SemanticMapNode => {
      const kind = n.layer === 'library' ? 'library' : n.layer === 'rec' ? 'suggestion' : 'seen'
      // Colour follows whichever grouping is active — never both (47-H).
      let color: string | undefined
      if (groupBy === 'clusters') {
        color =
          typeof n.cluster_id === 'number' ? clusterColors.get(n.cluster_id)?.color : undefined
      } else if (groupBy === 'year') {
        color =
          yearRange && typeof n.year === 'number' && n.year > 1800
            ? yearRampColor(n.year, yearRange.lo, yearRange.hi)
            : MAP_INK.ambientSoft
      } else if (groupBy === 'branches' && n.layer === 'rec') {
        color = n.branch_id ? branchColors.get(n.branch_id)?.color : branchMapColor(0)
      }
      return {
        id: n.paper_id,
        x: n.x,
        // Flip Y so higher-y sits at the top, like a chart (legacy convention).
        y: 1 - n.y,
        kind,
        color,
        sizeValue: n.layer === 'rec' ? (n.score ?? null) : null,
        clusterId: n.cluster_id ?? undefined,
        clusterLabel: n.cluster_label ?? undefined,
        dimmed:
          (groupBy === 'branches' &&
            highlightBranch != null &&
            n.layer === 'rec' &&
            n.branch_id !== highlightBranch) ||
          (groupBy === 'clusters' &&
            typeof n.cluster_id === 'number' &&
            dimmedClusters.has(n.cluster_id)) ||
          // Cluster focus from a paper click: outside recedes, never hides.
          (focusClusterId != null && n.cluster_id !== focusClusterId),
        halo: newRecIds.has(n.paper_id),
      }
    })
  }, [nodes, groupBy, clusterColors, branchColors, highlightBranch, newRecIds, dimmedClusters, yearRange, focusClusterId])

  const mapEdges = useMemo(
    () =>
      edges
        .filter((e) => !hiddenEdgeTypes.has(e.edge_type))
        .map((e) => ({
        source: e.source,
        target: e.target,
        color: EDGE_LAYER_COLORS[e.edge_type] ?? EDGE_LAYER_FALLBACK_COLOR,
      })),
    [edges, hiddenEdgeTypes],
  )

  // Area score per cluster — mean suggestion score, for the hover card.
  const clusterAreaScores = useMemo(() => {
    const acc = new Map<number, { sum: number; n: number }>()
    for (const n of nodes) {
      if (n.layer !== 'rec' || typeof n.score !== 'number') continue
      const cid = typeof n.cluster_id === 'number' ? n.cluster_id : -1
      if (cid < 0) continue
      const row = acc.get(cid)
      if (row) {
        row.sum += n.score
        row.n += 1
      } else acc.set(cid, { sum: n.score, n: 1 })
    }
    return new Map([...acc.entries()].map(([cid, { sum, n }]) => [cid, sum / n]))
  }, [nodes])

  const cancelRegion = () => region.clear()
  const adoptRegion = () => {
    if (!region.ids || !region.description?.sufficient) return
    onAdoptDirection?.({
      label: region.description.label,
      terms: region.description.top_terms,
      member_paper_ids: region.ids,
    })
    region.clear()
    setSelectMode(false)
  }

  if (!lensId) {
    return (
      <div className="flex h-[420px] items-center justify-center rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        Select a lens to plot its frontier.
      </div>
    )
  }
  if (query.isError && !data) {
    return (
      <ErrorState
        title="The Suggestions Map could not be loaded"
        message="Its saved layout is still intact. Retry the map request."
        actionLabel="Try again"
        onAction={() => void query.refetch()}
        actionPending={query.isFetching}
        className="min-h-[420px]"
      />
    )
  }
  if (!data) {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
        {building
          ? 'Building the semantic layout — this runs once, then it’s cached…'
          : 'Loading the cached map…'}
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      {/* Toolbar — a real bar above the plate, never buttons floating on it
          (user call 2026-07-25; also the 50-I controls contract). */}
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-border)] bg-surface-2 px-3 py-2">
        <button
          type="button"
          onClick={() => setShowSeen((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showSeen
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title="Show the top papers you've seen but not acted on — the frontier"
        >
          {showSeen ? <Eye className="h-3.5 w-3.5" /> : <EyeOff className="h-3.5 w-3.5" />}
          Show everything I’ve seen
        </button>
        <button
          type="button"
          onClick={() => setShowEdges((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showEdges
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title="Draw citation links (shared references + cited-together) between the papers on the map"
        >
          <Share2 className="h-3.5 w-3.5" />
          Citation links{showEdges && counts?.edges ? ` · ${counts.edges}` : ''}
        </button>
        <button
          type="button"
          onClick={() => setShowNames((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showNames && groupBy === 'clusters'
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title={groupBy === 'clusters' ? 'Cluster names on the map' : 'Names show in the Clusters grouping'}
        >
          <Type className="h-3.5 w-3.5" />
          Names
        </button>
        <MapTuningPopover>
          <MapDisplayTuningRows
            sizeScale={sizeScale}
            onSizeScale={setSizeScale}
            dotOpacity={dotOpacity}
            onDotOpacity={setDotOpacity}
            terrainOpacity={terrainOpacity}
            onTerrainOpacity={setTerrainOpacity}
            wordScale={wordScale}
            onWordScale={setWordScale}
            wordCount={wordCount}
            onWordCount={setWordCount}
          />
        </MapTuningPopover>
        <button
          type="button"
          onClick={() => setShowTerrain((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showTerrain
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title="Preference terrain — the space-owned signal field (all your ratings, saves, removals + engine scores) washed under the dots. Composes with any grouping; the same whatever layers are shown (view only)"
        >
          <Mountain className="h-3.5 w-3.5" />
          Terrain
        </button>
        {/* 47-H: one grouping at a time — this is a switch, not two toggles. */}
        {clusterColors.size > 0 && (
          <div className="inline-flex overflow-hidden rounded-sm border border-[var(--color-border)]">
            {(['branches', 'clusters', 'year'] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => {
                  setGroupBy(mode)
                  setHighlightBranch(null)
                  setDimmedClusters(new Set())
                }}
                className={cn(
                  'px-2 py-1 text-xs font-medium transition-colors',
                  groupBy === mode
                    ? 'bg-accent-soft text-alma-folio'
                    : 'bg-control-well text-slate-600 hover:bg-control-quiet',
                )}
                title={
                  mode === 'branches'
                    ? 'Colour suggestions by the lens branch that found them'
                    : mode === 'clusters'
                      ? 'Colour every paper by its corpus cluster'
                      : 'Recency ramp — older fades, newer leads'
                }
              >
                {mode === 'branches' ? 'Branches' : mode === 'clusters' ? 'Clusters' : 'Year'}
              </button>
            ))}
          </div>
        )}
        {onAdoptDirection && (
          <button
            type="button"
            onClick={() => {
              setSelectMode((s) => !s)
              cancelRegion()
            }}
            className={cn(
              'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
              selectMode
                ? 'border-accent-edge bg-accent-soft text-alma-folio'
                : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
            )}
            title="Drag a box around a cluster of papers to name it and explore that direction"
          >
            <LassoSelect className="h-3.5 w-3.5" />
            Select a direction
          </button>
        )}
        <span className="ml-auto">
          <MapDataStatus
            phase={
              building
                ? 'building'
                : query.isFetching || (showTerrain && signalField.isFetching)
                  ? 'refreshing'
                  : 'idle'
            }
          />
        </span>
      </div>

      <SemanticMap
        nodes={mapNodes}
        edges={mapEdges}
        showEdges={showEdges}
        showToponyms={showNames && groupBy === 'clusters'}
        sizeScale={sizeScale}
        dotOpacity={dotOpacity}
        toponymScale={wordScale}
        toponymWordCount={wordCount}
        heatField={showTerrain ? terrain.points : undefined}
        terrainOpacity={terrainOpacity}
        height={520}
        lassoMode={selectMode}
        onLasso={(ids, anchor) => region.select(ids, anchor)}
        onClickNode={(id) => {
          if (id == null) {
            setFocusClusterId(null)
            return
          }
          const n = nodesById.get(id)
          if (!n) return
          // Popup is owned by SemanticMap. The ONLY click side effect is
          // cluster focus. Navigating to a recommendation's list row requires
          // the explicit "Go to paper" action inside its popup.
          setFocusClusterId(
            typeof n.cluster_id === 'number' && n.cluster_id >= 0 ? n.cluster_id : null,
          )
        }}
        renderClick={(id, close) => {
          const n = nodesById.get(id)
          if (!n) return null
          const neighbours = frontierNeighbours(n, nodes, edges)
          if (n.layer === 'rec') return renderRecommendationPopup(n, close, neighbours)
          return (
            <CorpusMapPaperPopup
              paperId={n.paper_id}
              onClose={close}
              onOpenDetails={() => {
                close()
                onSelectPaper(n.paper_id)
              }}
              fallback={{
                id: n.paper_id,
                title: n.title || n.paper_id,
                year: n.year,
                score: n.score,
                statusLabel: n.layer === 'library' ? 'In your library' : 'Seen',
                branchLabel: n.branch_label,
                clusterLabel: n.cluster_label,
                neighbours,
              }}
            />
          )
        }}
        renderHover={(id) => {
          const n = nodesById.get(id)
          if (!n) return null
          return (
            <>
              <p className="line-clamp-2 font-medium text-alma-800">{n.title || n.paper_id}</p>
              <p className="mt-0.5 text-slate-500">
                {n.layer === 'library' ? 'In your library' : n.layer === 'rec' ? 'Suggestion' : 'Seen'}
                {n.year ? ` · ${n.year}` : ''}
              </p>
              {(() => {
                const area =
                  typeof n.cluster_id === 'number' && n.cluster_id >= 0
                    ? clusterAreaScores.get(n.cluster_id)
                    : undefined
                const hasScore = typeof n.score === 'number' && n.layer === 'rec'
                if (!hasScore && area == null) return null
                return (
                  <p className="mt-0.5 font-medium text-alma-800">
                    {hasScore ? `Score ${Math.round(n.score as number)}/100` : ''}
                    {hasScore && area != null ? ' · ' : ''}
                    {area != null ? `area ${Math.round(area)}/100` : ''}
                  </p>
                )
              })()}
              {n.branch_label && <p className="mt-0.5 text-slate-500">branch: {n.branch_label}</p>}
              {n.cluster_label && n.cluster_label !== 'Unclustered' && (
                <p className="mt-0.5 text-slate-400">cluster: {n.cluster_label}</p>
              )}
            </>
          )
        }}
        viewStateKey="frontier"
        className="rounded-none border-0"
      >
        {/* Region popover — the describe payload + adopt action. Meaning
            (label + terms + counts) is shown before the action, per 47 §8. */}
        {region.ids && (
          <MapRegionCard
            kind="Direction"
            icon={<Sparkles className="h-3.5 w-3.5 text-alma-folio" />}
            count={region.ids.length}
            pending={region.describing || !region.description}
            insufficient={region.description?.sufficient === false}
            insufficientMessage="Too few papers to characterize — select a larger cluster (5+)."
            onClose={cancelRegion}
            actions={
              <CreateSelectionLensButton
                ids={region.ids}
                scope="corpus"
                selectionKind="papers"
                name={`${region.description?.label ?? 'Discovery direction'} · map selection`}
                onCreated={() => {
                  cancelRegion()
                  setSelectMode(false)
                }}
              />
            }
          >
            {region.description && (
              <>
                <p className="text-sm font-semibold capitalize text-alma-800">
                  {region.description.label}
                </p>
                {region.description.top_terms.length > 0 && (
                  <div className="mt-1.5 flex flex-wrap gap-1">
                    {region.description.top_terms.slice(0, 6).map((t) => (
                      <StatusBadge key={t} tone="neutral" size="sm">
                        {t}
                      </StatusBadge>
                    ))}
                  </div>
                )}
                <p className="mt-2 text-[11px] text-slate-500">
                  {region.description.counts.library} in library ·{' '}
                  {region.description.counts.recs} suggestions ·{' '}
                  {region.description.counts.seen} seen here
                </p>
                {region.description.sample.length > 0 && (
                  <ul className="mt-1.5 space-y-0.5">
                    {region.description.sample.map((s, i) => (
                      <li key={i} className="line-clamp-1 text-[11px] text-slate-400">
                        · {s}
                      </li>
                    ))}
                  </ul>
                )}
                <div className="mt-3 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={adoptRegion}
                    className="inline-flex flex-1 items-center justify-center gap-1.5 rounded-sm bg-alma-800 px-2.5 py-1.5 text-xs font-medium text-alma-cream hover:bg-alma-900"
                  >
                    <Sparkles className="h-3.5 w-3.5" />
                    Explore this direction
                  </button>
                  {onFilterList && (
                    <button
                      type="button"
                      onClick={() => {
                        if (region.ids) onFilterList(region.ids)
                        cancelRegion()
                        setSelectMode(false)
                      }}
                      className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1.5 text-xs text-slate-600 hover:bg-control-quiet"
                      title="Show only these papers in the list below"
                    >
                      Filter list
                    </button>
                  )}
                  <button
                    type="button"
                    onClick={cancelRegion}
                    className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1.5 text-xs text-slate-600 hover:bg-control-quiet"
                  >
                    Cancel
                  </button>
                </div>
              </>
            )}
          </MapRegionCard>
        )}

      </SemanticMap>

      {/* Legend — its own section BELOW the plate (user call 2026-07-25):
          ownership semantics from the registry (filled vs hollow), grouping
          chips per the active mode. Never an overlay hiding dots. */}
      <div className="border-t border-[var(--color-border)] bg-surface-2 px-3 py-2.5 text-xs">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
            <span className="inline-flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-full"
                style={{ background: MAP_INK.library }}
              />
              Library {counts ? `(${counts.library})` : ''} — filled
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span
                className="inline-block h-2.5 w-2.5 rounded-full border-2 bg-transparent"
                style={{ borderColor: branchMapColor(0) }}
              />
              Suggestions {counts ? `(${counts.recs})` : ''} — hollow
            </span>
            {groupBy === 'year' && yearRange && yearStats && (
              // Ramp clamps at p10–p90 for contrast; extend notation
              // ("≤2004") keeps the real data range honest.
              <ColourBarLegend
                gradient={RAMP_GRADIENTS.year}
                min={yearStats.min < yearRange.lo ? `≤${yearRange.lo}` : String(yearRange.lo)}
                max={yearStats.max > yearRange.hi ? `≥${yearRange.hi}` : String(yearRange.hi)}
                mean={String(Math.round(yearStats.mean))}
              />
            )}
            {showTerrain && terrainStats && (
              // Fixed semantic valence domain, shared with every map.
              <ColourBarLegend
                gradient={RAMP_GRADIENTS.terrain}
                min="-1"
                mid="0"
                max="1"
                mean={terrainStats.mean.toFixed(2)}
              />
            )}
            {showSeen && (
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span
                  className="inline-block h-1.5 w-1.5 rounded-full"
                  style={{ background: MAP_INK.ambientSoft }}
                />
                {counts
                  ? `showing ${counts.seen_shown} nearest of ${counts.seen_total} seen` +
                    (data?.seen_ranked_by === 'lens' ? ' (nearest to this lens)' : '')
                  : 'seen'}
              </span>
            )}
          </div>
          {showSeen && (
            <p className="mt-1 text-[11px] text-slate-400">
              Seen = surfaced in an EARLIER refresh and never acted on (not saved, not dismissed).
              They are not in the current deck — each new refresh builds a fresh one — but they are
              your unworked frontier: lasso a patch of them to explore it as a Direction.
            </p>
          )}
          {showEdges && edges.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {Object.entries(
                edges.reduce<Record<string, number>>((acc, e) => {
                  acc[e.edge_type] = (acc[e.edge_type] ?? 0) + 1
                  return acc
                }, {}),
              ).map(([type, count]) => {
                const off = hiddenEdgeTypes.has(type)
                return (
                  <button
                    key={type}
                    type="button"
                    aria-pressed={!off}
                    onClick={() =>
                      setHiddenEdgeTypes((prev) => {
                        const next = new Set(prev)
                        if (next.has(type)) next.delete(type)
                        else next.add(type)
                        return next
                      })
                    }
                    className={cn(
                      'inline-flex items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
                      off && 'opacity-40 line-through',
                    )}
                    title={off ? `Show ${type} links` : `Hide ${type} links`}
                  >
                    <span
                      className="inline-block h-1.5 w-3 rounded-full"
                      style={{ background: EDGE_LAYER_COLORS[type] ?? EDGE_LAYER_FALLBACK_COLOR }}
                    />
                    {type === 'bibliographic_coupling' ? 'Shared references' : type === 'co_citation' ? 'Cited together' : type} · {count}
                  </button>
                )
              })}
            </div>
          )}
          {/* Branch chips — highlight on click, and (when the lens is available)
              steer the branch inline. Boost/mute here write the SAME
              branch_controls Branch Studio writes, through the shared hook: one
              state, two views. */}
          {groupBy === 'branches' && branchColors.size > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {[...branchColors.entries()].map(([id, b]) => {
                const state = lens ? branchControls.stateOf(id) : 'normal'
                return (
                  <span
                    key={id}
                    className={cn(
                      'inline-flex items-center gap-1 rounded-full border transition-colors',
                      highlightBranch === id
                        ? 'border-transparent text-white'
                        : 'border-control-edge bg-control-quiet text-slate-600',
                      state === 'muted' && 'opacity-50',
                    )}
                    style={highlightBranch === id ? { background: b.color } : undefined}
                  >
                    <button
                      type="button"
                      onClick={() => setHighlightBranch((h) => (h === id ? null : id))}
                      className="inline-flex items-center gap-1 rounded-full py-0.5 pl-1.5 hover:opacity-80"
                      title={`Highlight the "${b.label}" branch`}
                    >
                      <span
                        className="inline-block h-2 w-2 rounded-full"
                        style={{ background: b.color }}
                      />
                      {b.label} · {b.count}
                    </button>
                    {lens && (
                      <span className="flex items-center pr-1">
                        <button
                          type="button"
                          onClick={() => branchControls.cycleBranchState(id, 'boosted')}
                          disabled={branchControls.isPending}
                          className={cn(
                            'rounded-full p-0.5 transition-colors hover:text-alma-folio',
                            state === 'boosted' ? 'text-alma-folio' : 'opacity-50 hover:opacity-100',
                          )}
                          title={state === 'boosted' ? 'Remove boost' : 'Boost this branch'}
                        >
                          <ChevronUp className="h-3 w-3" />
                        </button>
                        <button
                          type="button"
                          onClick={() => branchControls.cycleBranchState(id, 'muted')}
                          disabled={branchControls.isPending}
                          className={cn(
                            'rounded-full p-0.5 transition-colors hover:text-warning-700',
                            state === 'muted' ? 'text-warning-700' : 'opacity-50 hover:opacity-100',
                          )}
                          title={state === 'muted' ? 'Unmute this branch' : 'Mute this branch'}
                        >
                          <ChevronDown className="h-3 w-3" />
                        </button>
                      </span>
                    )}
                  </span>
                )
              })}
            </div>
          )}

          {/* Cluster chips — TOGGLES: click to dim a cluster's dots (they
              recede, never vanish), click again to restore. Corpus clusters
              aren't steerable, so this is a reading aid, not a signal. */}
          {groupBy === 'clusters' && clusterColors.size > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {[...clusterColors.entries()].slice(0, 8).map(([id, c]) => {
                const dimmed = dimmedClusters.has(id)
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() =>
                      setDimmedClusters((prev) => {
                        const next = new Set(prev)
                        if (next.has(id)) next.delete(id)
                        else next.add(id)
                        return next
                      })
                    }
                    aria-pressed={!dimmed}
                    className={cn(
                      'inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
                      dimmed && 'opacity-40',
                    )}
                    title={
                      dimmed
                        ? `Show "${c.label}" (${c.count} papers)`
                        : `Dim "${c.label}" (${c.count} papers)`
                    }
                  >
                    <span
                      className="inline-block h-2 w-2 shrink-0 rounded-full"
                      style={{ background: c.color }}
                    />
                    <span className={cn('truncate', dimmed && 'line-through')}>{c.label}</span>
                    <span className="shrink-0 text-slate-400">· {c.count}</span>
                  </button>
                )
              })}
            </div>
          )}
          {counts && counts.recs_unplaced > 0 && (
            <p className="mt-1.5 text-[11px] text-slate-400">
              {counts.recs_unplaced} suggestion{counts.recs_unplaced === 1 ? '' : 's'} not on the map (no abstract yet)
            </p>
          )}
        </div>
    </div>
  )
}
