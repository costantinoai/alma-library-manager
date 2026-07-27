/**
 * GraphMapView — the ONE host for GraphData-shaped maps (task 50-K).
 *
 * The Map page (corpus/library paper map) and the Authors network render
 * through THIS component: same `<SemanticMap>` plate, same `MapToolbar`
 * idioms, same legend, same cluster dim-toggles, same search behaviour.
 * Hosts contribute meaning only — which endpoint, what a node opens, extra
 * toolbar/legend slots. The old per-surface stack (ForceGraph + the
 * "cluster studio" + live physics) is retired: substrate coordinates are
 * the one physics everywhere.
 */
import { useCallback, useEffect, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, Mountain, Search, Share2, Type } from 'lucide-react'

import { type GraphData, type GraphNode } from '@/api/client'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'
import { ErrorState } from '@/components/ui/ErrorState'
import {
  ClusterLegendChips,
  ColourBarLegend,
  MapDataStatus,
  MapLegend,
  MapModeSwitch,
  MapToggle,
  MapToolbar,
  type ClusterChipEntry,
} from './MapChrome'
import { graphQueryOptions } from './mapQueries'
import {
  PAPER_MAP_DEFAULTS,
  useMapSessionSet,
  useMapSessionState,
} from './mapSessionState'
import { SemanticMap, type SemanticMapNode } from './SemanticMap'
import { useMapField } from './useMapField'
import {
  EDGE_LAYER_COLORS,
  EDGE_LAYER_FALLBACK_COLOR,
  EDGE_LAYER_LABELS,
  HOLLOW_STROKE_WIDTH,
  MAP_INK,
  MAP_NODE_DRAW_ORDER,
  MAP_NODE_STYLES,
  RAMP_GRADIENTS,
  SCORE_LEGEND,
  scoreRampColor,
  summarizeValues,
  TERRAIN_LEGEND,
  yearRampColor,
  yearRampLimits,
  type MapNodeKind,
} from './mapNodeStyle'

export interface GraphMapViewProps {
  /** `/graphs/<endpoint>` — paper-map or author-network. */
  endpoint: 'paper-map' | 'author-network'
  /** Query params (scope, resolution, …). Changing them refetches. */
  params: Record<string, string>
  /** Node meaning: which registry kind each payload node renders as. */
  nodeKind: (node: GraphNode) => MapNodeKind
  /** Open the node in the host's own surface (paper panel / author drawer). */
  onOpenNode?: (node: GraphNode) => void
  /** Background click — the host's deselect (clear selection + cluster focus). */
  onBackgroundClick?: () => void
  /** Hover-card body for a node — host vocabulary, shared shell. */
  hoverCard: (node: GraphNode) => React.ReactNode
  /** Interactive dot-card body. SemanticMap owns its anchor and dismissal;
   *  the host supplies the node-specific content and mutations. */
  renderClickCard?: (node: GraphNode, close: () => void) => React.ReactNode
  /** Dashed-halo marker (e.g. followed authors). Meaning documented by host.
   *  A PREDICATE, not an id set: hosts whose payload ids live in a different
   *  namespace (or casing) than their own records must be able to fold the id
   *  before comparing — an id set silently matched nothing (2026-07-26). */
  nodeHalo?: (node: GraphNode) => boolean
  /** Persistent gold provenance outline, independent of dot colour. */
  nodeSuggestionOutline?: (node: GraphNode) => boolean
  /** Draw the typed link layers. Off for hosts whose map has no link layer:
   *  the author map's adjacency already IS collaboration, so lines re-state
   *  position as topology and bury the dots (user call 2026-07-26). */
  showLinks?: boolean
  /** Host slots — extra toolbar controls / legend rows. */
  toolbarExtras?: React.ReactNode
  legendExtras?: React.ReactNode
  /** Overlays pinned INSIDE the plate (region card). Feedback for a spatial
   *  gesture belongs where the gesture ended, never below the fold. */
  plateOverlay?: React.ReactNode
  height?: number
  /** 50-M drill-down hooks (the Map page host): the raw payload for
   *  inspector panels, an accent-ring selection, a cluster FOCUS (everything
   *  outside it dims), and a dot-size knob. */
  onPayload?: (data: GraphData) => void
  selectedNodeId?: string | null
  focusClusterId?: number | null
  sizeScale?: number
  dotOpacity?: number
  terrainOpacity?: number
  toponymScale?: number
  toponymWordCount?: number
  /** Colour modes this host offers (default: all). Authors drop Year —
   *  an author has no single publication year (user call 2026-07-25). */
  colourModes?: ReadonlyArray<'clusters' | 'year' | 'score'>
  /** Rectangle-select (region) mode — drag selects instead of panning. */
  lassoMode?: boolean
  /** Region selection landed: the node ids under the rectangle. */
  /** Lassoed node ids plus the screen anchor of the drag. The anchor used to
   *  be dropped here, which made an at-gesture region card impossible for
   *  every GraphMapView host (2026-07-26). */
  onLasso?: (ids: string[], anchor: { x: number; y: number }) => void
}

export function GraphMapView({
  endpoint,
  params,
  nodeKind,
  onOpenNode,
  onBackgroundClick,
  hoverCard,
  renderClickCard,
  nodeHalo,
  nodeSuggestionOutline,
  showLinks = true,
  toolbarExtras,
  legendExtras,
  plateOverlay,
  height = 560,
  onPayload,
  selectedNodeId,
  focusClusterId,
  sizeScale = 1,
  dotOpacity = 1,
  terrainOpacity = 1,
  toponymScale = 1,
  toponymWordCount = 3,
  colourModes = ['clusters', 'year', 'score'],
  lassoMode = false,
  onLasso,
}: GraphMapViewProps) {
  const mapStateKey = endpoint === 'paper-map' ? 'paper-map' : 'author-map'
  const [showEdges, setShowEdges] = useMapSessionState(
    mapStateKey,
    'showEdges',
    false,
  )
  const [showToponyms, setShowToponyms] = useMapSessionState(
    mapStateKey,
    'showToponyms',
    true,
  )
  // Colour restores the old color-by knob on the shared stack: cluster hues,
  // a year ramp, or a score ramp. DATA ramps (year / score) are deliberate
  // exceptions to the chip valence contract. Score = the engine's INTERNAL
  // relevance score (latest recommendation, 0–100), NOT the user's star
  // rating (user call 2026-07-25).
  const [colourMode, setColourMode] = useMapSessionState<
    'clusters' | 'year' | 'score'
  >(mapStateKey, 'colourMode', 'clusters')
  // Terrain (formerly "Heat") is an OVERLAY, not a colour mode — the
  // preference field composes with ANY dot colouring (user call 2026-07-25).
  const [showTerrain, setShowTerrain] = useMapSessionState(
    mapStateKey,
    'showTerrain',
    false,
  )
  // Per-layer link chips (the old typed-edge toggles): view-only filters.
  const [hiddenEdgeTypes, setHiddenEdgeTypes] = useMapSessionSet<string>(
    mapStateKey,
    'hiddenEdgeTypes',
  )
  const [dimmedClusters, setDimmedClusters] = useMapSessionSet<number>(
    mapStateKey,
    'dimmedClusters',
  )
  const [search, setSearch] = useMapSessionState(mapStateKey, 'search', '')

  const queryClient = useQueryClient()
  const graphQuery = useQuery(graphQueryOptions(queryClient, endpoint, params))
  const data = graphQuery.data?.payload
  const building = graphQuery.data?.build
  useEffect(() => {
    if (data) onPayload?.(data)
  }, [data, onPayload])

  const nodes = useMemo(() => data?.nodes ?? [], [data])
  const nodesById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes])

  // Cluster hue map. The hue identifies WHICH region of the space a cluster is,
  // so it belongs to the space, not to the current selection: the backend ranks
  // clusters over the whole layout and ships `hue_index`, and Library — a subset
  // of the very same corpus layout — therefore paints every cluster exactly as
  // Corpus does. Ranking the RENDERED nodes here instead recoloured the entire
  // map on a scope switch (measured 2026-07-26: the top Library cluster was hue
  // #0 in Library and #194 in Corpus). Local ranking survives only as the
  // fallback for a payload built before `hue_index` existed.
  const clusterColors = useMemo(() => {
    const spaceHue = new Map<number, number>()
    for (const cluster of (data?.metadata?.clusters ?? []) as Array<{
      id?: number
      hue_index?: number
    }>) {
      if (typeof cluster?.id === 'number' && typeof cluster?.hue_index === 'number') {
        spaceHue.set(cluster.id, cluster.hue_index)
      }
    }
    const tally = new Map<number, { label: string; count: number }>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      const label = String(n.metadata?.cluster_label ?? '') || `Cluster ${n.cluster_id}`
      const row = tally.get(n.cluster_id)
      if (row) row.count += 1
      else tally.set(n.cluster_id, { label, count: 1 })
    }
    const ordered = [...tally.entries()].sort((a, b) => b[1].count - a[1].count)
    return new Map(
      ordered.map(([id, v], i) => [
        id,
        { ...v, color: branchMapColor(spaceHue.get(id) ?? i) },
      ]),
    )
  }, [nodes, data])

  const yearRange = useMemo(
    () => yearRampLimits(nodes.map((n) => Number(n.metadata?.year))),
    [nodes],
  )

  // 50-J terrain + live scores. BOTH map families read them from a live field
  // rather than from the cached layout payload: scores move with every
  // Discovery refresh while a materialized view does not, and colouring from
  // the baked-in score left every dot grey on a stale view (user catch
  // 2026-07-25). Paper maps use the space-owned `/graphs/signal-field`; the
  // author network has its own layout space, so it reads the id-keyed
  // `/graphs/author-field` — same `signal_valence` weights, same liveness
  // (2026-07-26).
  const isPaperMap = endpoint === 'paper-map'
  const fieldNeeded = showTerrain || colourMode === 'score'

  // Which coordinate space this payload's dots live in. The Advanced knobs can
  // re-fit the layout (cluster detail → fresh UMAP, layout blend → fused
  // positions), and terrain painted at SUBSTRATE coordinates over a re-fitted
  // layout is one map's landscape on another map's land (user report
  // 2026-07-26). The backend declares the frame; the fallback covers payloads
  // cached before it did, and only the default request can claim substrate.
  const layoutFrame = (data?.metadata?.layout as { frame?: string } | undefined)?.frame
  const requestedDefaultLayout = useMemo(() => {
    const resolution = Number(params.cluster_resolution ?? PAPER_MAP_DEFAULTS.resolution)
    const blended = ['w_coauthorship', 'w_bibliographic', 'w_cocitation'].some(
      (key) => Number(params[key] ?? 0) > 0,
    )
    return (
      Math.abs(resolution - PAPER_MAP_DEFAULTS.resolution) < 1e-6 && !blended
    )
  }, [params])

  // ONE owner of field + terrain for every map surface (`useMapField`). This
  // used to be inlined here AND in Discovery's FrontierMap, which is how the
  // ±0.5 terrain domain reached one map and not the other.
  const fieldNodes = useMemo(
    () => nodes.map((n) => ({ id: n.id, x: n.x, y: n.y })),
    [nodes],
  )
  const field = useMapField({
    kind: isPaperMap ? 'paper' : 'author',
    enabled: fieldNeeded,
    nodes: fieldNodes,
    frame: layoutFrame,
    fallbackIsSubstrate: requestedDefaultLayout,
  })
  const liveScore = useCallback(
    (n: GraphNode): number | null => {
      const live = field.scoreFor(n.id)
      if (typeof live === 'number') return live
      // The payload's baked score is the pre-load fallback so a hover card is
      // never blank while the field is in flight.
      const raw = n.metadata?.score
      return typeof raw === 'number' ? raw : null
    },
    [field],
  )

  const dataColour = useCallback(
    (n: GraphNode): string | undefined => {
      if (colourMode === 'year' && yearRange) {
        const y = Number(n.metadata?.year)
        if (!Number.isFinite(y) || y < 1800) return MAP_INK.ambientSoft
        // Older = receding slate, newer = folio — recency reads as presence.
        return yearRampColor(y, yearRange.lo, yearRange.hi)
      }
      if (colourMode === 'score') {
        // Fixed 0–100 domain, SEQUENTIAL: 20 is not "disliked", it is weakly
        // ranked. The ramp lives in mapNodeStyle.
        const s = liveScore(n)
        if (s == null) return MAP_INK.ambientSoft
        return `rgb(${scoreRampColor(s).join(',')})`
      }
      return undefined
    },
    [colourMode, yearRange, liveScore],
  )

  const terrain = field.terrain

  // Colourbar stats — the numbers the legend owes the reader.
  const yearStats = useMemo(
    () => summarizeValues(nodes.map((n) => Number(n.metadata?.year)).filter((y) => y > 1800)),
    [nodes],
  )
  const scoreStats = useMemo(
    () =>
      summarizeValues(
        nodes.map((n) => liveScore(n)).filter((s): s is number => s != null),
      ),
    [nodes, liveScore],
  )
  // Terrain bar: always the stats of the values ACTUALLY splatted, so the ±max
  // the bar labels is the ±max the plate normalised against. On the substrate
  // frame that is the space's own stats (stable across every layer toggle);
  // on a re-fitted layout it is that layout's own population.
  const terrainStats = terrain.stats

  const query = search.trim().toLowerCase()
  const mapNodes = useMemo<SemanticMapNode[]>(
    () =>
      nodes.map((n): SemanticMapNode => {
        const cid = typeof n.cluster_id === 'number' ? n.cluster_id : undefined
        const label = cid != null && cid >= 0 ? clusterColors.get(cid)?.label : undefined
        return {
          id: n.id,
          x: n.x,
          // Same Y convention as every other host (higher-y at the top).
          y: 1 - n.y,
          kind: nodeKind(n),
          color:
            colourMode === 'year' || colourMode === 'score'
              ? dataColour(n)
              : cid != null && cid >= 0
                ? clusterColors.get(cid)?.color
                : undefined,
          sizeValue: typeof n.size === 'number' ? n.size : null,
          clusterId: cid,
          clusterLabel: label,
          dimmed:
            (cid != null && dimmedClusters.has(cid)) ||
            (query.length > 1 && !n.name.toLowerCase().includes(query)) ||
            // 50-M cluster focus: clicking a paper highlights its cluster —
            // everything OUTSIDE it recedes (dimmed, never hidden).
            (focusClusterId != null && cid !== focusClusterId),
          halo: nodeHalo?.(n) ?? false,
          suggestionOutline: nodeSuggestionOutline?.(n) ?? false,
        }
      }),
    [
      nodes,
      clusterColors,
      dimmedClusters,
      query,
      nodeKind,
      nodeHalo,
      nodeSuggestionOutline,
      focusClusterId,
      colourMode,
      dataColour,
    ],
  )

  const mapEdges = useMemo(
    () =>
      (data?.edges ?? [])
        .filter((e) => !hiddenEdgeTypes.has(String(e.edge_type ?? '')))
        .map((e) => ({
        source: String(e.source),
        target: String(e.target),
        weight: e.weight,
        color: EDGE_LAYER_COLORS[String(e.edge_type ?? '')] ?? EDGE_LAYER_FALLBACK_COLOR,
      })),
    [data, hiddenEdgeTypes],
  )

  const chipEntries: ClusterChipEntry[] = useMemo(
    () => [...clusterColors.entries()].map(([id, c]) => ({ id, ...c })),
    [clusterColors],
  )

  // Which membership tiers this plate actually shows, in draw order — the
  // legend renders exactly these, from the registry.
  const legendKinds = useMemo(() => {
    const present = new Set(mapNodes.map((n) => n.kind))
    return MAP_NODE_DRAW_ORDER.filter((kind) => present.has(kind))
  }, [mapNodes])

  const metadata = data?.metadata as Record<string, unknown> | undefined
  const layout = metadata?.layout as
    | { computed_at?: string; new_vectors_since_build?: number }
    | undefined
  const delivery = metadata?.delivery as
    | {
        source?: 'materialized_view' | 'variant_cache'
        computed_at?: string
        compute_ms?: number
        rebuilding?: boolean
      }
    | undefined
  const serverRebuilding = delivery?.rebuilding === true
  const layoutComputedAt = layout?.computed_at || delivery?.computed_at
  const omittedUnplaced =
    typeof metadata?.omitted_unplaced === 'number' ? metadata.omitted_unplaced : 0
  // Dots positioned by interpolation since the last full fit rather than by the
  // fit itself. Same contract as omittedUnplaced: an approximation the reader
  // can SEE, because an approximate dot renders identically to a computed one.
  const approximatePositions =
    typeof metadata?.approximate_positions === 'number' ? metadata.approximate_positions : 0

  if (graphQuery.isError && !data) {
    return (
      <ErrorState
        title="The map could not be loaded"
        message="Its saved layout is still intact. Retry the map request."
        actionLabel="Try again"
        onAction={() => void graphQuery.refetch()}
        actionPending={graphQuery.isFetching}
        className="min-h-[420px]"
      />
    )
  }

  if (!data) {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
        {building
          ? 'Building this layout in the background — it appears when ready…'
          : 'Loading the cached map…'}
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      <MapToolbar>
        {toolbarExtras}
        {showLinks && (
          <MapToggle
            active={showEdges}
            onClick={() => setShowEdges((s) => !s)}
            title="Draw the typed links (semantic, shared references, shared authors, cited together)"
          >
            <Share2 className="h-3.5 w-3.5" />
            Links{showEdges && data ? ` · ${data.edges.length}` : ''}
          </MapToggle>
        )}
        <MapToggle
          active={showToponyms}
          onClick={() => setShowToponyms((s) => !s)}
          title="Cluster names on the map"
        >
          <Type className="h-3.5 w-3.5" />
          Names
        </MapToggle>
        <MapToggle
          active={showTerrain}
          onClick={() => setShowTerrain((s) => !s)}
          title={
            isPaperMap
              ? 'Preference terrain — the space-owned signal field (ratings, saves, removals + engine scores) washed under the dots. Composes with any colouring; the same whatever layers are shown (view-only)'
              : 'Preference terrain — each author carries the mean signal of the papers of theirs you have an opinion on (saves, ratings, removals + engine scores), washed under the dots. Authors you have no signal on leave the paper bare. Composes with any colouring (view-only)'
          }
        >
          <Mountain className="h-3.5 w-3.5" />
          Terrain
        </MapToggle>
        <MapModeSwitch
          value={colourMode}
          onChange={setColourMode}
          options={(
            [
              { value: 'clusters', label: 'Clusters', title: 'Colour by corpus cluster' },
              { value: 'year', label: 'Year', title: 'Recency ramp — older fades, newer leads' },
              { value: 'score', label: 'Score', title: 'Engine relevance (latest suggestion score, 0–100) — pale blue weak, deep blue strong; never-scored grey' },
            ] as const
          ).filter((o) => colourModes.includes(o.value))}
        />
        <MapDataStatus
          phase={
            building || serverRebuilding
              ? 'building'
              : graphQuery.isFetching ||
                  (fieldNeeded && field.isFetching)
                ? 'refreshing'
                : 'idle'
          }
        />
        <span className="relative ml-auto">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Find on map…"
            className={cn(
              'h-7 w-44 rounded-sm border border-control-edge bg-control-well pl-7 pr-2 text-xs text-alma-800',
              'placeholder:text-slate-400 focus:border-control-edge-strong focus:outline-none',
            )}
          />
        </span>
      </MapToolbar>

      <SemanticMap
        nodes={mapNodes}
        edges={mapEdges}
        showEdges={showLinks && showEdges}
        showToponyms={showToponyms}
        height={height}
        sizeScale={sizeScale}
        dotOpacity={dotOpacity}
        terrainOpacity={terrainOpacity}
        toponymScale={toponymScale}
        toponymWordCount={toponymWordCount}
        heatField={showTerrain ? terrain.points : undefined}
        lassoMode={lassoMode}
        onLasso={onLasso}
        selectedIds={selectedNodeId ? new Set([selectedNodeId]) : undefined}
        renderHover={(id) => {
          const n = nodesById.get(id)
          return n ? hoverCard(n) : null
        }}
        renderClick={
          renderClickCard
            ? (id, close) => {
                const n = nodesById.get(id)
                return n ? renderClickCard(n, close) : null
              }
            : undefined
        }
        onClickNode={(id) => {
          if (id == null) {
            onBackgroundClick?.()
            return
          }
          const n = nodesById.get(id)
          if (n) onOpenNode?.(n)
        }}
        viewStateKey={mapStateKey}
        className="rounded-none border-0"
      >
        {plateOverlay}
      </SemanticMap>

      <MapLegend>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
          {/* The registry IS the legend source (50-E): one swatch per node kind
              actually present, in draw order, wearing that kind's own fill,
              opacity and words. Hand-written legend lines went stale the moment
              a host added a tier — the Authors map showed "In your library" for
              a plate with no library dots on it. */}
          {legendKinds.map((kind) => {
            const style = MAP_NODE_STYLES[kind]
            return (
              <span key={kind} className="inline-flex items-center gap-1.5">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={
                    style.filled
                      ? { background: style.defaultColor, opacity: Math.max(0.45, style.opacity) }
                      : {
                          border: `${HOLLOW_STROKE_WIDTH}px solid ${style.defaultColor}`,
                          background: 'transparent',
                        }
                  }
                />
                {style.legend}
              </span>
            )
          })}
          <span className="text-slate-400">{nodes.length} on the map</span>
          {omittedUnplaced > 0 && (
            // An omission the reader can SEE. These authors have no embedded
            // paper, so they have no semantic position — they used to be drawn
            // on an invented ring around the centre, which read as a real
            // structure (user call 2026-07-26).
            <span
              className="text-slate-400"
              title="These authors have no paper with a vector yet, so they have no position on a semantic map. They appear once their papers are embedded."
            >
              · {omittedUnplaced} not placed (no embedded paper)
            </span>
          )}
          {approximatePositions > 0 && (
            <span
              className="text-slate-400"
              title="These papers gained a vector after the last full layout, so their position is interpolated from their nearest already-placed neighbours rather than computed by the fit. The next full rebuild places them exactly."
            >
              · {approximatePositions} placed approximately
            </span>
          )}
          {colourMode === 'year' && yearRange && yearStats && (
            // Ramp clamps at p10–p90 for contrast; the labels say so with
            // extend notation ("≤2004" = everything older saturates at the
            // dark end) so the bar never hides the real data range.
            <ColourBarLegend
              gradient={RAMP_GRADIENTS.year}
              min={yearStats.min < yearRange.lo ? `≤${yearRange.lo}` : String(yearRange.lo)}
              max={yearStats.max > yearRange.hi ? `≥${yearRange.hi}` : String(yearRange.hi)}
              mean={String(Math.round(yearStats.mean))}
            />
          )}
          {colourMode === 'score' && (
            // Absolute, centred on the neutral 50 — the internal score has a
            // fixed 0–100 domain, so the scale never restretches per view.
            <ColourBarLegend
              gradient={SCORE_LEGEND.gradient}
              min={SCORE_LEGEND.min}
              mid={SCORE_LEGEND.mid}
              max={SCORE_LEGEND.max}
              mean={scoreStats ? String(Math.round(scoreStats.mean)) : undefined}
            />
          )}
          {showTerrain && terrainStats && (
            // Fixed semantic valence domain: weak populations stay weak and
            // every map uses a directly comparable colour. The endpoints are
            // DERIVED from the ramp constant — hardcoding "-1"/"1" made the
            // colourbar claim a domain the ramp had stopped using.
            <ColourBarLegend
              gradient={TERRAIN_LEGEND.gradient}
              min={TERRAIN_LEGEND.min}
              mid={TERRAIN_LEGEND.mid}
              max={TERRAIN_LEGEND.max}
              mean={terrainStats.mean.toFixed(2)}
            />
          )}
          {showTerrain && field.model?.fitted && (
            // The terrain is mostly INFERRED, and a reader who thinks they are
            // looking at recorded opinions everywhere would badly misread it.
            // Say the split out loud: faint regions are the model guessing.
            <span
              className="text-slate-400"
              title={
                `Fitted from ${field.model.n_labels.toLocaleString()} of your signals in ` +
                `SPECTER2 space. Faded areas are low-confidence predictions, not recorded opinions.`
              }
            >
              {field.model.n_observed.toLocaleString()} recorded ·{' '}
              {field.model.n_predicted.toLocaleString()} inferred
            </span>
          )}
          {showTerrain && terrain.frame === 'own' && (
            // A tuned layout is its own space. The terrain follows it — and
            // only papers WITH a place in it can appear, so say which
            // population the bar above describes instead of implying the
            // whole corpus.
            <span className="text-slate-400">
              terrain over this layout&rsquo;s {terrain.coverage.total.toLocaleString()} papers
            </span>
          )}
          {layoutComputedAt && (
            <span
              className="text-slate-400"
              title={
                delivery?.compute_ms
                  ? `Stored layout computed in ${delivery.compute_ms} ms`
                  : 'Stored layout artifact'
              }
            >
              cached layout {String(layoutComputedAt).slice(0, 10)}
              {layout?.new_vectors_since_build
                ? ` · ${layout.new_vectors_since_build} new since (placed live, folded in on the next refresh)`
                : ''}
            </span>
          )}
          {legendExtras}
        </div>
        {showLinks && showEdges && data && data.edges.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
            {Object.entries(
              (data.edges as Array<{ edge_type?: string }>).reduce<Record<string, number>>((acc, e) => {
                const t = String(e.edge_type ?? 'link')
                acc[t] = (acc[t] ?? 0) + 1
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
                  {EDGE_LAYER_LABELS[type] ?? type} · {count}
                </button>
              )
            })}
          </div>
        )}
        <ClusterLegendChips
          clusters={chipEntries}
          dimmed={dimmedClusters}
          onToggle={(id) =>
            setDimmedClusters((prev) => {
              const next = new Set(prev)
              if (next.has(id)) next.delete(id)
              else next.add(id)
              return next
            })
          }
        />
      </MapLegend>
    </div>
  )
}
