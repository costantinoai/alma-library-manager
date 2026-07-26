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
import { AlertTriangle, Loader2, Mountain, Search, Share2, Type } from 'lucide-react'

import { type GraphData, type GraphNode } from '@/api/client'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'
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
  useMapSessionSet,
  useMapSessionState,
} from './mapSessionState'
import { SemanticMap, type SemanticMapNode } from './SemanticMap'
import { useAuthorField } from './useAuthorField'
import { useSignalField } from './useSignalField'
import {
  EDGE_LAYER_COLORS,
  EDGE_LAYER_FALLBACK_COLOR,
  EDGE_LAYER_LABELS,
  HOLLOW_STROKE_WIDTH,
  MAP_INK,
  MAP_NODE_DRAW_ORDER,
  MAP_NODE_STYLES,
  RAMP_GRADIENTS,
  summarizeValues,
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

  // Cluster hue map — largest first on the SAME ramp every host uses, so the
  // biggest cluster is always hue #0 wherever you meet it.
  const clusterColors = useMemo(() => {
    const tally = new Map<number, { label: string; count: number }>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      const label = String(n.metadata?.cluster_label ?? '') || `Cluster ${n.cluster_id}`
      const row = tally.get(n.cluster_id)
      if (row) row.count += 1
      else tally.set(n.cluster_id, { label, count: 1 })
    }
    const ordered = [...tally.entries()].sort((a, b) => b[1].count - a[1].count)
    return new Map(ordered.map(([id, v], i) => [id, { ...v, color: branchMapColor(i) }]))
  }, [nodes])

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
  const signalField = useSignalField(fieldNeeded && isPaperMap)
  const authorField = useAuthorField(String(params.scope ?? 'library'), fieldNeeded && !isPaperMap)

  /** OpenAlex author ids are case-insensitive and the payload's casing does
   *  not match the authors table's — fold before every id lookup. */
  const nodeKey = (n: GraphNode) => n.id.trim().toLowerCase()

  const liveScore = useCallback(
    (n: GraphNode): number | null => {
      if (isPaperMap) return signalField.scoresById.get(n.id) ?? null
      // Live field first; the payload's baked score is the pre-load fallback
      // so the hover card is never blank while the field is in flight.
      const live = authorField.scoresById.get(nodeKey(n))
      if (typeof live === 'number') return live
      const raw = n.metadata?.score
      return typeof raw === 'number' ? raw : null
    },
    [isPaperMap, signalField.scoresById, authorField.scoresById],
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
        // Internal relevance score (0–100), divergent about the neutral 50:
        // red = the engine scored it weak, green = strong. Never-scored
        // papers (no recommendation yet) stay recessive.
        const s = liveScore(n)
        if (s == null) return MAP_INK.ambientSoft
        const t = Math.max(-1, Math.min(1, (s - 50) / 50))
        const mix = (a: number, b: number, k: number) => Math.round(a + (b - a) * k)
        return t < 0
          ? `rgb(${mix(220, 233, 1 + t)}, ${mix(68, 196, 1 + t)}, ${mix(61, 76, 1 + t)})`
          : `rgb(${mix(233, 64, t)}, ${mix(196, 160, t)}, ${mix(76, 92, t)})`
      }
      return undefined
    },
    [colourMode, yearRange, liveScore],
  )

  // Author terrain: the live valence of each RENDERED author that carries one.
  // Authors you have no signal on contribute NOTHING — they used to be pushed
  // in as hard zeros, and in corpus scope ~90% of authors have no signal, so
  // the splat's local mean was diluted to ≈0 everywhere and the terrain read as
  // flat yellow (user catch 2026-07-26). Pale paper where you have no opinion
  // is the honest render; the paper map can afford neutral filler because its
  // substrate must stay hole-free, the author map cannot.
  const terrainValues = useMemo(() => {
    if (!showTerrain || isPaperMap) return undefined
    const m = new Map<string, number>()
    for (const n of nodes) {
      const v = authorField.valenceById.get(n.id.trim().toLowerCase())
      if (typeof v === 'number') m.set(n.id, v)
    }
    return m
  }, [showTerrain, isPaperMap, nodes, authorField.valenceById])

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
  // Terrain bar: for paper maps the SPACE's own stats (stable across view
  // state). For the author map, the stats of the values actually splatted —
  // so the ±max the bar labels is the ±max the plate normalised against.
  const terrainStats = useMemo(() => {
    if (isPaperMap) return signalField.stats
    return terrainValues?.size ? summarizeValues([...terrainValues.values()]) : null
  }, [isPaperMap, signalField.stats, terrainValues])

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

  if (graphQuery.isError && !data) {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-3 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <AlertTriangle className="h-5 w-5 text-slate-500" />
        <span>The map could not be loaded.</span>
        <button
          type="button"
          onClick={() => void graphQuery.refetch()}
          className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet"
        >
          Try again
        </button>
      </div>
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
              { value: 'score', label: 'Score', title: 'Engine relevance (latest suggestion score, 0–100) — red weak, green strong; never-scored grey' },
            ] as const
          ).filter((o) => colourModes.includes(o.value))}
        />
        <MapDataStatus
          phase={
            building || serverRebuilding
              ? 'building'
              : graphQuery.isFetching ||
                  (fieldNeeded &&
                    (isPaperMap
                      ? signalField.isFetching
                      : authorField.isFetching))
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
        toponymScale={toponymScale}
        toponymWordCount={toponymWordCount}
        heatValues={terrainValues}
        heatField={showTerrain && isPaperMap ? signalField.points : undefined}
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
              gradient={RAMP_GRADIENTS.divergent}
              min="0"
              mid="50"
              max="100"
              mean={scoreStats ? String(Math.round(scoreStats.mean)) : undefined}
            />
          )}
          {showTerrain && terrainStats && (
            // SYMMETRIC about zero at the field's REAL max |value| —
            // dynamic, the exact scale the splat uses (user contract
            // 2026-07-25). Narrow yellow band = neutral; strong red/green
            // right off zero.
            <ColourBarLegend
              gradient={RAMP_GRADIENTS.terrain}
              min={(-Math.max(Math.abs(terrainStats.min), Math.abs(terrainStats.max))).toFixed(2)}
              mid="0"
              max={Math.max(Math.abs(terrainStats.min), Math.abs(terrainStats.max)).toFixed(2)}
              mean={terrainStats.mean.toFixed(2)}
            />
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
