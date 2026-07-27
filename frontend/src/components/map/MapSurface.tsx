/**
 * MapSurface — THE map host. Every map on every page renders through this.
 *
 * Why it exists (task 64 P1). There used to be two hosts: `GraphMapView` (the
 * Map page's papers + the Authors network) and Discovery's `FrontierMap`. They
 * were parallel implementations of one job — each wired its own session state,
 * its own terrain, its own colourbars, its own `SemanticMap` invocation — and
 * the cost was not theoretical. When the terrain ramp moved to a ±0.5 domain,
 * the Map page followed and Discovery did not: its colourbar went on claiming
 * `-1 … +1` beside a gradient that no longer used it. Nothing failed, because
 * nothing tied the two together.
 *
 * The root cause was that `GraphMapView` FETCHED its own data, so a surface
 * with a different endpoint and node shape could not reuse it and grew a copy
 * instead. So this component fetches nothing. It **receives** nodes in a shape
 * that says nothing about papers, authors or recommendations, and hosts keep
 * exactly what is theirs: their query, their loading states, what a click
 * opens, and any extra controls their surface genuinely needs.
 *
 * The split, precisely:
 *
 *   SemanticMap   pure renderer — canvas, hit-test, zoom/pan, toponyms.
 *      ↑
 *   MapSurface    THIS. Session state, colour modes, terrain overlay, legend,
 *                 colourbars, edge/group chips, search, the plate itself.
 *      ↑
 *   hosts         MapPage / AuthorMapPanel / FrontierMap: fetch, normalise,
 *                 and supply meaning-only slots.
 *
 * **Colour modes are DERIVED from the data present**, never configured. A node
 * set with no `year` simply does not offer Year; one with no `score` does not
 * offer Score. That is what stops `colourModes={[...]}` lists drifting apart per
 * host — the previous hosts disagreed about which modes existed and each had to
 * be told separately.
 */
import { useCallback, useMemo } from 'react'
import { Loader2, Mountain, Search, Share2, Type } from 'lucide-react'

import { cn } from '@/lib/utils'
import {
  ClusterLegendChips,
  ColourBarLegend,
  MapDataStatus,
  MapDisplayTuningRows,
  MapLegend,
  MapModeSwitch,
  MapToggle,
  MapToolbar,
  MapTuningPopover,
  type ClusterChipEntry,
} from './MapChrome'
import {
  MAP_TERRAIN_OPACITY_DEFAULT,
  useMapSessionSet,
  useMapSessionState,
} from './mapSessionState'
import { SemanticMap, type SemanticMapNode } from './SemanticMap'
import { useMapField, type MapFieldKind } from './useMapField'
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
  terrainLegendFor,
  yearRampColor,
  yearRampLimits,
  type MapNodeKind,
} from './mapNodeStyle'

/** The ONLY thing a host must say about each of its nodes.
 *
 *  Deliberately free of any vocabulary: no paper, no author, no
 *  recommendation. A host translates its own row into this and keeps the row. */
export interface MapSurfaceNode {
  id: string
  /** World coordinates in the substrate's unit square, y NOT yet flipped. */
  x: number
  y: number
  kind: MapNodeKind
  /** Which group this node belongs to — cluster, branch, whatever grouping
   *  means on this surface. Presence enables the Groups colour mode. */
  groupId?: number | null
  /** Hue for that group. Supplied by the host, because the hue answers WHICH
   *  ONE and the space that owns the ranking also owns the hue. */
  groupColor?: string
  /** The one magnitude channel (citations, publications, score…). */
  sizeValue?: number | null
  /** Presence anywhere in the set enables the Year colour mode. */
  year?: number | null
  /** Baked score, used only until the live field answers. Presence anywhere
   *  enables the Score colour mode. */
  score?: number | null
  /** Searched by the toolbar's find box. */
  name?: string
  /** Human name of this node's group, for toponyms and chips. */
  groupLabel?: string
  /** Dashed halo — a temporal fact (new / followed), never a colour change. */
  halo?: boolean
  /** Persistent gold provenance outline, independent of dot colour. */
  outline?: boolean
  /** Host-driven dimming, ORed with this surface's own (search, group chips,
   *  focus). A host may only ADD reasons to recede, never remove them. */
  dimmed?: boolean
}

export interface MapSurfaceEdge {
  source: string
  target: string
  weight?: number
  /** Typed link layer — drives colour, the legend chip and its filter. */
  type?: string
}

/** One way of colouring the plate that the surface cannot derive on its own.
 *
 *  The surface always offers Groups / Year / Score when the data supports
 *  them. A grouping here is a genuinely different QUESTION about the same
 *  points — Discovery's "which lens branch found this" is not a corpus
 *  cluster — so it is host meaning, and the host supplies both the hue and the
 *  chips that read it. */
export interface MapGrouping {
  id: string
  label: string
  title: string
  /** Hue for a node under this grouping; undefined ⇒ registry default. */
  colorFor: (node: MapSurfaceNode) => string | undefined
  /** Extra dimming while this grouping is active (branch highlight). */
  dimmed?: (node: MapSurfaceNode) => boolean
  /** Chips rendered under the legend while this grouping is active. */
  chips?: React.ReactNode
  /** Suppress toponyms under this grouping (they name CLUSTERS, so they lie
   *  under a grouping that is not clusters). */
  hideToponyms?: boolean
}

export interface MapSurfaceProps {
  /** Session namespace — every knob on this surface persists under it. */
  stateKey: string
  /** Which field family backs the terrain and live scores. */
  fieldKind: MapFieldKind
  nodes: ReadonlyArray<MapSurfaceNode>
  edges?: ReadonlyArray<MapSurfaceEdge>
  /** `metadata.layout.frame` from the rendered payload, when it has one. */
  frame?: unknown
  /** Frame to assume for payloads that predate the declaration. */
  fallbackIsSubstrate?: boolean

  /** Hover-card body — host vocabulary, shared shell. */
  hoverCard?: (id: string) => React.ReactNode
  /** Interactive dot-card body. SemanticMap owns anchor and dismissal. */
  renderClickCard?: (id: string, close: () => void) => React.ReactNode
  onOpenNode?: (id: string) => void
  onBackgroundClick?: () => void

  /** Extra groupings beyond the derived ones, in display order. */
  groupings?: ReadonlyArray<MapGrouping>
  /** Start on this grouping/mode the first time the surface is seen. */
  defaultColourMode?: string

  toolbarExtras?: React.ReactNode
  /** Rendered at the START of the legend row — the host's own counts. */
  legendCounts?: React.ReactNode
  legendExtras?: React.ReactNode
  /** Overlays pinned INSIDE the plate. Feedback for a spatial gesture belongs
   *  where the gesture ended, never below the fold. */
  plateOverlay?: React.ReactNode

  height?: number
  /** Draw the typed-link layer. Off for maps whose position already IS the
   *  topology (the author network: its adjacency is collaboration, so lines
   *  re-state position and bury the dots). */
  showLinks?: boolean
  /** Word for the links toggle, e.g. "Citation links". */
  linksLabel?: string
  /** Controlled links toggle. Pass BOTH when the host's edges are a fetch
   *  parameter rather than a filter over an already-loaded payload (Discovery
   *  asks the server for its citation edges), so the toggle and the query
   *  cannot disagree. Omit and the surface owns it in session state. */
  showEdges?: boolean
  onShowEdgesChange?: (next: boolean) => void
  /** Show the find-on-map box. */
  searchable?: boolean
  /** Offer the Score colour mode.
   *
   *  Not derived like Year and Groups, and deliberately so: the live field can
   *  score ANY node in a paper or author space, so "is a score present" is
   *  always true and would derive nothing. What varies is whether this surface
   *  has already spent a channel on the score — Discovery draws it as dot SIZE,
   *  so colouring by it too would say one thing twice. */
  offerScoreMode?: boolean
  /** Expose the display-tuning popover (dot size, opacities, words) INSIDE
   *  the toolbar. Hosts with their own Advanced popover pass false. */
  tuningInToolbar?: boolean

  /** Data status, from the host's own query. */
  building?: boolean
  refreshing?: boolean

  selectedNodeId?: string | null
  /** Group focus: everything outside it recedes. */
  focusGroupId?: number | null
  lassoMode?: boolean
  onLasso?: (ids: string[], anchor: { x: number; y: number }) => void

  /** Display knobs a host drives itself (the Map page's Advanced popover).
   *  Omitted ⇒ this surface owns them in its own session state. */
  sizeScale?: number
  dotOpacity?: number
  terrainOpacity?: number
  toponymScale?: number
  toponymWordCount?: number
}

const DERIVED_GROUPS_ID = 'groups'

export function MapSurface({
  stateKey,
  fieldKind,
  nodes,
  edges = [],
  frame,
  fallbackIsSubstrate = true,
  hoverCard,
  renderClickCard,
  onOpenNode,
  onBackgroundClick,
  groupings,
  defaultColourMode,
  toolbarExtras,
  legendCounts,
  legendExtras,
  plateOverlay,
  height = 560,
  showLinks = true,
  linksLabel = 'Links',
  showEdges: showEdgesProp,
  onShowEdgesChange,
  searchable = true,
  offerScoreMode = true,
  tuningInToolbar = false,
  building = false,
  refreshing = false,
  selectedNodeId,
  focusGroupId,
  lassoMode = false,
  onLasso,
  sizeScale: sizeScaleProp,
  dotOpacity: dotOpacityProp,
  terrainOpacity: terrainOpacityProp,
  toponymScale: toponymScaleProp,
  toponymWordCount: toponymWordCountProp,
}: MapSurfaceProps) {
  // ── Session state. ONE block, for every surface. ────────────────────────
  const [ownShowEdges, setOwnShowEdges] = useMapSessionState(stateKey, 'showEdges', false)
  const showEdges = showEdgesProp ?? ownShowEdges
  const toggleEdges = () =>
    onShowEdgesChange ? onShowEdgesChange(!showEdges) : setOwnShowEdges((s) => !s)
  const [showToponyms, setShowToponyms] = useMapSessionState(stateKey, 'showToponyms', true)
  // Terrain is an OVERLAY, not a colour mode: the preference field composes
  // with ANY dot colouring (user call 2026-07-25).
  const [showTerrain, setShowTerrain] = useMapSessionState(stateKey, 'showTerrain', false)
  const [hiddenEdgeTypes, setHiddenEdgeTypes] = useMapSessionSet<string>(
    stateKey,
    'hiddenEdgeTypes',
  )
  const [dimmedGroups, setDimmedGroups] = useMapSessionSet<number>(stateKey, 'dimmedClusters')
  const [search, setSearch] = useMapSessionState(stateKey, 'search', '')
  // Display knobs the surface owns when the host does not drive them.
  const [ownSizeScale, setOwnSizeScale] = useMapSessionState(stateKey, 'sizeScale', 1)
  const [ownDotOpacity, setOwnDotOpacity] = useMapSessionState(stateKey, 'dotOpacity', 1)
  const [ownTerrainOpacity, setOwnTerrainOpacity] = useMapSessionState(
    stateKey,
    'terrainOpacity',
    MAP_TERRAIN_OPACITY_DEFAULT,
  )
  const [ownWordScale, setOwnWordScale] = useMapSessionState(stateKey, 'wordScale', 1)
  const [ownWordCount, setOwnWordCount] = useMapSessionState(stateKey, 'wordCount', 3)

  const sizeScale = sizeScaleProp ?? ownSizeScale
  const dotOpacity = dotOpacityProp ?? ownDotOpacity
  const terrainOpacity = terrainOpacityProp ?? ownTerrainOpacity
  const toponymScale = toponymScaleProp ?? ownWordScale
  const toponymWordCount = toponymWordCountProp ?? ownWordCount

  // ── Which colour modes this DATA supports. Derived, never configured. ───
  const hasYear = useMemo(
    () => nodes.some((n) => typeof n.year === 'number' && n.year > 1800),
    [nodes],
  )
  const hasGroups = useMemo(
    () => nodes.some((n) => typeof n.groupId === 'number' && n.groupId >= 0),
    [nodes],
  )

  // Group hue + label + population, from what the host supplied. The hue
  // belongs to the SPACE, so the host passes it per node rather than letting
  // this component rank the rendered subset — ranking locally recoloured the
  // whole map on a scope switch (measured 2026-07-26).
  const groups = useMemo(() => {
    const tally = new Map<number, { label: string; count: number; color?: string }>()
    for (const n of nodes) {
      if (typeof n.groupId !== 'number' || n.groupId < 0) continue
      const row = tally.get(n.groupId)
      if (row) row.count += 1
      else
        tally.set(n.groupId, {
          label: n.groupLabel || `Cluster ${n.groupId}`,
          count: 1,
          color: n.groupColor,
        })
    }
    return new Map([...tally.entries()].sort((a, b) => b[1].count - a[1].count))
  }, [nodes])

  const modes = useMemo(() => {
    const out: MapGrouping[] = [...(groupings ?? [])]
    if (!groupings?.length && hasGroups) {
      out.push({
        id: DERIVED_GROUPS_ID,
        label: 'Clusters',
        title: 'Colour by corpus cluster',
        colorFor: (n) => n.groupColor,
      })
    }
    return out
  }, [groupings, hasGroups])

  const availableModes = useMemo(() => {
    const out = modes.map((m) => ({ value: m.id, label: m.label, title: m.title }))
    if (hasYear) {
      out.push({
        value: 'year',
        label: 'Year',
        title: 'Recency ramp — older fades, newer leads',
      })
    }
    if (offerScoreMode) {
      out.push({
        value: 'score',
        label: 'Score',
        title:
          'Engine relevance (latest suggestion score, 0–100) — pale blue weak, deep blue strong; never-scored grey',
      })
    }
    return out
  }, [modes, hasYear, offerScoreMode])

  const [storedMode, setColourMode] = useMapSessionState<string>(
    stateKey,
    'colourMode',
    defaultColourMode ?? modes[0]?.id ?? 'year',
  )
  // A stored mode whose data has gone (a payload with no years, a grouping the
  // host stopped offering) must not leave the plate uncoloured and the switch
  // pointing at nothing.
  const colourMode = availableModes.some((m) => m.value === storedMode)
    ? storedMode
    : (availableModes[0]?.value ?? 'year')
  const activeGrouping = modes.find((m) => m.id === colourMode)

  // The field is fetched only when something on screen actually reads it —
  // the terrain wash or the Score ramp.
  const fieldNodes = useMemo(
    () => nodes.map((n) => ({ id: n.id, x: n.x, y: n.y })),
    [nodes],
  )
  const field = useMapField({
    kind: fieldKind,
    enabled: showTerrain || colourMode === 'score',
    nodes: fieldNodes,
    frame,
    fallbackIsSubstrate,
  })

  const liveScore = useCallback(
    (n: MapSurfaceNode): number | null => {
      const live = field.scoreFor(n.id)
      if (typeof live === 'number') return live
      // The payload's baked score is the pre-load fallback, so a hover card is
      // never blank while the field is in flight.
      return typeof n.score === 'number' ? n.score : null
    },
    [field],
  )

  const yearRange = useMemo(
    () => yearRampLimits(nodes.map((n) => Number(n.year))),
    [nodes],
  )

  const terrain = field.terrain

  // ── Colourbar stats — the numbers the legend owes the reader. ───────────
  const yearStats = useMemo(
    () => summarizeValues(nodes.map((n) => Number(n.year)).filter((y) => y > 1800)),
    [nodes],
  )
  const scoreStats = useMemo(
    () => summarizeValues(nodes.map((n) => liveScore(n)).filter((s): s is number => s != null)),
    [nodes, liveScore],
  )
  const terrainStats = terrain.stats

  const query = search.trim().toLowerCase()
  const mapNodes = useMemo<SemanticMapNode[]>(
    () =>
      nodes.map((n): SemanticMapNode => {
        const gid = typeof n.groupId === 'number' && n.groupId >= 0 ? n.groupId : undefined
        let color: string | undefined
        if (colourMode === 'year') {
          const y = Number(n.year)
          color =
            yearRange && Number.isFinite(y) && y > 1800
              ? yearRampColor(y, yearRange.lo, yearRange.hi)
              : MAP_INK.ambientSoft
        } else if (colourMode === 'score') {
          const s = liveScore(n)
          // Fixed 0–100 domain, SEQUENTIAL: 20 is not "disliked", it is weakly
          // ranked. The ramp lives in mapNodeStyle.
          color = s == null ? MAP_INK.ambientSoft : `rgb(${scoreRampColor(s).join(',')})`
        } else {
          color = activeGrouping?.colorFor(n)
        }
        return {
          id: n.id,
          x: n.x,
          // Same Y convention as every host: higher y draws at the top.
          y: 1 - n.y,
          kind: n.kind,
          color,
          sizeValue: n.sizeValue ?? null,
          clusterId: gid,
          clusterLabel: gid != null ? groups.get(gid)?.label : n.groupLabel,
          dimmed:
            (n.dimmed ?? false) ||
            (gid != null && dimmedGroups.has(gid)) ||
            (query.length > 1 && !(n.name ?? '').toLowerCase().includes(query)) ||
            // Group focus: clicking a dot highlights its group — everything
            // OUTSIDE it recedes (dimmed, never hidden).
            (focusGroupId != null && n.groupId !== focusGroupId) ||
            (activeGrouping?.dimmed?.(n) ?? false),
          halo: n.halo ?? false,
          suggestionOutline: n.outline ?? false,
        }
      }),
    [
      nodes,
      colourMode,
      activeGrouping,
      groups,
      dimmedGroups,
      query,
      focusGroupId,
      yearRange,
      liveScore,
    ],
  )

  const mapEdges = useMemo(
    () =>
      edges
        .filter((e) => !hiddenEdgeTypes.has(String(e.type ?? '')))
        .map((e) => ({
          source: e.source,
          target: e.target,
          weight: e.weight,
          color: EDGE_LAYER_COLORS[String(e.type ?? '')] ?? EDGE_LAYER_FALLBACK_COLOR,
        })),
    [edges, hiddenEdgeTypes],
  )

  const edgeTypeCounts = useMemo(() => {
    const acc: Record<string, number> = {}
    for (const e of edges) {
      const t = String(e.type ?? 'link')
      acc[t] = (acc[t] ?? 0) + 1
    }
    return acc
  }, [edges])

  const chipEntries: ClusterChipEntry[] = useMemo(
    () =>
      [...groups.entries()].map(([id, g]) => ({
        id,
        label: g.label,
        count: g.count,
        color: g.color ?? MAP_INK.ambientSoft,
      })),
    [groups],
  )

  // Which membership tiers this plate actually shows, in draw order — the
  // legend renders exactly these, from the registry. Hand-written legend lines
  // went stale the moment a host added a tier.
  const legendKinds = useMemo(() => {
    const present = new Set(mapNodes.map((n) => n.kind))
    return MAP_NODE_DRAW_ORDER.filter((kind) => present.has(kind))
  }, [mapNodes])

  const nodesById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes])

  return (
    <div className="overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      <MapToolbar>
        {toolbarExtras}
        {showLinks && (
          <MapToggle
            active={showEdges}
            onClick={toggleEdges}
            title="Draw the typed links (semantic, shared references, shared authors, cited together)"
          >
            <Share2 className="h-3.5 w-3.5" />
            {linksLabel}
            {showEdges && edges.length ? ` · ${edges.length}` : ''}
          </MapToggle>
        )}
        <MapToggle
          active={showToponyms}
          onClick={() => setShowToponyms((s) => !s)}
          title={
            activeGrouping?.hideToponyms
              ? 'Names show in the Clusters grouping'
              : 'Cluster names on the map'
          }
        >
          <Type className="h-3.5 w-3.5" />
          Names
        </MapToggle>
        {tuningInToolbar && (
          <MapTuningPopover>
            <MapDisplayTuningRows
              sizeScale={sizeScale}
              onSizeScale={setOwnSizeScale}
              dotOpacity={dotOpacity}
              onDotOpacity={setOwnDotOpacity}
              terrainOpacity={terrainOpacity}
              onTerrainOpacity={setOwnTerrainOpacity}
              wordScale={toponymScale}
              onWordScale={setOwnWordScale}
              wordCount={toponymWordCount}
              onWordCount={setOwnWordCount}
            />
          </MapTuningPopover>
        )}
        <MapToggle
          active={showTerrain}
          onClick={() => setShowTerrain((s) => !s)}
          title={
            fieldKind === 'paper'
              ? 'Preference terrain — the space-owned signal field (ratings, saves, removals + engine scores) washed under the dots. Composes with any colouring; the same whatever layers are shown (view-only)'
              : 'Preference terrain — each author carries the mean signal of the papers of theirs you have an opinion on (saves, ratings, removals + engine scores), washed under the dots. Composes with any colouring (view-only)'
          }
        >
          <Mountain className="h-3.5 w-3.5" />
          Terrain
        </MapToggle>
        {availableModes.length > 1 && (
          <MapModeSwitch value={colourMode} onChange={setColourMode} options={availableModes} />
        )}
        <MapDataStatus
          phase={
            building
              ? 'building'
              : refreshing || (showTerrain && field.isFetching)
                ? 'refreshing'
                : 'idle'
          }
        />
        {searchable && (
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
        )}
      </MapToolbar>

      <SemanticMap
        nodes={mapNodes}
        edges={mapEdges}
        showEdges={showLinks && showEdges}
        showToponyms={showToponyms && !activeGrouping?.hideToponyms}
        height={height}
        sizeScale={sizeScale}
        dotOpacity={dotOpacity}
        terrainOpacity={terrainOpacity}
        toponymScale={toponymScale}
        toponymWordCount={toponymWordCount}
        heatField={showTerrain ? terrain.points : undefined}
        heatFieldAbsMax={terrain.absMax}
        lassoMode={lassoMode}
        onLasso={onLasso}
        selectedIds={selectedNodeId ? new Set([selectedNodeId]) : undefined}
        renderHover={hoverCard ? (id) => (nodesById.has(id) ? hoverCard(id) : null) : undefined}
        renderClick={
          renderClickCard
            ? (id, close) => (nodesById.has(id) ? renderClickCard(id, close) : null)
            : undefined
        }
        onClickNode={(id) => {
          if (id == null) {
            onBackgroundClick?.()
            return
          }
          if (nodesById.has(id)) onOpenNode?.(id)
        }}
        viewStateKey={stateKey}
        className="rounded-none border-0"
      >
        {plateOverlay}
      </SemanticMap>

      <MapLegend>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
          {legendCounts ?? (
            <>
              {/* The registry IS the legend source (50-E): one swatch per node
                  kind actually present, in draw order, wearing that kind's own
                  fill, opacity and words. */}
              {legendKinds.map((kind) => {
                const style = MAP_NODE_STYLES[kind]
                return (
                  <span key={kind} className="inline-flex items-center gap-1.5">
                    <span
                      className="inline-block h-2.5 w-2.5 rounded-full"
                      style={
                        style.filled
                          ? {
                              background: style.defaultColor,
                              opacity: Math.max(0.45, style.opacity),
                            }
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
            </>
          )}
          {colourMode === 'year' && yearRange && yearStats && (
            // Ramp clamps at p10–p90 for contrast; the labels say so with
            // extend notation ("≤2004" = everything older saturates at the dark
            // end) so the bar never hides the real data range.
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
            // Bounds and gradient BOTH derived from the field's own range, so
            // this bar cannot drift from any other map's (Discovery's said
            // -1..1 while the ramp used ±0.5, until this was centralised).
            <ColourBarLegend
              gradient={terrainLegendFor(terrain.absMax).gradient}
              min={terrainLegendFor(terrain.absMax).min}
              mid={terrainLegendFor(terrain.absMax).mid}
              max={terrainLegendFor(terrain.absMax).max}
              mean={terrainStats.mean.toFixed(2)}
            />
          )}
          {showTerrain && field.model?.fitted && (
            // The terrain is mostly INFERRED, and a reader who thinks they are
            // looking at recorded opinions everywhere would badly misread it.
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
            // A tuned layout is its own space. The terrain follows it — and only
            // papers WITH a place in it can appear, so say which population the
            // bar above describes instead of implying the whole corpus.
            <span className="text-slate-400">
              terrain over this layout&rsquo;s {terrain.coverage.total.toLocaleString()} papers
            </span>
          )}
          {legendExtras}
        </div>

        {showLinks && showEdges && edges.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
            {Object.entries(edgeTypeCounts).map(([type, count]) => {
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

        {/* The active grouping's own chips, when it has them (branch steering);
            otherwise the shared cluster chips — click to dim, click to restore. */}
        {activeGrouping?.chips ?? (
          <ClusterLegendChips
            clusters={chipEntries}
            dimmed={dimmedGroups}
            onToggle={(id) =>
              setDimmedGroups((prev) => {
                const next = new Set(prev)
                if (next.has(id)) next.delete(id)
                else next.add(id)
                return next
              })
            }
          />
        )}
      </MapLegend>
    </div>
  )
}

/** Shared empty/loading plate, so a map that is still building looks the same
 *  wherever it is. Hosts own their own error state (they own the retry). */
export function MapSurfaceLoading({ building, message }: { building?: boolean; message?: string }) {
  return (
    <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
      <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
      {building
        ? (message ?? 'Building this layout in the background — it appears when ready…')
        : 'Loading the cached map…'}
    </div>
  )
}
