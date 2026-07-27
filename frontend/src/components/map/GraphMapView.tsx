/**
 * GraphMapView — fetch a `GraphData` payload, hand it to `MapSurface`.
 *
 * This used to BE the map host, 766 lines of it, and that is precisely why
 * Discovery could not reuse it: it fetched its own data, so a surface with a
 * different endpoint and node shape had no way in and grew a parallel copy
 * (task 64 §0.2). Everything shared now lives in `MapSurface`, which receives
 * nodes; what stays here is the only thing that was ever specific — the
 * `/graphs/<endpoint>` query, its loading and error states, and the translation
 * from a `GraphNode` into the surface's vocabulary-free node shape.
 *
 * The Map page (corpus/library papers) and the Authors network both render
 * through this, and their remaining differences are props, not code paths.
 */
import { useCallback, useEffect, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'

import { type GraphData, type GraphNode } from '@/api/client'
import { branchMapColor } from '@/lib/palette'
import { ErrorState } from '@/components/ui/ErrorState'
import { graphQueryOptions } from './mapQueries'
import { PAPER_MAP_DEFAULTS } from './mapSessionState'
import { MapSurface, MapSurfaceLoading, type MapSurfaceNode } from './MapSurface'
import { type MapNodeKind } from './mapNodeStyle'

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
  /** Offer the Year ramp. Authors drop it — an author has no single
   *  publication year (user call 2026-07-25) — but the surface derives that
   *  from the data now, so this only exists for a host that wants to suppress
   *  a mode its data WOULD support. */
  offerScoreMode?: boolean
  /** Rectangle-select (region) mode — drag selects instead of panning. */
  lassoMode?: boolean
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
  sizeScale,
  dotOpacity,
  terrainOpacity,
  toponymScale,
  toponymWordCount,
  offerScoreMode = true,
  lassoMode = false,
  onLasso,
}: GraphMapViewProps) {
  const stateKey = endpoint === 'paper-map' ? 'paper-map' : 'author-map'

  const queryClient = useQueryClient()
  const graphQuery = useQuery(graphQueryOptions(queryClient, endpoint, params))
  const data = graphQuery.data?.payload
  const building = graphQuery.data?.build
  useEffect(() => {
    if (data) onPayload?.(data)
  }, [data, onPayload])

  const graphNodes = useMemo(() => data?.nodes ?? [], [data])
  const nodesById = useMemo(() => new Map(graphNodes.map((n) => [n.id, n])), [graphNodes])

  // Cluster hue map. The hue identifies WHICH region of the space a cluster is,
  // so it belongs to the space, not to the current selection: the backend ranks
  // clusters over the whole layout and ships `hue_index`, and Library — a subset
  // of the very same corpus layout — therefore paints every cluster exactly as
  // Corpus does. Ranking the RENDERED nodes here instead recoloured the entire
  // map on a scope switch (measured 2026-07-26: the top Library cluster was hue
  // #0 in Library and #194 in Corpus). Local ranking survives only as the
  // fallback for a payload built before `hue_index` existed.
  const clusterHues = useMemo(() => {
    const spaceHue = new Map<number, number>()
    for (const cluster of (data?.metadata?.clusters ?? []) as Array<{
      id?: number
      hue_index?: number
    }>) {
      if (typeof cluster?.id === 'number' && typeof cluster?.hue_index === 'number') {
        spaceHue.set(cluster.id, cluster.hue_index)
      }
    }
    const counts = new Map<number, number>()
    for (const n of graphNodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      counts.set(n.cluster_id, (counts.get(n.cluster_id) ?? 0) + 1)
    }
    const ordered = [...counts.entries()].sort((a, b) => b[1] - a[1])
    return new Map(ordered.map(([id], i) => [id, branchMapColor(spaceHue.get(id) ?? i)]))
  }, [graphNodes, data])

  // GraphNode → the surface's vocabulary-free shape. The ONLY translation.
  const surfaceNodes = useMemo<MapSurfaceNode[]>(
    () =>
      graphNodes.map((n) => {
        const cid = typeof n.cluster_id === 'number' && n.cluster_id >= 0 ? n.cluster_id : null
        const year = Number(n.metadata?.year)
        return {
          id: n.id,
          x: n.x,
          y: n.y,
          kind: nodeKind(n),
          groupId: cid,
          groupColor: cid != null ? clusterHues.get(cid) : undefined,
          groupLabel: String(n.metadata?.cluster_label ?? '') || undefined,
          sizeValue: typeof n.size === 'number' ? n.size : null,
          year: Number.isFinite(year) ? year : null,
          score: typeof n.metadata?.score === 'number' ? n.metadata.score : null,
          name: n.name,
          halo: nodeHalo?.(n) ?? false,
          outline: nodeSuggestionOutline?.(n) ?? false,
        }
      }),
    [graphNodes, nodeKind, clusterHues, nodeHalo, nodeSuggestionOutline],
  )

  const surfaceEdges = useMemo(
    () =>
      (data?.edges ?? []).map((e) => ({
        source: String(e.source),
        target: String(e.target),
        weight: e.weight,
        type: String(e.edge_type ?? ''),
      })),
    [data],
  )

  // Which coordinate space this payload's dots live in. A non-default cluster
  // detail re-runs UMAP, and terrain painted at SUBSTRATE coordinates over a
  // re-fitted layout is one map's landscape on another map's land (user report
  // 2026-07-26). The backend declares the frame; the fallback covers payloads
  // cached before it did, and only the default request can claim substrate.
  //
  // Cluster detail is now the ONLY knob that can re-fit: the layout-blend
  // sliders were removed on 2026-07-28 so placement is semantic everywhere.
  const layoutFrame = (data?.metadata?.layout as { frame?: string } | undefined)?.frame
  const requestedDefaultLayout = useMemo(() => {
    const resolution = Number(params.cluster_resolution ?? PAPER_MAP_DEFAULTS.resolution)
    return Math.abs(resolution - PAPER_MAP_DEFAULTS.resolution) < 1e-6
  }, [params])

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
  const layoutComputedAt = layout?.computed_at || delivery?.computed_at
  const omittedUnplaced =
    typeof metadata?.omitted_unplaced === 'number' ? metadata.omitted_unplaced : 0
  // Dots positioned by interpolation since the last full fit rather than by the
  // fit itself. Same contract as omittedUnplaced: an approximation the reader
  // can SEE, because an approximate dot renders identically to a computed one.
  const approximatePositions =
    typeof metadata?.approximate_positions === 'number' ? metadata.approximate_positions : 0

  const hover = useCallback(
    (id: string) => {
      const n = nodesById.get(id)
      return n ? hoverCard(n) : null
    },
    [nodesById, hoverCard],
  )

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
  if (!data) return <MapSurfaceLoading building={Boolean(building)} />

  return (
    <MapSurface
      stateKey={stateKey}
      fieldKind={endpoint === 'paper-map' ? 'paper' : 'author'}
      nodes={surfaceNodes}
      edges={surfaceEdges}
      frame={layoutFrame}
      fallbackIsSubstrate={requestedDefaultLayout}
      hoverCard={hover}
      renderClickCard={
        renderClickCard
          ? (id, close) => {
              const n = nodesById.get(id)
              return n ? renderClickCard(n, close) : null
            }
          : undefined
      }
      onOpenNode={(id) => {
        const n = nodesById.get(id)
        if (n) onOpenNode?.(n)
      }}
      onBackgroundClick={onBackgroundClick}
      toolbarExtras={toolbarExtras}
      legendExtras={
        <>
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
        </>
      }
      plateOverlay={plateOverlay}
      height={height}
      showLinks={showLinks}
      offerScoreMode={offerScoreMode}
      building={Boolean(building) || delivery?.rebuilding === true}
      refreshing={graphQuery.isFetching}
      selectedNodeId={selectedNodeId}
      focusGroupId={focusClusterId}
      lassoMode={lassoMode}
      onLasso={onLasso}
      sizeScale={sizeScale}
      dotOpacity={dotOpacity}
      terrainOpacity={terrainOpacity}
      toponymScale={toponymScale}
      toponymWordCount={toponymWordCount}
    />
  )
}
