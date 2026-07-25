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
import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Search, Share2, Type } from 'lucide-react'

import { api, type GraphData, type GraphNode } from '@/api/client'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'
import {
  ClusterLegendChips,
  MapLegend,
  MapModeSwitch,
  MapToggle,
  MapToolbar,
  type ClusterChipEntry,
} from './MapChrome'
import { SemanticMap, type SemanticMapNode } from './SemanticMap'
import {
  EDGE_LAYER_COLORS,
  EDGE_LAYER_FALLBACK_COLOR,
  EDGE_LAYER_LABELS,
  MAP_INK,
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
  /** Hover-card body for a node — host vocabulary, shared shell. */
  hoverCard: (node: GraphNode) => React.ReactNode
  /** Dashed-halo marker (e.g. followed authors). Meaning documented by host. */
  haloIds?: ReadonlySet<string>
  /** Host slots — extra toolbar controls / legend rows. */
  toolbarExtras?: React.ReactNode
  legendExtras?: React.ReactNode
  height?: number
  /** 50-M drill-down hooks (the Map page host): the raw payload for
   *  inspector panels, an accent-ring selection, a cluster FOCUS (everything
   *  outside it dims), and a dot-size knob. */
  onPayload?: (data: GraphData) => void
  selectedNodeId?: string | null
  focusClusterId?: number | null
  sizeScale?: number
  toponymScale?: number
  toponymWordCount?: number
}

export function GraphMapView({
  endpoint,
  params,
  nodeKind,
  onOpenNode,
  hoverCard,
  haloIds,
  toolbarExtras,
  legendExtras,
  height = 560,
  onPayload,
  selectedNodeId,
  focusClusterId,
  sizeScale = 1,
  toponymScale = 1,
  toponymWordCount = 3,
}: GraphMapViewProps) {
  const [showEdges, setShowEdges] = useState(false)
  const [showToponyms, setShowToponyms] = useState(true)
  // Colour restores the old color-by knob on the shared stack: cluster hues,
  // a year ramp, a rating ramp, or the 50-J heat wash. DATA ramps (year /
  // rating / heat) are deliberate exceptions to the chip valence contract.
  const [colourMode, setColourMode] = useState<'clusters' | 'year' | 'rating' | 'heat'>('clusters')
  // Per-layer link chips (the old typed-edge toggles): view-only filters.
  const [hiddenEdgeTypes, setHiddenEdgeTypes] = useState<Set<string>>(new Set())
  const [dimmedClusters, setDimmedClusters] = useState<Set<number>>(new Set())
  const [search, setSearch] = useState('')

  const qs = new URLSearchParams(params).toString()
  const { data: raw, isLoading } = useQuery<GraphData & { status?: string }>({
    queryKey: ['graph', endpoint, params],
    queryFn: () => api.get<GraphData & { status?: string }>(`/graphs/${endpoint}?${qs}`),
    staleTime: 60_000,
    // /graphs/* never computes in-request: poll while a background build runs.
    refetchInterval: (q) => (q.state.data?.status === 'building' ? 2500 : false),
  })
  const building = raw?.status === 'building'
  const data = building ? undefined : raw
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

  const yearRange = useMemo(() => {
    let lo = Infinity
    let hi = -Infinity
    for (const n of nodes) {
      const y = Number(n.metadata?.year)
      if (Number.isFinite(y) && y > 1800) {
        lo = Math.min(lo, y)
        hi = Math.max(hi, y)
      }
    }
    return lo <= hi ? { lo, hi } : null
  }, [nodes])

  const dataColour = (n: GraphNode): string | undefined => {
    if (colourMode === 'year' && yearRange) {
      const y = Number(n.metadata?.year)
      if (!Number.isFinite(y) || y < 1800) return MAP_INK.ambientSoft
      const t = (y - yearRange.lo) / Math.max(1, yearRange.hi - yearRange.lo)
      // Older = receding slate, newer = folio — recency reads as presence.
      const mix = (a: number, b: number) => Math.round(a + (b - a) * t)
      return `rgb(${mix(203, 47)}, ${mix(213, 128)}, ${mix(225, 196)})`
    }
    if (colourMode === 'rating') {
      const r = Number(n.metadata?.rating)
      if (!Number.isFinite(r) || r <= 0) return MAP_INK.ambientSoft
      const t = Math.max(-1, Math.min(1, (r - 3) / 2))
      const mix = (a: number, b: number, k: number) => Math.round(a + (b - a) * k)
      return t < 0
        ? `rgb(${mix(220, 233, 1 + t)}, ${mix(68, 196, 1 + t)}, ${mix(61, 76, 1 + t)})`
        : `rgb(${mix(233, 64, t)}, ${mix(196, 160, t)}, ${mix(76, 92, t)})`
    }
    return undefined
  }

  // 50-J heat: valence per node — your own signals where present (rating),
  // membership as a mild positive, tracked-neutral otherwise. View-only.
  const heatValues = useMemo(() => {
    if (colourMode !== 'heat') return undefined
    const m = new Map<string, number>()
    for (const n of nodes) {
      const r = Number(n.metadata?.rating)
      if (Number.isFinite(r) && r > 0) m.set(n.id, Math.max(-1, Math.min(1, (r - 3) / 2)))
      else if (n.in_library !== false) m.set(n.id, 0.35)
      else m.set(n.id, 0)
    }
    return m
  }, [colourMode, nodes])

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
            colourMode === 'year' || colourMode === 'rating'
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
          halo: haloIds?.has(n.id) ?? false,
        }
      }),
    [nodes, clusterColors, dimmedClusters, query, nodeKind, haloIds, focusClusterId, colourMode, yearRange],
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

  const layout = (data?.metadata as Record<string, unknown> | undefined)?.layout as
    | { computed_at?: string; new_vectors_since_build?: number }
    | undefined

  if (isLoading || building) {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
        {building ? 'Building this view in the background — it appears when ready…' : 'Loading the map…'}
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      <MapToolbar>
        {toolbarExtras}
        <MapToggle
          active={showEdges}
          onClick={() => setShowEdges((s) => !s)}
          title="Draw the typed links (semantic, shared references, shared authors, cited together)"
        >
          <Share2 className="h-3.5 w-3.5" />
          Links{showEdges && data ? ` · ${data.edges.length}` : ''}
        </MapToggle>
        <MapToggle
          active={showToponyms}
          onClick={() => setShowToponyms((s) => !s)}
          title="Cluster names on the map"
        >
          <Type className="h-3.5 w-3.5" />
          Names
        </MapToggle>
        <MapModeSwitch
          value={colourMode}
          onChange={setColourMode}
          options={[
            { value: 'clusters', label: 'Clusters', title: 'Colour by corpus cluster' },
            { value: 'year', label: 'Year', title: 'Recency ramp — older fades, newer leads' },
            { value: 'rating', label: 'Rating', title: 'Your ratings — red to green, unrated grey' },
            { value: 'heat', label: 'Heat', title: 'Local signal wash — red negative, green positive (view-only)' },
          ]}
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
        showEdges={showEdges}
        showToponyms={showToponyms}
        height={height}
        sizeScale={sizeScale}
        toponymScale={toponymScale}
        toponymWordCount={toponymWordCount}
        heatValues={heatValues}
        selectedIds={selectedNodeId ? new Set([selectedNodeId]) : undefined}
        renderHover={(id) => {
          const n = nodesById.get(id)
          return n ? hoverCard(n) : null
        }}
        onClickNode={(id) => {
          const n = nodesById.get(id)
          if (n) onOpenNode?.(n)
        }}
        className="rounded-none border-0"
      >
      </SemanticMap>

      <MapLegend>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: MAP_INK.library }} />
            In your library — filled
          </span>
          <span className="text-slate-400">{nodes.length} on the map</span>
          {layout?.computed_at && (
            <span className="text-slate-400">
              layout {String(layout.computed_at).slice(0, 10)}
              {layout.new_vectors_since_build
                ? ` · ${layout.new_vectors_since_build} new since (placed live, folded in on the next refresh)`
                : ''}
            </span>
          )}
          {legendExtras}
        </div>
        {showEdges && data && data.edges.length > 0 && (
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
