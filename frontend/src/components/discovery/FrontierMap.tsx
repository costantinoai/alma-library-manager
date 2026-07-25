import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronUp,
  Eye,
  EyeOff,
  LassoSelect,
  Loader2,
  Maximize2,
  Share2,
  Sparkles,
  X,
} from 'lucide-react'

import { describeRegion, getFrontier, type FrontierNode, type Lens, type RegionDescription } from '@/api/client'
import { useBranchControls } from '@/hooks/useBranchControls'
import { LAYER_COLORS, LAYER_FALLBACK_COLOR } from '@/components/graphs/graphConfig'
import { StatusBadge } from '@/components/ui/status-badge'
import { FRONTIER_MAP, branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'

const VB_W = 1000
const VB_H = 620
const PAD = 40

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
}

/** A pending region selection: the ids under the lasso + its describe payload. */
interface RegionSelection {
  ids: string[]
  anchor: { x: number; y: number }
  description?: RegionDescription
}

interface Placed extends FrontierNode {
  px: number
  py: number
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

export function FrontierMap({ lensId, lens, onSelectPaper, onAdoptDirection }: FrontierMapProps) {
  const [showSeen, setShowSeen] = useState(false)
  const [showEdges, setShowEdges] = useState(false)
  const [highlightBranch, setHighlightBranch] = useState<string | null>(null)
  const [hover, setHover] = useState<{ node: Placed; branchColor?: string } | null>(null)
  // Pan/zoom transform.
  const [view, setView] = useState({ tx: 0, ty: 0, scale: 1 })
  const dragRef = useRef<{ x: number; y: number; moved: boolean } | null>(null)
  // Region selection: in select mode a drag draws a rectangle (viewBox coords)
  // instead of panning; on release the papers inside become a candidate
  // Direction. `region` holds the pending selection + its describe payload.
  // 47-H: ONE grouping at a time. Branch colouring is the frontier's default
  // (the recs are its hero layer); corpus clusters are the alternative lens on
  // the same points. Never both — two colourings on one scatter is a lie about
  // which structure you're looking at.
  const [groupBy, setGroupBy] = useState<'branches' | 'clusters'>('branches')
  const [selectMode, setSelectMode] = useState(false)
  const [selRect, setSelRect] = useState<{ x1: number; y1: number; x2: number; y2: number } | null>(null)
  const selRef = useRef<{ x1: number; y1: number } | null>(null)
  const [region, setRegion] = useState<RegionSelection | null>(null)

  const query = useQuery({
    queryKey: ['frontier', lensId, showSeen, showEdges],
    queryFn: () => getFrontier(lensId as string, showSeen ? 300 : 0, showEdges),
    enabled: !!lensId,
    // Poll while the corpus layout is still building.
    refetchInterval: (q) => (q.state.data?.status === 'building' ? 2500 : false),
    staleTime: 30_000,
  })

  const nodes = useMemo(() => query.data?.nodes ?? [], [query.data])
  const edges = useMemo(() => query.data?.edges ?? [], [query.data])
  const counts = query.data?.counts
  const branchColors = useBranchColors(nodes)
  const branchControls = useBranchControls(lens)

  // Corpus clusters present on the map, largest first, each with a stable hue.
  // Unclustered (-1) is deliberately excluded from the legend: it is the
  // absence of a group, not a group.
  const clusterColors = useMemo(() => {
    const tally = new Map<number, { label: string; count: number }>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      const row = tally.get(n.cluster_id)
      if (row) row.count += 1
      else tally.set(n.cluster_id, { label: n.cluster_label || `Cluster ${n.cluster_id}`, count: 1 })
    }
    const ordered = [...tally.entries()].sort((a, b) => b[1].count - a[1].count)
    return new Map(
      ordered.map(([id, v], i) => [id, { ...v, color: branchMapColor(i), index: i }]),
    )
  }, [nodes])

  // Recs that were NOT in the previous payload — the visible end of the
  // adopt-a-direction loop. Tracked across refetches for this session only;
  // an empty previous set (first load) marks nothing, so a cold open is calm.
  const prevRecIds = useRef<Set<string> | null>(null)
  const [newRecIds, setNewRecIds] = useState<Set<string>>(new Set())
  useEffect(() => {
    if (query.data?.status !== 'ready') return
    const current = new Set(nodes.filter((n) => n.layer === 'rec').map((n) => n.paper_id))
    const previous = prevRecIds.current
    if (previous && previous.size > 0) {
      const fresh = new Set([...current].filter((id) => !previous.has(id)))
      if (fresh.size > 0) setNewRecIds(fresh)
    }
    prevRecIds.current = current
  }, [nodes, query.data?.status])

  // Normalize raw layout coords → viewBox pixels (fit bounds).
  const placed = useMemo<Placed[]>(() => {
    if (nodes.length === 0) return []
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity
    for (const n of nodes) {
      minX = Math.min(minX, n.x); maxX = Math.max(maxX, n.x)
      minY = Math.min(minY, n.y); maxY = Math.max(maxY, n.y)
    }
    const spanX = maxX - minX || 1
    const spanY = maxY - minY || 1
    return nodes.map((n) => ({
      ...n,
      px: PAD + ((n.x - minX) / spanX) * (VB_W - 2 * PAD),
      // Flip Y so higher-y sits at the top, like a chart.
      py: PAD + (1 - (n.y - minY) / spanY) * (VB_H - 2 * PAD),
    }))
  }, [nodes])

  // Back-to-front so the hero (recs) sit on top of the terrain and frontier.
  const seenNodes = placed.filter((n) => n.layer === 'seen')
  const libraryNodes = placed.filter((n) => n.layer === 'library')
  const recNodes = placed.filter((n) => n.layer === 'rec')

  // Citation edges between placed nodes (drawn under the dots when the overlay
  // is on). Resolve endpoints to their pixel coords; drop any pointing at an
  // unplaced node (e.g. a rec with no corpus coords).
  const placedById = useMemo(() => {
    const m = new Map<string, Placed>()
    for (const n of placed) m.set(n.paper_id, n)
    return m
  }, [placed])
  const drawnEdges = useMemo(
    () =>
      edges
        .map((e) => ({ a: placedById.get(e.source), b: placedById.get(e.target), type: e.edge_type }))
        .filter((e): e is { a: Placed; b: Placed; type: (typeof edges)[number]['edge_type'] } =>
          !!e.a && !!e.b,
        ),
    [edges, placedById],
  )

  const onWheel = (e: React.WheelEvent) => {
    e.preventDefault()
    const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect()
    const mx = ((e.clientX - rect.left) / rect.width) * VB_W
    const my = ((e.clientY - rect.top) / rect.height) * VB_H
    setView((v) => {
      const next = Math.min(6, Math.max(0.5, v.scale * (e.deltaY < 0 ? 1.12 : 1 / 1.12)))
      // Keep the cursor point fixed while zooming.
      const k = next / v.scale
      return { scale: next, tx: mx - k * (mx - v.tx), ty: my - k * (my - v.ty) }
    })
  }
  const describeMutation = useMutation({
    mutationFn: (ids: string[]) => describeRegion(ids),
    onSuccess: (desc) => setRegion((r) => (r ? { ...r, description: desc } : r)),
  })

  // Pointer → viewBox coordinates (matches the wheel-zoom math).
  const toViewBox = (e: React.PointerEvent) => {
    const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect()
    return {
      x: ((e.clientX - rect.left) / rect.width) * VB_W,
      y: ((e.clientY - rect.top) / rect.height) * VB_H,
    }
  }

  const onPointerDown = (e: React.PointerEvent) => {
    ;(e.currentTarget as SVGSVGElement).setPointerCapture(e.pointerId)
    if (selectMode) {
      const p = toViewBox(e)
      selRef.current = { x1: p.x, y1: p.y }
      setSelRect({ x1: p.x, y1: p.y, x2: p.x, y2: p.y })
      setRegion(null)
      return
    }
    dragRef.current = { x: e.clientX, y: e.clientY, moved: false }
  }
  const onPointerMove = (e: React.PointerEvent) => {
    if (selectMode && selRef.current) {
      const p = toViewBox(e)
      setSelRect({ x1: selRef.current.x1, y1: selRef.current.y1, x2: p.x, y2: p.y })
      return
    }
    if (!dragRef.current) return
    const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect()
    const dx = ((e.clientX - dragRef.current.x) / rect.width) * VB_W
    const dy = ((e.clientY - dragRef.current.y) / rect.height) * VB_H
    if (Math.abs(e.clientX - dragRef.current.x) + Math.abs(e.clientY - dragRef.current.y) > 3) {
      dragRef.current.moved = true
    }
    dragRef.current.x = e.clientX
    dragRef.current.y = e.clientY
    setView((v) => ({ ...v, tx: v.tx + dx, ty: v.ty + dy }))
  }
  const onPointerUp = () => {
    if (selectMode && selRef.current && selRect) {
      const x1 = Math.min(selRect.x1, selRect.x2)
      const x2 = Math.max(selRect.x1, selRect.x2)
      const y1 = Math.min(selRect.y1, selRect.y2)
      const y2 = Math.max(selRect.y1, selRect.y2)
      selRef.current = null
      setSelRect(null)
      // Hit-test placed nodes against the rect in screen (post-transform) coords.
      if (x2 - x1 > 4 && y2 - y1 > 4) {
        const ids: string[] = []
        for (const n of placed) {
          const sx = view.tx + view.scale * n.px
          const sy = view.ty + view.scale * n.py
          if (sx >= x1 && sx <= x2 && sy >= y1 && sy <= y2) ids.push(n.paper_id)
        }
        if (ids.length) {
          setRegion({ ids, anchor: { x: x2, y: y2 } })
          describeMutation.mutate(ids.slice(0, 300))
        }
      }
      return
    }
    dragRef.current = null
  }
  const resetView = () => setView({ tx: 0, ty: 0, scale: 1 })

  const cancelRegion = () => {
    setRegion(null)
    setSelRect(null)
    selRef.current = null
  }
  const adoptRegion = () => {
    if (!region?.description || !region.description.sufficient) return
    onAdoptDirection?.({
      label: region.description.label,
      terms: region.description.top_terms,
      member_paper_ids: region.ids,
    })
    setRegion(null)
    setSelectMode(false)
  }

  const nodeClick = (n: Placed) => {
    if (dragRef.current?.moved) return
    onSelectPaper(n.paper_id)
  }

  if (!lensId) {
    return (
      <div className="flex h-[420px] items-center justify-center rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        Select a lens to plot its frontier.
      </div>
    )
  }
  if (query.isLoading || query.data?.status === 'building') {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
        {query.data?.status === 'building'
          ? 'Building the semantic layout — this runs once, then it’s cached…'
          : 'Loading the map…'}
      </div>
    )
  }

  return (
    <div className="relative overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      {/* Controls */}
      <div className="absolute left-3 top-3 z-10 flex items-center gap-2">
        <button
          type="button"
          onClick={() => setShowSeen((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showSeen
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-[var(--color-border)] bg-surface-2 text-slate-600 hover:bg-surface-3',
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
              : 'border-[var(--color-border)] bg-surface-2 text-slate-600 hover:bg-surface-3',
          )}
          title="Draw citation links (shared references + cited-together) between the papers on the map"
        >
          <Share2 className="h-3.5 w-3.5" />
          Citation links{showEdges && counts?.edges ? ` · ${counts.edges}` : ''}
        </button>
        {/* 47-H: one grouping at a time — this is a switch, not two toggles. */}
        {clusterColors.size > 0 && (
          <div className="inline-flex overflow-hidden rounded-sm border border-[var(--color-border)]">
            {(['branches', 'clusters'] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => {
                  setGroupBy(mode)
                  setHighlightBranch(null)
                }}
                className={cn(
                  'px-2 py-1 text-xs font-medium transition-colors',
                  groupBy === mode
                    ? 'bg-accent-soft text-alma-folio'
                    : 'bg-surface-2 text-slate-600 hover:bg-surface-3',
                )}
                title={
                  mode === 'branches'
                    ? 'Colour suggestions by the lens branch that found them'
                    : 'Colour every paper by its corpus cluster'
                }
              >
                {mode === 'branches' ? 'Branches' : 'Clusters'}
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
                : 'border-[var(--color-border)] bg-surface-2 text-slate-600 hover:bg-surface-3',
            )}
            title="Drag a box around a cluster of papers to name it and explore that direction"
          >
            <LassoSelect className="h-3.5 w-3.5" />
            Select a direction
          </button>
        )}
        <button
          type="button"
          onClick={resetView}
          className="inline-flex items-center gap-1.5 rounded-sm border border-[var(--color-border)] bg-surface-2 px-2 py-1 text-xs text-slate-600 hover:bg-surface-3"
          title="Reset view"
        >
          <Maximize2 className="h-3.5 w-3.5" />
        </button>
      </div>

      <svg
        viewBox={`0 0 ${VB_W} ${VB_H}`}
        preserveAspectRatio="xMidYMid meet"
        className={cn(
          'h-[520px] w-full touch-none select-none',
          selectMode ? 'cursor-crosshair' : 'cursor-grab active:cursor-grabbing',
        )}
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerLeave={onPointerUp}
        role="img"
        aria-label="Semantic frontier map of your library, suggestions, and seen papers"
      >
        <g transform={`translate(${view.tx} ${view.ty}) scale(${view.scale})`}>
          {/* Citation edges — drawn first so they sit UNDER the nodes. Coupling
              (shared references) + co-citation (cited together), colored by the
              same layer palette as the Analytics graph. */}
          {showEdges && (
            <g className="frontier-layer-edges">
              {drawnEdges.map((e, i) => (
                <line
                  key={i}
                  x1={e.a.px}
                  y1={e.a.py}
                  x2={e.b.px}
                  y2={e.b.py}
                  stroke={LAYER_COLORS[e.type] ?? LAYER_FALLBACK_COLOR}
                  strokeWidth={0.5}
                />
              ))}
            </g>
          )}
          {/* Seen — faint frontier (fades in last visually, drawn first) */}
          {showSeen && (
            <g className="frontier-layer-seen" style={{ opacity: 0.55 }}>
              {seenNodes.map((n) => (
                <circle
                  key={n.paper_id}
                  cx={n.px}
                  cy={n.py}
                  r={1.6}
                  fill={FRONTIER_MAP.seen}
                  onMouseEnter={() => setHover({ node: n })}
                  onMouseLeave={() => setHover(null)}
                  onClick={() => nodeClick(n)}
                  className="cursor-pointer"
                />
              ))}
            </g>
          )}
          {/* Library — the terrain */}
          <g className="frontier-layer-library">
            {libraryNodes.map((n) => (
              <circle
                key={n.paper_id}
                cx={n.px}
                cy={n.py}
                r={3}
                fill={
                  groupBy === 'clusters' && typeof n.cluster_id === 'number'
                    ? (clusterColors.get(n.cluster_id)?.color ?? FRONTIER_MAP.library)
                    : FRONTIER_MAP.library
                }
                fillOpacity={0.85}
                onMouseEnter={() => setHover({ node: n })}
                onMouseLeave={() => setHover(null)}
                onClick={() => nodeClick(n)}
                className="cursor-pointer"
              />
            ))}
          </g>
          {/* Recs — the hero layer, colored by branch, sized by score */}
          <g className="frontier-layer-rec">
            {recNodes.map((n) => {
              // Colour follows whichever grouping is active — never both.
              const bc = n.branch_id ? branchColors.get(n.branch_id) : undefined
              const cc =
                typeof n.cluster_id === 'number' ? clusterColors.get(n.cluster_id) : undefined
              const color =
                groupBy === 'clusters'
                  ? (cc?.color ?? FRONTIER_MAP.library)
                  : (bc?.color ?? branchMapColor(0))
              const dim =
                groupBy === 'branches' && highlightBranch != null && n.branch_id !== highlightBranch
              const isNew = newRecIds.has(n.paper_id)
              const r = 4 + Math.max(0, Math.min(1, (n.score ?? 0) / 100)) * 4
              return (
                <g key={n.paper_id}>
                  {/* A halo, not a different colour: "new" is a temporal fact
                      about the same node, so it must not fight the grouping. */}
                  {isNew && (
                    <circle
                      cx={n.px}
                      cy={n.py}
                      r={r + 3.5}
                      fill="none"
                      stroke={color}
                      strokeWidth={1}
                      strokeOpacity={dim ? 0.15 : 0.55}
                      strokeDasharray="2 2"
                    />
                  )}
                  <circle
                    cx={n.px}
                    cy={n.py}
                    r={r}
                    fill={color}
                    fillOpacity={dim ? 0.18 : 0.9}
                    stroke="var(--color-surface-0)"
                    strokeWidth={0.8}
                    onMouseEnter={() => setHover({ node: n, branchColor: color })}
                    onMouseLeave={() => setHover(null)}
                    onClick={() => nodeClick(n)}
                    className="cursor-pointer transition-opacity"
                  />
                </g>
              )
            })}
          </g>
        </g>
        {/* Selection rectangle — drawn in raw viewBox coords (outside the
            pan/zoom group) since selRect is captured in screen viewBox space. */}
        {selRect && (
          <rect
            x={Math.min(selRect.x1, selRect.x2)}
            y={Math.min(selRect.y1, selRect.y2)}
            width={Math.abs(selRect.x2 - selRect.x1)}
            height={Math.abs(selRect.y2 - selRect.y1)}
            fill="var(--color-accent-soft)"
            fillOpacity={0.25}
            stroke="var(--color-alma-folio)"
            strokeWidth={1}
            strokeDasharray="4 3"
          />
        )}
      </svg>

      {/* Region popover — the describe payload + adopt action. Appears when a
          selection has been made; meaning (label + terms + counts) is shown
          before the action, per 47 §8. */}
      {region && (
        <div className="absolute right-3 top-14 z-20 w-72 rounded-sm border border-[var(--color-border)] bg-surface-2 p-3 shadow-paper-lg">
          <div className="mb-2 flex items-start justify-between gap-2">
            <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
              <Sparkles className="h-3.5 w-3.5 text-alma-folio" />
              Direction
            </div>
            <button
              type="button"
              onClick={cancelRegion}
              className="rounded-sm p-0.5 text-slate-400 hover:bg-surface-3 hover:text-slate-600"
              aria-label="Cancel selection"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
          {describeMutation.isPending || !region.description ? (
            <div className="flex items-center gap-2 py-3 text-xs text-slate-500">
              <Loader2 className="h-4 w-4 animate-spin text-alma-folio" />
              Characterizing {region.ids.length} papers…
            </div>
          ) : !region.description.sufficient ? (
            <p className="py-2 text-xs text-slate-500">
              Too few papers to characterize — select a larger cluster (5+).
            </p>
          ) : (
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
                <button
                  type="button"
                  onClick={cancelRegion}
                  className="rounded-sm border border-[var(--color-border)] bg-surface-3 px-2.5 py-1.5 text-xs text-slate-600 hover:bg-surface-2"
                >
                  Cancel
                </button>
              </div>
            </>
          )}
        </div>
      )}

      {/* Hover tooltip */}
      {hover && (
        <div className="pointer-events-none absolute bottom-3 right-3 z-10 max-w-xs rounded-sm border border-[var(--color-border)] bg-surface-3 px-3 py-2 text-xs shadow-paper-md">
          <p className="line-clamp-2 font-medium text-alma-800">{hover.node.title || hover.node.paper_id}</p>
          <p className="mt-0.5 text-slate-500">
            {hover.node.layer === 'library' ? 'In your library' : hover.node.layer === 'rec' ? 'Suggestion' : 'Seen'}
            {hover.node.year ? ` · ${hover.node.year}` : ''}
            {hover.node.branch_label ? ` · ${hover.node.branch_label}` : ''}
          </p>
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-3 left-3 z-10 max-w-[60%] rounded-sm border border-[var(--color-border)] bg-surface-2/90 p-2.5 text-xs backdrop-blur-sm">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: FRONTIER_MAP.library }} />
            Library {counts ? `(${counts.library})` : ''}
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: branchMapColor(0) }} />
            Suggestions {counts ? `(${counts.recs})` : ''}
          </span>
          {showSeen && (
            <span className="inline-flex items-center gap-1.5 text-slate-400">
              <span className="inline-block h-1.5 w-1.5 rounded-full" style={{ background: FRONTIER_MAP.seen }} />
              {counts
                ? `showing ${counts.seen_shown} nearest of ${counts.seen_total} seen` +
                  (query.data?.seen_ranked_by === 'lens' ? ' (nearest to this lens)' : '')
                : 'seen'}
            </span>
          )}
        </div>
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
                      : 'border-[var(--color-border)] bg-surface-1 text-slate-600',
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

        {/* Cluster chips — identity only; corpus clusters aren't steerable. */}
        {groupBy === 'clusters' && clusterColors.size > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
            {[...clusterColors.entries()].slice(0, 8).map(([id, c]) => (
              <span
                key={id}
                className="inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-[var(--color-border)] bg-surface-1 px-1.5 py-0.5 text-slate-600"
                title={`${c.label} · ${c.count} papers`}
              >
                <span
                  className="inline-block h-2 w-2 shrink-0 rounded-full"
                  style={{ background: c.color }}
                />
                <span className="truncate">{c.label}</span>
                <span className="shrink-0 text-slate-400">· {c.count}</span>
              </span>
            ))}
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
