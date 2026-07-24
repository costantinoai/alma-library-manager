import { useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Eye, EyeOff, Loader2, Maximize2, Share2 } from 'lucide-react'

import { getFrontier, type FrontierNode } from '@/api/client'
import { LAYER_COLORS, LAYER_FALLBACK_COLOR } from '@/components/graphs/graphConfig'
import { FRONTIER_MAP, branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'

const VB_W = 1000
const VB_H = 620
const PAD = 40

interface FrontierMapProps {
  lensId: string | null
  /** Open the shared PaperDetailPanel for a paper (same as the list views). */
  onSelectPaper: (paperId: string) => void
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

export function FrontierMap({ lensId, onSelectPaper }: FrontierMapProps) {
  const [showSeen, setShowSeen] = useState(false)
  const [showEdges, setShowEdges] = useState(false)
  const [highlightBranch, setHighlightBranch] = useState<string | null>(null)
  const [hover, setHover] = useState<{ node: Placed; branchColor?: string } | null>(null)
  // Pan/zoom transform.
  const [view, setView] = useState({ tx: 0, ty: 0, scale: 1 })
  const dragRef = useRef<{ x: number; y: number; moved: boolean } | null>(null)

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
  const onPointerDown = (e: React.PointerEvent) => {
    dragRef.current = { x: e.clientX, y: e.clientY, moved: false }
    ;(e.currentTarget as SVGSVGElement).setPointerCapture(e.pointerId)
  }
  const onPointerMove = (e: React.PointerEvent) => {
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
    dragRef.current = null
  }
  const resetView = () => setView({ tx: 0, ty: 0, scale: 1 })

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
        className="h-[520px] w-full cursor-grab touch-none select-none active:cursor-grabbing"
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
                fill={FRONTIER_MAP.library}
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
              const bc = n.branch_id ? branchColors.get(n.branch_id) : undefined
              const color = bc?.color ?? branchMapColor(0)
              const dim = highlightBranch != null && n.branch_id !== highlightBranch
              const r = 4 + Math.max(0, Math.min(1, (n.score ?? 0) / 100)) * 4
              return (
                <circle
                  key={n.paper_id}
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
              )
            })}
          </g>
        </g>
      </svg>

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
              {counts ? `showing ${counts.seen_shown} nearest of ${counts.seen_total} seen` : 'seen'}
            </span>
          )}
        </div>
        {branchColors.size > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
            {[...branchColors.entries()].map(([id, b]) => (
              <button
                key={id}
                type="button"
                onClick={() => setHighlightBranch((h) => (h === id ? null : id))}
                className={cn(
                  'inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 transition-colors',
                  highlightBranch === id
                    ? 'border-transparent text-white'
                    : 'border-[var(--color-border)] bg-surface-1 text-slate-600 hover:bg-surface-3',
                )}
                style={highlightBranch === id ? { background: b.color } : undefined}
                title={`Highlight the "${b.label}" branch`}
              >
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: b.color }} />
                {b.label} · {b.count}
              </button>
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
