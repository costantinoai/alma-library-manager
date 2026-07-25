/**
 * SemanticMap — the ONE scatter-map renderer (task 50 M2).
 *
 * Every map host (Discovery frontier panel, the Map page, Authors network
 * scatter) draws through this component, so visuals, semantics, and
 * interactions can never fork per surface:
 *
 *   - node meaning comes from the `mapNodeStyle` registry (50-E) — hosts pass
 *     a `kind` + optional grouping colour, never raw styles;
 *   - all map text goes through the collision-free `labelLayout` pass (50-H):
 *     cluster names render as TOPONYMS — letterspaced uppercase place names,
 *     sized by cluster mass, dropped (never stacked) when ground runs out;
 *   - canvas, not SVG: 8k+ corpus dots pan/zoom smoothly (the old SVG
 *     frontier degraded past ~5k DOM nodes). Fixed substrate coordinates —
 *     no force simulation;
 *   - viewport culling + a uniform-grid hit index keep draw + hover O(visible);
 *   - selection ring is ALWAYS the folio accent; the lasso is the accent too
 *     (accent = selected, per the control contract).
 *
 * The component owns rendering + spatial interaction only. Data fetching,
 * legends, control bars, hover cards, and region popovers belong to hosts —
 * they receive screen anchors through the callbacks and overlay HTML on top.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { cn } from '@/lib/utils'
import {
  placeLabels,
  suppressNearbyDuplicateWords,
  toponymTerms,
  type LabelInput,
} from './labelLayout'
import {
  DIMMED_OPACITY,
  HOLLOW_FILL_ALPHA,
  HOLLOW_STROKE_WIDTH,
  HOVER_RING,
  MAP_FIELD,
  MAP_INK,
  MAP_NODE_STYLES,
  SELECTION_RING,
  radiusFor,
  type MapNodeKind,
} from './mapNodeStyle'
import { fitViewport, useMapViewport, worldToScreen } from './useMapViewport'

export interface SemanticMapNode {
  id: string
  /** World coordinates in the substrate's unit square. */
  x: number
  y: number
  kind: MapNodeKind
  /** Grouping colour (branch / cluster hue). Registry default when absent. */
  color?: string
  /** Magnitude for the single size channel (citations, publications…). */
  sizeValue?: number | null
  clusterId?: number
  clusterLabel?: string
  /** Dimmed by an active host filter — drawn faint, never hidden. */
  dimmed?: boolean
  /** New-in-latest-set marker: a dashed halo in the node's own grouping
   *  colour — a temporal fact about the same node, never a colour change. */
  halo?: boolean
}

export interface SemanticMapEdge {
  source: string
  target: string
  weight?: number
  color?: string
}

export interface SemanticMapProps {
  nodes: SemanticMapNode[]
  edges?: SemanticMapEdge[]
  showEdges?: boolean
  /** Cluster toponyms on/off (50-H pass). */
  showToponyms?: boolean
  /** Word-size multiplier for toponyms (host knob). */
  toponymScale?: number
  /** Words per cluster (density knob, 1–3). */
  toponymWordCount?: number
  height?: number
  selectedIds?: ReadonlySet<string>
  onHover?: (id: string | null, anchor: { x: number; y: number } | null) => void
  /** Click on a dot fires its id; click on the BACKGROUND fires null — the
   *  host's deselect (clear cluster focus / inspector back to overview). */
  onClickNode?: (id: string | null) => void
  /** Hover card content for a node. Rendered BY the plate at the hover
   *  point (edge-flipped, pointer-transparent) so every host gets the same
   *  at-cursor behaviour — hosts supply only the words. */
  renderHover?: (id: string) => React.ReactNode
  /** Dot-size multiplier (host knob; 1 = registry default). */
  sizeScale?: number
  /** 50-J score heatmap: per-node valence in [-1, +1]. When present, a
   *  smoothed divergent wash (red → yellow → green) renders UNDER the dots —
   *  the local average of the values around each point. View-only; a DATA
   *  ramp (like Meter tones), deliberately outside the chip valence
   *  contract. Keyed to RENDERED nodes — use only when the map always shows
   *  every signal carrier (e.g. the author network). */
  heatValues?: ReadonlyMap<string, number>
  /** SPACE-OWNED heat field: valence points in WORLD coords, independent of
   *  which dots the current view renders (user call 2026-07-25: the stats of
   *  the space belong to the space — toggling layers must never change the
   *  terrain). Takes precedence over `heatValues`. Paper-map hosts feed this
   *  from the shared /graphs/signal-field endpoint. */
  heatField?: ReadonlyArray<{ x: number; y: number; v: number }>
  /** Rectangle-select mode: drag selects instead of panning. */
  lassoMode?: boolean
  onLasso?: (ids: string[], anchor: { x: number; y: number }) => void
  /** Host HTML overlays (hover card, region popover) — absolutely positioned. */
  children?: React.ReactNode
  className?: string
}

const NODE_HIT_RADIUS = 9
const GRID_CELL = 36
// Edge-drawing budget (user call 2026-07-25): links are NEVER drawn beyond
// this count — a 500+ library's full edge set melts the frame budget. Below
// the small-graph floor everything draws; above it, links appear only for a
// SUB-SELECTION (focused cluster / search / selection dimming) or once
// zoomed past the threshold, and even then the strongest ones first.
const MAX_DRAWN_EDGES = 1200
const SMALL_GRAPH_EDGE_FLOOR = 800
const EDGE_ZOOM_FACTOR = 1.5

/** Toponym type ramp: mass → font px. Small caps + tracking happen at draw. */
function toponymFontPx(count: number, maxCount: number): number {
  const t = maxCount > 1 ? Math.sqrt(count) / Math.sqrt(maxCount) : 1
  return Math.round(10 + t * 7)
}

export function SemanticMap({
  nodes,
  edges = [],
  showEdges = false,
  showToponyms = true,
  toponymScale = 1,
  toponymWordCount = 3,
  height = 560,
  selectedIds,
  sizeScale = 1,
  heatValues,
  heatField,
  onHover,
  onClickNode,
  renderHover,
  lassoMode = false,
  onLasso,
  children,
  className,
}: SemanticMapProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const [width, setWidth] = useState(800)
  const { viewport, fit, zoomAt, panStart, panMove, panEnd, isPanning } = useMapViewport(width, height)
  const [hoverId, setHoverId] = useState<string | null>(null)
  const [lassoRect, setLassoRect] = useState<{ x1: number; y1: number; x2: number; y2: number } | null>(null)
  const lassoRef = useRef<{ x1: number; y1: number } | null>(null)
  const movedRef = useRef(false)

  // Track the rendered width responsively.
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver((entries) => {
      const w = Math.round(entries[0]?.contentRect.width ?? 800)
      if (w > 0) setWidth(w)
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Refit when the node set identity changes materially (first data arrival).
  const hasNodes = nodes.length > 0
  useEffect(() => {
    fit()
  }, [fit, hasNodes])

  const byId = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes])
  // Strongest-first order, computed once per edge set — the draw loop stops
  // at the budget, so it must meet the best edges first.
  const sortedEdges = useMemo(
    () => [...edges].sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0)),
    [edges],
  )
  const maxSizeValue = useMemo(
    () => Math.max(1, ...nodes.map((n) => n.sizeValue ?? 0)),
    [nodes],
  )

  // ── Screen positions + spatial index (rebuilt per draw inputs) ───────────
  const screenPos = useMemo(() => {
    const pos = new Map<string, [number, number]>()
    for (const n of nodes) pos.set(n.id, worldToScreen(viewport, n.x, n.y))
    return pos
  }, [nodes, viewport])

  const grid = useMemo(() => {
    const g = new Map<string, string[]>()
    for (const [id, [sx, sy]] of screenPos) {
      if (sx < -GRID_CELL || sy < -GRID_CELL || sx > width + GRID_CELL || sy > height + GRID_CELL) continue
      const key = `${Math.floor(sx / GRID_CELL)}:${Math.floor(sy / GRID_CELL)}`
      const bucket = g.get(key)
      if (bucket) bucket.push(id)
      else g.set(key, [id])
    }
    return g
  }, [screenPos, width, height])

  const hitTest = useCallback(
    (sx: number, sy: number): string | null => {
      const cx = Math.floor(sx / GRID_CELL)
      const cy = Math.floor(sy / GRID_CELL)
      let best: string | null = null
      let bestD = NODE_HIT_RADIUS * NODE_HIT_RADIUS
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          for (const id of grid.get(`${cx + dx}:${cy + dy}`) ?? []) {
            const p = screenPos.get(id)
            if (!p) continue
            const d = (p[0] - sx) ** 2 + (p[1] - sy) ** 2
            if (d < bestD) {
              bestD = d
              best = id
            }
          }
        }
      }
      return best
    },
    [grid, screenPos],
  )

  // ── Toponyms: PER-TERM labels, placed where the mass sits (50-H) ─────────
  // Each cluster contributes its top terms as SEPARATE labels — never one
  // joined string. The cluster's points are ordered along their principal
  // axis and chunked, one chunk per term; each term lands at its chunk's
  // centroid, so words spread across the territory they describe. The
  // primary term carries the cluster's full priority + size; later terms
  // step down, so under collision the primaries win the ground.
  const toponyms = useMemo(() => {
    if (!showToponyms) return []
    const clusters = new Map<number, { label: string; pts: Array<[number, number]> }>()
    for (const n of nodes) {
      if (typeof n.clusterId !== 'number' || n.clusterId < 0 || !n.clusterLabel) continue
      const p = screenPos.get(n.id)
      if (!p) continue
      const c = clusters.get(n.clusterId)
      if (c) c.pts.push(p)
      else clusters.set(n.clusterId, { label: n.clusterLabel, pts: [p] })
    }
    const maxCount = Math.max(1, ...[...clusters.values()].map((c) => c.pts.length))
    const inputs: Array<LabelInput & { word: string }> = []
    const texts = new Map<string, { text: string; fontPx: number }>()
    for (const [cid, c] of clusters) {
      const terms = toponymTerms(c.label, toponymWordCount)
      if (terms.length === 0) continue
      // Principal axis of the cluster's screen points (2-D PCA angle).
      const n = c.pts.length
      const mx = c.pts.reduce((a, p) => a + p[0], 0) / n
      const my = c.pts.reduce((a, p) => a + p[1], 0) / n
      let sxx = 0
      let sxy = 0
      let syy = 0
      for (const [px, py] of c.pts) {
        sxx += (px - mx) ** 2
        sxy += (px - mx) * (py - my)
        syy += (py - my) ** 2
      }
      const angle = 0.5 * Math.atan2(2 * sxy, sxx - syy)
      const ux = Math.cos(angle)
      const uy = Math.sin(angle)
      const ordered = [...c.pts].sort(
        (a, b) => (a[0] - mx) * ux + (a[1] - my) * uy - ((b[0] - mx) * ux + (b[1] - my) * uy),
      )
      // One contiguous chunk of the cluster per term; tiny clusters keep
      // only as many terms as they have points to anchor.
      const termCount = Math.min(terms.length, Math.max(1, Math.floor(n / 2)))
      const chunk = Math.ceil(ordered.length / termCount)
      const basePx = Math.round(toponymFontPx(n, maxCount) * toponymScale)
      for (let i = 0; i < termCount; i++) {
        const part = ordered.slice(i * chunk, (i + 1) * chunk)
        if (part.length === 0) continue
        const cx = part.reduce((a, p) => a + p[0], 0) / part.length
        const cy = part.reduce((a, p) => a + p[1], 0) / part.length
        const fontPx = Math.max(9, Math.round(basePx * (i === 0 ? 1 : i === 1 ? 0.85 : 0.75)))
        const text = terms[i].toUpperCase()
        const w = text.length * fontPx * 0.68 + text.length * 1.5
        const id = `c${cid}:t${i}`
        inputs.push({
          id,
          x: cx,
          y: cy,
          width: w,
          height: fontPx + 4,
          priority: n * (1 - 0.2 * i),
          word: terms[i],
        })
        texts.set(id, { text, fontPx })
      }
    }
    // Same word repeated by neighbouring clusters ("face face face") says
    // nothing new — within a quarter-plate radius only the strongest
    // instance keeps its ground; far apart, both survive.
    const deduped = suppressNearbyDuplicateWords(inputs, Math.min(width, height) * 0.25)
    const placed = placeLabels(deduped, width, height)
    return placed.map((p) => ({ ...p, ...texts.get(p.id)! }))
  }, [nodes, screenPos, showToponyms, toponymScale, toponymWordCount, width, height])

  // ── Draw ──────────────────────────────────────────────────────────────────
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const dpr = window.devicePixelRatio || 1
    canvas.width = width * dpr
    canvas.height = height * dpr
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    ctx.scale(dpr, dpr)

    // The cool field under warm paper — the cartographic inversion.
    ctx.fillStyle = MAP_FIELD.background
    ctx.fillRect(0, 0, width, height)

    const visible = (sx: number, sy: number) =>
      sx >= -20 && sy >= -20 && sx <= width + 20 && sy <= height + 20

    // 50-J heatmap — gaussian splats on a coarse offscreen grid, colorized
    // through a divergent red→yellow→green ramp, composited UNDER everything.
    // Cheap: grid is 1/8 resolution; weights and values accumulate per cell,
    // colour = local MEAN valence, alpha = local density (so empty sea stays
    // paper, dense strong regions read loudest).
    //
    // Source points: a SPACE-OWNED `heatField` (world-coord valences covering
    // every signal carrier, whether or not its dot is rendered) when the host
    // supplies one; otherwise per-rendered-node `heatValues`. The field keeps
    // the terrain invariant across layer toggles — hiding dots never hides
    // their preferences.
    const heatPoints: Array<[number, number, number]> = []
    if (heatField && heatField.length > 0) {
      for (const p of heatField) {
        const [sx, sy] = worldToScreen(viewport, p.x, p.y)
        if (visible(sx, sy)) heatPoints.push([sx, sy, p.v])
      }
    } else if (heatValues && heatValues.size > 0) {
      for (const [id, value] of heatValues) {
        const p = screenPos.get(id)
        if (p && visible(p[0], p[1])) heatPoints.push([p[0], p[1], value])
      }
    }
    if (heatPoints.length > 0) {
      const cell = 8
      const gw = Math.max(1, Math.ceil(width / cell))
      const gh = Math.max(1, Math.ceil(height / cell))
      const wsum = new Float32Array(gw * gh)
      const vsum = new Float32Array(gw * gh)
      const radius = 5 // grid cells (~40 px)
      for (const [px, py, value] of heatPoints) {
        const gx = px / cell
        const gy = py / cell
        const x0 = Math.max(0, Math.floor(gx - radius))
        const x1 = Math.min(gw - 1, Math.ceil(gx + radius))
        const y0 = Math.max(0, Math.floor(gy - radius))
        const y1 = Math.min(gh - 1, Math.ceil(gy + radius))
        for (let yy = y0; yy <= y1; yy++) {
          for (let xx = x0; xx <= x1; xx++) {
            const d2 = (xx - gx) ** 2 + (yy - gy) ** 2
            const wgt = Math.exp(-d2 / (2 * (radius / 2) ** 2))
            if (wgt < 0.01) continue
            const idx = yy * gw + xx
            wsum[idx] += wgt
            vsum[idx] += wgt * value
          }
        }
      }
      const heatCanvas = document.createElement('canvas')
      heatCanvas.width = gw
      heatCanvas.height = gh
      const hctx = heatCanvas.getContext('2d')
      if (hctx) {
        const img = hctx.createImageData(gw, gh)
        let maxW = 0
        for (let i = 0; i < wsum.length; i++) if (wsum[i] > maxW) maxW = wsum[i]
        // TWO-SLOPE normalisation (matplotlib TwoSlopeNorm): zero is ALWAYS
        // yellow (neutral must mean neutral), but each side stretches
        // independently to its own observed range, at the 98th percentile
        // so one extreme pocket can't flatten its side. Needed because the
        // full-coverage field carries a lot of neutral mass — a single
        // symmetric ±max scale left every local mean near zero and the
        // whole plate yellow (user call 2026-07-25).
        const negMeans: number[] = []
        const posMeans: number[] = []
        for (let i = 0; i < wsum.length; i++) {
          if (wsum[i] <= 0.02) continue
          const m = vsum[i] / wsum[i]
          if (m < 0) negMeans.push(-m)
          else if (m > 0) posMeans.push(m)
        }
        const p98 = (vs: number[]) => {
          if (vs.length === 0) return 1
          vs.sort((a, b) => a - b)
          return Math.max(1e-6, vs[Math.min(vs.length - 1, Math.floor(vs.length * 0.98))])
        }
        const negMax = p98(negMeans)
        const posMax = p98(posMeans)
        for (let i = 0; i < wsum.length; i++) {
          if (wsum[i] <= 0.02) continue
          const mean = vsum[i] / wsum[i]
          // Divergent ramp: -1 red (220,68,61) → 0 yellow (233,196,76) → +1 green (64,160,92)
          const t = Math.max(-1, Math.min(1, mean < 0 ? mean / negMax : mean / posMax))
          const [r, g, b] =
            t < 0
              ? [220 + (233 - 220) * (1 + t), 68 + (196 - 68) * (1 + t), 61 + (76 - 61) * (1 + t)]
              : [233 + (64 - 233) * t, 196 + (160 - 196) * t, 76 + (92 - 76) * t]
          const alpha = Math.min(0.4, 0.12 + 0.5 * (wsum[i] / maxW)) * 255
          const o = i * 4
          img.data[o] = r
          img.data[o + 1] = g
          img.data[o + 2] = b
          img.data[o + 3] = alpha
        }
        hctx.putImageData(img, 0, 0)
        ctx.imageSmoothingEnabled = true
        ctx.drawImage(heatCanvas, 0, 0, gw, gh, 0, 0, width, height)
      }
    }

    // Edges first, under everything — BUDGETED (never more than
    // MAX_DRAWN_EDGES). Draw when the graph is small, a sub-selection is
    // active (some nodes dimmed → links of the visible subset), or the view
    // is zoomed in; strongest links win the budget.
    let edgesDrawn = 0
    let edgesEligible = false
    if (showEdges && sortedEdges.length > 0) {
      const anyDimming = nodes.some((n) => n.dimmed)
      const zoomedIn = viewport.scale > fitViewport(width, height).scale * EDGE_ZOOM_FACTOR
      edgesEligible = anyDimming || zoomedIn || sortedEdges.length <= SMALL_GRAPH_EDGE_FLOOR
      if (edgesEligible) {
        ctx.lineWidth = 0.6
        for (const e of sortedEdges) {
          if (edgesDrawn >= MAX_DRAWN_EDGES) break
          const na = byId.get(e.source)
          const nb = byId.get(e.target)
          if (anyDimming && (na?.dimmed || nb?.dimmed)) continue
          const a = screenPos.get(e.source)
          const b = screenPos.get(e.target)
          if (!a || !b) continue
          if (!visible(a[0], a[1]) && !visible(b[0], b[1])) continue
          ctx.strokeStyle = e.color ?? MAP_FIELD.edgeLine
          ctx.globalAlpha = 0.25 + 0.45 * (e.weight ?? 0.5)
          ctx.beginPath()
          ctx.moveTo(a[0], a[1])
          ctx.lineTo(b[0], b[1])
          ctx.stroke()
          edgesDrawn += 1
        }
        ctx.globalAlpha = 1
      }
    }
    if (showEdges && sortedEdges.length > 0 && edgesDrawn === 0) {
      // Honest hint instead of a silently empty layer.
      ctx.font = '11px ui-sans-serif, system-ui, sans-serif'
      ctx.fillStyle = MAP_INK.ambient
      ctx.textBaseline = 'bottom'
      ctx.fillText(
        edgesEligible
          ? 'No links in the current focus'
          : 'Zoom in — or focus a cluster / search — to draw links',
        10,
        height - 8,
      )
    }

    // Nodes by layer weight: ambient first, hero last (draw order = z order).
    const drawOrder: MapNodeKind[] = ['seen', 'corpus', 'author', 'library', 'suggestion']
    for (const kind of drawOrder) {
      const style = MAP_NODE_STYLES[kind]
      for (const n of nodes) {
        if (n.kind !== kind) continue
        const p = screenPos.get(n.id)
        if (!p || !visible(p[0], p[1])) continue
        const r = radiusFor(kind, n.sizeValue ?? null, maxSizeValue) * sizeScale
        const color = n.color ?? style.defaultColor
        ctx.globalAlpha = n.dimmed ? DIMMED_OPACITY : style.opacity
        if (n.halo) {
          // "New this refresh": dashed halo in the grouping colour.
          ctx.beginPath()
          ctx.arc(p[0], p[1], r + 3.5, 0, Math.PI * 2)
          ctx.lineWidth = 1
          ctx.strokeStyle = color
          ctx.setLineDash([2, 2])
          ctx.stroke()
          ctx.setLineDash([])
        }
        ctx.beginPath()
        ctx.arc(p[0], p[1], r, 0, Math.PI * 2)
        if (style.filled) {
          ctx.fillStyle = color
          ctx.fill()
        } else {
          // Hollow = not yours yet (50-E): ring dot with a faint wash of its
          // own hue inside — a dot, not a hole, beside the filled points.
          ctx.fillStyle = MAP_FIELD.background
          ctx.fill()
          const wash = ctx.globalAlpha
          ctx.globalAlpha = wash * HOLLOW_FILL_ALPHA
          ctx.fillStyle = color
          ctx.fill()
          ctx.globalAlpha = wash
          ctx.lineWidth = HOLLOW_STROKE_WIDTH
          ctx.strokeStyle = color
          ctx.stroke()
        }
      }
    }
    ctx.globalAlpha = 1

    // Transient rings: hover (ink) under selection (accent — always accent).
    const ring = (id: string, spec: { color: string; width: number }, pad: number) => {
      const n = byId.get(id)
      const p = screenPos.get(id)
      if (!n || !p) return
      const r = radiusFor(n.kind, n.sizeValue ?? null, maxSizeValue) * sizeScale
      ctx.beginPath()
      ctx.arc(p[0], p[1], r + pad, 0, Math.PI * 2)
      ctx.lineWidth = spec.width
      ctx.strokeStyle = spec.color
      ctx.stroke()
    }
    if (hoverId && (!selectedIds || !selectedIds.has(hoverId))) ring(hoverId, HOVER_RING, 2.5)
    for (const id of selectedIds ?? []) ring(id, SELECTION_RING, 3)

    // Toponyms — the atlas plate. Halo first, then tracked uppercase ink.
    for (const t of toponyms) {
      ctx.font = `600 ${t.fontPx}px ui-sans-serif, system-ui, sans-serif`
      const letterSpacing = `${Math.max(1, Math.round(t.fontPx / 8))}px`
      // letterSpacing is supported in all evergreen canvases; harmless if not.
      ;(ctx as CanvasRenderingContext2D & { letterSpacing?: string }).letterSpacing = letterSpacing
      ctx.textBaseline = 'top'
      ctx.lineWidth = 3
      ctx.strokeStyle = MAP_INK.toponymHalo
      ctx.strokeText(t.text, t.left, t.top)
      ctx.fillStyle = MAP_INK.toponym
      ctx.fillText(t.text, t.left, t.top)
      ;(ctx as CanvasRenderingContext2D & { letterSpacing?: string }).letterSpacing = '0px'
    }

    // Active lasso rectangle — accent, like every selection.
    if (lassoRect) {
      const { x1, y1, x2, y2 } = lassoRect
      ctx.strokeStyle = SELECTION_RING.color
      ctx.lineWidth = 1.5
      ctx.setLineDash([5, 4])
      ctx.strokeRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1))
      ctx.setLineDash([])
      ctx.fillStyle = 'rgba(47, 128, 196, 0.08)'
      ctx.fillRect(Math.min(x1, x2), Math.min(y1, y2), Math.abs(x2 - x1), Math.abs(y2 - y1))
    }
  }, [
    nodes,
    sortedEdges,
    showEdges,
    byId,
    screenPos,
    toponyms,
    hoverId,
    selectedIds,
    lassoRect,
    viewport,
    heatValues,
    heatField,
    width,
    height,
    maxSizeValue,
    sizeScale,
  ])

  // ── Pointer interactions ──────────────────────────────────────────────────
  const localPoint = (e: React.PointerEvent): [number, number] => {
    const rect = canvasRef.current!.getBoundingClientRect()
    return [e.clientX - rect.left, e.clientY - rect.top]
  }

  const handlePointerDown = (e: React.PointerEvent) => {
    const [sx, sy] = localPoint(e)
    movedRef.current = false
    ;(e.target as HTMLElement).setPointerCapture(e.pointerId)
    if (lassoMode) {
      lassoRef.current = { x1: sx, y1: sy }
      setLassoRect({ x1: sx, y1: sy, x2: sx, y2: sy })
    } else {
      panStart(sx, sy, viewport)
    }
  }

  const handlePointerMove = (e: React.PointerEvent) => {
    const [sx, sy] = localPoint(e)
    if (lassoRef.current) {
      movedRef.current = true
      setLassoRect({ ...lassoRef.current, x2: sx, y2: sy })
      return
    }
    if (isPanning()) {
      movedRef.current = true
      panMove(sx, sy)
      return
    }
    const id = hitTest(sx, sy)
    if (id !== hoverId) {
      setHoverId(id)
      const p = id ? screenPos.get(id) : null
      onHover?.(id, p ? { x: p[0], y: p[1] } : null)
    }
  }

  const handlePointerUp = (e: React.PointerEvent) => {
    const [sx, sy] = localPoint(e)
    if (lassoRef.current) {
      const r = { ...lassoRef.current, x2: sx, y2: sy }
      lassoRef.current = null
      setLassoRect(null)
      const left = Math.min(r.x1, r.x2)
      const right = Math.max(r.x1, r.x2)
      const top = Math.min(r.y1, r.y2)
      const bottom = Math.max(r.y1, r.y2)
      if (right - left > 6 && bottom - top > 6) {
        const ids: string[] = []
        for (const [id, [px, py]] of screenPos) {
          if (px >= left && px <= right && py >= top && py <= bottom) ids.push(id)
        }
        if (ids.length > 0) onLasso?.(ids, { x: right, y: top })
      }
      return
    }
    panEnd()
    if (!movedRef.current) {
      // null = background click — hosts treat it as deselect.
      onClickNode?.(hitTest(sx, sy))
    }
  }

  // Wheel zoom must never scroll the page. React's synthetic onWheel is
  // registered PASSIVE, so preventDefault inside it is silently ignored (the
  // "zooms but also moves the page" bug) — attach a native non-passive
  // listener instead.
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const onWheel = (e: WheelEvent) => {
      e.preventDefault()
      const rect = canvas.getBoundingClientRect()
      zoomAt(e.clientX - rect.left, e.clientY - rect.top, e.deltaY < 0 ? 1.18 : 1 / 1.18)
    }
    canvas.addEventListener('wheel', onWheel, { passive: false })
    return () => canvas.removeEventListener('wheel', onWheel)
  }, [zoomAt])

  return (
    <div
      ref={containerRef}
      className={cn('relative w-full overflow-hidden rounded-lg border border-edge-1', className)}
      style={{ height }}
    >
      <canvas
        ref={canvasRef}
        role="img"
        aria-label={`Semantic map with ${nodes.length} papers`}
        style={{ width, height, cursor: lassoMode ? 'crosshair' : isPanning() ? 'grabbing' : 'grab', touchAction: 'none', display: 'block' }}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerLeave={() => {
          setHoverId(null)
          onHover?.(null, null)
        }}
      />
      {/* At-cursor hover card — positioned at the dot, flipped away from the
          nearest edges so it never clips. Hosts only supply the content. */}
      {renderHover && hoverId && (() => {
        const p = screenPos.get(hoverId)
        if (!p) return null
        const flipX = p[0] > width - 300
        const flipY = p[1] > height - 150
        return (
          <div
            className="pointer-events-none absolute z-20 w-max max-w-[18rem] rounded-sm border border-[var(--color-border)] bg-surface-3 px-3 py-2 text-xs shadow-paper-md"
            style={{
              left: p[0] + (flipX ? -14 : 14),
              top: p[1] + (flipY ? -14 : 14),
              transform: `translate(${flipX ? '-100%' : '0'}, ${flipY ? '-100%' : '0'})`,
            }}
          >
            {renderHover(hoverId)}
          </div>
        )
      })()}

      {/* Fit-to-view — the one navigation affordance the canvas itself owns. */}
      <button
        type="button"
        onClick={fit}
        className="absolute bottom-2 right-2 rounded-md border border-control-edge bg-control-quiet px-2 py-1 text-[11px] text-slate-600 hover:bg-control-quiet-hover"
        title="Fit map to view"
      >
        Fit
      </button>
      {children}
    </div>
  )
}
