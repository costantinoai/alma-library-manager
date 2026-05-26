import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { Crosshair, Minus, Plus } from 'lucide-react'

import type { AuthorNeighbourhood } from '@/api/client'
import { Button } from '@/components/ui/button'

/**
 * 2D ego-network renderer — the author's neighbourhood as a navigable map on
 * ALMa's cool paper. Code-split into its own lazy chunk via the dialog, so it
 * loads only when opened.
 *
 * Colours are NOT hand-picked: the scene reads the design-system tokens
 * (`--color-accent`, `--color-success-500`, …) at runtime and feeds the
 * resolved values to the canvas, so the graph inherits the same palette as the
 * rest of the app. Node colour encodes the relation to the focal author; size
 * encodes tie strength; citation links carry flowing particles; every node is
 * named on-canvas. Drag = pan, scroll = zoom; clicking a node frames it, and
 * the scene auto-fits once the layout settles.
 */

interface GraphPalette {
  background: string
  surface: string
  edge: string
  ink: string
  center: string
  coauthor: string
  citation: string
  similar: string
}

// Layout spread. Charge is the inter-node repulsion (more negative = pushes
// nodes further apart); link distance is the rest length of each edge; link
// strength < 1 loosens the pull so the dense center-star + co-author mesh
// doesn't collapse into a blob. All in graph units while node radius is fixed
// (nodeRelSize), so raising them genuinely de-crowds rather than just zooming.
const CHARGE_STRENGTH = -1400
const LINK_DISTANCE = 150
const LINK_STRENGTH = 0.25

const RELATION_LABEL: Record<string, string> = {
  center: 'Focal author',
  coauthor: 'Co-author',
  citation: 'Citation',
  similar: 'Similar',
}

/** "rgb(r, g, b)" → "rgba(r, g, b, a)" so we can derive muted / faint tints
 *  from a resolved token without inventing a new colour. */
function withAlpha(rgb: string, alpha: number): string {
  const match = rgb.match(/rgba?\(([^)]+)\)/)
  if (!match) return rgb
  const [r, g, b] = match[1].split(',').map((s) => s.trim())
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function escapeHtml(value: string): string {
  return value.replace(
    /[&<>"]/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c] as string,
  )
}

/** Resolve design-system color tokens to concrete rgb() the canvas can paint.
 *  A hidden probe element forces the browser to resolve nested vars (e.g.
 *  `--color-accent: var(--color-alma-folio)`) and any colour space down to rgb
 *  — `getPropertyValue` alone would hand back the literal `var(…)`. */
function useGraphPalette(): GraphPalette {
  return useMemo(() => {
    const fallback: GraphPalette = {
      background: '#f0f9ff',
      surface: '#fffdf7',
      edge: '#d8d2c4',
      ink: '#1f2937',
      center: '#b0894e',
      coauthor: '#10b981',
      citation: '#0ea5e9',
      similar: '#caa64a',
    }
    if (typeof document === 'undefined') return fallback
    const probe = document.createElement('span')
    probe.style.display = 'none'
    document.body.appendChild(probe)
    const read = (token: string, fb: string): string => {
      probe.style.color = ''
      probe.style.color = `var(${token})`
      return getComputedStyle(probe).color || fb
    }
    const palette: GraphPalette = {
      // A lighter, cool-leaning stage (the cold end of ALMa's palette) so the
      // graph reads against the warm parchment chrome around it.
      background: read('--color-info-50', fallback.background),
      surface: read('--color-surface-3', fallback.surface),
      edge: read('--color-edge-3', fallback.edge),
      ink: getComputedStyle(document.body).color || fallback.ink,
      center: read('--color-accent', fallback.center),
      coauthor: read('--color-success-500', fallback.coauthor),
      citation: read('--color-info-500', fallback.citation),
      similar: read('--color-gold-500', fallback.similar),
    }
    probe.remove()
    return palette
  }, [])
}

interface Size {
  width: number
  height: number
}

/** Track the container's pixel size so the canvas fills it exactly. */
function useMeasuredSize(): [React.RefObject<HTMLDivElement | null>, Size] {
  const ref = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState<Size>({ width: 0, height: 0 })
  // useLayoutEffect + a synchronous first measure: the canvas must mount at the
  // container's real pixel size, else react-force-graph defaults to the whole
  // window and the graph renders off-centre with dead-feeling controls.
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const measure = () =>
      setSize({ width: Math.floor(el.clientWidth), height: Math.floor(el.clientHeight) })
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(el)
    return () => observer.disconnect()
  }, [])
  return [ref, size]
}

interface D3Force {
  strength?: (value: number) => unknown
  distance?: (value: number) => unknown
}

interface ForceGraphHandle {
  zoom: (k?: number, ms?: number) => number
  centerAt: (x?: number, y?: number, ms?: number) => void
  zoomToFit: (ms?: number, px?: number) => void
  d3Force: (name: string) => D3Force | undefined
  d3ReheatSimulation: () => void
}

export default function AuthorNeighbourhoodGraph({ data }: { data: AuthorNeighbourhood }) {
  const palette = useGraphPalette()
  const muted = useMemo(() => withAlpha(palette.ink, 0.7), [palette.ink])
  const relationColor = useMemo<Record<string, string>>(
    () => ({
      center: palette.center,
      coauthor: palette.coauthor,
      citation: palette.citation,
      similar: palette.similar,
    }),
    [palette],
  )

  // react-force-graph mutates the arrays it's given (adds x/y, resolves link
  // endpoints), so hand it a fresh clone rather than the cached query object.
  const graphData = useMemo(
    () => ({
      nodes: data.nodes.map((n) => ({ ...n })),
      links: data.links.map((l) => ({ ...l })),
    }),
    [data],
  )

  const fgRef = useRef<ForceGraphHandle | null>(null)
  const [containerRef, size] = useMeasuredSize()
  // The graph only mounts (and `fgRef` only attaches) once the container has
  // real pixel size — so the force config must (re)run when `ready` flips true,
  // not just on `data`, or it bails on the null ref and the layout stays at
  // d3's default -30 charge (the "single blob").
  const ready = size.width > 0 && size.height > 0

  // Frame the whole graph once the layout settles (first stop only, so we
  // don't yank the camera back after the user has navigated).
  const framedRef = useRef(false)
  useEffect(() => {
    framedRef.current = false
    const fg = fgRef.current
    if (!fg || !ready) return
    // Push nodes apart: strong charge repulsion + long, loose links, then
    // reheat so the layout re-settles with the new forces.
    fg.d3Force('charge')?.strength?.(CHARGE_STRENGTH)
    const link = fg.d3Force('link')
    link?.distance?.(LINK_DISTANCE)
    link?.strength?.(LINK_STRENGTH)
    fg.d3ReheatSimulation()
  }, [data, ready])
  const handleEngineStop = useCallback(() => {
    if (framedRef.current) return
    framedRef.current = true
    fgRef.current?.zoomToFit(600, 60)
  }, [])

  const handleNodeClick = useCallback((node: { x?: number; y?: number }) => {
    const fg = fgRef.current
    if (!fg || node?.x == null) return
    fg.centerAt(node.x, node.y, 600)
    fg.zoom(3, 600)
  }, [])

  // Explicit camera controls (drag-pan / scroll-zoom stay too).
  const recenter = useCallback(() => fgRef.current?.zoomToFit(500, 60), [])
  const zoomBy = useCallback((factor: number) => {
    const fg = fgRef.current
    if (!fg) return
    fg.zoom((fg.zoom() || 1) * factor, 250)
  }, [])

  /* eslint-disable @typescript-eslint/no-explicit-any */
  // Hover tooltip — full detail, styled from the resolved tokens.
  const nodeLabel = useCallback(
    (n: any) => {
      const rel = RELATION_LABEL[n.relation] ?? ''
      const color = relationColor[n.relation] ?? palette.edge
      const affiliation = n.affiliation
        ? `<div style="color:${muted};font-size:11px;margin-top:1px;white-space:normal">${escapeHtml(
            String(n.affiliation),
          )}</div>`
        : ''
      return `<div style="font-family:ui-sans-serif,system-ui,sans-serif;background:${palette.surface};border:1px solid ${palette.edge};border-radius:8px;padding:6px 10px;box-shadow:0 8px 24px rgba(15,23,42,.16);max-width:260px">
        <div style="color:${palette.ink};font-weight:600;font-size:12.5px">${escapeHtml(String(n.name ?? ''))}</div>
        ${affiliation}
        <div style="color:${color};font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;margin-top:3px">${rel}</div>
      </div>`
    },
    [relationColor, palette, muted],
  )

  // Persistent on-canvas name labels (drawn after the default node circle).
  // The focal author is always named; neighbours label in once zoomed enough,
  // so a dense graph doesn't turn into a wall of overlapping text.
  const nodeCanvasObject = useCallback(
    (node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
      const isCenter = !!node.is_center
      if (!isCenter && globalScale < 1.2) return
      const raw = String(node.name ?? '')
      if (!raw) return
      const label = raw.length > 26 ? `${raw.slice(0, 25)}…` : raw
      const fontSize = (isCenter ? 13 : 11) / globalScale
      ctx.font = `${isCenter ? 600 : 500} ${fontSize}px ui-sans-serif, system-ui, sans-serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'top'
      ctx.fillStyle = isCenter ? palette.center : muted
      const radius = Math.sqrt(Math.max(1, node.val ?? 1)) * 5
      ctx.fillText(label, node.x, node.y + radius + 2 / globalScale)
    },
    [palette, muted],
  )
  /* eslint-enable @typescript-eslint/no-explicit-any */

  return (
    <div ref={containerRef} className="relative h-full w-full">
      {ready ? (
        <ForceGraph2D
          ref={fgRef as never}
          width={size.width}
          height={size.height}
          graphData={graphData}
          backgroundColor={palette.background}
          d3VelocityDecay={0.3}
          nodeRelSize={5}
          linkDirectionalParticleWidth={1.8}
          linkDirectionalParticleSpeed={0.006}
          onEngineStop={handleEngineStop}
          onNodeClick={handleNodeClick as never}
          nodeLabel={nodeLabel}
          nodeCanvasObjectMode={() => 'after'}
          nodeCanvasObject={nodeCanvasObject as never}
          /* react-force-graph's accessor generics reject hand-written param
             shapes (its NodeObject index signature types every named prop as
             `unknown`), so the accessor args are untyped here — the one canvas
             boundary where that's the pragmatic call. */
          /* eslint-disable @typescript-eslint/no-explicit-any */
          nodeColor={(n: any) => relationColor[n.relation] ?? palette.edge}
          nodeVal={(n: any) => (n.is_center ? 16 : 2 + Math.min(n.weight ?? 0, 6) * 1.4)}
          linkColor={(l: any) => withAlpha(relationColor[l.relation] ?? palette.edge, 0.5)}
          linkWidth={(l: any) => 0.4 + Math.min(l.weight ?? 0, 6) * 0.4}
          linkDirectionalParticles={(l: any) =>
            l.relation === 'citation' ? 2 : l.relation === 'coauthor' ? 1 : 0
          }
          linkDirectionalParticleColor={(l: any) => relationColor[l.relation] ?? palette.edge}
          /* eslint-enable @typescript-eslint/no-explicit-any */
        />
      ) : null}

      {/* Camera controls — design-system Button primitives over the canvas. */}
      <div className="absolute right-3 top-3 flex flex-col gap-1.5">
        <Button
          variant="outline"
          size="icon-sm"
          aria-label="Recenter view"
          title="Recenter"
          onClick={recenter}
        >
          <Crosshair className="h-4 w-4" aria-hidden />
        </Button>
        <Button
          variant="outline"
          size="icon-sm"
          aria-label="Zoom in"
          title="Zoom in"
          onClick={() => zoomBy(1.4)}
        >
          <Plus className="h-4 w-4" aria-hidden />
        </Button>
        <Button
          variant="outline"
          size="icon-sm"
          aria-label="Zoom out"
          title="Zoom out"
          onClick={() => zoomBy(0.7)}
        >
          <Minus className="h-4 w-4" aria-hidden />
        </Button>
      </div>
    </div>
  )
}
