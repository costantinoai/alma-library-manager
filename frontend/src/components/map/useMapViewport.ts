/**
 * useMapViewport — pan/zoom/fit state for a fixed-coordinate map.
 *
 * World space is the substrate's unit square (x, y ∈ [0, 1]); screen space is
 * canvas px. One transform object owns the mapping, so hit-testing, drawing,
 * label placement, and the lasso all share the same math — no per-consumer
 * drift.
 */

import { useCallback, useRef, useState } from 'react'

export interface Viewport {
  /** Screen px per world unit. */
  scale: number
  /** Screen-px translation applied AFTER scaling. */
  tx: number
  ty: number
}

export const MIN_ZOOM_FACTOR = 0.5
export const MAX_ZOOM_FACTOR = 14

export function worldToScreen(v: Viewport, wx: number, wy: number): [number, number] {
  return [wx * v.scale + v.tx, wy * v.scale + v.ty]
}

export function screenToWorld(v: Viewport, sx: number, sy: number): [number, number] {
  return [(sx - v.tx) / v.scale, (sy - v.ty) / v.scale]
}

/** The transform that fits the unit square into w×h with padding. */
export function fitViewport(w: number, h: number, pad = 32): Viewport {
  const scale = Math.max(1, Math.min(w - pad * 2, h - pad * 2))
  return { scale, tx: (w - scale) / 2, ty: (h - scale) / 2 }
}

export function useMapViewport(width: number, height: number) {
  const fitted = fitViewport(width, height)
  const [viewport, setViewport] = useState<Viewport>(fitted)
  // Pan bookkeeping lives in a ref: dragging must not re-render per event —
  // the canvas redraws from the viewport state set at rAF pace by the host.
  const panRef = useRef<{ sx: number; sy: number; tx0: number; ty0: number } | null>(null)

  const fit = useCallback(() => setViewport(fitViewport(width, height)), [width, height])

  const zoomAt = useCallback(
    (sx: number, sy: number, factor: number) => {
      setViewport((v) => {
        const base = fitViewport(width, height).scale
        const next = Math.min(base * MAX_ZOOM_FACTOR, Math.max(base * MIN_ZOOM_FACTOR, v.scale * factor))
        const k = next / v.scale
        // Keep the world point under the cursor stationary.
        return { scale: next, tx: sx - (sx - v.tx) * k, ty: sy - (sy - v.ty) * k }
      })
    },
    [width, height],
  )

  const panStart = useCallback((sx: number, sy: number, v: Viewport) => {
    panRef.current = { sx, sy, tx0: v.tx, ty0: v.ty }
  }, [])

  const panMove = useCallback((sx: number, sy: number) => {
    const p = panRef.current
    if (!p) return
    setViewport((v) => ({ ...v, tx: p.tx0 + (sx - p.sx), ty: p.ty0 + (sy - p.sy) }))
  }, [])

  const panEnd = useCallback(() => {
    panRef.current = null
  }, [])

  return { viewport, setViewport, fit, zoomAt, panStart, panMove, panEnd, isPanning: () => panRef.current != null }
}
