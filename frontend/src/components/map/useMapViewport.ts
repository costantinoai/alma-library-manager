/**
 * useMapViewport — pan/zoom/fit state for a fixed-coordinate map.
 *
 * World space is the substrate's unit square (x, y ∈ [0, 1]); screen space is
 * canvas px. One transform object owns the mapping, so hit-testing, drawing,
 * label placement, and the lasso all share the same math — no per-consumer
 * drift.
 */

import { useCallback, useEffect, useMemo, useRef } from 'react'

import { useMapSessionState } from './mapSessionState'

export interface Viewport {
  /** Screen px per world unit. */
  scale: number
  /** Screen-px translation applied AFTER scaling. */
  tx: number
  ty: number
}

/** Resolution-independent camera stored across page unmounts. */
export interface MapCamera {
  /** World coordinate at the centre of the plate. */
  centerX: number
  centerY: number
  /** Multiplier over the fitted unit-square scale. */
  zoom: number
}

export const FITTED_CAMERA: MapCamera = {
  centerX: 0.5,
  centerY: 0.5,
  zoom: 1,
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

export function cameraToViewport(
  camera: MapCamera,
  width: number,
  height: number,
): Viewport {
  const scale = fitViewport(width, height).scale * camera.zoom
  return {
    scale,
    tx: width / 2 - camera.centerX * scale,
    ty: height / 2 - camera.centerY * scale,
  }
}

export function viewportToCamera(
  viewport: Viewport,
  width: number,
  height: number,
): MapCamera {
  const baseScale = fitViewport(width, height).scale
  return {
    centerX: (width / 2 - viewport.tx) / viewport.scale,
    centerY: (height / 2 - viewport.ty) / viewport.scale,
    zoom: viewport.scale / baseScale,
  }
}

export function useMapViewport(
  width: number,
  height: number,
  storageKey?: string,
) {
  const [camera, setCamera] = useMapSessionState<MapCamera>(
    storageKey ?? 'volatile',
    'camera',
    FITTED_CAMERA,
    { persist: !!storageKey, writeDelayMs: 150 },
  )
  const viewport = useMemo(
    () => cameraToViewport(camera, width, height),
    [camera, height, width],
  )
  // Pan bookkeeping is stable for the full gesture. Pointer events are
  // coalesced to one camera update per animation frame.
  const panRef = useRef<{
    sx: number
    sy: number
    camera: MapCamera
    scale: number
  } | null>(null)
  const pendingPanRef = useRef<{ sx: number; sy: number } | null>(null)
  const panFrameRef = useRef<number | null>(null)

  const fit = useCallback(() => setCamera(FITTED_CAMERA), [setCamera])

  const zoomAt = useCallback(
    (sx: number, sy: number, factor: number) => {
      setCamera((current) => {
        const currentViewport = cameraToViewport(current, width, height)
        const [worldX, worldY] = screenToWorld(currentViewport, sx, sy)
        const zoom = Math.min(
          MAX_ZOOM_FACTOR,
          Math.max(MIN_ZOOM_FACTOR, current.zoom * factor),
        )
        const nextScale = fitViewport(width, height).scale * zoom
        // Keep the world point under the cursor stationary.
        return {
          centerX: worldX - (sx - width / 2) / nextScale,
          centerY: worldY - (sy - height / 2) / nextScale,
          zoom,
        }
      })
    },
    [height, setCamera, width],
  )

  const panStart = useCallback(
    (sx: number, sy: number, currentViewport: Viewport) => {
      panRef.current = {
        sx,
        sy,
        camera: viewportToCamera(currentViewport, width, height),
        scale: currentViewport.scale,
      }
    },
    [height, width],
  )

  const panMove = useCallback((sx: number, sy: number) => {
    if (!panRef.current) return
    pendingPanRef.current = { sx, sy }
    if (panFrameRef.current != null) return
    panFrameRef.current = window.requestAnimationFrame(() => {
      panFrameRef.current = null
      const start = panRef.current
      const pending = pendingPanRef.current
      if (!start || !pending) return
      setCamera({
        centerX: start.camera.centerX - (pending.sx - start.sx) / start.scale,
        centerY: start.camera.centerY - (pending.sy - start.sy) / start.scale,
        zoom: start.camera.zoom,
      })
    })
  }, [setCamera])

  const panEnd = useCallback(() => {
    if (panFrameRef.current != null) {
      window.cancelAnimationFrame(panFrameRef.current)
      panFrameRef.current = null
      const start = panRef.current
      const pending = pendingPanRef.current
      if (start && pending) {
        setCamera({
          centerX: start.camera.centerX - (pending.sx - start.sx) / start.scale,
          centerY: start.camera.centerY - (pending.sy - start.sy) / start.scale,
          zoom: start.camera.zoom,
        })
      }
    }
    panRef.current = null
    pendingPanRef.current = null
  }, [setCamera])

  useEffect(
    () => () => {
      if (panFrameRef.current != null) {
        window.cancelAnimationFrame(panFrameRef.current)
      }
    },
    [],
  )

  return {
    viewport,
    fit,
    zoomAt,
    panStart,
    panMove,
    panEnd,
    isPanning: () => panRef.current != null,
  }
}
