/**
 * terrainTexture — the terrain rendered ONCE, in world space.
 *
 * The colour at a place on the map answers "what sits here", so it must depend
 * on the place and nothing else. The splat used to be accumulated in SCREEN
 * space, which quietly made it depend on the camera in three separate ways:
 *
 *   - the gaussian kernel was a fixed pixel radius, so zooming in shrank the
 *     world area each cell averaged over and the landscape re-formed;
 *   - field points outside the viewport were culled before accumulating, so
 *     panning changed the values near the edges;
 *   - the colour and alpha scales were normalised over the VISIBLE points, so
 *     the same region changed colour depending on what else was on screen.
 *
 * Switching a map's scope moves the camera, so all three showed up as "Library
 * and Corpus have different terrain" — when Library is a subset of the very same
 * space and must look identical under the dots it does show.
 *
 * So: bake the field into a world-space texture, then let the viewport transform
 * scale it like any other part of the map. The texture is a pure function of the
 * field; the camera can only decide which part of it you are looking at.
 */
import { TERRAIN_SCALE_ABS_MAX, terrainColor } from './mapNodeStyle'
import type { TerrainPoint } from './terrainField'

/** Texture resolution over the substrate's unit square. Terrain is a smooth
 *  wash, so this is upscaled with interpolation rather than drawn per pixel. */
export const TERRAIN_TEXTURE_SIZE = 192
/** Gaussian kernel radius in WORLD units — the scale at which "nearby" is
 *  judged. Fixed, because a neighbourhood is a property of the space. */
export const TERRAIN_KERNEL_WORLD = 0.08
/** A no-opinion point still covers ground (the substrate has no holes) but must
 *  not drown the pockets that do carry signal — see the 2026-07-25 contract. */
const NEUTRAL_WEIGHT = 0.15

export interface TerrainGrid {
  /** RGBA over a `size` x `size` grid covering the world unit square. */
  rgba: Uint8ClampedArray
  size: number
  /** The fixed semantic ±scale the plate uses — what the colourbar labels. */
  absMax: number
}

export interface TerrainTexture {
  canvas: HTMLCanvasElement
  absMax: number
}

/**
 * Bake `points` into a world-space RGBA grid covering the unit square.
 *
 * Deliberately takes NOTHING but the points: there is no viewport parameter to
 * pass, so the terrain cannot depend on the camera even by accident. That is the
 * whole fix — the previous version accumulated in screen space and therefore
 * changed with zoom, pan, and (because scope switching moves the camera) with
 * which subset of dots was on screen.
 */
export function buildTerrainGrid(
  points: ReadonlyArray<TerrainPoint>,
): TerrainGrid | null {
  if (!points.length) return null

  const size = TERRAIN_TEXTURE_SIZE
  const wsum = new Float32Array(size * size) // signal-weighted mass → colour
  const dsum = new Float32Array(size * size) // plain density → alpha
  const vsum = new Float32Array(size * size)
  const radius = TERRAIN_KERNEL_WORLD * size
  const twoSigmaSq = 2 * (radius / 2) ** 2

  const absMax = TERRAIN_SCALE_ABS_MAX

  for (const p of points) {
    const gx = p.x * size
    const gy = p.y * size
    const x0 = Math.max(0, Math.floor(gx - radius))
    const x1 = Math.min(size - 1, Math.ceil(gx + radius))
    const y0 = Math.max(0, Math.floor(gy - radius))
    const y1 = Math.min(size - 1, Math.ceil(gy + radius))
    const signalW = NEUTRAL_WEIGHT + (1 - NEUTRAL_WEIGHT) * Math.abs(p.v)
    for (let yy = y0; yy <= y1; yy++) {
      const dy2 = (yy - gy) ** 2
      for (let xx = x0; xx <= x1; xx++) {
        const d2 = (xx - gx) ** 2 + dy2
        const wgt = Math.exp(-d2 / twoSigmaSq)
        if (wgt < 0.01) continue
        const idx = yy * size + xx
        dsum[idx] += wgt
        wsum[idx] += wgt * signalW
        vsum[idx] += wgt * signalW * p.v
      }
    }
  }

  const rgba = new Uint8ClampedArray(size * size * 4)

  // Density reference over the WHOLE world, so a region's opacity says how much
  // evidence sits there rather than how much of it happens to be on screen.
  let maxDensity = 0
  for (let i = 0; i < dsum.length; i++) if (dsum[i] > maxDensity) maxDensity = dsum[i]
  if (maxDensity <= 0) maxDensity = 1

  for (let i = 0; i < wsum.length; i++) {
    if (wsum[i] <= 0.02) continue
    const mean = vsum[i] / wsum[i]
    const t = Math.max(-1, Math.min(1, mean / TERRAIN_SCALE_ABS_MAX))
    const [r, g, b] = terrainColor(t)
    const densityAlpha = Math.min(0.9, 0.45 + 0.6 * (dsum[i] / maxDensity))
    const o = i * 4
    rgba[o] = r
    rgba[o + 1] = g
    rgba[o + 2] = b
    rgba[o + 3] = densityAlpha * (0.6 + 0.4 * Math.abs(t)) * 255
  }
  return { rgba, size, absMax }
}


/** The grid as a canvas the renderer can `drawImage` through its transform. */
export function buildTerrainTexture(
  points: ReadonlyArray<TerrainPoint>,
): TerrainTexture | null {
  const grid = buildTerrainGrid(points)
  if (!grid) return null
  const canvas = document.createElement('canvas')
  canvas.width = grid.size
  canvas.height = grid.size
  const ctx = canvas.getContext('2d')
  if (!ctx) return null
  const img = ctx.createImageData(grid.size, grid.size)
  img.data.set(grid.rgba)
  ctx.putImageData(img, 0, 0)
  return { canvas, absMax: grid.absMax }
}
