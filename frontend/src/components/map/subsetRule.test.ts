/**
 * THE SUBSET RULE (user, restated 2026-07-26):
 *
 *   "The library is A SUBSET of the corpus. When I switch between corpus and
 *    library I want to see the same exact space and terrain, just with all or a
 *    subset of the dots. The colour/heat is a property of the terrain — it does
 *    not change based on what we show. It depends on what points sit in a point
 *    of space."
 *
 * Three separate things had to be true for that, and none of them were:
 * the field, the terrain picture built from it, and the cluster hues.
 */
import { describe, expect, it } from 'vitest'

import { TERRAIN_SCALE_ABS_MAX, terrainColor } from './mapNodeStyle'
import { buildTerrainField } from './terrainField'
import { buildTerrainGrid } from './terrainTexture'

/** One space: five papers, three of them also in the library. */
const SPACE_FIELD = [
  { x: 0.15, y: 0.2, v: -0.8 },
  { x: 0.2, y: 0.25, v: 0.5 },
  { x: 0.6, y: 0.7, v: 0 },
  { x: 0.62, y: 0.72, v: 0.35 },
  { x: 0.9, y: 0.1, v: -0.6 },
]

const CORPUS_NODES = [
  { id: 'p1', x: 0.15, y: 0.8 },
  { id: 'p2', x: 0.2, y: 0.75 },
  { id: 'p3', x: 0.6, y: 0.3 },
  { id: 'p4', x: 0.62, y: 0.28 },
  { id: 'p5', x: 0.9, y: 0.9 },
]
const LIBRARY_NODES = CORPUS_NODES.slice(0, 3)

const VALENCE = new Map([
  ['p1', -0.8],
  ['p2', 0.5],
  ['p4', 0.35],
  ['p5', -0.6],
])

describe('the library is a subset of the corpus', () => {
  it('gives both scopes the identical terrain field', () => {
    const corpus = buildTerrainField({
      frame: 'substrate',
      fallbackIsSubstrate: true,
      nodes: CORPUS_NODES,
      spacePoints: SPACE_FIELD,
      valenceById: VALENCE,
    })
    const library = buildTerrainField({
      frame: 'substrate',
      fallbackIsSubstrate: true,
      nodes: LIBRARY_NODES,
      spacePoints: SPACE_FIELD,
      valenceById: VALENCE,
    })

    // Fewer dots, same landscape — including the pockets whose dots the Library
    // view does not draw at all.
    expect(library.points).toEqual(corpus.points)
    expect(library.stats).toEqual(corpus.stats)
  })

  it('gives both scopes the identical terrain picture', () => {
    const corpus = buildTerrainGrid(SPACE_FIELD)
    const library = buildTerrainGrid(SPACE_FIELD)
    expect(library!.rgba).toEqual(corpus!.rgba)
    expect(library!.absMax).toBe(corpus!.absMax)
  })

  it('cannot be made to depend on the camera', () => {
    // The regression this replaces: the splat was accumulated in SCREEN space,
    // so zoom changed the kernel's world size, panning changed which points were
    // counted, and the scales were normalised over the visible subset. Switching
    // scope moves the camera, so the terrain moved with it.
    //
    // There is now no camera to pass. Point ORDER is the only thing a caller can
    // vary, and it must not matter either.
    const forward = buildTerrainGrid(SPACE_FIELD)
    const reversed = buildTerrainGrid([...SPACE_FIELD].reverse())
    expect(reversed!.rgba).toEqual(forward!.rgba)
    expect(buildTerrainGrid.length).toBe(1)
  })

  it('colours a place by what sits there, not by what is drawn', () => {
    // Remove the DOTS of the strongly negative pocket, keep its points in the
    // field: that corner of the world must keep its colour.
    const grid = buildTerrainGrid(SPACE_FIELD)!
    const size = grid.size
    const at = (wx: number, wy: number) => {
      const i = (Math.floor(wy * size) * size + Math.floor(wx * size)) * 4
      return [grid.rgba[i], grid.rgba[i + 1], grid.rgba[i + 2], grid.rgba[i + 3]]
    }
    const negativePocket = at(0.15, 0.2)
    const positivePocket = at(0.62, 0.72)

    // Red over the pocket the user pushed away, green where they saved.
    expect(negativePocket[0]).toBeGreaterThan(negativePocket[1])
    expect(positivePocket[1]).toBeGreaterThan(positivePocket[0])
    // Both are actually painted — a transparent "terrain" proves nothing.
    expect(negativePocket[3]).toBeGreaterThan(0)
    expect(positivePocket[3]).toBeGreaterThan(0)
  })

  it('uses one semantic scale so weak populations cannot saturate', () => {
    const weak = buildTerrainGrid([{ x: 0.5, y: 0.5, v: 0.03 }])!
    const i = (Math.floor(0.5 * weak.size) * weak.size + Math.floor(0.5 * weak.size)) * 4
    const actual = [...weak.rgba.slice(i, i + 3)]
    // The grid paints `terrainColor(v / TERRAIN_SCALE_ABS_MAX)`. Comparing
    // against `terrainColor(v)` silently assumed a ±1 domain and broke the
    // moment the ramp was narrowed to the range valence actually occupies.
    const expected = terrainColor(0.03 / TERRAIN_SCALE_ABS_MAX)

    expect(weak.absMax).toBe(TERRAIN_SCALE_ABS_MAX)
    expect(actual).toEqual(expected)
    expect(actual).not.toEqual(terrainColor(1))
  })

})
