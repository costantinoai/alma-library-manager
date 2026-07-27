import { describe, expect, it } from 'vitest'

import { buildTerrainField, VALENCE_NO_SIGNAL } from './terrainField'

const SUBSTRATE = [
  { x: 0.1, y: 0.9, v: -0.8 },
  { x: 0.5, y: 0.5, v: 0.35 },
  { x: 0.9, y: 0.1, v: 0 },
]

// A tuned layout: the same papers, re-fitted somewhere else entirely.
const REFITTED_NODES = [
  { id: 'p1', x: 0.72, y: 0.15 },
  { id: 'p2', x: 0.74, y: 0.18 },
]

const VALENCE = new Map([
  ['p1', -0.8],
  ['p2', 0.35],
])

describe('terrainField', () => {
  it('splats the whole space-owned field on the substrate frame', () => {
    const field = buildTerrainField({
      frame: 'substrate',
      fallbackIsSubstrate: true,
      nodes: REFITTED_NODES,
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })

    // Every point of the space, including papers this view does not draw: a
    // Library view is a subset of the DOTS, never a smaller terrain.
    expect(field.frame).toBe('substrate')
    expect(field.points).toHaveLength(3)
    expect(field.stats?.min).toBeCloseTo(-0.8)
  })

  it('follows the layout when the payload fitted its own frame', () => {
    const field = buildTerrainField({
      frame: 'own',
      fallbackIsSubstrate: true,
      nodes: REFITTED_NODES,
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })

    expect(field.frame).toBe('own')
    // Coordinates come from the rendered nodes, not the substrate.
    expect(field.points.map((p) => p.x)).toEqual([0.72, 0.74])
    // …and y is flipped into the shared plate convention exactly once.
    expect(field.points[0].y).toBeCloseTo(1 - 0.15)
    expect(field.points[0].v).toBeCloseTo(-0.8)
  })

  it('keeps the paper terrain hole-free', () => {
    const field = buildTerrainField({
      frame: 'own',
      fallbackIsSubstrate: false,
      nodes: [...REFITTED_NODES, { id: 'no-opinion', x: 0.2, y: 0.2 }],
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })

    expect(field.points).toHaveLength(3)
    expect(field.points[2].v).toBe(VALENCE_NO_SIGNAL)
    expect(field.coverage).toEqual({ valued: 2, total: 3 })
  })

  it('never lets an undeclared variant claim the substrate frame', () => {
    // Payloads cached before the frame was declared carry nothing. Only a
    // request for the DEFAULT layout can fall back to substrate.
    const tuned = buildTerrainField({
      frame: undefined,
      fallbackIsSubstrate: false,
      nodes: REFITTED_NODES,
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })
    expect(tuned.frame).toBe('own')

    const plain = buildTerrainField({
      frame: undefined,
      fallbackIsSubstrate: true,
      nodes: REFITTED_NODES,
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })
    expect(plain.frame).toBe('substrate')
  })

  it('reports stats of the values actually splatted', () => {
    const field = buildTerrainField({
      frame: 'own',
      fallbackIsSubstrate: false,
      nodes: REFITTED_NODES,
      spacePoints: SUBSTRATE,
      valenceById: VALENCE,
    })
    // The colourbar labels what the plate normalised against, so the numbers
    // must come from this layout's population, not the substrate's.
    expect(field.stats?.min).toBeCloseTo(-0.8)
    expect(field.stats?.max).toBeCloseTo(0.35)
  })
})
