/**
 * terrainField — THE one place a map's Terrain overlay gets its points.
 *
 * **The library is a subset of the corpus** (locked rule, restated by the user
 * 2026-07-26). Switching scope shows the same space with fewer dots, so the
 * terrain must be byte-for-byte the same landscape: colour at a place answers
 * "what sits here", and that cannot depend on which dots are currently drawn.
 *
 * Two consequences, and this module exists to keep them from being confused:
 *
 * 1. **Scope is not a space.** A Library view and a Corpus view are one space,
 *    so both splat the WHOLE field — every signal-carrying point, including the
 *    ones whose dots this view does not render. Hosts pass `spacePoints` from
 *    the endpoint that owns the space's field (`/graphs/signal-field` for
 *    papers, `/graphs/author-field` for authors), never from their own nodes.
 *
 * 2. **A re-fit IS a space.** The Advanced knobs can refit the layout (a
 *    non-default cluster detail re-runs UMAP; a layout blend re-solves positions
 *    from fused distances). Those coordinates are a different arrangement, and
 *    nothing outside that payload has a position in it, so the field is joined
 *    onto the payload's own nodes by id. The backend says which case applies via
 *    `metadata.layout.frame`.
 *
 * The render half of the contract lives in `terrainTexture.ts`: the field is
 * baked in world space so the camera cannot change it either.
 */
import { summarizeValues } from './mapNodeStyle'

/** No opinion recorded. The paper substrate is deliberately hole-free: a point
 *  with no signal still covers space so the splat has no gaps (2026-07-25). */
export const VALENCE_NO_SIGNAL = 0

export interface TerrainPoint {
  x: number
  y: number
  v: number
}

export interface TerrainField {
  /** World-coordinate points for `SemanticMap`'s `heatField`. */
  points: ReadonlyArray<TerrainPoint>
  /** Stats of the values actually splatted — what the colourbar owes the reader. */
  stats: { min: number; max: number; mean: number } | null
  /** Which frame the points are expressed in, after fallback resolution. */
  frame: 'substrate' | 'own'
  /** How many of the layout's nodes carry a real opinion (own frame only). */
  coverage: { valued: number; total: number }
}

export interface TerrainNode {
  id: string
  x: number
  y: number
}

const EMPTY: TerrainField = {
  points: [],
  stats: null,
  frame: 'substrate',
  coverage: { valued: 0, total: 0 },
}

/** True when a payload's coordinates are the space's own (not a private re-fit). */
export function isSubstrateFrame(
  frame: unknown,
  fallbackIsSubstrate: boolean,
): boolean {
  if (frame === 'substrate') return true
  if (frame === 'own') return false
  // Payloads cached before the frame was declared carry nothing. The host knows
  // whether it asked for the default layout, which is the only configuration
  // that can be the space itself — so a stale variant can never claim to be one.
  return fallbackIsSubstrate
}

export function buildTerrainField({
  frame,
  fallbackIsSubstrate,
  nodes,
  spacePoints,
  valenceById,
}: {
  /** `metadata.layout.frame` from the rendered payload. */
  frame?: unknown
  /** Frame to assume when the payload predates the declaration. */
  fallbackIsSubstrate: boolean
  /** This payload's nodes — used ONLY to place a re-fitted layout's field. */
  nodes: ReadonlyArray<TerrainNode>
  /** The whole space's field, at the space's coordinates, y already flipped. */
  spacePoints: ReadonlyArray<TerrainPoint>
  /** Valence per node id, live. */
  valenceById: ReadonlyMap<string, number>
}): TerrainField {
  if (isSubstrateFrame(frame, fallbackIsSubstrate)) {
    if (!spacePoints.length) return EMPTY
    return {
      points: spacePoints,
      stats: summarizeValues(spacePoints.map((p) => p.v)),
      frame: 'substrate',
      coverage: { valued: spacePoints.length, total: spacePoints.length },
    }
  }

  const points: TerrainPoint[] = []
  let valued = 0
  for (const node of nodes) {
    const v = valenceById.get(node.id)
    const known = typeof v === 'number'
    if (known) valued += 1
    points.push({
      x: node.x,
      // Same y convention as every host: higher y draws at the top.
      y: 1 - node.y,
      v: known ? v : VALENCE_NO_SIGNAL,
    })
  }
  if (!points.length) {
    return { ...EMPTY, frame: 'own', coverage: { valued: 0, total: nodes.length } }
  }
  return {
    points,
    stats: summarizeValues(points.map((p) => p.v)),
    frame: 'own',
    coverage: { valued, total: nodes.length },
  }
}
