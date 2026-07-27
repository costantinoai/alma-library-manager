/**
 * useMapField — THE one place a map gets its preference field.
 *
 * Every map surface needs the same four things: which field endpoint to read,
 * the live score per node, the terrain points to splat, and the model summary
 * that says how much of the field is inferred. Until 2026-07-27 that logic
 * existed TWICE — once in `GraphMapView` (Map page + Authors) and once in
 * `FrontierMap` (Discovery) — because `GraphMapView` fetches its own graph and
 * Discovery's payload has a different shape, so it could not be reused.
 *
 * The cost was not theoretical. When the terrain ramp moved to a ±0.5 domain,
 * the Map page followed and Discovery did not: its colourbar went on claiming
 * `-1 … +1` beside a gradient that no longer used it. Nothing failed, because
 * nothing tied the two together.
 *
 * So this hook is deliberately **data-agnostic**. It takes the minimal node
 * shape any map can produce — an id and a position — and knows nothing about
 * papers, authors, recommendations or endpoints beyond which field family to
 * read. Hosts keep their own fetching and their own meaning; they no longer
 * keep their own copy of this.
 */
import { useCallback, useMemo } from 'react'

import { buildTerrainField, type TerrainField } from './terrainField'
import { useAuthorField } from './useAuthorField'
import { useSignalField, type SignalFieldModel } from './useSignalField'

/** The only thing a map must tell this hook about each of its nodes. */
export interface MapFieldNode {
  id: string
  x: number
  y: number
}

/** Which field family backs this surface.
 *
 *  `paper` reads the space-owned `/graphs/signal-field`; `author` reads the
 *  id-keyed `/graphs/author-field`. Same `signal_valence` weights either way —
 *  the split exists because the author network has its own layout space. */
export type MapFieldKind = 'paper' | 'author'

export interface UseMapFieldArgs {
  kind: MapFieldKind
  /** Skip the fetch entirely when neither Terrain nor Score is on. */
  enabled: boolean
  nodes: ReadonlyArray<MapFieldNode>
  /** `metadata.layout.frame` from the rendered payload, when it has one. */
  frame?: unknown
  /** Frame to assume for payloads that predate the declaration. */
  fallbackIsSubstrate: boolean
}

export interface MapField {
  /** Live internal score (0–100) for a node id, or null if never scored. */
  scoreFor: (id: string) => number | null
  /** Every score on the surface — for the legend's mean. */
  scores: ReadonlyMap<string, number>
  terrain: TerrainField
  /** What the paper field was fitted from; null for author fields, which are
   *  observed-only and predict nothing. */
  model: SignalFieldModel | null
  isFetching: boolean
}

/** OpenAlex author ids are case-insensitive and payload casing does not match
 *  the authors table's, so author lookups fold before comparing. Paper ids are
 *  opaque and must NOT be folded. */
const foldFor = (kind: MapFieldKind) => (id: string) =>
  kind === 'author' ? id.trim().toLowerCase() : id

export function useMapField({
  kind,
  enabled,
  nodes,
  frame,
  fallbackIsSubstrate,
}: UseMapFieldArgs): MapField {
  const isPaper = kind === 'paper'
  const signalField = useSignalField(enabled && isPaper)
  const authorField = useAuthorField(enabled && !isPaper)

  const fold = useMemo(() => foldFor(kind), [kind])
  const scores = isPaper ? signalField.scoresById : authorField.scoresById
  const valenceById = isPaper ? signalField.valenceById : authorField.valenceById
  const points = isPaper ? signalField.points : authorField.points

  const scoreFor = useCallback(
    (id: string) => scores.get(fold(id)) ?? null,
    [scores, fold],
  )

  const terrain = useMemo(
    () =>
      buildTerrainField({
        // A paper payload can be a private re-fit of the layout; the author
        // network always has its own complete space, so it is always its own
        // frame and never had the substrate/re-fit mismatch the paper map did.
        frame: isPaper ? frame : 'substrate',
        fallbackIsSubstrate: !isPaper || fallbackIsSubstrate,
        nodes,
        // The WHOLE space's points, at the space's coordinates. Scope filters
        // dots, never the field: a Library view is a subset of the same
        // terrain, not a smaller terrain.
        spacePoints: points,
        valenceById,
        // Only the paper field is FITTED, so only it can report a value it
        // inferred rather than observed. The author field is observed-only
        // until task 64 Phase 2 generalises the estimator, so it passes no
        // confidence and every author reads as fully believed.
        confidenceById: isPaper ? signalField.confidenceById : undefined,
      }),
    [
      isPaper,
      frame,
      fallbackIsSubstrate,
      nodes,
      points,
      valenceById,
      signalField.confidenceById,
    ],
  )

  return {
    scoreFor,
    scores,
    terrain,
    model: isPaper ? signalField.model : null,
    isFetching: isPaper ? signalField.isFetching : authorField.isFetching,
  }
}
