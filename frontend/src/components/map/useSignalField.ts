/**
 * useSignalField — the ONE client of `/graphs/signal-field`, the
 * space-owned preference field (user call 2026-07-25).
 *
 * The field is a property of the corpus substrate, not of any view: one
 * valence per signal-carrying paper at its substrate coordinates,
 * regardless of which dots a host currently renders. Every paper-map
 * host (Discovery frontier, Map page) feeds SemanticMap's `heatField`
 * from here, so Heat shows the SAME terrain everywhere and never shifts
 * when a layer is toggled.
 *
 * Coordinates are flipped here once (y → 1 - y) into the shared plate
 * convention every host already uses for its nodes.
 */
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { api } from '@/api/client'

export interface SignalFieldPoint {
  id: string
  x: number
  y: number
  v: number
  /** How much `v` is to be believed, 0–1: 1.0 for a signal you gave, the
   *  fitted field's explained-variance fraction for one it inferred. */
  c: number
  /** What produced `v` — `rating` / `library` / `removed` / `engagement` /
   *  `negative_action` / `engine` / `predicted` / `unknown`. */
  src: string
  /** Raw internal score (0–100, latest recommendation) — null if never
   *  recommended. Rides along so Score mode colours LIVE instead of from
   *  the cached layout payload (which goes stale between refreshes). */
  score: number | null
}

export interface SignalFieldStats {
  min: number
  max: number
  mean: number
  count: number
}

/** What the field was fitted from — surfaced so the map can say so. */
export interface SignalFieldModel {
  fitted: boolean
  n_labels: number
  n_observed: number
  n_predicted: number
  n_unknown: number
  bandwidth: number | null
  noise: number
  reason: string | null
}

interface SignalFieldResponse {
  status: string
  points: SignalFieldPoint[]
  stats: SignalFieldStats | null
  model?: SignalFieldModel
}

export function useSignalField(enabled: boolean): {
  points: SignalFieldPoint[]
  stats: SignalFieldStats | null
  model: SignalFieldModel | null
  /** Live internal score per paper id — the Score colour mode's source. */
  scoresById: ReadonlyMap<string, number>
  /** Live valence per paper id. The frame-independent half of the field: a
   *  tuned layout has its own coordinates, so its terrain joins THESE values
   *  onto its own nodes (see `terrainField.ts`). */
  valenceById: ReadonlyMap<string, number>
  /** Live confidence per paper id. Travels with `valenceById` for the same
   *  reason — a re-fitted layout needs both joined onto its own nodes. */
  confidenceById: ReadonlyMap<string, number>
  isFetching: boolean
} {
  const query = useQuery({
    queryKey: ['signal-field'],
    queryFn: () => api.get<SignalFieldResponse>('/graphs/signal-field'),
    enabled,
    // Mutations invalidate this key immediately; staleTime only prevents
    // navigation/remount refetches between signal changes.
    staleTime: 5 * 60_000,
  })

  const points = useMemo(
    () => (query.data?.points ?? []).map((p) => ({ ...p, y: 1 - p.y })),
    [query.data],
  )

  const scoresById = useMemo(() => {
    const m = new Map<string, number>()
    for (const p of query.data?.points ?? []) {
      if (typeof p.score === 'number') m.set(p.id, p.score)
    }
    return m
  }, [query.data])

  const valenceById = useMemo(() => {
    const m = new Map<string, number>()
    for (const p of query.data?.points ?? []) {
      if (typeof p.v === 'number') m.set(p.id, p.v)
    }
    return m
  }, [query.data])

  const confidenceById = useMemo(() => {
    const m = new Map<string, number>()
    for (const p of query.data?.points ?? []) {
      if (typeof p.c === 'number') m.set(p.id, p.c)
    }
    return m
  }, [query.data])

  return {
    points,
    stats: query.data?.stats ?? null,
    model: query.data?.model ?? null,
    scoresById,
    valenceById,
    confidenceById,
    isFetching: query.isFetching,
  }
}
