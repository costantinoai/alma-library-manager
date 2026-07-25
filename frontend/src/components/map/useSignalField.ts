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

interface SignalFieldResponse {
  status: string
  points: SignalFieldPoint[]
  stats: SignalFieldStats | null
}

export function useSignalField(enabled: boolean): {
  points: SignalFieldPoint[]
  stats: SignalFieldStats | null
  /** Live internal score per paper id — the Score colour mode's source. */
  scoresById: ReadonlyMap<string, number>
} {
  const query = useQuery({
    queryKey: ['signal-field'],
    queryFn: () => api.get<SignalFieldResponse>('/graphs/signal-field'),
    enabled,
    // The field moves with signals (saves, ratings, refreshes), not with
    // view state — a few minutes of staleness is invisible.
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

  return { points, stats: query.data?.stats ?? null, scoresById }
}
