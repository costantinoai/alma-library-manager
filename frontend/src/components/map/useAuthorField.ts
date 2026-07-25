/**
 * useAuthorField — the ONE client of `/graphs/author-field`, the author map's
 * live preference + score field (2026-07-26).
 *
 * The author analogue of `useSignalField`, and it exists for the same two
 * reasons that endpoint does:
 *
 *  * **Live, not baked.** The author network is a materialized view; the score
 *    baked into its payload is as old as the last rebuild, so Score mode greyed
 *    out and the terrain froze. Signals move on every save, rating and Discovery
 *    refresh — they have to be read separately from the layout.
 *  * **Valence, not bare score.** An author's valence is the mean
 *    `paper_valence` over the papers of theirs you actually have an opinion
 *    about, so it uses the SAME weights as the paper map's terrain. Authors with
 *    no signal at all come back `v: null` and are simply absent from
 *    `valenceById` — the host omits them from the splat instead of feeding it a
 *    fake zero. (Flooding the splat with zeros is exactly what flattened the old
 *    terrain to uniform yellow.)
 *
 * Keyed by author id rather than coordinates: the author map always draws every
 * author in scope, so there is no off-view author for a coordinate-keyed field
 * to cover — id-keyed is view-independent here for the same reason.
 */
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { api } from '@/api/client'

export interface AuthorFieldEntry {
  id: string
  /** Mean valence in [-1, +1] over this author's SIGNALLED papers; null when
   *  you have no signal on any of them. */
  v: number | null
  /** Mean internal relevance score (0–100) over their SCORED papers. */
  score: number | null
  /** How many papers the valence rests on. */
  signal_papers: number
  /** How many in-scope papers they have at all. */
  papers: number
}

export interface AuthorFieldStats {
  min: number
  max: number
  mean: number
  count: number
}

interface AuthorFieldResponse {
  status: string
  authors: AuthorFieldEntry[]
  stats: AuthorFieldStats | null
}

export function useAuthorField(
  scope: string,
  enabled: boolean,
): {
  /** Valence per author id — ONLY authors carrying a signal. */
  valenceById: ReadonlyMap<string, number>
  /** Live mean internal score per author id — the Score colour mode's source. */
  scoresById: ReadonlyMap<string, number>
  /** Evidence behind each valence, for the hover card. */
  entriesById: ReadonlyMap<string, AuthorFieldEntry>
  stats: AuthorFieldStats | null
  isFetching: boolean
} {
  const query = useQuery({
    queryKey: ['author-field', scope],
    queryFn: () => api.get<AuthorFieldResponse>(`/graphs/author-field?scope=${scope}`),
    enabled,
    // Mutations invalidate this key immediately; staleTime only prevents
    // navigation/remount refetches between signal changes.
    staleTime: 5 * 60_000,
  })

  return useMemo(() => {
    const valenceById = new Map<string, number>()
    const scoresById = new Map<string, number>()
    const entriesById = new Map<string, AuthorFieldEntry>()
    for (const a of query.data?.authors ?? []) {
      // OpenAlex ids are case-insensitive; the network's node ids and the
      // authors table disagree on case, so every id channel is folded once,
      // here and at every host lookup.
      const key = a.id.trim().toLowerCase()
      entriesById.set(key, a)
      if (typeof a.v === 'number') valenceById.set(key, a.v)
      if (typeof a.score === 'number') scoresById.set(key, a.score)
    }
    return {
      valenceById,
      scoresById,
      entriesById,
      stats: query.data?.stats ?? null,
      isFetching: query.isFetching,
    }
  }, [query.data, query.isFetching])
}
