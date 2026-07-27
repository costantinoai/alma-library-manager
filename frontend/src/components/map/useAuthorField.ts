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
 * Keyed by author id rather than coordinates: the author map draws a subset of
 * ONE space, so an id-keyed field is view-independent by construction — the
 * host joins it onto whichever nodes it is drawing.
 *
 * NOT scoped. How you feel about an author is a fact about the author, so it is
 * averaged over ALL their papers. Scoping it gave the same person two different
 * colours depending on whether Library or Corpus was selected.
 */
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import { api } from '@/api/client'

export interface AuthorFieldEntry {
  id: string
  /** Mean valence in [-1, +1] over this author's SIGNALLED papers; null when
   *  you have no signal on any of them. */
  v: number | null
  /** How much `v` is to be believed, 0–1: 1.0 observed, the fitted field's
   *  explained-variance fraction when inferred. */
  c?: number
  /** `observed` | `predicted` | `unknown`. */
  src?: string
  /** Mean internal relevance score (0–100) over their SCORED papers. */
  score: number | null
  /** How many papers the valence rests on. */
  signal_papers: number
  /** How many papers they have at all. */
  papers: number
  /** Position in THE author space (the corpus layout), or null when this author
   *  has no place in it yet. Present so the terrain can cover the whole space
   *  regardless of which subset of authors the current view draws. */
  x: number | null
  y: number | null
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

export function useAuthorField(enabled: boolean): {
  /** Whole-space terrain points: every placed author carrying a signal, at its
   *  position in the author space. Not the drawn subset — see the module note. */
  points: Array<{ x: number; y: number; v: number }>
  /** Valence per author id — ONLY authors carrying a signal. */
  valenceById: ReadonlyMap<string, number>
  /** Confidence per author id — travels with the valence so an inferred
   *  author cannot be drawn as strongly as one you actually rated. */
  confidenceById: ReadonlyMap<string, number>
  /** Live mean internal score per author id — the Score colour mode's source. */
  scoresById: ReadonlyMap<string, number>
  /** Evidence behind each valence, for the hover card. */
  entriesById: ReadonlyMap<string, AuthorFieldEntry>
  stats: AuthorFieldStats | null
  isFetching: boolean
} {
  const query = useQuery({
    queryKey: ['author-field'],
    queryFn: () => api.get<AuthorFieldResponse>('/graphs/author-field'),
    enabled,
    // Mutations invalidate this key immediately; staleTime only prevents
    // navigation/remount refetches between signal changes.
    staleTime: 5 * 60_000,
  })

  return useMemo(() => {
    const valenceById = new Map<string, number>()
    const confidenceById = new Map<string, number>()
    const scoresById = new Map<string, number>()
    const entriesById = new Map<string, AuthorFieldEntry>()
    const points: Array<{ x: number; y: number; v: number }> = []
    for (const a of query.data?.authors ?? []) {
      // OpenAlex ids are case-insensitive; the network's node ids and the
      // authors table disagree on case, so every id channel is folded once,
      // here and at every host lookup.
      const key = a.id.trim().toLowerCase()
      entriesById.set(key, a)
      if (typeof a.v === 'number') valenceById.set(key, a.v)
      if (typeof a.c === 'number') confidenceById.set(key, a.c)
      if (typeof a.score === 'number') scoresById.set(key, a.score)
      if (typeof a.v === 'number' && typeof a.x === 'number' && typeof a.y === 'number') {
        // Flipped once, here, into the plate convention every host draws in.
        points.push({ x: a.x, y: 1 - a.y, v: a.v })
      }
    }
    return {
      points,
      valenceById,
      confidenceById,
      scoresById,
      entriesById,
      stats: query.data?.stats ?? null,
      isFetching: query.isFetching,
    }
  }, [query.data, query.isFetching])
}
