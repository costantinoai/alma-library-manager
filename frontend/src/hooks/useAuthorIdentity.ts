/**
 * useAuthorIdentity — the one place "which human is this dot / row / card"
 * is answered.
 *
 * The Authors page and the Author Map both need the same four lookups, and
 * they used to derive them side by side in one component. Once the map moved
 * to the Map page (2026-07-27) that would have become two copies of a subtle
 * matching rule, on two pages, free to drift. It lives here instead; both
 * surfaces read the same React Query caches, so they cannot disagree about who
 * is followed or who is currently being suggested.
 *
 * ── Map identity: one folded key per human ────────────────────────────────
 * The map's node ids are `publication_authors.openalex_id`, stored UPPER-case;
 * `authors.id` / `followed_authors.author_id` are the same OpenAlex ids stored
 * LOWER-case. A raw `===` therefore matched NOTHING: the followed halo never
 * drew, and every popup lookup fell through to the payload's fallback.
 * OpenAlex ids are case-insensitive identifiers, so every id crossing this
 * boundary is folded through `authorKey` (2026-07-26).
 */
import { useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'

import {
  api,
  listFollowedAuthors,
  type Author,
  type AuthorSuggestion,
  type GraphNode,
} from '@/api/client'
import { authorSuggestionsQueryOptions } from '@/components/authors/authorSuggestionQueries'

/** Fold any author identifier to its comparison key. */
export function authorKey(value: string | null | undefined): string {
  return (value ?? '').trim().toLowerCase()
}

export interface AuthorIdentity {
  authors: Author[]
  followedIds: Set<string>
  /** Every known id (row id AND OpenAlex id) → the local author row. */
  authorsByKey: Map<string, Author>
  /** Keys of everyone you follow, including their OpenAlex spelling. */
  followedKeys: Set<string>
  /** Who the engine is offering right now, by every id that names them. */
  suggestionsByKey: Map<string, AuthorSuggestion>
  /** The suggestion behind a map node, or null. */
  suggestionForNode: (node: GraphNode) => AuthorSuggestion | null
  /** The single owner row (set during onboarding) → "This is you". */
  ownerId: string | null
  isLoading: boolean
  suggestions: AuthorSuggestion[]
}

export function useAuthorIdentity(): AuthorIdentity {
  const authorsQuery = useQuery({
    queryKey: ['authors'],
    queryFn: () => api.get<Author[]>('/authors'),
    retry: 1,
  })
  const followedAuthorsQuery = useQuery({
    queryKey: ['library-followed-authors'],
    queryFn: listFollowedAuthors,
    retry: 1,
  })
  // The same cached query the suggestions rail uses, so the rail and the map
  // can never disagree about who is being suggested right now.
  const suggestionsQuery = useQuery(authorSuggestionsQueryOptions())

  const authors = useMemo(() => authorsQuery.data ?? [], [authorsQuery.data])

  const followedIds = useMemo(
    () => new Set((followedAuthorsQuery.data ?? []).map((item) => item.author_id)),
    [followedAuthorsQuery.data],
  )

  const authorsByKey = useMemo(() => {
    const map = new Map<string, Author>()
    for (const a of authors) {
      map.set(authorKey(a.id), a)
      // A local row may carry its OpenAlex id in the dedicated column instead
      // of (or as well as) in `id` — index both so either reaches the person.
      if (a.openalex_id) map.set(authorKey(a.openalex_id), a)
    }
    return map
  }, [authors])

  const followedKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const item of followedAuthorsQuery.data ?? []) {
      keys.add(authorKey(item.author_id))
      const local = authorsByKey.get(authorKey(item.author_id))
      if (local?.openalex_id) keys.add(authorKey(local.openalex_id))
    }
    return keys
  }, [followedAuthorsQuery.data, authorsByKey])

  const suggestionsByKey = useMemo(() => {
    const map = new Map<string, AuthorSuggestion>()
    for (const s of suggestionsQuery.data ?? []) {
      for (const value of [s.key, s.openalex_id, s.existing_author_id]) {
        const key = authorKey(value)
        if (key) map.set(key, s)
      }
      // Same-human dedup collapses split OpenAlex profiles onto one row; the
      // dropped ids still name the same person on the map.
      for (const alt of s.alt_openalex_ids ?? []) map.set(authorKey(alt), s)
    }
    return map
  }, [suggestionsQuery.data])

  const suggestionForNode = useCallback(
    (node: GraphNode): AuthorSuggestion | null => {
      const direct =
        suggestionsByKey.get(authorKey(node.id)) ??
        suggestionsByKey.get(
          authorKey(
            typeof node.metadata?.openalex_id === 'string'
              ? node.metadata.openalex_id
              : undefined,
          ),
        )
      if (direct) return direct
      const local = authorsByKey.get(authorKey(node.id))
      return local
        ? suggestionsByKey.get(authorKey(local.id)) ??
            suggestionsByKey.get(authorKey(local.openalex_id)) ??
            null
        : null
    },
    [authorsByKey, suggestionsByKey],
  )

  const ownerId = useMemo(
    () => (followedAuthorsQuery.data ?? []).find((item) => item.is_owner)?.author_id ?? null,
    [followedAuthorsQuery.data],
  )

  return {
    authors,
    followedIds,
    authorsByKey,
    followedKeys,
    suggestionsByKey,
    suggestionForNode,
    ownerId,
    isLoading: authorsQuery.isLoading || followedAuthorsQuery.isLoading,
    suggestions: suggestionsQuery.data ?? [],
  }
}
