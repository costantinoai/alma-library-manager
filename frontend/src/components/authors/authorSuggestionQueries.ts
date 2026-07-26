import { queryOptions } from '@tanstack/react-query'

import { listAuthorSuggestions } from '@/api/client'

// The server route enforces limit <= 30. Fetch one stable plate/rail payload;
// actions explicitly invalidate it, while periodic background jobs own remote
// suggestion discovery. Navigating to Authors must never start that work.
export const AUTHOR_SUGGESTION_FETCH_COUNT = 30
export const AUTHOR_SUGGESTIONS_STALE_TIME = 6 * 60 * 60_000

export function authorSuggestionsQueryOptions() {
  return queryOptions({
    queryKey: ['author-suggestions', AUTHOR_SUGGESTION_FETCH_COUNT],
    queryFn: () => listAuthorSuggestions(AUTHOR_SUGGESTION_FETCH_COUNT),
    retry: 1,
    staleTime: AUTHOR_SUGGESTIONS_STALE_TIME,
  })
}
