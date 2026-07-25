import type { AuthorSuggestion } from '@/api/client'

/** Human-readable provenance bucket shared by the rail and both map drilldowns. */
export function authorSuggestionSourceLabel(kind: string): string {
  if (kind === 'library_core') return 'Library-heavy'
  if (kind === 'collaborator') return 'Coauthor network'
  if (kind === 'cited_by_high_signal') return 'Cited by your ★4+ papers'
  if (kind === 'semantic_similar') return 'Semantic similarity'
  if (kind === 'openalex_related') return 'OpenAlex related authors'
  if (kind === 's2_related') return 'Semantic Scholar network'
  if (kind === 'online_search') return 'Search result'
  return 'Adjacent research area'
}

/**
 * Concrete reasons for a suggestion. New payloads carry ordered engine
 * signals; the fallback keeps older cached rows explainable.
 */
export function authorSuggestionReasons(suggestion: AuthorSuggestion): string[] {
  const signalled = (suggestion.signals ?? [])
    .map((signal) => signal.label.trim())
    .filter(Boolean)
  if (signalled.length > 0) return signalled.slice(0, 4)

  const reasons: string[] = []
  if (suggestion.shared_followed_authors.length > 0) {
    reasons.push(
      `Connected through ${suggestion.shared_followed_authors.slice(0, 2).join(' and ')}`,
    )
  } else if (suggestion.shared_followed_count > 0) {
    reasons.push(`${suggestion.shared_followed_count} shared followed authors`)
  }
  if (suggestion.shared_paper_count > 0) {
    reasons.push(`${suggestion.shared_paper_count} papers overlap with your network`)
  }
  if (suggestion.shared_topics.length > 0) {
    reasons.push(`Shared topics: ${suggestion.shared_topics.slice(0, 3).join(', ')}`)
  }
  if (suggestion.shared_venues.length > 0) {
    reasons.push(`Shared venues: ${suggestion.shared_venues.slice(0, 2).join(', ')}`)
  }
  if (reasons.length === 0 && suggestion.local_paper_count > 0) {
    reasons.push(`${suggestion.local_paper_count} tracked papers in the corpus`)
  }
  return reasons.slice(0, 4)
}
