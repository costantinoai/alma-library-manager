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
