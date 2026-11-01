import type { AuthorSuggestion } from '@/api/client'
import { signalLabel, type SignalKind } from '@/components/shared/signalKinds'

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

/** The provenance bucket in a word, for the card's chip. */
export function authorSuggestionSourceShort(kind: string): string {
  if (kind === 'library_core') return 'Library'
  if (kind === 'collaborator') return 'Coauthors'
  if (kind === 'cited_by_high_signal') return 'Cited'
  if (kind === 'semantic_similar') return 'Similar'
  if (kind === 'openalex_related') return 'OpenAlex'
  if (kind === 's2_related') return 'S2'
  if (kind === 'online_search') return 'Search'
  return 'Adjacent'
}

export interface AuthorChip {
  key: string
  label: string
  kind: SignalKind
  explain: string
  /** Compared across the rail to find how rare this chip is. */
  rarityKey: string
}

const SIGNAL_CHIP: Record<string, { kind: SignalKind; label: (n: string) => string }> = {
  library_paper_count: { kind: 'collection', label: (n) => `${n} saved` },
  recent_paper_count: { kind: 'year', label: (n) => `${n} recent` },
  cited_in_saved: { kind: 'cocitation', label: (n) => `Cited ×${n}` },
  cited_paper_count: { kind: 'cocitation', label: (n) => `${n} cited` },
  specter_cosine: { kind: 'topic', label: (n) => `Topic ${n}` },
  embedded_paper_count: { kind: 'meta', label: (n) => `${n} vectors` },
  shared_refs: { kind: 'coupling', label: (n) => signalLabel('coupling', n) },
  seed_cooccurrence: { kind: 'consensus', label: (n) => `Seed ×${n}` },
  coauthor: { kind: 'authors', label: () => 'Co-author' },
  shared_topics: { kind: 'topic', label: (n) => (n === '1' ? '1 topic' : `${n} topics`) },
  shared_venues: { kind: 'venue', label: (n) => (n === '1' ? '1 venue' : `${n} venues`) },
}

/** Every chip of a suggestion: short word on the card, full wording on hover or tap. */
export function authorSuggestionChips(suggestion: AuthorSuggestion): AuthorChip[] {
  const chips: AuthorChip[] = []
  const source = authorSuggestionSourceLabel(suggestion.suggestion_type)
  chips.push({
    key: 'source',
    label: authorSuggestionSourceShort(suggestion.suggestion_type),
    kind: 'channel',
    explain: `Suggested by: ${source}.`,
    rarityKey: `source:${suggestion.suggestion_type}`,
  })
  if (suggestion.consensus_count && suggestion.consensus_count >= 2) {
    chips.push({
      key: 'consensus',
      label: signalLabel('consensus', suggestion.consensus_count),
      kind: 'consensus',
      explain: `Found by ${suggestion.consensus_count} sources: ${(suggestion.consensus_buckets ?? []).join(', ')}.`,
      rarityKey: 'consensus',
    })
  }
  const pull = suggestion.paper_signal_adjustment
  if (typeof pull === 'number' && Math.abs(pull) >= 1) {
    chips.push({
      key: 'taste',
      label: pull > 0 ? `+${pull.toFixed(1)} saves` : `${pull.toFixed(1)} rejects`,
      kind: pull > 0 ? 'taste-match' : 'taste-avoid',
      explain: 'Net pull from your saved, rated and removed papers in this area.',
      rarityKey: `taste:${pull > 0}`,
    })
  }
  ;(suggestion.signals ?? []).forEach((signal, index) => {
    const spec = SIGNAL_CHIP[signal.kind]
    const value = String(signal.count ?? signal.label.match(/\d+(\.\d+)?/)?.[0] ?? '')
    const full = signal.subject && signal.subject !== signal.label
      ? `${signal.label}: ${signal.subject}.`
      : `${signal.label}.`
    chips.push({
      key: `${signal.kind}-${index}`,
      // The backend reuses this kind for corpus counts; those are not saves.
      label: signal.label.includes('corpus') ? `${value} corpus` : spec ? spec.label(value) : signal.label,
      kind: spec?.kind ?? 'meta',
      explain: full.charAt(0).toUpperCase() + full.slice(1),
      rarityKey: signal.kind,
    })
  })
  return chips
}
