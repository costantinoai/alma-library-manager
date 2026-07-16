export const SIGNAL_COLORS: Record<string, string> = {
  source_relevance: '#3B82F6',
  topic_score: '#8B5CF6',
  text_similarity: '#EC4899',
  author_affinity: '#F59E0B',
  journal_affinity: '#10B981',
  recency_boost: '#06B6D4',
  citation_quality: '#6366F1',
  feedback_adj: '#F97316',
  preference_affinity: '#14B8A6',
  usefulness_boost: '#0F766E',
}

export const SIGNAL_LABELS: Record<string, string> = {
  source_relevance: 'Source Relevance',
  topic_score: 'Topic Overlap',
  text_similarity: 'Text Similarity',
  author_affinity: 'Author Affinity',
  journal_affinity: 'Journal Affinity',
  recency_boost: 'Recency',
  citation_quality: 'Citation Quality',
  feedback_adj: 'Feedback Adj.',
  preference_affinity: 'Preference Affinity',
  usefulness_boost: 'Usefulness Boost',
}

export const SIGNAL_DESCRIPTIONS: Record<string, string> = {
  source_relevance: 'Position in retrieval results (1st = highest)',
  topic_score: 'Topic overlap with your rated papers',
  text_similarity: 'Hybrid semantic and terminology similarity to your top-rated papers',
  author_affinity: 'Author overlap with papers you follow',
  journal_affinity: 'Published in a journal you read',
  recency_boost: 'Publication recency (newer = higher)',
  citation_quality: 'Citation count quality indicator',
  feedback_adj: 'Adjusted based on your past feedback',
  preference_affinity: 'Learned affinity from your accumulated feedback profile',
  usefulness_boost: 'Rewards timely, credible, and less redundant papers',
}

/** Compact card wording. Kept separate from the more analytical labels above,
 * but shared by PaperCard and PaperHoverCard so those two surfaces cannot drift.
 */
export const PAPER_SIGNAL_META: Record<string, { label: string; description: string }> = {
  source_relevance: { label: 'Source Relevance', description: 'Position in retrieval results (1st = highest)' },
  topic_score: { label: 'Topic Match', description: 'Topic overlap with your rated papers' },
  text_similarity: { label: 'Text Similarity', description: 'Semantic similarity to your top-rated papers' },
  author_affinity: { label: 'Author Affinity', description: 'Author overlap with papers you follow' },
  journal_affinity: { label: 'Journal Affinity', description: 'Published in a journal you read' },
  recency_boost: { label: 'Recency', description: 'Publication recency (newer = higher)' },
  citation_quality: { label: 'Citation Quality', description: 'Citation count quality indicator' },
  feedback_adj: { label: 'Your Feedback', description: 'Adjusted based on your past feedback' },
  preference_affinity: { label: 'Preference Match', description: 'Affinity learned from your accumulated feedback interactions' },
  usefulness_boost: { label: 'Usefulness', description: 'Rewards timely, credible, and less redundant papers' },
}

export const SIGNAL_ORDER = [
  'source_relevance',
  'topic_score',
  'text_similarity',
  'author_affinity',
  'journal_affinity',
  'recency_boost',
  'citation_quality',
  'feedback_adj',
  'preference_affinity',
  'usefulness_boost',
] as const

import type { ScoreBreakdown, ScoreSignalDetail } from '@/api/client'

/** Return only real scoring-signal entries from a mixed breakdown payload.
 * Backend breakdowns also contain scalar diagnostics such as `final_score`
 * and `text_similarity_mode`; card renderers must not treat those as signals.
 */
export function scoreSignalEntries(
  breakdown: object | null | undefined,
): Array<[string, ScoreSignalDetail]> {
  const signals: Array<[string, ScoreSignalDetail]> = []
  for (const [key, value] of Object.entries(breakdown ?? {})) {
    if (
      typeof value === 'object'
      && value !== null
      && typeof value.value === 'number'
      && typeof value.weight === 'number'
      && typeof value.weighted === 'number'
    ) {
      signals.push([key, value as ScoreSignalDetail])
    }
  }
  return signals
}

/** Return a mode-aware description for a scoring signal. */
export function getSignalDescription(key: string, breakdown?: ScoreBreakdown): string {
  if (key === 'text_similarity' && breakdown?.text_similarity_mode) {
    const m = breakdown.text_similarity_mode
    if (m === 'lexical') return 'Keyword similarity (no embeddings)'
    if (m === 'semantic') return 'Semantic similarity to your top-rated papers'
    if (m === 'hybrid') return 'Hybrid semantic + terminology similarity against your top-rated papers'
    return 'Text similarity (no data)'
  }
  if (key === 'topic_score' && breakdown?.topic_match_mode) {
    const m = breakdown.topic_match_mode
    if (m === 'keyword') return 'Keyword topic overlap (no embeddings)'
    if (m === 'semantic') return 'Semantic topic overlap with your rated papers'
    return 'Topic overlap (no data)'
  }
  return SIGNAL_DESCRIPTIONS[key] ?? ''
}

/** True when a signal is running in degraded (non-embedding) mode. */
export function isSignalDegraded(key: string, breakdown?: ScoreBreakdown): boolean {
  if (key === 'text_similarity') return breakdown?.text_similarity_mode === 'lexical'
  if (key === 'topic_score') return breakdown?.topic_match_mode === 'keyword'
  return false
}

export const SOURCE_TYPE_LABELS: Record<string, string> = {
  openalex_related: 'Related Works',
  openalex_topic: 'Topic Search',
  followed_author: 'Followed Authors',
  coauthor_network: 'Co-author Network',
  citation_chain: 'Citation Chain',
  semantic_scholar: 'Semantic Scholar',
  preprint_lane: 'Preprint Lane',
  taste_topic: 'Favorite Topic',
  taste_author: 'Favorite Author',
  taste_venue: 'Favorite Venue',
  recent_win: 'Recent Win',
}
