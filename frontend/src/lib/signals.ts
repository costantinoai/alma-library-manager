/* The score vocabulary, in ONE place.
 *
 * This file used to hold four drifted label maps (SIGNAL_LABELS,
 * SIGNAL_DESCRIPTIONS, PAPER_SIGNAL_META, plus a local copy in OnlineSearchTab
 * and a fifth on the backend's explain route), so `topic_score` was "Topic
 * Overlap" on one surface, "Topic Match" on another and "Topics" on a third.
 *
 * There is now no label map at all: every family and atom carries its own
 * `label` and `description` straight from the backend's `ranker.FAMILY_SPECS`,
 * which is the same table that computes the score. A name shown in the UI is
 * therefore, by construction, the name of a thing that moved the number.
 *
 * Family → colour lives in `lib/palette.ts` (the single source for categorical
 * colour): import FAMILY_COLORS / SIGNAL_FALLBACK_COLOR from there.
 */

import type { ScoreBreakdown, ScoreExplanation, ScoreFamily } from '@/api/client'

/** Canonical family order — the backend's default-weight order. Used only to
 *  stabilise rendering; the points themselves come from the payload. */
export const FAMILY_ORDER = [
  'semantic',
  'topic',
  'retrieval',
  'author',
  'lexical',
  'recency',
  'citation',
  'feedback',
  'preference',
  'venue',
] as const

/** Pull the closed score decomposition off a breakdown payload.
 *
 * Returns null for rows persisted before the current ranker, so callers render
 * "no explanation stored" rather than inventing one from the raw diagnostics
 * that sit alongside it.
 */
export function scoreExplanation(
  breakdown: ScoreBreakdown | null | undefined,
): ScoreExplanation | null {
  const explanation = breakdown?.explanation
  if (!explanation || !Array.isArray(explanation.families)) return null
  return explanation
}

/** Families that actually contributed, strongest first.
 *
 * Unavailable families and zero-point families are dropped: a row that
 * contributed nothing is noise in a "why this score" summary, and an
 * unavailable one would read as "measured zero" when it was never measured.
 */
export function contributingFamilies(
  explanation: ScoreExplanation | null | undefined,
): ScoreFamily[] {
  if (!explanation) return []
  return explanation.families
    .filter((family) => family.available && family.points > 0)
    .sort((a, b) => b.points - a.points)
}

/** The top N drivers, for compact surfaces (hover card, card teaser). */
export function topFamilies(
  breakdown: ScoreBreakdown | null | undefined,
  limit = 3,
): ScoreFamily[] {
  return contributingFamilies(scoreExplanation(breakdown)).slice(0, limit)
}

/** A family's share of the positive points, for proportional bars. */
export function familyShare(family: ScoreFamily, total: number): number {
  return total > 0 ? family.points / total : 0
}

/** Total positive points — the denominator for share-of-score bars. */
export function totalFamilyPoints(families: ScoreFamily[]): number {
  return families.reduce((sum, family) => sum + Math.max(0, family.points), 0)
}

/** True when a family is running in degraded (non-embedding) mode.
 *  Surfaced as a "keyword" tag so a low semantic/topic score reads as
 *  "embeddings unavailable" rather than "this paper is unrelated". */
export function isFamilyDegraded(key: string, breakdown?: ScoreBreakdown): boolean {
  if (key === 'semantic' || key === 'lexical') {
    return breakdown?.text_similarity_mode === 'lexical'
  }
  if (key === 'topic') return breakdown?.topic_match_mode === 'keyword'
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
  manual_search: 'Online Search',
  followed_author_monitor: 'Author Monitor',
  topic_monitor: 'Topic Monitor',
  venue_monitor: 'Venue Monitor',
  preprint_monitor: 'Preprint Monitor',
  branch_monitor: 'Branch Monitor',
  query_monitor: 'Query Monitor',
}
