import type { PaperReaction } from '@/components/discovery/PaperActionBar'

/**
 * The reaction a star rating stands for — the ONE frontend copy, mirroring the
 * backend's valence (`application/library.py::rating_signal_value`): 5★ love,
 * 4★ like, 1–2★ dislike, anything else no reaction.
 *
 * Three surfaces had their own copy and two disagreed with the backend: the
 * Author and Paper detail panels matched 4 / 5 / 1 exactly, so a 2★ paper the
 * ranker counts as disliked showed no reaction there while Discovery showed
 * "disliked".
 */
export function reactionFromRating(rating: number | null | undefined): PaperReaction {
  const value = Number(rating ?? 0)
  if (value >= 5) return 'love'
  if (value >= 4) return 'like'
  if (value === 1 || value === 2) return 'dislike'
  return null
}
