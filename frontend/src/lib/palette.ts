/**
 * palette.ts — the SINGLE source for CATEGORICAL color.
 *
 * A *categorical* color is a hue that encodes a data CATEGORY (which signal,
 * which import source, which template kind) — NOT a semantic STATE. Semantic
 * state/role color stays on the design tokens and their primitives:
 *   - interactive identity → `accent` (links, active nav, selected/on, focus)
 *   - heavy button fill    → `primary`
 *   - state               → `success` / `warning` / `critical` / `info`
 *   - trim                → `gold`;  text ramp → `slate`;  surfaces → `surface-N`
 *
 * Categorical hues have no such token (there is no "author-affinity color"), so
 * they live here — ONE place, DRY, retunable — instead of being copy-pasted as
 * raw Tailwind classes across components. Before this module the two SIGNAL
 * maps (PaperCard + PaperHoverCard) had already DRIFTED apart.
 *
 * This is the only non-primitive file allowed to spell raw Tailwind color
 * families; `src/test/surface-guard.test.ts` enforces that every other
 * component routes color through here or the semantic tokens.
 */

/**
 * Signal-score component → progress-dot color. Where a component's meaning
 * lines up with a semantic state we reuse that token (topic=success,
 * similarity=info, recency=warning, feedback=critical); the remainder are true
 * categorical hues with no token equivalent.
 */
export const SIGNAL_COLORS: Record<string, string> = {
  source_relevance: 'bg-alma-500',
  topic_score: 'bg-success-500',
  text_similarity: 'bg-info-500',
  author_affinity: 'bg-violet-500',
  journal_affinity: 'bg-indigo-400',
  recency_boost: 'bg-warning-500',
  citation_quality: 'bg-orange-400',
  feedback_adj: 'bg-critical-500',
  preference_affinity: 'bg-fuchsia-400',
  usefulness_boost: 'bg-teal-500',
}
/** Unknown signal key → neutral dot. */
export const SIGNAL_FALLBACK_COLOR = 'bg-slate-400'

/**
 * Provenance source → Library chip classes (background + text together).
 * `feed` reuses the info token; `manual` is the neutral surface chip.
 */
export const SOURCE_COLORS: Record<string, string> = {
  import: 'bg-indigo-100 text-indigo-700',
  feed: 'bg-info-100 text-info-700',
  discovery: 'bg-violet-100 text-violet-700',
  discovery_save: 'bg-violet-100 text-violet-700',
  discovery_like: 'bg-violet-100 text-violet-700',
  discovery_manual: 'bg-violet-100 text-violet-700',
  manual: 'bg-surface-2 text-slate-600',
  library_similarity: 'bg-teal-100 text-teal-700',
  online_search: 'bg-cyan-100 text-cyan-700',
}
/** Unknown source → neutral surface chip. */
export const SOURCE_FALLBACK_COLOR = 'bg-surface-2 text-slate-600'

/**
 * Alert-template category → icon color. `feed_monitor`/`branch` reuse semantic
 * tokens (success/info); `author`/`collection` are categorical hues; the
 * fallback (workflow) uses the warning token.
 */
export const CATEGORY_ICON_COLORS: Record<string, string> = {
  author: 'text-indigo-600',
  collection: 'text-violet-600',
  feed_monitor: 'text-success-600',
  branch: 'text-info-600',
}
/** Unknown template category → warning-toned icon. */
export const CATEGORY_ICON_FALLBACK_COLOR = 'text-warning-600'

/**
 * Triage "Queue" action identity — violet, deliberately distinct from amber
 * Save / emerald Like / rose Dismiss (which are semantic tokens). Queue is the
 * one triage tone whose color is a pure identity, not a state, so it lives here.
 */
export const ACTION_QUEUE_CLASSES = {
  icon: 'text-violet-600',
  hover: 'hover:bg-violet-50 hover:text-violet-800',
  active: 'border-violet-200 bg-violet-50 text-violet-800',
}
