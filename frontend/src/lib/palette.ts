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
 *
 * IDENTITY colour: the hue answers *where did this paper come from*, not how
 * good it is — the documented exception to the valence contract. Every fill
 * uses the SAME wash formula as `StatusBadge` (`hue-700 @ 10%` over
 * `hue-800` text) so an identity chip has identical weight, radius and
 * metrics to a semantic one; only the hue differs. They previously used the
 * heavier `hue-100 / hue-700` pair and read as a louder class of pill.
 */
export const SOURCE_COLORS: Record<string, string> = {
  import: 'bg-indigo-700/10 text-indigo-800',
  feed: 'bg-info-700/10 text-info-800',
  discovery: 'bg-violet-700/10 text-violet-800',
  discovery_save: 'bg-violet-700/10 text-violet-800',
  discovery_like: 'bg-violet-700/10 text-violet-800',
  discovery_manual: 'bg-violet-700/10 text-violet-800',
  library_similarity: 'bg-teal-700/10 text-teal-800',
  online_search: 'bg-cyan-700/10 text-cyan-800',
  // `manual` has no identity hue — it falls through to the shell's neutral.
}
/**
 * Unknown / hueless source → EMPTY, which leaves the `StatusBadge` shell on
 * its own `neutral` tone. Deliberately not a copy of that tone's classes:
 * the old fallback spelled `bg-surface-2`, a cream chip that dissolved into
 * the cream table it sat in, and any literal copy here would be a second
 * definition free to drift from the one in `status-badge.tsx`.
 */
export const SOURCE_FALLBACK_COLOR = ''

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
 * Feed monitor TYPE → chip classes (background + text) for the "why this
 * surfaced" pills on Feed cards. `venue` reuses the accent (folio) identity so
 * the pill matches the venue chip on the paper card; `author` reuses the
 * indigo it carries elsewhere; the rest are categorical hues with no token.
 *
 * Same wash formula as `SOURCE_COLORS` and `StatusBadge`: `hue-700 @ 10%`
 * over `hue-800` text. Hue says WHICH monitor; weight is identical to every
 * other pill in the app.
 */
export const MONITOR_TYPE_CHIP: Record<string, string> = {
  author: 'bg-indigo-700/10 text-indigo-800',
  topic: 'bg-success-700/10 text-success-800',
  venue: 'bg-alma-folio/10 text-alma-folio',
  preprint: 'bg-orange-700/10 text-orange-800',
  query: 'bg-cyan-700/10 text-cyan-800',
  branch: 'bg-violet-700/10 text-violet-800',
}
/** Unknown monitor type → EMPTY: the `StatusBadge` shell's own neutral tone.
 *  See the note on `SOURCE_FALLBACK_COLOR`. */
export const MONITOR_TYPE_CHIP_FALLBACK = ''

/**
 * Inbox capture channel → chip classes. IDENTITY colour, same documented
 * exception and the same wash formula as `MONITOR_TYPE_CHIP` above: the hue
 * answers *which transport did I flick this from*, never how good the paper is.
 *
 * Slack is the only channel today. A new transport registered in
 * `services/inbox_channels` adds a line here; until it does it falls through to
 * the shell's neutral, which is honest rather than mislabelled.
 */
export const CAPTURE_CHANNEL_CHIP: Record<string, string> = {
  slack: 'bg-violet-700/10 text-violet-800',
}
/** Unknown channel → EMPTY: the shell's neutral tone. */
export const CAPTURE_CHANNEL_CHIP_FALLBACK = ''

/** Human label for a capture channel id. */
export const CAPTURE_CHANNEL_LABEL: Record<string, string> = {
  slack: 'Slack',
}

/**
 * Home's inflow strip — one fill per SERIES. Categorical, because "Feed" and
 * "Discovery" are two sources, not two levels of goodness.
 *
 * Deliberately the same two hues those surfaces already wear as Library
 * provenance chips (`SOURCE_COLORS.feed`, `SOURCE_COLORS.discovery`), so a
 * colour learned on one page still means the same surface on another.
 */
export const HOME_TREND_SERIES = {
  feed: 'bg-info-500',
  discovery: 'bg-violet-500',
} as const

/**
 * Feed monitor-type mix ribbon on Home. Matches `MONITOR_TYPE_CHIP` above,
 * as solid fills for a `Meter` segment (a chip wash is too faint for a 6px
 * rail). `other` has no monitor identity, so it takes the neutral ink.
 */
export const MONITOR_MIX_FILL = {
  authors: 'bg-indigo-500',
  journals: 'bg-alma-folio',
  other: 'bg-slate-400',
} as const

/**
 * Discovery frontier map — the three layers + the branch hue ramp. This is the
 * ONE place SVG node fills are spelled. Neutrals use design tokens (var()); the
 * branch ramp is vivid categorical hues (the recs are the map's hero layer),
 * cycled by branch index. Raw hues live here only (surface-guard).
 */
export const FRONTIER_MAP = {
  /** Library = the terrain: solid neutral, grounded, not attention-grabbing. */
  library: 'var(--color-slate-500)',
  libraryEdge: 'var(--color-slate-600)',
  /** Seen = the faint frontier: ambient, receding. */
  seen: 'var(--color-slate-300)',
}
/** Branch → node color for the recommendation (hero) layer. */
export const BRANCH_MAP_COLORS: string[] = [
  '#2F80C4', // folio blue
  '#14B8A6', // teal
  '#8B5CF6', // violet
  '#F97316', // orange
  '#10B981', // emerald
  '#E11D6B', // rose
  '#06B6D4', // cyan
  '#F59E0B', // amber
  '#6366F1', // indigo
  '#D946A6', // fuchsia
]
export function branchMapColor(index: number): string {
  const n = BRANCH_MAP_COLORS.length
  return BRANCH_MAP_COLORS[((index % n) + n) % n]
}

