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
 * Home category → title, pill, sticky-note, and chart families.
 *
 * These are categorical, not status colours: green always means Inbox,
 * violet Reading list, gold the editorial Picked shelf, teal Discovery, and
 * magenta Feed. Every consumer derives from this registry.
 */
export type HomeSectionThemeKey =
  | 'inbox'
  | 'reading'
  | 'picked'
  | 'discovery'
  | 'feed'

export interface HomeSectionTheme {
  title: string
  icon: string
  chip: string
  /**
   * Chip worn by anything sitting ON the sticky note (a tile's eyebrow).
   *
   * A note is a coloured sheet, so a chip that keeps its own hue there reads as
   * a clash, not as information — a purple Slack chip on the green Inbox note
   * (user report 2026-07-27). One step stronger than `chip` because it
   * composites over the note's own wash rather than over paper, and it must
   * still separate from it.
   */
  noteChip: string
  noteSurface: string
  notePlate: string
  noteFold: string
  /**
   * SOLID fill for a bar / column / legend dot in this theme's colour.
   *
   * Always `hue-600` — the same step `Meter`'s semantic fills use, one brighter
   * than the `hue-700` of this theme's text and chip wash. A bar is bare colour
   * with nothing around it to establish the hue, so it carries one step more
   * than a wash sitting behind words. These used to be a private per-theme
   * guess (`success-500`, `violet-500`, `gold-400`, `cyan-600`, `fuchsia-500`):
   * five different steps, so Home's inflow columns and the "Feed + Discovery"
   * caption under them were visibly different colours.
   */
  series: string
}

export const HOME_SECTION_THEMES: Record<HomeSectionThemeKey, HomeSectionTheme> = {
  inbox: {
    title: 'text-success-800',
    icon: 'text-success-700',
    chip: 'border-success-700/10 bg-success-700/10 text-success-800',
    noteChip: 'border-success-700/20 bg-success-700/[0.16] text-success-800',
    noteSurface: '!border-success-700/20 !bg-success-700/[0.07]',
    notePlate: '!border-success-700/20 !bg-success-700/[0.11]',
    noteFold: 'border-t-success-700/25',
    series: 'bg-success-600',
  },
  reading: {
    title: 'text-violet-800',
    icon: 'text-violet-700',
    chip: 'border-violet-700/10 bg-violet-700/10 text-violet-800',
    noteChip: 'border-violet-700/20 bg-violet-700/[0.15] text-violet-800',
    noteSurface: '!border-violet-700/20 !bg-violet-700/[0.065]',
    notePlate: '!border-violet-700/20 !bg-violet-700/[0.1]',
    noteFold: 'border-t-violet-700/25',
    series: 'bg-violet-600',
  },
  picked: {
    title: 'text-gold-700',
    icon: 'text-gold-600',
    chip: 'border-gold-700/10 bg-gold-700/10 text-gold-700',
    noteChip: 'border-gold-700/20 bg-gold-700/[0.17] text-gold-700',
    noteSurface: '!border-gold-700/20 !bg-gold-700/[0.075]',
    notePlate: '!border-gold-700/20 !bg-gold-700/[0.115]',
    noteFold: 'border-t-gold-700/25',
    series: 'bg-gold-600',
  },
  discovery: {
    title: 'text-cyan-800',
    icon: 'text-cyan-700',
    chip: 'border-cyan-700/10 bg-cyan-700/10 text-cyan-800',
    noteChip: 'border-cyan-700/20 bg-cyan-700/[0.16] text-cyan-800',
    noteSurface: '!border-cyan-700/20 !bg-cyan-700/[0.07]',
    notePlate: '!border-cyan-700/20 !bg-cyan-700/[0.105]',
    noteFold: 'border-t-cyan-700/25',
    series: 'bg-cyan-600',
  },
  feed: {
    title: 'text-fuchsia-800',
    icon: 'text-fuchsia-700',
    chip: 'border-fuchsia-700/10 bg-fuchsia-700/10 text-fuchsia-800',
    noteChip: 'border-fuchsia-700/20 bg-fuchsia-700/[0.15] text-fuchsia-800',
    noteSurface: '!border-fuchsia-700/20 !bg-fuchsia-700/[0.06]',
    notePlate: '!border-fuchsia-700/20 !bg-fuchsia-700/[0.095]',
    noteFold: 'border-t-fuchsia-700/25',
    series: 'bg-fuchsia-600',
  },
}

/**
 * Page → identity colour.
 *
 * Answers *which surface am I on*, never *how good is this* — the documented
 * identity-colour exception, same rule as Feed monitor chips and Library
 * provenance chips. The hue appears in exactly one place per page: the glyph
 * medallion in that page's `PageIntro`, plus the banded sections that belong
 * to it. Everything else on the page keeps colour for valence.
 *
 * Reuses the hues the app had already settled on where one existed — Feed is
 * magenta and Discovery teal because `HOME_SECTION_THEMES` already says so on
 * the desk, Authors is indigo because `CATEGORY_ICON_COLORS.author` is, and
 * Library is violet because that is the Reading hue. Deriving them here rather
 * than re-typing the classes is what keeps the desk and the page agreeing.
 *
 * Settings is deliberately hueless: it is plumbing, not a subject.
 */
export type PageThemeKey =
  | 'home'
  | 'feed'
  | 'discovery'
  | 'map'
  | 'authors'
  | 'library'
  | 'alerts'
  | 'health'
  | 'insights'
  | 'settings'

export interface PageTheme {
  /** Glyph tint. */
  icon: string
  /** Wash behind the glyph — the shared chip formula, `hue-700 @ 10%`. */
  medallion: string
  /** Count-pill classes, shaped to drop straight into `PageSection`'s
   *  `categoryTheme` so a page's bands inherit its identity. */
  chip: string
  /**
   * Ink for a MASTHEAD lede — the one place a page's identity hue carries the
   * words themselves rather than just the glyph beside them.
   *
   * Optional, and today only Home defines it: Home's lede is a greeting, not a
   * thesis, and set in the same `alma-800` as every heading on the page it read
   * as one more section title. A page whose lede is a thesis keeps ink.
   */
  masthead?: string
}

export const PAGE_THEMES: Record<PageThemeKey, PageTheme> = {
  home: {
    icon: HOME_SECTION_THEMES.picked.icon,
    medallion: 'bg-gold-700/10',
    chip: HOME_SECTION_THEMES.picked.chip,
    masthead: HOME_SECTION_THEMES.picked.title,
  },
  feed: {
    icon: HOME_SECTION_THEMES.feed.icon,
    medallion: 'bg-fuchsia-700/10',
    chip: HOME_SECTION_THEMES.feed.chip,
  },
  // Green, not the cyan the Home desk uses for the Discovery *category*
  // (user call 2026-07-27). NOTE the hazard: `success` is emerald, and a
  // Discovery card is full of emerald valence chips ("Matches what you save").
  // Identity green and success green sit on the same page, so this hue is kept
  // to structural chrome only — never a chip, never a meter.
  discovery: {
    icon: 'text-green-700',
    medallion: 'bg-green-700/10',
    chip: 'border-green-700/10 bg-green-700/10 text-green-800',
  },
  map: {
    icon: 'text-teal-700',
    medallion: 'bg-teal-700/10',
    chip: 'border-teal-700/10 bg-teal-700/10 text-teal-800',
  },
  authors: {
    icon: 'text-indigo-700',
    medallion: 'bg-indigo-700/10',
    chip: 'border-indigo-700/10 bg-indigo-700/10 text-indigo-800',
  },
  library: {
    icon: HOME_SECTION_THEMES.reading.icon,
    medallion: 'bg-violet-700/10',
    chip: HOME_SECTION_THEMES.reading.chip,
  },
  alerts: {
    icon: 'text-orange-700',
    medallion: 'bg-orange-700/10',
    chip: 'border-orange-700/10 bg-orange-700/10 text-orange-800',
  },
  health: {
    icon: 'text-emerald-700',
    medallion: 'bg-emerald-700/10',
    chip: 'border-emerald-700/10 bg-emerald-700/10 text-emerald-800',
  },
  insights: {
    icon: 'text-sky-700',
    medallion: 'bg-sky-700/10',
    chip: 'border-sky-700/10 bg-sky-700/10 text-sky-800',
  },
  settings: {
    icon: 'text-slate-600',
    medallion: 'bg-slate-700/10',
    chip: 'border-slate-700/10 bg-slate-700/10 text-slate-700',
  },
}

/**
 * Ranking family → bar / dot colour. Identity colour, not valence: the hue
 * answers WHICH family, never how good the paper is (a big red `feedback` bar
 * is your own strong approval, not a warning). Where a family's meaning lines
 * up with a semantic state we reuse that token; the rest are true categorical
 * hues with no token equivalent.
 *
 * Keys are the ten `ranker.FAMILY_SPECS` keys — the same strings the backend
 * puts in `explanation.families[].key`.
 */
export const FAMILY_COLORS: Record<string, string> = {
  semantic: 'bg-info-500',
  topic: 'bg-success-500',
  retrieval: 'bg-alma-500',
  author: 'bg-violet-500',
  // Lexical and citation are deliberately DARKER siblings of semantic and
  // recency rather than neighbouring hues: ten categorical colours exhaust the
  // legible hue wheel, so the last two pairs separate on value instead. A
  // sky-400 lexical beside an info-500 semantic, and an orange-400 citation
  // beside a warning-500 recency, were indistinguishable in the 10px swatches
  // and in the composition ribbon (2026-07-28).
  lexical: 'bg-teal-600',
  recency: 'bg-warning-500',
  citation: 'bg-amber-700',
  feedback: 'bg-critical-500',
  preference: 'bg-fuchsia-400',
  venue: 'bg-indigo-400',
}
/** Unknown family key → neutral dot. */
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
  feed: HOME_SECTION_THEMES.feed.chip,
  discovery: HOME_SECTION_THEMES.discovery.chip,
  discovery_save: HOME_SECTION_THEMES.discovery.chip,
  discovery_like: HOME_SECTION_THEMES.discovery.chip,
  discovery_manual: HOME_SECTION_THEMES.discovery.chip,
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
  feed: HOME_SECTION_THEMES.feed.series,
  discovery: HOME_SECTION_THEMES.discovery.series,
} as const

/**
 * Feed monitor-type mix ribbon on Home. Matches `MONITOR_TYPE_CHIP` above,
 * as solid fills for a `Meter` segment (a chip wash is too faint for a 6px
 * rail). `other` has no monitor identity, so it takes the neutral ink.
 *
 * Same rule as `HomeSectionTheme.series`: `hue-600`, the shared solid-bar step.
 * `indigo-500` beside an `indigo-800` chip label read as two different
 * categories.
 */
export const MONITOR_MIX_FILL = {
  authors: 'bg-indigo-600',
  journals: 'bg-alma-folio',
  other: 'bg-slate-500',
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
