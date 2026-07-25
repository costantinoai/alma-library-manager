/**
 * mapNodeStyle — the ONE owner of what a dot on a semantic map means (50-E).
 *
 * Mirrors the SignalChip contract: nothing outside this registry picks a map
 * colour, radius, opacity, or ring. Channels never double-book:
 *
 *   fill vs hollow  = ownership     (yours = FILLED · candidate = HOLLOW ring)
 *   colour          = grouping only (branch / cluster hue, library ink,
 *                     seen slate) — never quality, never state
 *   opacity         = layer weight  (context 1.0 · hero 0.9 · ambient 0.25 ·
 *                     dimmed-by-filter 0.15 — dimmed, never hidden)
 *   outline ring    = transient state (selected = folio accent, hover = ink)
 *   size            = one magnitude at a time (citations log-scale or uniform)
 *
 * Hosts (Discovery frontier, Map page, Authors network) pass a `kind` per
 * node; the renderer and the legend both read THIS table, so they can never
 * disagree. Dismissed/removed papers are never rendered at all (D6/D3) — that
 * exclusion happens server-side and is not a style.
 */

/** Resolved CSS custom properties are unavailable inside a <canvas>, so the
 *  registry carries literal hexes for the two neutral inks. They mirror
 *  `--color-slate-*` / alma navy from index.css — change them together. */
export const MAP_INK = {
  /** Library ink — navy alma-800, the app's "yours" colour. */
  library: '#1E3A5F',
  libraryEdge: '#16293F',
  /** Seen / unclustered — receding slate. */
  ambient: '#94A3B8',
  ambientSoft: '#CBD5E1',
  /** Folio accent — selection ring + lasso ONLY (accent = selected, always). */
  accent: '#2F80C4',
  /** Hover ring — ink, weaker than selection. */
  hoverRing: '#475569',
  /** Toponym (cluster label) ink on the cool map field. */
  toponym: '#334155',
  toponymHalo: 'rgba(248, 250, 252, 0.85)',
} as const

export type MapNodeKind =
  | 'library' // a paper you saved — the terrain
  | 'suggestion' // a candidate the engine surfaced — the hero layer
  | 'seen' // seen-but-unacted — ambient history
  | 'corpus' // tracked corpus paper (Map page base layer)
  | 'author' // an author node (Authors network host)

export interface MapNodeStyle {
  /** FILLED disc (yours) vs HOLLOW ring-dot (not yours yet). */
  filled: boolean
  /** Layer opacity — see channel table above. */
  opacity: number
  /** Base radius in px at zoom 1 (before any size-by scaling). */
  radius: number
  /** Fallback colour when the host provides no grouping colour. */
  defaultColor: string
  /** Legend line — the registry IS the legend source (50-E). */
  legend: string
}

export const MAP_NODE_STYLES: Record<MapNodeKind, MapNodeStyle> = {
  library: {
    filled: true,
    opacity: 1.0,
    radius: 3.5,
    defaultColor: MAP_INK.library,
    legend: 'In your library — filled',
  },
  suggestion: {
    filled: false,
    opacity: 0.9,
    // Close to the library radius: ownership is the fill-vs-hollow channel,
    // so size must not ALSO scream "different species" (user call 2026-07-25).
    radius: 3.8,
    defaultColor: MAP_INK.accent,
    legend: 'Suggested — hollow, coloured by branch',
  },
  seen: {
    filled: true,
    opacity: 0.25,
    radius: 2.5,
    defaultColor: MAP_INK.ambient,
    legend: 'Seen, not acted on — faint',
  },
  corpus: {
    filled: true,
    opacity: 0.55,
    radius: 3,
    defaultColor: MAP_INK.ambient,
    legend: 'Tracked corpus paper',
  },
  author: {
    filled: true,
    opacity: 1.0,
    radius: 4,
    defaultColor: MAP_INK.library,
    legend: 'Author — sized by publications',
  },
}

/** Opacity applied to nodes OUTSIDE the active selection/filter. Dimmed,
 *  never hidden — the territory stays visible while a region is in focus. */
export const DIMMED_OPACITY = 0.15

/** Ring widths (px at zoom 1). Selection ring is ALWAYS the folio accent. */
export const SELECTION_RING = { color: MAP_INK.accent, width: 2.5 }
export const HOVER_RING = { color: MAP_INK.hoverRing, width: 1.5 }

/** Hollow ring stroke width for suggestion dots. */
export const HOLLOW_STROKE_WIDTH = 1.4

/** Faint interior wash inside hollow dots (same hue as the ring, mostly
 *  transparent) — keeps them readable as dots, not holes, next to the small
 *  filled library points. */
export const HOLLOW_FILL_ALPHA = 0.18

/**
 * The map field sits ON the app's warm paper ladder — one step brighter than
 * its host card (surface-3 white-cream), so it reads as the plate the dots
 * are printed on without ever going cool. (A first pass used a cool
 * blue-grey field; it clashed with the parchment tones — user call
 * 2026-07-25. The atlas identity lives in the toponyms, not the water.)
 */
export const MAP_FIELD = {
  background: '#FFFEF9', // = --color-surface-3
  vignette: 'rgba(30, 58, 95, 0.04)',
  edgeLine: 'rgba(120, 108, 84, 0.30)', // warm ink hairline, not slate
} as const

/** Size-by policies — exactly one magnitude channel at a time (50-E). */
export function radiusFor(kind: MapNodeKind, sizeValue: number | null, maxValue: number): number {
  const base = MAP_NODE_STYLES[kind].radius
  if (sizeValue == null || maxValue <= 0) return base
  // log scale: a 10k-citation classic reads bigger, not 3000× bigger.
  const t = Math.log1p(Math.max(0, sizeValue)) / Math.log1p(maxValue)
  // Gentle range (≤ ~1.7× base): magnitude is a whisper, not a second
  // ownership channel — big hollow rings next to small filled dots read as
  // two different objects (user call 2026-07-25).
  return base + t * base * 0.7
}
