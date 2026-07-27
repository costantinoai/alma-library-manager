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
 *   outline ring    = state/provenance (selected = folio accent, hover = ink,
 *                     author suggestion = brand gold)
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
  /** Brand gold — persistent author-suggestion provenance outline. */
  suggestionGold: '#C49A45',
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
  // Authors map — the SAME three-tier common space as the paper maps, on the
  // same channels (fill = yours, hollow = suggested, faint = context). One
  // shared plate: the people you already follow or have saved, the people the
  // engine is offering you, and the rest of the corpus, all placed by what they
  // write about. Separate kinds from the paper tiers only so the registry can
  // own author-accurate legend words (2026-07-26).
  | 'author_library' // followed, or a co-author of a paper you saved
  | 'author_suggested' // currently offered in the author suggestions
  | 'author_corpus' // every other author in scope — context

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
  author_library: {
    filled: true,
    opacity: 1.0,
    radius: 3.6,
    defaultColor: MAP_INK.library,
    legend: 'Yours — followed or in your library',
  },
  author_suggested: {
    filled: false,
    opacity: 0.9,
    // Same near-equal radius rule as the paper tiers: ownership is the
    // fill-vs-hollow channel, so size must not also shout "other species".
    radius: 3.9,
    defaultColor: MAP_INK.accent,
    legend: 'Suggested to follow — hollow',
  },
  author_corpus: {
    filled: true,
    opacity: 0.4,
    radius: 3,
    defaultColor: MAP_INK.ambient,
    legend: 'Other author in scope — faint',
  },
}

/**
 * Draw order = z order: ambient context first, hero layer last. The renderer
 * paints in this order and the legend LISTS in this order, so the reading
 * order of the key matches the stacking order on the plate.
 */
export const MAP_NODE_DRAW_ORDER: readonly MapNodeKind[] = [
  'seen',
  'corpus',
  'author_corpus',
  'author_library',
  'library',
  'author_suggested',
  'suggestion',
]

/** Opacity applied to nodes OUTSIDE the active selection/filter. Dimmed,
 *  never hidden — the territory stays visible while a region is in focus. */
export const DIMMED_OPACITY = 0.15

/** Ring widths (px at zoom 1). Selection ring is ALWAYS the folio accent. */
export const SELECTION_RING = { color: MAP_INK.accent, width: 2.5 }
export const HOVER_RING = { color: MAP_INK.hoverRing, width: 1.5 }
export const SUGGESTION_OUTLINE = { color: MAP_INK.suggestionGold, width: 2 }

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

/** Edge-layer palette — one owner for every map's citation/coupling lines
 *  (moved from graphs/graphConfig at 50-K retirement; the legacy stack is
 *  gone, this registry is the only place an edge picks a colour). */
export const EDGE_LAYER_COLORS: Record<string, string> = {
  semantic: 'rgba(59,130,246,0.45)',
  bibliographic_coupling: 'rgba(139,92,246,0.38)',
  co_authorship: 'rgba(16,185,129,0.38)',
  co_citation: 'rgba(236,72,153,0.38)',
  topic: 'rgba(245,158,11,0.35)',
}
export const EDGE_LAYER_FALLBACK_COLOR = 'rgba(203,213,225,0.30)'
export const EDGE_LAYER_LABELS: Record<string, string> = {
  semantic: 'Semantic (nearest work)',
  bibliographic_coupling: 'Shared references',
  co_authorship: 'Shared authors',
  co_citation: 'Cited together',
  topic: 'Shared topic',
}

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


/** Percentile colour limits for the Year ramp: p10–p90 of the observed
 *  years (clamped), so one 1950s outlier can't flatten everything modern
 *  into a single dark tone. Returns null when there's nothing to ramp. */
export function yearRampLimits(years: number[]): { lo: number; hi: number } | null {
  const ys = years.filter((y) => Number.isFinite(y) && y > 1800).sort((a, b) => a - b)
  if (ys.length === 0) return null
  const lo = ys[Math.floor(ys.length * 0.1)]
  const hi = ys[Math.min(ys.length - 1, Math.floor(ys.length * 0.9))]
  return hi > lo ? { lo, hi } : { lo: ys[0], hi: ys[ys.length - 1] || ys[0] + 1 }
}

/** Viridis — perceptually uniform, colour-blind-safe; the standard for a
 *  sequential data ramp (user call 2026-07-25). Five control points,
 *  linearly interpolated. */
const VIRIDIS_STOPS: Array<[number, number, number]> = [
  [68, 1, 84], // 0.00 #440154
  [59, 82, 139], // 0.25 #3B528B
  [33, 145, 140], // 0.50 #21918C
  [94, 201, 98], // 0.75 #5EC962
  [253, 231, 37], // 1.00 #FDE725
]

export function viridis(t: number): string {
  const x = Math.max(0, Math.min(1, t)) * (VIRIDIS_STOPS.length - 1)
  const i = Math.min(VIRIDIS_STOPS.length - 2, Math.floor(x))
  const f = x - i
  const a = VIRIDIS_STOPS[i]
  const b = VIRIDIS_STOPS[i + 1]
  const mix = (k: number) => Math.round(a[k] + (b[k] - a[k]) * f)
  return `rgb(${mix(0)}, ${mix(1)}, ${mix(2)})`
}

/** Viridis recency ramp for a year within [lo, hi] (clamped): dark violet =
 *  oldest, bright yellow = newest. */
export function yearRampColor(year: number, lo: number, hi: number): string {
  return viridis((year - lo) / Math.max(1, hi - lo))
}


/** Min / max / mean of a value list — the numbers every colourbar owes the
 *  reader. Null on empty input. */
export function summarizeValues(values: number[]): { min: number; max: number; mean: number } | null {
  const vs = values.filter((v) => Number.isFinite(v))
  if (vs.length === 0) return null
  let min = Infinity
  let max = -Infinity
  let sum = 0
  for (const v of vs) {
    if (v < min) min = v
    if (v > max) max = v
    sum += v
  }
  return { min, max, mean: sum / vs.length }
}

type Stop = readonly [number, readonly [number, number, number]]

/** Interpolate a stop table at `x`. THE one ramp implementation.
 *
 *  Every colour ramp on the map goes through this. Score used to carry a
 *  hand-rolled red/yellow/green mix inlined in `GraphMapView`, which both
 *  duplicated the maths and made Score look identical to Terrain — two
 *  different questions wearing the same colours. */
function interpolateStops(stops: ReadonlyArray<Stop>, x: number): [number, number, number] {
  const lo = stops[0][0]
  const hi = stops[stops.length - 1][0]
  const v = Math.max(lo, Math.min(hi, x))
  for (let i = 0; i < stops.length - 1; i++) {
    const [t0, c0] = stops[i]
    const [t1, c1] = stops[i + 1]
    if (v <= t1 || i === stops.length - 2) {
      const k = t1 === t0 ? 0 : (v - t0) / (t1 - t0)
      return [
        Math.round(c0[0] + (c1[0] - c0[0]) * k),
        Math.round(c0[1] + (c1[1] - c0[1]) * k),
        Math.round(c0[2] + (c1[2] - c0[2]) * k),
      ]
    }
  }
  const last = stops[stops.length - 1][1]
  return [last[0], last[1], last[2]]
}

/** CSS gradient GENERATED from a stop table, so a bar can never disagree with
 *  the pixels it labels. Positions are derived from each stop's own value. */
function gradientFromStops(stops: ReadonlyArray<Stop>): string {
  const lo = stops[0][0]
  const hi = stops[stops.length - 1][0]
  const span = hi - lo || 1
  const parts = stops.map(
    ([t, c]) => `rgb(${c.join(',')}) ${Math.round(((t - lo) / span) * 100)}%`,
  )
  return `linear-gradient(to right, ${parts.join(', ')})`
}

/** Terrain scale — FIXED, never data-derived, so two maps stay comparable and
 *  a weak +0.03 cannot become the green endpoint merely because it is the
 *  largest value in an author population (user catch, 2026-07-26).
 *
 *  ±0.5 rather than ±1 because that is where the valence contract lives:
 *  `signal_valence` puts a saved paper at +0.35 and caps engine evidence at
 *  ±0.5, so ±1 needs an explicit 1★/5★ rating. Measured on the real field,
 *  96.9% of points sit inside ±0.5 and the median is 0.16 — on a ±1 domain the
 *  map used only the middle sliver of the ramp and read as washed-out cream.
 *  Beyond ±0.5 values clamp, which is the honest reading of "as strong as this
 *  scale can say". */
export const TERRAIN_SCALE_ABS_MAX = 0.5
/** Floor / ceiling for the auto-derived terrain scale. */
export const TERRAIN_SCALE_MIN = 0.08
export const TERRAIN_SCALE_MAX = 1
/** Which quantile of |valence| becomes the scale endpoint.
 *
 *  **The colourbar is the field's real range, symmetric about zero**, trimmed
 *  at p95 so a handful of outliers cannot set it. Only the strongest 5% clip.
 *
 *  ONE knob, for every terrain on every map. `terrainScaleFor` is reached only
 *  through `terrainField.ts` → `useMapField`, which is what all three hosts
 *  (Map papers, Map authors, Discovery) use — so changing this number changes
 *  every colourbar at once, and no host can hold a different opinion about what
 *  green means. `oneFieldOwner.test.ts` is what keeps that true.
 *
 *  It was p75, which is a QUARTER of every point drawn at full saturation, and
 *  on a skewed field it collapses (measured on dev, 2026-07-28):
 *
 *  | field | p75 \|v\| | p95 \|v\| | max \|v\| |
 *  |---|---|---|---|
 *  | papers  | 0.26  | 0.455 | 1.00 |
 *  | authors | 0.071 | 0.333 | 0.48 |
 *
 *  The author field is mostly small POSITIVE predictions (83% above zero,
 *  median 0.015), so a p75 endpoint of 0.071 — below the 0.08 floor — rendered
 *  a 0.02 prediction at t=0.25 and a 0.05 one at t=0.6. The map came out green
 *  nearly everywhere, claiming an opinion the numbers do not support. That is
 *  the user report this replaces (2026-07-28).
 *
 *  Why the earlier "p75 or it reads pale" reasoning no longer holds: two other
 *  things were fixed in the same session. The ramp is standard RdYlGn at
 *  natural spacing (yellow used to be squeezed into ±0.12, so anything outside
 *  a sliver ran to saturated colour), and alpha no longer fades a second time
 *  with |t|. A weak value now LOOKS weak instead of being erased, so the scale
 *  no longer has to lie about magnitude to make the plate visible.
 *
 *  The floor (TERRAIN_SCALE_MIN) is what guards the opposite failure — a
 *  near-empty field stretched until noise looks like strong opinion. */
export const TERRAIN_SCALE_QUANTILE = 0.95

/** The ±scale this field should be drawn on, derived from the field itself.
 *
 *  A FIXED domain is what kept making the map unreadable. Valence is bounded by
 *  ±1 in principle, but a real field never spends that range: papers sit mostly
 *  inside ±0.2, and the author field — where most values are GP predictions
 *  shrunk toward zero by low confidence — is smaller again. Drawn on a fixed
 *  ±0.5 both looked like blank paper.
 *
 *  The old rule this replaces was "never data-derived, or a weak population
 *  saturates". That concern is real but it is about the MAX, not about being
 *  fixed: normalising to the largest value lets one outlier define the scale.
 *  So this uses a robust high quantile, not the max, and clamps it — the floor
 *  stops a near-empty field being blown up into strong opinion, the ceiling
 *  keeps it inside the semantic domain. Two maps of comparable fields still get
 *  comparable scales, and the legend prints the number either way. */
export function terrainScaleFor(values: ReadonlyArray<number>): number {
  const magnitudes = values
    .map((v) => Math.abs(v))
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b)
  if (!magnitudes.length) return TERRAIN_SCALE_MIN
  const cut = magnitudes[
    Math.min(magnitudes.length - 1, Math.floor(magnitudes.length * TERRAIN_SCALE_QUANTILE))
  ]
  return Math.min(TERRAIN_SCALE_MAX, Math.max(TERRAIN_SCALE_MIN, Number(cut.toFixed(2))))
}

/** Colourbar descriptor for a given scale — gradient and bounds from one place. */
export function terrainLegendFor(absMax: number) {
  return {
    gradient: TERRAIN_LEGEND_GRADIENT,
    min: `−${absMax}`,
    mid: '0',
    max: `+${absMax}`,
  }
}

/** ColorBrewer **RdYlGn** at natural spacing — the standard diverging ramp.
 *
 *  Stops are in NORMALISED units (−1 … +1), i.e. valence divided by
 *  `TERRAIN_SCALE_ABS_MAX`. Previously yellow was squeezed into ±0.12 of this
 *  range, so anything outside a sliver ran to saturated red or green and a map
 *  of mostly-small values read as two angry blocks. */
const TERRAIN_STOPS: ReadonlyArray<Stop> = [
  [-1.0, [215, 25, 28]],
  [-0.5, [253, 174, 97]],
  [0.0, [255, 255, 191]],
  [0.5, [166, 217, 106]],
  [1.0, [26, 150, 65]],
]

/** Terrain colour for a NORMALISED valence in [-1, +1]. */
export function terrainColor(t: number): [number, number, number] {
  return interpolateStops(TERRAIN_STOPS, t)
}

/** Engine relevance score — a 0–100 magnitude, so a SEQUENTIAL ramp.
 *
 *  Deliberately not the terrain ramp. Terrain answers "do you like it" and is
 *  diverging about a meaningful zero; score answers "how strongly did the
 *  engine rank it" and has no negative half — 20 is not "disliked", it is
 *  "weakly ranked". Sharing red/yellow/green made the two read as the same
 *  measurement. Single-hue blue also keeps it clear of the year ramp (viridis).
 *  Low scores stay pale and recessive; high scores darken and draw the eye. */
const SCORE_STOPS: ReadonlyArray<Stop> = [
  [0, [198, 219, 239]],
  [50, [66, 146, 198]],
  [100, [8, 48, 107]],
]

/** Colour for an internal relevance score on its fixed 0–100 domain. */
export function scoreRampColor(score: number): [number, number, number] {
  return interpolateStops(SCORE_STOPS, score)
}

/** Everything a colourbar needs, DERIVED from the ramp it describes.
 *
 *  The legend used to hardcode "-1"/"0"/"1" beside a gradient built somewhere
 *  else; narrowing the terrain domain left the bar labelling a range the ramp
 *  no longer used. Bounds and gradient now come from one place, so changing a
 *  scale updates its bar automatically. */
const TERRAIN_LEGEND_GRADIENT = gradientFromStops(TERRAIN_STOPS)
export const TERRAIN_LEGEND = terrainLegendFor(TERRAIN_SCALE_ABS_MAX)

export const SCORE_LEGEND = {
  gradient: gradientFromStops(SCORE_STOPS),
  min: String(SCORE_STOPS[0][0]),
  mid: String(SCORE_STOPS[Math.floor(SCORE_STOPS.length / 2)][0]),
  max: String(SCORE_STOPS[SCORE_STOPS.length - 1][0]),
} as const

/** CSS gradients matching the canvas ramps exactly. */
export const RAMP_GRADIENTS = {
  terrain: TERRAIN_LEGEND.gradient,
  score: SCORE_LEGEND.gradient,
  year: 'linear-gradient(to right, #440154, #3B528B, #21918C, #5EC962, #FDE725)',
} as const
