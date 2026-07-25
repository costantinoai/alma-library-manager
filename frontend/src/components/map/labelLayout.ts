/**
 * labelLayout — collision-free placement for every word drawn on a map (50-H).
 *
 * One pass owns ALL map text: cluster toponyms, zoom-gated node labels, word
 * clouds. Rules:
 *
 *   - greedy by priority (cluster mass first, ties broken by id — DETERMINISTIC:
 *     the same input always yields the same plate; a map that shuffles its
 *     place names on every render reads as broken);
 *   - each label tries a small ladder of candidate anchors around its point
 *     (centred, above, below, right, left) and takes the first that fits;
 *   - a label that fits nowhere is DROPPED, never stacked — the cluster stays
 *     hoverable, and the next zoom level gives it room.
 *
 * Pure function, no canvas dependency: the caller supplies measured text
 * sizes, so it unit-tests without a DOM.
 */

export interface LabelInput {
  id: string
  /** Anchor point in SCREEN px (already through the viewport transform). */
  x: number
  y: number
  /** Measured box of the rendered text in px. */
  width: number
  height: number
  /** Higher wins the ground. Cluster size for toponyms. */
  priority: number
}

export interface PlacedLabel extends LabelInput {
  /** Top-left of the resolved box. */
  left: number
  top: number
  /** Which anchor slot won (useful for leader-line decisions + tests). */
  slot: 'center' | 'above' | 'below' | 'right' | 'left'
}

interface Box {
  left: number
  top: number
  right: number
  bottom: number
}

const GAP = 4 // minimum breathing room between label boxes, px

function overlaps(a: Box, b: Box): boolean {
  return a.left < b.right + GAP && a.right > b.left - GAP && a.top < b.bottom + GAP && a.bottom > b.top - GAP
}

function inside(box: Box, w: number, h: number): boolean {
  return box.left >= 0 && box.top >= 0 && box.right <= w && box.bottom <= h
}

/** Candidate anchor ladder, in preference order. Offsets are from the anchor
 *  point to the box CENTRE; the vertical steps clear the dot itself. */
function candidates(l: LabelInput): Array<{ slot: PlacedLabel['slot']; cx: number; cy: number }> {
  const clear = l.height / 2 + 8
  return [
    { slot: 'center', cx: l.x, cy: l.y },
    { slot: 'above', cx: l.x, cy: l.y - clear },
    { slot: 'below', cx: l.x, cy: l.y + clear },
    { slot: 'right', cx: l.x + l.width / 2 + 10, cy: l.y },
    { slot: 'left', cx: l.x - l.width / 2 - 10, cy: l.y },
  ]
}

/**
 * Place `labels` inside a `width`×`height` screen, collision-free.
 *
 * Returns only the labels that found ground, each with its resolved box.
 * Deterministic: sort is total (priority desc, then id asc).
 */
export function placeLabels(
  labels: LabelInput[],
  width: number,
  height: number,
): PlacedLabel[] {
  const ordered = [...labels].sort(
    (a, b) => b.priority - a.priority || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
  )
  const taken: Box[] = []
  const placed: PlacedLabel[] = []

  for (const label of ordered) {
    for (const c of candidates(label)) {
      const box: Box = {
        left: c.cx - label.width / 2,
        top: c.cy - label.height / 2,
        right: c.cx + label.width / 2,
        bottom: c.cy + label.height / 2,
      }
      if (!inside(box, width, height)) continue
      if (taken.some((t) => overlaps(box, t))) continue
      taken.push(box)
      placed.push({ ...label, left: box.left, top: box.top, slot: c.slot })
      break
    }
    // No slot fits → dropped (never stacked). The next zoom level frees room.
  }
  return placed
}

/**
 * Split a c-TF-IDF cluster label ("lateral, visual, stream, level visual")
 * into SEPARATE toponym terms — never a joined string (user call
 * 2026-07-25: each word falls on the map where its mass sits; no comma
 * piles, no long lines). Rules:
 *   - terms keep label order (c-TF-IDF already ranks by weight);
 *   - a term whose words ALL appeared in earlier kept terms is redundant
 *     and dropped ("visual" after "level visual" adds nothing);
 *   - at most `maxTerms`; every term hard-capped at `maxChars` on a word
 *     edge (single-word overflow is truncated with an ellipsis).
 */
export function toponymTerms(label: string, maxTerms = 3, maxChars = 16): string[] {
  const seen = new Set<string>()
  const kept: string[] = []
  for (const rawTerm of label.split(',')) {
    let term = rawTerm.trim()
    if (!term) continue
    const words = term.toLowerCase().split(/\s+/)
    if (words.every((w) => seen.has(w))) continue // fully redundant
    if (term.length > maxChars) {
      const cut = term.slice(0, maxChars + 1)
      const at = cut.lastIndexOf(' ')
      term = at > 3 ? cut.slice(0, at).trimEnd() : cut.slice(0, maxChars).trimEnd() + '\u2026'
    }
    kept.push(term)
    words.forEach((w) => seen.add(w))
    if (kept.length >= maxTerms) break
  }
  return kept
}

/**
 * Drop repeated words that would land near each other (user call
 * 2026-07-25: neighbouring clusters often share a top term, so the plate
 * grew "face face face" zones). For each distinct word, the
 * highest-priority instance keeps its ground; any other instance of the
 * SAME word within `minDist` px of a kept one is dropped. Instances far
 * apart both survive \u2014 the same word CAN name two distant territories.
 * Deterministic: priority desc, id asc, like `placeLabels`.
 */
export function suppressNearbyDuplicateWords<T extends LabelInput & { word: string }>(
  inputs: T[],
  minDist: number,
): T[] {
  const ordered = [...inputs].sort(
    (a, b) => b.priority - a.priority || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
  )
  const keptSpots = new Map<string, Array<[number, number]>>()
  const keep = new Set<string>()
  const d2 = minDist * minDist
  for (const input of ordered) {
    const word = input.word.trim().toLowerCase()
    const spots = keptSpots.get(word)
    if (spots?.some(([x, y]) => (x - input.x) ** 2 + (y - input.y) ** 2 < d2)) continue
    keep.add(input.id)
    if (spots) spots.push([input.x, input.y])
    else keptSpots.set(word, [[input.x, input.y]])
  }
  return inputs.filter((i) => keep.has(i.id))
}
