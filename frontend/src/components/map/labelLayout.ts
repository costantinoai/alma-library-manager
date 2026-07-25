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
 * Compact a c-TF-IDF cluster label ("lateral, visual, stream, level visual")
 * into a printable toponym. Raw labels are comma piles: long lines place
 * badly, and later terms often repeat earlier words. Rules:
 *   - keep terms in order, but DROP a term whose words all already appeared
 *     (redundancy: "level visual" after "visual" adds only "level" — kept
 *     only if it brings a new word);
 *   - stop at `maxTerms` kept terms or `maxChars` total;
 *   - join with " · " (a place name, not a list).
 */
export function compactToponym(label: string, maxTerms = 2, maxChars = 24): string {
  const seen = new Set<string>()
  const kept: string[] = []
  for (const rawTerm of label.split(',')) {
    const term = rawTerm.trim()
    if (!term) continue
    const words = term.toLowerCase().split(/\s+/)
    if (words.every((w) => seen.has(w))) continue // fully redundant
    const candidate = [...kept, term].join(' · ')
    if (kept.length > 0 && candidate.length > maxChars) break
    kept.push(term)
    words.forEach((w) => seen.add(w))
    if (kept.length >= maxTerms) break
  }
  const out = kept.join(' · ')
  // A single pathological first term still gets a hard cap, on a word edge.
  if (out.length > maxChars) {
    const cut = out.slice(0, maxChars + 1)
    const at = cut.lastIndexOf(' ')
    return (at > 8 ? cut.slice(0, at) : cut.slice(0, maxChars)).trimEnd() + '…'
  }
  return out
}
