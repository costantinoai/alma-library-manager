import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative, sep } from 'node:path'

/**
 * surface-guard — the completeness gate for the centralized design system.
 *
 * Every surface color, semantic color, and accent must flow through the
 * tokens + primitives, never a raw Tailwind ramp class hand-written in a
 * component. This test scans the source for the banned raw classes and
 * fails if any appear outside the allowlist (the token source lives in
 * index.css, which is not scanned; the primitive *definitions* in
 * components/ui legitimately spell the raw classes the rest of the app
 * must route through; PaperCard owns the SIGNAL_META data palette).
 *
 * The offender list is printed on failure (file:line + the offending class)
 * so any regression is immediately actionable.
 */
const ROOT = join(process.cwd(), 'src')

/** Files allowed to contain raw classes: the primitive definitions + the
 * one data-driven color palette. Everything else must use the ladder /
 * semantic tokens / primitives. Paths are relative to src/, posix-style. */
function isAllowlisted(rel: string): boolean {
  // All primitive definitions live under components/ui/.
  if (rel.startsWith('components/ui/')) return true
  // This guard file names the banned classes in its own patterns.
  if (rel === 'test/surface-guard.test.ts') return true
  // Categorical data palettes (signal dots, source chips, category icons) now
  // live in the single `lib/palette.ts` module (44.5) — a `.ts` file, which
  // this `.tsx`-only walk never scans. No component may spell a raw hue: they
  // import the named maps from that one source instead.
  return false
}

const BANNED: Array<{ name: string; re: RegExp }> = [
  // Raw surface ramps used as surfaces — must be bg-surface-N / a primitive.
  { name: 'parchment surface', re: /\bbg-parchment-(50|100)\b/g },
  { name: 'white surface', re: /\bbg-white\b/g },
  { name: 'slate surface', re: /\bbg-slate-(50|100)\b/g },
  { name: 'aliased surface token', re: /\bbg-alma-(content|chrome)(-elev)?\b/g },
  { name: 'legacy paper surface', re: /\bbg-alma-paper\b/g },
  // Raw semantic colors — must route through success/warning/critical/info.
  // The full CHROMATIC Tailwind palette is banned (44.5): the old list only
  // covered emerald|amber|rose|sky|red|green, so a hand-rolled `bg-blue-50`
  // callout (44.4) slipped straight through. `slate` stays exempt (the neutral
  // TEXT ramp); genuine data-category palettes are exempted per-file above.
  {
    name: 'raw semantic color',
    re: /\b(bg|text|border|ring|fill|stroke|divide)-(red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\d{2,3}\b/g,
  },
  // Arbitrary hex in a utility — must be a token.
  { name: 'arbitrary hex', re: /\b(bg|text|border|ring|fill|stroke)-\[#[0-9a-fA-F]{3,8}\]/g },
]

function walk(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) walk(full, acc)
    else if (full.endsWith('.tsx')) acc.push(full)
  }
  return acc
}

function scan(): string[] {
  const hits: string[] = []
  for (const file of walk(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (isAllowlisted(rel)) continue
    const lines = readFileSync(file, 'utf8').split('\n')
    lines.forEach((line, i) => {
      // Skip data-driven inline styles (style={{ backgroundColor: ... }}).
      if (/style=\{\{/.test(line)) return
      for (const { re } of BANNED) {
        for (const m of line.matchAll(re)) hits.push(`${rel}:${i + 1}  ${m[0]}`)
      }
    })
  }
  return hits
}

/* ─────────────────────────────────────────────────────────────────────────
 * control-guard — "controls are ink, surfaces are paper".
 *
 * The surface ladder is for CONTAINERS. A control (button, field, chip,
 * toggle, rail) fills from the translucent `control-*` ink ladder instead,
 * so it composites the same over every host. Three things used to go wrong
 * and each has a rule below:
 *
 *   1. A control wearing its host's own cream (ListControlBar was a
 *      `bg-surface-1` button inside a `bg-surface-1` bar — invisible fill).
 *   2. A control pinned to a fixed paper step, so it changed appearance with
 *      elevation (`Input` was `bg-surface-0`: a well in a Card, a dark slab
 *      in a near-white popover).
 *   3. Parchment fills, which are the warm paper ramp by another name.
 * ───────────────────────────────────────────────────────────────────────── */

/** Primitives that define a CONTROL. None may paint itself from the paper
 *  ladder. `surface-4` is exempt — it is the RAISED KNOB (switch/slider
 *  thumb, active segment), which must be the brightest thing on an ink rail. */
const CONTROL_PRIMITIVES = [
  'components/ui/badge.tsx',
  'components/ui/button-variants.ts',
  'components/ui/checkbox.tsx',
  'components/ui/input-group.tsx',
  'components/ui/input.tsx',
  'components/ui/kbd.tsx',
  'components/ui/meter.tsx',
  'components/ui/progress.tsx',
  'components/ui/radio-group.tsx',
  'components/ui/skeleton.tsx',
  'components/ui/slider.tsx',
  'components/ui/status-badge.tsx',
  'components/ui/switch.tsx',
  'components/ui/tabs.tsx',
  'components/ui/textarea.tsx',
  'components/ui/toggle-variants.ts',
]

/** Fills only. `border-`/`ring-`/`text-` on these ramps stay legal: the ramps
 *  are still the app's text and hairline vocabulary — it's using them as a
 *  SURFACE under a control that breaks the contract. */
const CONTROL_BANNED: Array<{ name: string; re: RegExp; scope: 'all' | 'app' }> = [
  // Parchment is the warm paper ramp — gone as a fill AND as a hairline.
  // (Fills-only was too narrow: `hover:border-parchment-400` survived the
  // first sweep on a control in LensManager.)
  { name: 'parchment', re: /\b(bg|border|ring|divide)-parchment-\d+\b/g, scope: 'all' },
  // A pill that borrows a paper step dissolves into the paper under it.
  // (`surface-4` excluded: the raised knob.)
  {
    name: 'cream pill',
    re: /(?=.*\brounded-full\b).*?\b(bg-surface-[0-3])\b/g,
    scope: 'all',
  },
  // Cool near-whites and the slate TEXT ramp are not control fills either —
  // they were the other half of the drift (`bg-alma-50` hovers,
  // `bg-slate-200` on-states). Categorical data fills live in lib/palette.ts.
  { name: 'alma ramp fill', re: /\bbg-alma-(50|100|200|300|400)\b/g, scope: 'app' },
  { name: 'slate ramp fill', re: /\bbg-slate-(200|300)\b/g, scope: 'app' },
]

/** Markers that make an element interactive — i.e. a control, not a surface.
 *  A table/list ROW highlight is the one legitimate paper hover, so the two
 *  row primitives that own it are allowlisted below. */
const INTERACTIVE =
  /hover:(bg|border|text|ring)-|focus(-visible)?:(bg|border|ring)-|active:(bg|border)-|data-\[state=(on|checked|active|selected)|cursor-pointer|aria-pressed/

/** `lib/palette.ts` owns categorical colour and is exempt by design. */
function isPaletteModule(rel: string): boolean {
  return rel === 'lib/palette.ts'
}

function walkAll(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) walkAll(full, acc)
    else if (full.endsWith('.tsx') || full.endsWith('.ts')) acc.push(full)
  }
  return acc
}

function scanControls(): string[] {
  const hits: string[] = []
  for (const file of walkAll(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (rel === 'test/surface-guard.test.ts' || isPaletteModule(rel)) continue
    const isPrimitive = rel.startsWith('components/ui/')
    const lines = readFileSync(file, 'utf8').split('\n')
    lines.forEach((line, i) => {
      // Comments cite the retired classes on purpose ("was `bg-surface-0`").
      if (/^\s*(\/\/|\/\*|\*)/.test(line)) return
      for (const { name, re, scope } of CONTROL_BANNED) {
        if (scope === 'app' && isPrimitive) continue
        for (const m of line.matchAll(re)) {
          hits.push(`${rel}:${i + 1}  ${m[1] ?? m[0]}  (${name})`)
        }
      }
      // A control primitive may not paint from the paper ladder at all.
      if (CONTROL_PRIMITIVES.includes(rel)) {
        for (const m of line.matchAll(/\bbg-surface-[0-3]\b/g)) {
          hits.push(`${rel}:${i + 1}  ${m[0]}  (control on the paper ladder)`)
        }
      }
      // ...and OUTSIDE the primitives, anything that reacts to the pointer or
      // carries a toggle state is a control by definition, whoever wrote it.
      // This is the rule that catches a hand-rolled button — the first sweep
      // only grepped for parchment/alma/slate fills, so every control that
      // had picked a `bg-surface-N` instead (the whole paper-card action bar,
      // the Feed and Library tab strips, a dozen icon buttons) sailed through.
      if (!isPrimitive && INTERACTIVE.test(line)) {
        for (const m of line.matchAll(/\bbg-surface-[0-3]\b/g)) {
          hits.push(`${rel}:${i + 1}  ${m[0]}  (interactive element on the paper ladder)`)
        }
      }
    })
  }
  return hits
}

describe('surface-guard: one centralized design system', () => {
  it('contains no raw surface/semantic/hex classes outside the primitives', () => {
    const hits = scan()
    expect(hits, `\n${hits.join('\n')}\n`).toHaveLength(0)
  })

  it('keeps the SURFACE_BG / SURFACE_BORDER literal maps intact (Tailwind purge safety)', () => {
    const surface = readFileSync(join(ROOT, 'components/ui/surface-level.ts'), 'utf8')
    for (let n = 0; n <= 4; n++) {
      expect(surface).toContain(`bg-surface-${n}`)
      expect(surface).toContain(`border-edge-${n}`)
    }
  })
})

describe('control-guard: controls are ink, surfaces are paper', () => {
  it('has no control filled from the paper ladder or a raw neutral ramp', () => {
    const hits = scanControls()
    expect(hits, `\n${hits.join('\n')}\n`).toHaveLength(0)
  })

  it('keeps the control ink ladder defined in index.css', () => {
    const css = readFileSync(join(ROOT, 'index.css'), 'utf8')
    for (const token of [
      '--color-control-well',
      '--color-control-quiet',
      '--color-control-quiet-hover',
      '--color-control-track',
      '--color-control-edge',
      '--color-control-edge-strong',
    ]) {
      expect(css).toContain(token)
    }
  })
})
