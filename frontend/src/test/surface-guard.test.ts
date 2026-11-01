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
 * index.css, which is not scanned; lib/palette.ts owns the data palettes).
 *
 * The offender list is printed on failure (file:line + the offending class)
 * so any regression is immediately actionable.
 */
const ROOT = join(process.cwd(), 'src')

/** Files allowed to contain raw classes: the one data-driven colour palette.
 * Primitives are scanned like everything else. Paths are relative to src/,
 * posix-style. */
function isAllowlisted(rel: string): boolean {
  // This guard file names the banned classes in its own patterns.
  if (rel === 'test/surface-guard.test.ts') return true
  // Categorical data palettes (signal dots, source chips, category icons) live
  // in the single `lib/palette.ts` module. No component may spell a raw hue.
  return rel === 'lib/palette.ts'
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
    re: /\b(bg|text|border|ring|fill|stroke|divide|from|to|via|outline|shadow|accent|decoration)-(red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose|stone|zinc|gray|neutral)-\d{2,3}\b/g,
  },
  // Arbitrary hex in a utility — must be a token.
  // Ramps deleted from index.css, and the `Alert` variant that went with them.
  { name: 'deleted ramp', re: /\b(bg|text|border|ring)-(parchment|pale)-\d+\b|var\(--color-(parchment|pale|muted)\b/g },
  { name: 'arbitrary hex', re: /\b(bg|text|border|ring|fill|stroke)-\[#[0-9a-fA-F]{3,8}\]/g },
]

function walk(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) walk(full, acc)
    else if (/\.tsx?$/.test(full) && !/\.test\.tsx?$/.test(full)) acc.push(full)
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
      if (/^\s*(\/\/|\/\*|\*)/.test(line)) return
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
  'components/ui/toggle-group.tsx',
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
  // A tooltip's fill belongs to its `tone`, or nested text loses its contrast.
  { name: 'hand-painted tooltip', re: /<TooltipContent\b[^>]*\bclassName="[^"]*\b(bg-[a-z]+[-\w/]*)/g, scope: 'app' },
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

/* ───────────────────────────────────────
 * colour-guard — a colour is a token, wherever it is written.
 *
 * The scans above read `.tsx` class strings. A colour can also sit in a `.ts`
 * constant, a chart prop or an inline style, so this one reads every source
 * line, primitives included. Comments are skipped: they may name a value.
 * ─────────────────────────────────────── */
const COLOUR_OWNERS = new Set(['lib/palette.ts', 'test/surface-guard.test.ts'])

const COLOUR_BANNED: Array<{ name: string; re: RegExp }> = [
  { name: 'hex literal', re: /#[0-9a-fA-F]{6}(?:[0-9a-fA-F]{2})?\b/g },
  { name: 'rgb literal', re: /\brgba?\(\s*\d/g },
  { name: 'rgb utility', re: /\[rgba?\(/g },
  { name: 'black or white, use scrim / on-fill', re: /\b(bg|text|border)-(black|white)(\/[\d.]+)?(?![\w-])/g },
  // Only colours with a `--color-*` token compile. These two never had one.
  { name: 'class with no CSS', re: /\btext-accent-\d+\b/g },
]

function allSource(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) allSource(full, acc)
    else if (/\.tsx?$/.test(full) && !/\.test\.tsx?$/.test(full)) acc.push(full)
  }
  return acc
}

function scanColours(): string[] {
  const hits: string[] = []
  for (const file of allSource(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (COLOUR_OWNERS.has(rel)) continue
    readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
      const code = line.trim()
      if (code.startsWith('*') || code.startsWith('/*') || code.startsWith('//')) return
      for (const { name, re } of COLOUR_BANNED) {
        for (const m of line.matchAll(re)) hits.push(`${rel}:${i + 1}  ${m[0]}  (${name})`)
      }
    })
  }
  return hits
}

describe('colour-guard', () => {
  it('every colour outside lib/palette.ts is a token', () => {
    expect(scanColours()).toEqual([])
  })
})

/* ───────────────────────────────────────
 * layer-guard — one z-index scale, `--z-index-*` in index.css, one shadow scale,
 * `shadow-paper-*`, and two radii plus the pill: `rounded-card`,
 * `rounded-control`, `rounded-full`.
 * ─────────────────────────────────────── */
const RADIUS_BANNED = /(?<![\w-])rounded(?:-[tblr]{1,2})?(?:-(?:xs|sm|md|lg|xl|2xl|3xl|\[[^\]]*\]))?(?![\w-])(?!\/)/g
// Sizes and hairlines are tokens: `text-caption`, `border-divider`.
const TOKEN_BANNED = /\btext-\[[\d.]+(?:px|rem)\]|-\[(?:color:)?var\(--color-border/g
const SHADOW_BANNED = /(?<![\w-])shadow-(?:sm|md|lg|xl|2xl|inner|\[)/g
const LAYER_BANNED = /(?<![\w-])-?z-(\d+|\[\d+\])(?![\w-])|\bzIndex:\s*(\S+\s*\?\s*)?\d/g

function scanLayers(): string[] {
  const hits: string[] = []
  for (const file of allSource(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (rel === 'test/surface-guard.test.ts') continue
    readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
      const code = line.trim()
      if (code.startsWith('*') || code.startsWith('/*') || code.startsWith('//') || code.startsWith('{/*')) return
      for (const m of line.matchAll(LAYER_BANNED)) hits.push(`${rel}:${i + 1}  ${m[0]}`)
      for (const m of line.matchAll(TOKEN_BANNED)) hits.push(`${rel}:${i + 1}  ${m[0]}`)
      for (const m of line.matchAll(SHADOW_BANNED)) hits.push(`${rel}:${i + 1}  ${m[0]}`)
      // InflowStrip's 1 px caps are chart marks, not rectangles.
      if (rel === 'components/home/InflowStrip.tsx') return
      for (const m of line.matchAll(RADIUS_BANNED)) {
        if (m[0] !== 'rounded-[inherit]') hits.push(`${rel}:${i + 1}  ${m[0]}`)
      }
    })
  }
  return hits
}

describe('layer-guard', () => {
  it('no numeric z-index, shadow or radius outside the scales in index.css', () => {
    expect(scanLayers()).toEqual([])
  })
})

/* ───────────────────────────────────────
 * button-guard — a pressable thing is a primitive.
 *
 * `BUTTON_OWNERS` may write a bare `<button>`: the primitives, and chrome that
 * is a primitive in all but name. `GOING` names two grips that G3.2 and G5.1
 * delete; drop them from the list when those stages land.
 * ─────────────────────────────────────── */
const BUTTON_OWNERS = new Set([
  'components/ui/button.tsx',
  'components/ui/row-button.tsx',
  'components/ui/chip-button.tsx',
  'components/ui/data-table/DataTable.tsx', // sortable header cell, and GOING
  'components/discovery/LensManager.tsx', // GOING, reorder grip
  'components/StarRating.tsx',
  'components/library/ColorPicker.tsx',
  'components/layout/Sidebar.tsx',
  'components/settings/FeedMonitorTermsCard.tsx', // drag handle
  'components/shared/EtaHint.tsx',
  'components/shared/JargonHint.tsx',
  'components/shared/PaperCard.tsx',
  'components/shared/PaperTile.tsx',
  'components/shared/StatusChip.tsx',
  'components/shared/StatusRow.tsx',
])

/** A `Button` gets its colour, radius and shadow from `variant`, never by hand.
 *  The paper-card action bar is the one named exception: its tone map is the
 *  action contract in CLAUDE.md, valence per verb, and the collection menu sits
 *  in that bar. */
const BUTTON_REPAINT_OWNERS = new Set([
  'components/discovery/PaperActionBar.tsx',
  'components/discovery/AddToCollectionMenu.tsx',
])
const BUTTON_TAG = /<(Button|AlertDialogAction|AlertDialogCancel)\b(?:[^>"'{]|"[^"]*"|'[^']*'|\{(?:[^{}]|\{[^{}]*\})*\})*>/g
const BUTTON_PAINT =
  /(?<![\w:-])(?:[a-z-]+:)*(?:text-(?:slate|alma|critical|warning|success|info|accent|gold)|bg-|border-(?!0\b)|rounded|shadow-)[\w/[\].-]*/

function scanButtons(): string[] {
  const hits: string[] = []
  for (const file of allSource(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (rel === 'test/surface-guard.test.ts') continue
    const source = readFileSync(file, 'utf8')
    source.split('\n').forEach((line, i) => {
      if (/^\s*(\/\/|\*|\/\*|\{\/\*)/.test(line)) return
      if (!BUTTON_OWNERS.has(rel) && /<button\b/.test(line)) hits.push(`${rel}:${i + 1}  <button>`)
    })
    // Filled accent is onboarding's alone: everywhere else, navy is the one fill.
    if (!rel.startsWith('components/onboarding/') && !rel.startsWith('components/ui/')) {
      for (const m of source.matchAll(/<Button\b[^>]*\bvariant=(?:"accent"|\{[^}]*'accent')/g)) {
        hits.push(`${rel}:${source.slice(0, m.index).split('\n').length}  filled accent`)
      }
    }
    if (BUTTON_REPAINT_OWNERS.has(rel)) continue
    for (const m of source.matchAll(BUTTON_TAG)) {
      const cls = /className=(?:"([^"]*)"|\{([\s\S]*)\})/.exec(m[0])
      const paint = cls && BUTTON_PAINT.exec(cls[1] ?? cls[2])
      if (paint) hits.push(`${rel}:${source.slice(0, m.index).split('\n').length}  ${paint[0]}`)
    }
  }
  return hits
}

describe('button-guard', () => {
  it('no bare <button> outside its owners, and no repainted Button', () => {
    expect(scanButtons()).toEqual([])
  })
})

/* ───────────────────────────────────────
 * primitive-guard — a state box is an Alert, a pill is a StatusBadge, a fold is
 * a DisclosurePanel. The Activity family keeps its own quiet log tints, on the
 * one documented exception. `BranchExplorerPanel` loses its raw fold in G3.6.
 * ─────────────────────────────────────── */
const ACTIVITY = 'components/ActivityPanel.tsx'
/** May name a paper step: chrome, portals with no surface context (tour, toast,
 *  chart tooltip, autocomplete), shared tiles, list rows that must stay an `<li>`
 *  or a conditional state, and the three files G3 removes or rewrites. */
const PAPER_OWNERS = new Set([
  'components/layout/Sidebar.tsx',
  'components/layout/TopBar.tsx',
  'components/onboarding/OnboardingTour.tsx',
  'components/onboarding/steps/StepKeywords.tsx',
  'components/onboarding/steps/StepVision.tsx',
  'components/onboarding/steps/StepWelcome.tsx',
  'components/connector/ConnectorToastCard.tsx',
  'components/insights/InsightsOverviewTab.tsx',
  'components/shared/VenueAutocomplete.tsx',
  'components/shared/MetricTile.tsx',
  'components/settings/primitives.tsx',
  'components/settings/AIConfigCard.tsx',
  'components/health/RepairCard.tsx',
  'components/health/RepairConfirmDialog.tsx',
  'components/authors/AuthorsNeedsAttentionSection.tsx',
  'components/discovery/PaperDetailPanel.tsx',
  'components/discovery/RecommendationProvenance.tsx',
  'components/discovery/BranchExplorerPanel.tsx',
  'components/discovery/LensManager.tsx',
  'components/discovery/LensWeightsPanel.tsx',
])
const FOLD_OWNERS = new Set(['components/ui/disclosure-panel.tsx', ACTIVITY, 'components/discovery/BranchExplorerPanel.tsx'])

function scanPrimitives(): string[] {
  const hits: string[] = []
  for (const file of allSource(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (rel === 'test/surface-guard.test.ts') continue
    readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
      if (/^\s*(\/\/|\*|\/\*|\{\/\*)/.test(line)) return
      const at = `${rel}:${i + 1}`
      if (/\bbg-surface-[0-3]\b/.test(line) && !rel.startsWith('components/ui/') && !PAPER_OWNERS.has(rel)) {
        hits.push(`${at}  paper step, use Card or SubPanel`)
      }
      if (/<Badge\b/.test(line)) hits.push(`${at}  <Badge>, use StatusBadge`)
      if (/<details\b/.test(line) && !FOLD_OWNERS.has(rel)) hits.push(`${at}  <details>, use DisclosurePanel`)
      if (/\bbg-(critical|warning|success|info)-(50|100)\b/.test(line) && !rel.startsWith('components/ui/') && rel !== ACTIVITY) {
        hits.push(`${at}  state box, use Alert`)
      }
      if (/rounded-full.*\bbg-(critical|warning|success|info|alma|alma-folio)-\d+\/.*\bpx-/.test(line) && !rel.startsWith('components/ui/')) {
        hits.push(`${at}  hand pill, use StatusBadge or SignalChip`)
      }
    })
  }
  return hits
}

describe('primitive-guard', () => {
  it('state boxes, pills and folds go through their primitives', () => {
    expect(scanPrimitives()).toEqual([])
  })
})

/* ───────────────────────────────────────
 * class-guard — a colour class must compile. Tailwind drops a class with no
 * token in silence, which is how `text-accent-700` shipped as inherited colour.
 * ─────────────────────────────────────── */
function scanUnknownColours(): string[] {
  const css = readFileSync(join(ROOT, 'index.css'), 'utf8')
  const ramps = new Set([...css.matchAll(/--color-([a-z-]+?)-\d+:/g)].map((m) => m[1]))
  ramps.add('slate') // Tailwind's own, the neutral text ramp
  const hits: string[] = []
  for (const file of allSource(ROOT)) {
    const rel = relative(ROOT, file).split(sep).join('/')
    if (rel === 'test/surface-guard.test.ts' || rel === 'lib/palette.ts') continue
    readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
      if (/^\s*(\/\/|\*|\/\*)/.test(line)) return
      for (const m of line.matchAll(/\b(?:bg|text|border(?:-[lrtbxy])?|ring|fill|stroke|divide)-([a-z]+(?:-[a-z]+)?)-(\d{2,3})\b/g)) {
        if (!ramps.has(m[1])) hits.push(`${rel}:${i + 1}  ${m[0]}`)
      }
    })
  }
  return hits
}

describe('class-guard', () => {
  it('every ramp colour class has a token behind it', () => {
    expect(scanUnknownColours()).toEqual([])
  })
})
