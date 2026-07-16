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
