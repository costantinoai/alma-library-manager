/**
 * One owner of the map preference field.
 *
 * `GraphMapView` (Map page + Authors) and `FrontierMap` (Discovery) each used
 * to wire the field themselves — `useSignalField` + `buildTerrainField` +
 * their own legend bounds. The two drifted, and the drift shipped: when the
 * terrain ramp moved to a ±0.5 domain the Map page followed and Discovery did
 * not, so its colourbar went on claiming `-1 … +1` beside a gradient that no
 * longer used it. Nothing failed, because nothing tied them together.
 *
 * `useMapField` is now that tie. This test is what keeps it one: a new surface
 * that reaches for the field primitives directly fails here instead of quietly
 * becoming a third implementation.
 *
 * Deliberately a source scan rather than a render test — the property is
 * "nobody else imports these", which no amount of rendering can demonstrate.
 */
import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

const SRC = join(__dirname, '..', '..')

/** Primitives only `useMapField` may reach for. */
const OWNED = ['useSignalField', 'useAuthorField', 'buildTerrainField']

/** Files allowed to import them, each for a stated reason. */
const ALLOWED: Record<string, string> = {
  'components/map/useMapField.ts': 'the owner',
  'components/map/terrainField.ts': 'defines buildTerrainField',
  'components/map/useSignalField.ts': 'defines useSignalField',
  'components/map/useAuthorField.ts': 'defines useAuthorField',
  // Tests may exercise the primitives directly.
  'components/map/subsetRule.test.ts': 'unit-tests the builder',
  'components/map/terrainField.test.ts': 'unit-tests the builder',
  'components/map/oneFieldOwner.test.ts': 'this guard',
  // Reads `entriesById` — per-author drilldown detail for the panel and
  // popups, which is author-record data rather than map-field data and has no
  // meaning on the plate. Shares the same React Query cache either way.
  'components/map/AuthorMapPanel.tsx': 'needs entriesById, not the field',
}

/**
 * The second half of the same contract, added when `MapSurface` landed
 * (task 64 P1): the field was unified first, but the CHROME around it was
 * still duplicated — two toolbars, two legends, two `SemanticMap` calls — and
 * that is the surface a third map would have copied next.
 *
 * `useMapField` is now reachable only through `MapSurface`, which also owns
 * the plate and the legend, so "one field" and "one map" are the same
 * statement rather than two that can drift apart.
 */
const SURFACE_OWNED = ['useMapField', 'ColourBarLegend', 'SemanticMap']

const SURFACE_ALLOWED: Record<string, string> = {
  'components/map/MapSurface.tsx': 'the owner',
  'components/map/SemanticMap.tsx': 'defines SemanticMap',
  'components/map/MapChrome.tsx': 'defines ColourBarLegend',
  'components/map/useMapField.ts': 'defines useMapField',
  'components/map/oneFieldOwner.test.ts': 'this guard',
  'components/map/SemanticMap.click.test.tsx': 'renders the primitive directly',
  // Reads `scores` for the SIDE PANELS (hover score, cluster area scores) —
  // not to draw a plate. Deliberately the same hook, and therefore the same
  // React Query cache the plate uses, so a number in a panel cannot disagree
  // with the dot it describes.
  'pages/MapPage.tsx': 'panel scores, not a second plate',
}

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    if (entry === 'node_modules' || entry.startsWith('.')) continue
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) walk(full, out)
    else if (/\.tsx?$/.test(entry)) out.push(full)
  }
  return out
}

describe('the map preference field has one owner', () => {
  it('is imported only by useMapField', () => {
    const offenders: string[] = []
    for (const file of walk(SRC)) {
      const rel = file.slice(SRC.length + 1).split('\\').join('/')
      if (ALLOWED[rel]) continue
      const text = readFileSync(file, 'utf8')
      // Only IMPORT lines count: a comment mentioning the name is fine.
      const imports = text
        .split('\n')
        .filter((line) => line.trimStart().startsWith('import'))
        .join('\n')
      for (const name of OWNED) {
        if (imports.includes(name)) offenders.push(`${rel} imports ${name}`)
      }
    }
    expect(
      offenders,
      'Route the field through useMapField instead, or add an entry to ALLOWED ' +
        'with the reason:\n  ' + offenders.join('\n  '),
    ).toEqual([])
  })

  it('the allowlist has no dead entries', () => {
    const live = new Set(
      walk(SRC).map((f) => f.slice(SRC.length + 1).split('\\').join('/')),
    )
    expect(Object.keys(ALLOWED).filter((f) => !live.has(f))).toEqual([])
    expect(Object.keys(SURFACE_ALLOWED).filter((f) => !live.has(f))).toEqual([])
  })
})

describe('the map has one host', () => {
  it('only MapSurface renders the plate, the legend bars and the field', () => {
    const offenders: string[] = []
    for (const file of walk(SRC)) {
      const rel = file.slice(SRC.length + 1).split('\\').join('/')
      if (SURFACE_ALLOWED[rel]) continue
      const imports = readFileSync(file, 'utf8')
        .split('\n')
        .filter((line) => line.trimStart().startsWith('import'))
        .join('\n')
      for (const name of SURFACE_OWNED) {
        // `MapSurfaceLoading` / `MapSurfaceNode` are the host-facing exports;
        // matching on word boundaries keeps them out of the offender list.
        if (new RegExp(`\\b${name}\\b`).test(imports)) offenders.push(`${rel} imports ${name}`)
      }
    }
    expect(
      offenders,
      'Render through MapSurface instead of rebuilding a map host, or add an ' +
        'entry to SURFACE_ALLOWED with the reason:\n  ' + offenders.join('\n  '),
    ).toEqual([])
  })
})
