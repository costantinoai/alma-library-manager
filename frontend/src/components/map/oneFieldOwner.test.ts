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
  })
})
