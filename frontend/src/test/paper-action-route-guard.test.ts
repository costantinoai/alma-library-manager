import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative, sep } from 'node:path'

/**
 * paper-action-route-guard — every Add / Like / Love / Dislike from the UI goes
 * through the ONE paper-action contract: `applyPaperAction` (POST
 * /papers/{id}/action) for a local paper, `onlineImportSave` (POST
 * /library/import/search/save) for a remote one. Both apply the backend's
 * rating map AND its feedback signal.
 *
 * `POST /library/saved` sets membership + a hand-picked star value and records
 * no signal. Three components used it for valence buttons (Author panel
 * Like/Love, Reading list Save at 0★, Library "Like similar" at 0★), so those
 * likes never reached Discovery. No frontend code may post there now.
 */
const ROOT = join(process.cwd(), 'src')

function walk(dir: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) walk(full, acc)
    else if (/\.(ts|tsx)$/.test(full) && !full.endsWith('.test.ts')) acc.push(full)
  }
  return acc
}

describe('paper-action-route-guard', () => {
  it('no frontend code posts a valence save to /library/saved', () => {
    const offenders: string[] = []
    for (const file of walk(ROOT)) {
      const lines = readFileSync(file, 'utf8').split('\n')
      lines.forEach((line, i) => {
        if (/\bpost\b[^\n]*['"`]\/library\/saved['"`]/.test(line)) {
          offenders.push(`${relative(ROOT, file).split(sep).join('/')}:${i + 1}`)
        }
      })
    }
    expect(offenders, `route through applyPaperAction / onlineImportSave: ${offenders.join(', ')}`).toEqual([])
  })
})
