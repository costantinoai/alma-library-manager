import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const SOURCE = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'sonner.tsx'), 'utf8')

// Sonner injects its stylesheet unlayered, and an unlayered rule beats every
// Tailwind v4 utility, which lives in `@layer utilities`, whatever its
// specificity. jsdom has no cascade, so this pins the two routes that do win.
describe('Toaster theme', () => {
  it('never relies on a plain utility to restyle a Sonner toast', () => {
    expect(SOURCE).not.toMatch(/group-\[\./)
  })

  it('themes the toast through the variables Sonner reads', () => {
    expect(SOURCE).toContain("'--normal-bg': 'var(--color-surface-1)'")
    expect(SOURCE).toContain("'--normal-text': 'var(--color-alma-900)'")
    expect(SOURCE).toContain("'--normal-border': 'var(--color-border)'")
  })
})
