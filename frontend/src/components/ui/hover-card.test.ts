import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const SOURCE = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'hover-card.tsx'), 'utf8')

describe('HoverCardContent', () => {
  it('rebinds the faintest text step, which is under AA on its paper', () => {
    expect(SOURCE).toContain('[--color-slate-400:var(--color-slate-500)]')
  })
})
