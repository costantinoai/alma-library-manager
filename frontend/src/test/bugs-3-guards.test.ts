import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

const source = (path: string) => readFileSync(resolve(__dirname, '..', path), 'utf8')

describe('fixes from the live test of 2026-11-01', () => {
  it('no <Button> sits inside a <RowButton> in the Activity panel', () => {
    const rows = source('components/ActivityPanel.tsx').match(/<RowButton[\s\S]*?<\/RowButton>/g) ?? []
    expect(rows.filter((row) => row.includes('<Button'))).toEqual([])
  })

  it('the palette dialog is named for a screen reader', () => {
    expect(source('components/CommandPalette.tsx')).toContain('<DialogTitle')
  })

  it('the analytics link shows analytics, not the Saved list above its fold', () => {
    const page = source('pages/LibraryPage.tsx')
    expect(page).toContain("{(activeTab === 'saved' || activeTab === 'imports') && (")
    expect(page).toContain("activeTab === 'analytics' ? ''")
  })

  it('no picker asks for a fixed page of the Library', () => {
    for (const file of ['components/library/TagsTab.tsx', 'addons/auto_tagger/TagSuggestionsPanel.tsx']) {
      expect(source(file)).not.toMatch(/library\/saved\?limit=/)
    }
  })
})
