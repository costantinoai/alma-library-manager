import { beforeEach, describe, expect, it } from 'vitest'

import { migrateLocalStorage } from './storageMigrations'

describe('migrateLocalStorage', () => {
  beforeEach(() => window.localStorage.clear())

  it('removes the keys of the removed views and keeps the table sort', () => {
    window.localStorage.setItem('alma.library.saved.viewMode', 'cards')
    window.localStorage.setItem('alma.datatable.feed.compact', '{}')
    window.localStorage.setItem('alma.library.saved.compactSortKey', 'title')
    migrateLocalStorage()
    expect(window.localStorage.getItem('alma.library.saved.viewMode')).toBeNull()
    expect(window.localStorage.getItem('alma.datatable.feed.compact')).toBeNull()
    expect(window.localStorage.getItem('alma.library.saved.compactSortKey')).toBe('title')
  })

  it('does nothing once the version is current', () => {
    migrateLocalStorage()
    window.localStorage.setItem('alma.library.saved.viewMode', 'cards')
    migrateLocalStorage()
    expect(window.localStorage.getItem('alma.library.saved.viewMode')).toBe('cards')
  })
})
