import { describe, expect, it } from 'vitest'

import { selectionWithinVisible } from './useRegionSelection'

describe('map lasso subset contract', () => {
  it('keeps only unique ids rendered by the current payload', () => {
    const visible = new Set(['library-a', 'library-b'])
    expect(
      selectionWithinVisible(
        ['library-a', 'corpus-hidden', 'library-a', 'library-b'],
        visible,
      ),
    ).toEqual(['library-a', 'library-b'])
  })

  it('returns no selection when a stale payload owns every id', () => {
    expect(selectionWithinVisible(['old-corpus-node'], new Set(['new-library-node']))).toEqual([])
  })
})
