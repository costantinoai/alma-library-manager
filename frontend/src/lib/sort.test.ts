import { describe, it, expect } from 'vitest'
import { byWeightedDesc } from './sort'

describe('byWeightedDesc', () => {
  it('orders entries with a `weighted` field descending', () => {
    const items = [{ weighted: 1 }, { weighted: 5 }, { weighted: 3 }]
    items.sort(byWeightedDesc())
    expect(items.map((i) => i.weighted)).toEqual([5, 3, 1])
  })

  it('supports a nested accessor', () => {
    const items = [
      { signal: { weighted: 0.2 } },
      { signal: { weighted: 0.9 } },
      { signal: { weighted: 0.5 } },
    ]
    items.sort(byWeightedDesc((s) => s.signal.weighted))
    expect(items.map((i) => i.signal.weighted)).toEqual([0.9, 0.5, 0.2])
  })

  it('keeps ties in their relative order (stable comparator returns 0)', () => {
    expect(byWeightedDesc<{ weighted: number }>()({ weighted: 2 }, { weighted: 2 })).toBe(0)
  })
})
