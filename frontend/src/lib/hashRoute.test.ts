import { describe, expect, it } from 'vitest'

import { buildHashRoute, parseHashRoute } from './hashRoute'

describe('hashRoute', () => {
  it('routes Map instead of silently falling back to Home', () => {
    expect(parseHashRoute('#/map').page).toBe('map')
  })

  it('preserves owner deep-link parameters', () => {
    const route = buildHashRoute('feed', {
      scope: 'journals',
      monitor: 'm1',
      paper: 'p1',
    })
    const parsed = parseHashRoute(route)
    expect(parsed.page).toBe('feed')
    expect(Object.fromEntries(parsed.params)).toEqual({
      scope: 'journals',
      monitor: 'm1',
      paper: 'p1',
    })
  })
})
