import { describe, it, expect } from 'vitest'
import { formatPercent, formatYearMonth } from './format'

describe('formatPercent', () => {
  it('formats a unit ratio as a percent with the trailing sign', () => {
    expect(formatPercent(0.387, 1)).toBe('38.7%')
    expect(formatPercent(0.5)).toBe('50%')
  })

  it('rounds to the requested fraction digits', () => {
    expect(formatPercent(0.12345, 2)).toBe('12.35%')
  })

  it('drops the sign when withSign=false', () => {
    expect(formatPercent(0.42, 0, { withSign: false })).toBe('42')
  })

  it('treats null / undefined / NaN as 0', () => {
    expect(formatPercent(null)).toBe('0%')
    expect(formatPercent(undefined)).toBe('0%')
    expect(formatPercent(Number.NaN)).toBe('0%')
    expect(formatPercent(Infinity)).toBe('0%')
  })
})

describe('formatYearMonth', () => {
  it('renders an ISO date as "Mon YYYY"', () => {
    expect(formatYearMonth('2024-03-15')).toBe('Mar 2024')
  })

  it('returns empty string for falsy or unparseable input', () => {
    expect(formatYearMonth('')).toBe('')
    expect(formatYearMonth(null)).toBe('')
    expect(formatYearMonth(undefined)).toBe('')
    expect(formatYearMonth('not-a-date')).toBe('')
  })
})
