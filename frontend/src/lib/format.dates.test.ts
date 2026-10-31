import { describe, expect, it } from 'vitest'

import { formatDay, formatDayTime, formatPaperDate, formatPaperSource, plural } from './format'
import { formatDate, formatTimestamp } from './utils'

describe('one date format', () => {
  const noon = new Date(2026, 8, 15, 20, 8)

  it('writes a day and a day with time', () => {
    expect(formatDay(noon)).toBe('15 Sep 2026')
    expect(formatDayTime(noon)).toBe('15 Sep 2026, 20:08')
  })

  it('is what the older helpers write', () => {
    expect(formatPaperDate('2026-09-15')).toBe('15 Sep 2026')
    expect(formatPaperDate('2026-09')).toBe('Sep 2026')
    expect(formatDate('2026-09-15T12:00:00')).toBe('15 Sep 2026')
    expect(formatTimestamp('2026-09-15T12:00:00')).toMatch(/^15 Sep 2026, \d\d:\d\d$/)
  })
})

describe('plural', () => {
  it('agrees with the number', () => {
    expect(plural(1, 'collection')).toBe('1 collection')
    expect(plural(0, 'collection')).toBe('0 collections')
    expect(plural(2, 'branch', 'branches')).toBe('2 branches')
  })
})

describe('formatPaperSource', () => {
  it('names a source and never shows a raw key', () => {
    expect(formatPaperSource('browser_extension')).toBe('Browser extension')
    expect(formatPaperSource('discovery_like')).toBe('Discovery')
    expect(formatPaperSource('some_new_source')).toBe('Some new source')
  })
})
