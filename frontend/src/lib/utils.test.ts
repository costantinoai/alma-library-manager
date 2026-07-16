import { describe, it, expect } from 'vitest'
import {
  cn,
  formatNumber,
  truncate,
  normalizeAuthorName,
  parseAlmaTimestamp,
  repairDisplayText,
} from './utils'

describe('repairDisplayText (LaTeX dotless-ı + combining-mark repair)', () => {
  it('restores the precomposed accented letter from dotless-ı + combining mark', () => {
    // "Marı́a" = M a r + U+0131 (dotless i) + U+0301 (combining acute) + a.
    const broken = 'Marı́a Ruz'
    expect(repairDisplayText(broken)).toBe('María Ruz')
  })

  it('repairs the other diacritics seen in production', () => {
    expect(repairDisplayText('Taı̈eb')).toBe('Taïeb') // diaeresis
    expect(repairDisplayText('Benoı̂st')).toBe('Benoîst') // circumflex
    expect(repairDisplayText('Alı̀')).toBe('Alì') // grave
  })

  it('leaves clean text untouched and handles empty input', () => {
    expect(repairDisplayText('María Ruz')).toBe('María Ruz')
    expect(repairDisplayText('')).toBe('')
    expect(repairDisplayText(null)).toBe('')
    expect(repairDisplayText(undefined)).toBe('')
  })
})

describe('parseAlmaTimestamp (naive backend ISO is UTC)', () => {
  it('treats a tz-less ISO string as UTC (appends Z)', () => {
    const d = parseAlmaTimestamp('2026-04-25T12:00:00')
    expect(d.toISOString()).toBe('2026-04-25T12:00:00.000Z')
  })

  it('respects an explicit timezone marker', () => {
    expect(parseAlmaTimestamp('2026-04-25T12:00:00Z').toISOString()).toBe(
      '2026-04-25T12:00:00.000Z',
    )
    expect(parseAlmaTimestamp('2026-04-25T14:00:00+02:00').toISOString()).toBe(
      '2026-04-25T12:00:00.000Z',
    )
  })

  it('passes a Date through unchanged', () => {
    const now = new Date()
    expect(parseAlmaTimestamp(now)).toBe(now)
  })
})

describe('formatNumber', () => {
  it('abbreviates thousands and millions', () => {
    expect(formatNumber(1500)).toBe('1.5k')
    expect(formatNumber(2_000_000)).toBe('2.0M')
    expect(formatNumber(42)).toBe('42')
  })

  it('returns "0" for null / undefined', () => {
    expect(formatNumber(null)).toBe('0')
    expect(formatNumber(undefined)).toBe('0')
  })
})

describe('truncate', () => {
  it('adds an ellipsis past maxLen and leaves short strings alone', () => {
    expect(truncate('hello world', 8)).toBe('hello...')
    expect(truncate('short', 10)).toBe('short')
    expect(truncate(null, 5)).toBe('')
  })
})

describe('normalizeAuthorName', () => {
  it('lowercases, trims, and collapses interior whitespace', () => {
    expect(normalizeAuthorName('  JOHN   Smith ')).toBe('john smith')
    expect(normalizeAuthorName(null)).toBe('')
  })
})

describe('cn', () => {
  it('merges class names and resolves Tailwind conflicts (last wins)', () => {
    expect(cn('px-2', 'px-4')).toBe('px-4')
    expect(cn('text-sm', undefined, 'font-bold')).toBe('text-sm font-bold')
  })
})
