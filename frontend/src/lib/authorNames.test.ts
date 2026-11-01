import { describe, expect, it } from 'vitest'

import { formatAuthorNames, parseAuthorNames, uniqueAuthorNames } from '@/lib/authorNames'
import cases from '@/lib/authorNames.cases.json'

describe('parseAuthorNames', () => {
  it.each(cases)('$stored', ({ stored, names }) => {
    expect(parseAuthorNames(stored)).toEqual(names)
  })

  it('keeps one person with a Follow target, not one per comma', () => {
    expect(uniqueAuthorNames('Testerson, Alice; Probe, Bob')).toEqual(['Alice Testerson', 'Bob Probe'])
  })

  it('reads as one line in a table', () => {
    expect(formatAuthorNames('Testerson, Alice; Probe, Bob')).toBe('Alice Testerson, Bob Probe')
  })

  it('shows a repeated name once', () => {
    expect(uniqueAuthorNames('Ada Lovelace, Ada Lovelace, Alan Turing')).toEqual(['Ada Lovelace', 'Alan Turing'])
  })
})
