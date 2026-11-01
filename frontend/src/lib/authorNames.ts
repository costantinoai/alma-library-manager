import { normalizeAuthorName } from '@/lib/utils'

/**
 * Split a stored `papers.authors` string into "First Last" names, in order.
 *
 * Same rule as `parse_author_names` in `src/alma/core/author_names.py`. Both run
 * the cases in `authorNames.cases.json`, so change them together.
 */

const NAME_SUFFIXES = new Set(['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'phd', 'md', 'msc', 'ms', 'ma'])

function isNameSuffix(value: string): boolean {
  const token = value.toLowerCase().replace(/[^a-z0-9]+/g, '')
  return token.length > 0 && NAME_SUFFIXES.has(token)
}

function cleanNamePiece(value: string): string {
  return value.split(/\s+/).filter(Boolean).join(' ').replace(/^[ ,;]+|[ ,;]+$/g, '')
}

/** "Last, First[, Suffix]" as "First Last[ Suffix]"; any other name unchanged. */
function firstLastName(name: string): string {
  const text = cleanNamePiece(name)
  const parts = text.split(',').map((part) => part.trim()).filter(Boolean)
  if (parts.length < 2) return text
  const flipped = `${parts[1]} ${parts[0]}`
  const suffix = parts.slice(2).join(' ')
  return suffix && isNameSuffix(suffix) ? `${flipped} ${suffix}` : flipped
}

/** One bare word, or nothing but initials: the given half of "Last, First". */
function isGivenName(segment: string): boolean {
  const words = segment.split(' ')
  return words.length === 1 || words.every((word) => /^[\p{L}\p{N}_]\.?$/u.test(word))
}

/** A bare surname joins the given name after it; a word left alone is one person. */
function pairCommaSegments(segments: string[]): string[] {
  if (segments.length === 2 && !segments[0].includes(' ')) {
    return [firstLastName(segments.join(', '))]
  }
  const names: string[] = []
  let idx = 0
  while (idx < segments.length) {
    const segment = segments[idx]
    const following = segments[idx + 1] ?? ''
    if (!segment.includes(' ') && following && isGivenName(following) && !isNameSuffix(following)) {
      let name = `${following} ${segment}`
      idx += 2
      if (idx < segments.length && isNameSuffix(segments[idx])) {
        name = `${name} ${segments[idx]}`
        idx += 1
      }
      names.push(name)
      continue
    }
    names.push(segment)
    idx += 1
  }
  return names
}

export function parseAuthorNames(authors?: string | null): string[] {
  const text = String(authors ?? '').split(/\s+/).filter(Boolean).join(' ')
  if (!text) return []

  let names: string[]
  if (text.includes(';')) {
    names = text.split(';').map(firstLastName)
  } else if (text.includes(',')) {
    const segments = text
      .split(',')
      .map((part) => part.trim().replace(/^(and|&)\s+/i, ''))
      .filter(Boolean)
    names = pairCommaSegments(segments)
  } else {
    names = text.split(/\s+(?:and|&)\s+/i).map((part) => part.trim())
  }

  const isPerson = (name: string) => name && name.toLowerCase().replace(/\.+$/, '') !== 'et al'
  return names.filter(isPerson)
}

/** The same names without repeats: a card shows each person once. */
export function uniqueAuthorNames(authors?: string | null): string[] {
  const seen = new Set<string>()
  return parseAuthorNames(authors).filter((name) => {
    const key = normalizeAuthorName(name)
    if (!key || seen.has(key)) return false
    seen.add(key)
    return true
  })
}

/** The stored string as one readable line, for a table cell or a picker row. */
export function formatAuthorNames(authors?: string | null): string {
  return parseAuthorNames(authors).join(', ')
}
