import { describe, expect, it } from 'vitest'

import { DOCS_BASE_URL, docsUrl } from './docs'

describe('docsUrl', () => {
  it('resolves a docs-relative path, anchor included, under the site', () => {
    expect(docsUrl('/user-guide/reading-pdfs/#shadow-libraries')).toBe(
      `${DOCS_BASE_URL}user-guide/reading-pdfs/#shadow-libraries`,
    )
  })

  it('passes an absolute link through untouched', () => {
    expect(docsUrl('https://en.wikipedia.org/wiki/Anna%27s_Archive')).toBe(
      'https://en.wikipedia.org/wiki/Anna%27s_Archive',
    )
  })
})
