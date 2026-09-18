import { describe, expect, it } from 'vitest'

import type { PaperPdfAttempt, PaperPdfStored } from '@/api/client'

import { buildReaderHref, parseHashRoute, parseReaderRoute } from './hashRoute'
import { describeAttempt, pdfChip, pdfSourceLabel } from './pdf'

describe('PDF reader route', () => {
  it('round-trips a paper id and is not a page', () => {
    const href = buildReaderHref('a b/c')
    expect(href).toBe('#/read?paper=a%20b%2Fc')
    expect(parseReaderRoute(href)).toEqual({ paperId: 'a b/c' })
    expect(parseHashRoute(href).found).toBe(false)
  })

  it('ignores other routes and a missing paper', () => {
    expect(parseReaderRoute('#/library?paper=x')).toBeNull()
    expect(parseReaderRoute('#/read')).toBeNull()
    expect(parseReaderRoute('#/read?paper=%20')).toBeNull()
  })
})

const stored = (over: Partial<PaperPdfStored> = {}): PaperPdfStored => ({
  filename: 'Doe 2020 - Title.pdf',
  sha256: 'x',
  bytes: 2_500_000,
  pages: 12,
  origin: 'fetched',
  verification: 'doi',
  source_id: 'unpaywall',
  source_plugin: 'open_access',
  source_url: null,
  version: 'publishedVersion',
  license: 'cc-by',
  stored_at: '2026-09-19T00:00:00',
  ...over,
})

describe('PDF vocabulary', () => {
  it('picks the chip by what the file proves', () => {
    expect(pdfChip(stored())).toEqual({ kind: 'pdf', label: 'PDF · 12 pp · 2.4 MB' })
    expect(pdfChip(stored({ verification: 'unverified', pages: null })).kind).toBe('pdf-unverified')
    expect(pdfChip(stored({ verification: 'mismatch' }))).toEqual({
      kind: 'pdf-mismatch',
      label: 'PDF names another paper',
    })
  })

  it('describes attempts in plain words', () => {
    const attempt = (outcome: string, detail: string | null = null): PaperPdfAttempt => ({
      source_id: 'unpaywall',
      outcome,
      http_status: null,
      detail,
      candidate_url: null,
      job_id: null,
      attempted_at: '',
    })
    expect(describeAttempt(attempt('no_candidate'))).toBe('Unpaywall: no copy listed')
    expect(describeAttempt(attempt('skipped', 'needs a contact email'))).toBe(
      'Unpaywall: not set up (needs a contact email)',
    )
    expect(pdfSourceLabel('scihub')).toBe('Sci-Hub')
    expect(pdfSourceLabel(null)).toBe('your upload')
  })
})
