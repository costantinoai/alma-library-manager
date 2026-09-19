/**
 * Paper-PDF vocabulary shared by the detail panel, the reader page and the
 * import tab — one wording for what happened, spelled once.
 */
import type { PaperPdfAttempt, PaperPdfStored, PdfVerification } from '@/api/client'
import type { SignalKind } from '@/components/shared/signalKinds'
import { formatBytes } from '@/lib/format'

/** Plain words for a source's latest outcome (mirrors the backend's). */
export const PDF_OUTCOME_LABELS: Record<string, string> = {
  found: 'found',
  no_candidate: 'no copy listed',
  not_pdf: 'not a PDF',
  blocked: 'blocked by a bot check',
  http_error: 'server refused',
  too_large: 'file too large',
  mismatch: "a different paper's PDF",
  rejected: 'the file you marked wrong',
  skipped: 'not set up',
  error: 'unreachable',
}

/** Display names for source ids (fallback: the id itself). */
const SOURCE_LABELS: Record<string, string> = {
  arxiv: 'arXiv',
  pmc: 'PubMed Central',
  openalex: 'OpenAlex',
  unpaywall: 'Unpaywall',
  crossref: 'Crossref',
  publisher: 'Publisher page',
  openalex_content: 'OpenAlex full text',
  semantic_scholar: 'Semantic Scholar',
  scihub: 'Sci-Hub',
  annas_archive: "Anna's Archive",
}

export function pdfSourceLabel(sourceId: string | null | undefined): string {
  if (!sourceId) return 'your upload'
  return SOURCE_LABELS[sourceId] ?? sourceId
}

/** "Unpaywall: no copy listed" for one attempt row. A skipped source says
 *  why in its own words ("Unpaywall: needs a contact email — …"). */
export function describeAttempt(attempt: PaperPdfAttempt): string {
  const outcome =
    attempt.outcome === 'skipped' && attempt.detail
      ? attempt.detail
      : (PDF_OUTCOME_LABELS[attempt.outcome] ?? attempt.outcome)
  return `${pdfSourceLabel(attempt.source_id)}: ${outcome}`
}

/**
 * What each place said, for a source that tried several (one line per mirror
 * or listed location: "sci-hub.ru: Bot check page"). The backend joins them
 * as "host: words; host: words"; a single-place miss has none.
 */
export function attemptPlaces(attempt: PaperPdfAttempt): string[] {
  if (attempt.outcome === 'skipped' || attempt.outcome === 'found' || !attempt.detail) return []
  const places = attempt.detail.split('; ').filter(Boolean)
  return places.length > 1 ? places : []
}

/** The chip for a stored PDF: which kind, and its one-line label. */
export function pdfChip(stored: PaperPdfStored): { kind: SignalKind; label: string } {
  const kinds: Record<PdfVerification, SignalKind> = {
    doi: 'pdf',
    title: 'pdf',
    unverified: 'pdf-unverified',
    mismatch: 'pdf-mismatch',
  }
  const size = formatBytes(stored.bytes)
  const pages = stored.pages ? `${stored.pages} pp · ` : ''
  const label =
    stored.verification === 'mismatch'
      ? 'PDF names another paper'
      : stored.verification === 'unverified'
        ? `PDF · ${pages}${size} · unverified`
        : `PDF · ${pages}${size}`
  return { kind: kinds[stored.verification], label }
}
