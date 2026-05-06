import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatNumber(num: number | undefined | null): string {
  if (num == null) return '0'
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}k`
  return num.toString()
}

/**
 * Parse an ISO string emitted by the ALMa backend.
 *
 * Every Activity/operation_status timestamp is written via
 * `datetime.utcnow().isoformat()` — naive ISO with NO trailing `Z`.
 * The JavaScript `Date` constructor interprets such strings as LOCAL
 * time, which silently produces a 1–12 h offset between the server
 * and the browser (seen pre-2026-04-25 as "deep refresh appears to
 * run for 2h" in Vienna/Brussels during CEST).  Treat any string
 * without an explicit timezone marker (trailing `Z`, `+HH:MM`, or
 * `-HH:MM` after the seconds) as UTC by appending `Z`.
 */
export function parseAlmaTimestamp(value: string | number | Date | null | undefined): Date {
  if (value instanceof Date) return value
  if (typeof value !== 'string') return new Date(value as never)
  const trimmed = value.trim()
  if (!trimmed) return new Date('')
  // Look for a timezone marker in the time portion (after the `T`).
  // Naive backend strings look like `2026-04-25T12:34:56.789` or
  // `2026-04-25T12:34:56`; dates alone (`2026-04-25`) we leave to the
  // Date constructor since they're timezone-ambiguous by spec anyway.
  const timeIdx = trimmed.indexOf('T')
  if (timeIdx < 0) return new Date(trimmed)
  const timePart = trimmed.slice(timeIdx + 1)
  const hasTz = /[zZ]$/.test(timePart) || /[+-]\d{2}:?\d{2}$/.test(timePart)
  return new Date(hasTz ? trimmed : `${trimmed}Z`)
}

export function formatDate(dateStr: string): string {
  const date = parseAlmaTimestamp(dateStr)
  if (Number.isNaN(date.getTime())) return dateStr
  // dd/mm/yyyy — date-only, no time. Publication dates don't have a
  // meaningful hour component, so surfacing one is just noise.
  const dd = String(date.getDate()).padStart(2, '0')
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const yyyy = date.getFullYear()
  return `${dd}/${mm}/${yyyy}`
}

/**
 * Best-effort published-date label for a paper row. Prefers the full
 * publication_date (dd/mm/yyyy), falls back to year, then empty.
 * Lets per-surface table columns and detail panels share one rule
 * instead of redoing the ternary inline.
 */
export function formatPublicationDate(
  pub: { publication_date?: string | null; year?: number | null } | null | undefined,
): string {
  if (!pub) return ''
  if (pub.publication_date) return formatDate(pub.publication_date)
  return pub.year != null ? String(pub.year) : ''
}

export function truncate(str: string | undefined | null, maxLen: number): string {
  if (!str) return ''
  if (str.length <= maxLen) return str
  return str.slice(0, maxLen - 3) + '...'
}

/**
 * Repair LaTeX-style "dotless ı + combining diacritic" sequences and NFC-normalise.
 *
 * OpenAlex / Crossref / Semantic Scholar occasionally surface author names
 * where a regular `i` followed by a combining diacritic was rendered as
 * `\i` + accent in upstream LaTeX, then ingested as Unicode dotless-i
 * (U+0131) followed by a combining mark (U+0300–U+036F). Examples seen in
 * production: `Marı́a Ruz` (should be `María`), `Antoni Rodrı́guez-Fornells`,
 * `Alain Taı̈eb`, `Sliman J. Bensmaı̈a`, `Benoı̂st Schaal`, `Giuseppe Alı̀`.
 *
 * The dotless-ı + combining mark sequence does not NFC-collapse into a
 * precomposed character on its own — `ı + ◌́` is not a recognised
 * precomposition. We substitute the dotless-ı back to a regular `i` first,
 * then NFC collapses `i + ◌́ → í`, `i + ◌̈ → ï`, `i + ◌̂ → î`, `i + ◌̀ → ì`.
 *
 * We do NOT strip diacritics; we restore them to their canonical precomposed
 * form so they render cleanly in any font with even partial Latin coverage.
 * The Phase 2 author-hydration pipeline should apply the same repair at the
 * write boundary (`alma/openalex/client.py`, `application/feed.py`) so this
 * frontend pass becomes a defensive belt-and-braces step.
 */
const DOTLESS_I_PLUS_COMBINING = /ı([̀-ͯ])/g

export function repairDisplayText(value?: string | null): string {
  if (!value) return ''
  return String(value).replace(DOTLESS_I_PLUS_COMBINING, 'i$1').normalize('NFC')
}

/**
 * Canonical author-name normalization. Collapses whitespace, lowercases, and
 * trims so "John  Smith", "  JOHN SMITH ", and "john smith" all become
 * "john smith". Used by PaperCard's follow-button state derivation and by
 * usePaperAuthorFollow for Set membership checks — both must agree or a
 * followed author appears un-followed in the UI.
 */
export function normalizeAuthorName(value?: string | null): string {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ')
}

export function formatTimestamp(value?: string | null): string {
  if (!value) return 'Never'
  const parsed = parseAlmaTimestamp(value)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString()
}

export function formatRelativeTime(dateStr: string): string {
  const date = parseAlmaTimestamp(dateStr)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))

  if (diffDays === 0) return 'today'
  if (diffDays === 1) return 'yesterday'
  if (diffDays < 7) return `${diffDays}d ago`
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`
  if (diffDays < 365) return `${Math.floor(diffDays / 30)}mo ago`
  return `${Math.floor(diffDays / 365)}y ago`
}

export function formatRelativeShort(value?: string | null): string {
  if (!value) return 'never'
  const date = parseAlmaTimestamp(value)
  if (Number.isNaN(date.getTime())) return value
  const diffMs = Date.now() - date.getTime()
  if (diffMs < 0) return 'just now'
  const sec = Math.floor(diffMs / 1000)
  if (sec < 45) return 'just now'
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.floor(hr / 24)
  if (day < 7) return `${day}d ago`
  if (day < 30) return `${Math.floor(day / 7)}w ago`
  if (day < 365) return `${Math.floor(day / 30)}mo ago`
  return `${Math.floor(day / 365)}y ago`
}

export function formatMonitorTypeLabel(monitorType?: string | null): string {
  switch (String(monitorType || '').trim().toLowerCase()) {
    case 'author':
      return 'Author'
    case 'query':
      return 'Keyword'
    case 'topic':
      return 'Topic'
    case 'venue':
      return 'Venue'
    case 'preprint':
      return 'Preprint'
    case 'branch':
      return 'Branch'
    default:
      return monitorType ? String(monitorType) : 'Monitor'
  }
}

/**
 * Trigger a client-side download for a Blob. Creates and cleans up the
 * object URL itself so call sites don't have to keep track of it. Used by
 * the settings cards that hand out backups, BibTeX exports, and JSON dumps.
 */
export function downloadBlob(blob: Blob, filename: string): void {
  const objectUrl = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = objectUrl
  anchor.download = filename
  anchor.click()
  URL.revokeObjectURL(objectUrl)
}

/**
 * Fetch a binary asset from the backend and trigger a download. Honours the
 * server's `Content-Disposition: attachment; filename="…"` header when
 * present; otherwise falls back to `fallbackName`. Throws on non-2xx so the
 * caller can surface an error toast.
 */
export async function downloadFromUrl(
  url: string,
  fallbackName: string,
  init?: RequestInit,
): Promise<string> {
  const response = await fetch(url, init)
  if (!response.ok) throw new Error(`Download failed (${response.status})`)
  const blob = await response.blob()
  const disposition = response.headers.get('Content-Disposition') || ''
  const filename = disposition.match(/filename="(.+)"/)?.[1] || fallbackName
  downloadBlob(blob, filename)
  return filename
}

/**
 * Serialise a JSON-serialisable value and download it as a `.json` file.
 * Handy for settings exports where the payload is already in memory and
 * no round-trip to the backend is needed.
 */
export function downloadJson(data: unknown, filename: string): void {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
  downloadBlob(blob, filename)
}

