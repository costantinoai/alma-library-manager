import {
  discardPdfUpload,
  importPdf,
  retryPdfImport,
  waitForJob,
  type JobEnvelope,
  type PdfImportResult,
  type PendingPdfUpload,
} from '@/api/client'

/**
 * The PDF import queue — one module-scope owner, outside React's tree.
 *
 * A PDF import is a background Activity job that outlives the panel that
 * started it: the Import dialog's tabs unmount on a tab switch and the whole
 * dialog unmounts on close, so component state lost every running job, every
 * typed hint and every `upload_id` the user still owed a decision on (while
 * the file itself sat safely in the store's staging area for a day).
 *
 * The queue lives here instead, and the tab subscribes with
 * `useSyncExternalStore`. Unresolved rows are also seeded from the server's
 * pending list (`mergePendingUploads`), so they come back after a full reload
 * too — this module is the live view, the store's `.incoming` sidecars are the
 * durable truth.
 */

export type PdfImportRowStatus = 'waiting' | 'working' | 'done' | 'unresolved' | 'failed'

export interface PdfImportRow {
  /** Stable identity of the row (a local drop key, or the upload token when seeded). */
  key: string
  filename: string
  status: PdfImportRowStatus
  /** The one status line under the file name. */
  message: string
  /** The token addressing the kept file, once the server has one. */
  uploadId?: string
  /** What the user typed for this row — kept across a tab switch or close. */
  hint: string
  /** When the file was staged (ISO, naive UTC) — only for rows read back from the server. */
  stagedAt?: string
  /** Read back from the server's pending list rather than dropped here. */
  seeded?: boolean
  result?: PdfImportResult
}

export interface PdfImportQueueState {
  rows: PdfImportRow[]
  /** A drop the zone refused (nothing here was a PDF). */
  error: string | null
  /** Bumped whenever the server's pending set may have changed (refetch cue). */
  settled: number
  /** Bumped whenever a file actually landed in the Library (refresh cue). */
  imported: number
}

const EMPTY: PdfImportQueueState = { rows: [], error: null, settled: 0, imported: 0 }

let state: PdfImportQueueState = EMPTY
const listeners = new Set<() => void>()
/** Files run one at a time: each is its own job, and the server paces lookups. */
let chain: Promise<void> = Promise.resolve()
/** The dropped File per row — bytes, not render state, so out of the snapshot. */
const files = new Map<string, File>()

function publish(next: PdfImportQueueState): void {
  state = next
  for (const listener of listeners) listener()
}

function patchRow(key: string, patch: Partial<PdfImportRow>): void {
  const rows = state.rows.map((row) => (row.key === key ? { ...row, ...patch } : row))
  publish({ ...state, rows })
}

export function subscribeToPdfImports(listener: () => void): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function getPdfImportSnapshot(): PdfImportQueueState {
  return state
}

export function setPdfImportError(message: string | null): void {
  if (state.error === message) return
  publish({ ...state, error: message })
}

function outcomeLine(result: PdfImportResult): string {
  switch (result.outcome) {
    case 'imported':
      return `Saved to your Library: ${result.title}`
    case 'attached':
      return `Attached to ${result.title} (already in ALMa)`
    case 'already_stored':
      return `Already stored for ${result.title}`
    default:
      return 'Could not tell which paper this is'
  }
}

/** What the user typed: a DOI / doi.org link / arXiv id, or else a title. */
export function hintFrom(value: string): { doi?: string; title?: string } {
  const text = value.trim()
  const looksLikeId = /10\.\d{4,9}\/\S+/.test(text) || /arxiv|^\d{4}\.\d{4,5}(v\d+)?$/i.test(text)
  return looksLikeId ? { doi: text } : { title: text }
}

/** Start one job for `key` and follow it to its terminal Activity state. */
async function follow(key: string, start: () => Promise<JobEnvelope>): Promise<void> {
  patchRow(key, { status: 'working', message: 'Uploading…' })
  try {
    const envelope = await start()
    const result = await waitForJob<PdfImportResult>(envelope.job_id, {
      intervalMs: 1_000,
      timeoutMs: 5 * 60_000,
      onProgress: (status) => patchRow(key, { message: status.message ?? 'Working…' }),
    })
    const unresolved = result.outcome === 'unresolved'
    const settled: Partial<PdfImportRow> = {
      status: unresolved ? 'unresolved' : 'done',
      message: outcomeLine(result),
      uploadId: unresolved ? result.upload_id : undefined,
      result,
    }
    if (!unresolved) files.delete(key)
    // A pending-list refetch can land between the server keeping the file and
    // this result arriving, seeding a row for the very token this row is
    // about; the row that ran the job wins.
    const rows = state.rows
      .map((row) => (row.key === key ? { ...row, ...settled } : row))
      .filter((row) => !(row.seeded && row.key !== key && row.uploadId === result.upload_id))
    // Either way the staging area changed (a file kept, or one consumed).
    publish({
      ...state,
      rows,
      settled: state.settled + 1,
      imported: unresolved ? state.imported : state.imported + 1,
    })
  } catch (err) {
    patchRow(key, { status: 'failed', message: err instanceof Error ? err.message : String(err) })
  }
}

function enqueue(key: string, start: () => Promise<JobEnvelope>): void {
  chain = chain.then(() => follow(key, start))
}

/** Queue one job per dropped file, in the order they were given. */
export function addPdfFiles(dropped: File[]): void {
  const added: PdfImportRow[] = dropped.map((file) => ({
    key: `drop:${file.name}-${file.size}-${Math.random().toString(36).slice(2)}`,
    filename: file.name,
    status: 'waiting',
    message: 'Waiting…',
    hint: '',
  }))
  for (const [index, row] of added.entries()) files.set(row.key, dropped[index])
  publish({ ...state, rows: [...state.rows, ...added], error: null })
  for (const row of added) enqueue(row.key, () => importPdf(files.get(row.key) as File))
}

export function setPdfImportHint(key: string, hint: string): void {
  patchRow(key, { hint })
}

/** Retry an unresolved row with the DOI or title the user typed. */
export function retryPdfImportRow(key: string): void {
  const row = state.rows.find((candidate) => candidate.key === key)
  const value = (row?.hint ?? '').trim()
  if (!row || !row.uploadId || !value) return
  const uploadId = row.uploadId
  enqueue(key, () => retryPdfImport(uploadId, hintFrom(value)))
}

/** Give up on an unresolved row: the server drops the staged file, the row goes. */
export async function discardPdfImportRow(key: string): Promise<void> {
  const row = state.rows.find((candidate) => candidate.key === key)
  if (!row?.uploadId) return
  patchRow(key, { status: 'working', message: 'Giving up on this file…' })
  try {
    await discardPdfUpload(row.uploadId)
  } catch (err) {
    // A 404 means it is already gone — the row still has no reason to stay.
    const message = err instanceof Error ? err.message : String(err)
    if (!/404|already gone/i.test(message)) {
      patchRow(key, { status: 'unresolved', message })
      return
    }
  }
  files.delete(key)
  publish({
    ...state,
    rows: state.rows.filter((candidate) => candidate.key !== key),
    settled: state.settled + 1,
  })
}

/**
 * Reconcile the rows with the server's pending uploads.
 *
 * Adds a row for every kept upload this session has not seen (after a reload,
 * or dropped from another device), and removes seeded rows whose upload is
 * gone. Rows started here are never pruned: their own job is the authority on
 * what happened to them.
 */
export function mergePendingUploads(uploads: PendingPdfUpload[]): void {
  const live = new Set(uploads.map((upload) => upload.upload_id))
  const known = new Set(state.rows.map((row) => row.uploadId).filter(Boolean) as string[])
  const kept = state.rows.filter((row) => !row.seeded || (row.uploadId ? live.has(row.uploadId) : true))
  const added: PdfImportRow[] = uploads
    .filter((upload) => !known.has(upload.upload_id))
    .map((upload) => ({
      key: upload.upload_id,
      filename: upload.filename || 'A PDF with no name',
      status: 'unresolved',
      message: 'Could not tell which paper this is',
      uploadId: upload.upload_id,
      hint: '',
      stagedAt: upload.staged_at,
      seeded: true,
    }))
  if (added.length === 0 && kept.length === state.rows.length) return
  publish({ ...state, rows: [...kept, ...added] })
}

/** Test seam: forget everything (there is one queue per loaded app otherwise). */
export function resetPdfImportQueue(): void {
  files.clear()
  chain = Promise.resolve()
  publish(EMPTY)
}
