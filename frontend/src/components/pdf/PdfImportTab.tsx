import { useRef, useState } from 'react'
import { AlertCircle, CheckCircle, FileQuestion, Loader2 } from 'lucide-react'

import { importPdf, retryPdfImport, waitForJob, type JobEnvelope, type PdfImportResult } from '@/api/client'
import { FileDropZone } from '@/components/shared/FileDropZone'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

type RowStatus = 'waiting' | 'working' | 'done' | 'unresolved' | 'failed'

interface Row {
  key: string
  file: File
  status: RowStatus
  message: string
  result?: PdfImportResult
}

const isPdf = (file: File) => file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')

/** What the user typed: a DOI / doi.org link / arXiv id, or else a title. */
function hintFrom(value: string): { doi?: string; title?: string } {
  const text = value.trim()
  const looksLikeId = /10\.\d{4,9}\/\S+/.test(text) || /arxiv|^\d{4}\.\d{4,5}(v\d+)?$/i.test(text)
  return looksLikeId ? { doi: text } : { title: text }
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

/**
 * Import tab: drop the PDFs you already have.
 *
 * Each file is one `pdf.import` Activity job, run one after another: ALMa
 * reads the file's DOI / arXiv id / title, matches a paper you ALREADY have
 * first (never a duplicate), otherwise finds it online and saves it to your
 * Library, and keeps the PDF with it. A file it cannot place waits here for
 * a DOI or title you type.
 */
export function PdfImportTab({ onImportComplete }: { onImportComplete?: () => void }) {
  const [rows, setRows] = useState<Row[]>([])
  const [error, setError] = useState<string | null>(null)
  const [hints, setHints] = useState<Record<string, string>>({})
  const queue = useRef<Promise<void>>(Promise.resolve())

  const update = (key: string, patch: Partial<Row>) =>
    setRows((current) => current.map((row) => (row.key === key ? { ...row, ...patch } : row)))

  const follow = async (key: string, start: () => Promise<JobEnvelope>) => {
    update(key, { status: 'working', message: 'Uploading…' })
    try {
      const envelope = await start()
      const result = await waitForJob<PdfImportResult>(envelope.job_id, {
        intervalMs: 1_000,
        timeoutMs: 5 * 60_000,
        onProgress: (status) => update(key, { message: status.message ?? 'Working…' }),
      })
      update(key, {
        status: result.outcome === 'unresolved' ? 'unresolved' : 'done',
        message: outcomeLine(result),
        result,
      })
      if (result.outcome !== 'unresolved') onImportComplete?.()
    } catch (err) {
      update(key, { status: 'failed', message: err instanceof Error ? err.message : String(err) })
    }
  }

  // Files run one at a time: each is its own job, and the server paces lookups.
  const enqueue = (key: string, start: () => Promise<JobEnvelope>) => {
    queue.current = queue.current.then(() => follow(key, start))
  }

  const addFiles = (files: File[]) => {
    setError(null)
    const added = files.map((file) => ({
      key: `${file.name}-${file.size}-${Math.random().toString(36).slice(2)}`,
      file,
      status: 'waiting' as const,
      message: 'Waiting…',
    }))
    setRows((current) => [...current, ...added])
    for (const row of added) enqueue(row.key, () => importPdf(row.file))
  }

  const retry = (row: Row) => {
    const value = (hints[row.key] ?? '').trim()
    const uploadId = row.result?.upload_id
    if (!value || !uploadId) return
    enqueue(row.key, () => retryPdfImport(uploadId, hintFrom(value)))
  }

  return (
    <div className="space-y-4">
      <p className="text-sm text-slate-600">
        Drop PDFs you already have. ALMa reads each file&apos;s DOI or title, matches a paper you
        already have first, otherwise saves it to your Library, and keeps the PDF with it.
      </p>
      <FileDropZone
        accept="application/pdf,.pdf"
        multiple
        isAccepted={isPdf}
        onFiles={addFiles}
        onRejected={setError}
        rejectMessage="Please drop PDF files"
        prompt="Drop PDF files here or click to browse"
        hint="One or many; each is matched to its paper"
      />
      {error && (
        <p className="flex items-center gap-2 text-sm text-critical-700">
          <AlertCircle className="h-4 w-4" aria-hidden /> {error}
        </p>
      )}
      {rows.length > 0 && (
        <ul className="divide-y divide-[var(--color-border)] rounded-sm border border-[var(--color-border)]">
          {rows.map((row) => (
            <li key={row.key} className="space-y-2 px-3 py-2.5">
              <div className="flex items-start gap-2 text-sm">
                {row.status === 'done' && <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-success-600" aria-hidden />}
                {row.status === 'failed' && <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-critical-600" aria-hidden />}
                {row.status === 'unresolved' && <FileQuestion className="mt-0.5 h-4 w-4 shrink-0 text-slate-500" aria-hidden />}
                {(row.status === 'working' || row.status === 'waiting') && (
                  <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-slate-400" aria-hidden />
                )}
                <div className="min-w-0">
                  <p className="truncate font-medium text-alma-800">{row.file.name}</p>
                  <p className="text-xs text-slate-500" role="status">{row.message}</p>
                </div>
              </div>
              {row.status === 'unresolved' && (
                <form
                  className="flex gap-2"
                  onSubmit={(event) => {
                    event.preventDefault()
                    retry(row)
                  }}
                >
                  <Input
                    value={hints[row.key] ?? ''}
                    onChange={(event) => setHints((current) => ({ ...current, [row.key]: event.target.value }))}
                    placeholder="DOI, arXiv id or title"
                    aria-label={`DOI or title for ${row.file.name}`}
                    className="h-8 text-xs"
                  />
                  <Button type="submit" size="sm" variant="outline" disabled={!(hints[row.key] ?? '').trim()}>
                    Retry
                  </Button>
                </form>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
