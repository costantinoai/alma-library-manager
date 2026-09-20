import { useEffect, useRef, useSyncExternalStore } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { AlertCircle, CheckCircle, FileQuestion, Loader2 } from 'lucide-react'

import { FileDropZone } from '@/components/shared/FileDropZone'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { PENDING_PDF_UPLOADS_KEY, usePendingPdfUploads } from '@/hooks/usePendingPdfUploads'
import {
  addPdfFiles,
  discardPdfImportRow,
  getPdfImportSnapshot,
  mergePendingUploads,
  retryPdfImportRow,
  setPdfImportError,
  setPdfImportHint,
  subscribeToPdfImports,
  type PdfImportRow,
} from '@/lib/pdfImportQueue'
import { formatRelativeShort } from '@/lib/utils'

const isPdf = (file: File) => file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')

/**
 * Import tab: drop the PDFs you already have.
 *
 * Each file is one `pdf.import` Activity job, run one after another: ALMa
 * reads the file's DOI / arXiv id / title, matches a paper you ALREADY have
 * first (never a duplicate), otherwise finds it online and saves it to your
 * Library, and keeps the PDF with it. A file it cannot place waits here for
 * a DOI or title you type.
 *
 * The rows are NOT this component's state. Radix unmounts an inactive tab and
 * the dialog unmounts the lot on close, which used to take every running job
 * and every unresolved upload with it; the queue lives in
 * `lib/pdfImportQueue` and the unresolved ones are read back from the server,
 * so switching tabs, closing the dialog or reloading the page all keep them.
 */
export function PdfImportTab({ onImportComplete }: { onImportComplete?: () => void }) {
  const queue = useSyncExternalStore(subscribeToPdfImports, getPdfImportSnapshot, getPdfImportSnapshot)
  const queryClient = useQueryClient()
  const pending = usePendingPdfUploads()

  // Seed / reconcile the rows the server is still holding files for.
  useEffect(() => {
    if (pending.data) mergePendingUploads(pending.data)
  }, [pending.data])

  // A finished job changes what the server is holding; one that landed also
  // changes the Library. Compare against what this mount has already seen, so
  // a job that finished while the dialog was closed does not fire twice.
  const seen = useRef({ settled: queue.settled, imported: queue.imported })
  useEffect(() => {
    if (queue.settled !== seen.current.settled) {
      seen.current.settled = queue.settled
      void queryClient.invalidateQueries({ queryKey: PENDING_PDF_UPLOADS_KEY })
    }
    if (queue.imported !== seen.current.imported) {
      seen.current.imported = queue.imported
      onImportComplete?.()
    }
  }, [queue.settled, queue.imported, queryClient, onImportComplete])

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
        onFiles={addPdfFiles}
        onRejected={setPdfImportError}
        rejectMessage="Please drop PDF files"
        prompt="Drop PDF files here or click to browse"
        hint="One or many; each is matched to its paper"
      />
      {queue.error && (
        <p className="flex items-center gap-2 text-sm text-critical-700">
          <AlertCircle className="h-4 w-4" aria-hidden /> {queue.error}
        </p>
      )}
      {pending.isError && (
        <p className="flex items-center gap-2 text-sm text-critical-700" role="alert">
          <AlertCircle className="h-4 w-4" aria-hidden /> Could not read the PDFs still waiting for a
          DOI or title. They are kept for a day — reopen this tab to try again.
        </p>
      )}
      {queue.rows.length > 0 && (
        <ul className="divide-y divide-[var(--color-border)] rounded-sm border border-[var(--color-border)]">
          {queue.rows.map((row) => (
            <ImportRow key={row.key} row={row} />
          ))}
        </ul>
      )}
    </div>
  )
}

/** One dropped (or kept) file: its status line, and the hint form when unplaced. */
function ImportRow({ row }: { row: PdfImportRow }) {
  const waiting = row.status === 'unresolved' && Boolean(row.uploadId)
  return (
    <li className="space-y-2 px-3 py-2.5">
      <div className="flex items-start gap-2 text-sm">
        {row.status === 'done' && <CheckCircle className="mt-0.5 h-4 w-4 shrink-0 text-success-600" aria-hidden />}
        {row.status === 'failed' && <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-critical-600" aria-hidden />}
        {row.status === 'unresolved' && <FileQuestion className="mt-0.5 h-4 w-4 shrink-0 text-slate-500" aria-hidden />}
        {(row.status === 'working' || row.status === 'waiting') && (
          <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-slate-400" aria-hidden />
        )}
        <div className="min-w-0">
          <p className="truncate font-medium text-alma-800">{row.filename}</p>
          <p className="text-xs text-slate-500" role="status">
            {row.message}
            {row.stagedAt && ` · kept ${formatRelativeShort(row.stagedAt)}`}
          </p>
        </div>
      </div>
      {waiting && (
        <form
          className="flex gap-2"
          onSubmit={(event) => {
            event.preventDefault()
            retryPdfImportRow(row.key)
          }}
        >
          <Input
            value={row.hint}
            onChange={(event) => setPdfImportHint(row.key, event.target.value)}
            placeholder="DOI, arXiv id or title"
            aria-label={`DOI or title for ${row.filename}`}
            className="h-8 text-xs"
          />
          <Button type="submit" size="sm" variant="outline" disabled={!row.hint.trim()}>
            Retry
          </Button>
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={() => void discardPdfImportRow(row.key)}
            aria-label={`Give up on ${row.filename}`}
          >
            Give up
          </Button>
        </form>
      )}
    </li>
  )
}
