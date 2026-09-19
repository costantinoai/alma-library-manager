import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { FileSearch, Loader2, Paperclip } from 'lucide-react'

import {
  deletePaperPdf,
  fetchPaperPdf,
  getPaperPdfState,
  paperPdfUrl,
  uploadPaperPdf,
  type PaperPdfState,
  type PdfFetchResult,
} from '@/api/client'
import { PdfAttemptItem } from '@/components/pdf/PdfAttemptItem'
import { SignalChip } from '@/components/shared/SignalChip'
import { Button } from '@/components/ui/button'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { usePdfFetchEnabled } from '@/hooks/usePdfSources'
import { usePdfJob } from '@/hooks/usePdfJob'
import { errorToast, useToast } from '@/hooks/useToast'
import { pdfChip, pdfSourceLabel } from '@/lib/pdf'

interface PaperPdfSectionProps {
  paperId: string
  /** The `pdf` block of the paper's `/details` payload (null while loading). */
  pdf: PaperPdfState | null | undefined
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * The paper's PDF inside the detail dialog: what is kept, and the one action
 * that fits — Open it, or Find / Attach one. Everything secondary (what each
 * source said, provenance) sits in a fold. Each action is an Activity job; the
 * row shows its live step while it runs, then re-reads the paper's PDF state.
 */
export function PaperPdfSection({ paperId, pdf }: PaperPdfSectionProps) {
  const fetchEnabled = usePdfFetchEnabled()
  const job = usePdfJob()
  const { toast } = useToast()
  const queryClient = useQueryClient()
  const fileInput = useRef<HTMLInputElement>(null)
  const [state, setState] = useState<PaperPdfState | null>(pdf ?? null)

  useEffect(() => setState(pdf ?? null), [pdf])

  const refresh = async () => {
    try {
      setState(await getPaperPdfState(paperId))
    } finally {
      void queryClient.invalidateQueries({ queryKey: ['paper-pdf', paperId] })
    }
  }

  const find = async () => {
    try {
      const result = await job.run<PdfFetchResult>(() => fetchPaperPdf(paperId))
      if (result.found) toast({ title: 'PDF found', description: `From ${result.source}` })
      else errorToast('No PDF found', `Tried ${result.tried?.join(', ') ?? 'every source'}`)
    } catch (error) {
      errorToast('Could not look for a PDF', messageOf(error))
    } finally {
      await refresh()
    }
  }

  const attach = async (file: File | undefined) => {
    if (!file) return
    try {
      await job.run(() => uploadPaperPdf(paperId, file))
      toast({ title: 'PDF attached', description: file.name })
    } catch (error) {
      errorToast('Could not attach the PDF', messageOf(error))
    } finally {
      if (fileInput.current) fileInput.current.value = ''
      await refresh()
    }
  }

  const markWrong = async () => {
    try {
      await deletePaperPdf(paperId, true)
      toast({ title: 'PDF removed', description: 'That file will not be used for this paper again.' })
    } catch (error) {
      errorToast('Could not remove the PDF', messageOf(error))
    } finally {
      await refresh()
    }
  }

  const stored = state?.stored ?? null
  const available = Boolean(stored && state?.url)
  const attempts = state?.attempts ?? []
  const chip = stored ? pdfChip(stored) : null

  return (
    <section>
      <EyebrowLabel tone="muted" className="mb-1.5 inline-flex items-center gap-2">
        PDF
      </EyebrowLabel>
      <div className="flex flex-wrap items-center gap-2">
        {available && chip && (
          <SignalChip kind={chip.kind} size="default">{chip.label}</SignalChip>
        )}
        {available && (
          <Button asChild size="xs">
            <a href={paperPdfUrl(paperId)} target="_blank" rel="noopener">
              Open PDF
            </a>
          </Button>
        )}
        {!available && fetchEnabled && (
          <Button size="xs" variant="outline" onClick={find} disabled={job.running}>
            <FileSearch aria-hidden /> Find PDF
          </Button>
        )}
        <Button size="xs" variant="ghost" onClick={() => fileInput.current?.click()} disabled={job.running}>
          <Paperclip aria-hidden /> {available ? 'Replace…' : 'Attach PDF…'}
        </Button>
        {available && (
          <Button size="xs" variant="ghost" onClick={markWrong} disabled={job.running}>
            Wrong PDF
          </Button>
        )}
        <input
          ref={fileInput}
          type="file"
          accept="application/pdf,.pdf"
          className="sr-only"
          aria-label="Choose a PDF file"
          onChange={(event) => void attach(event.target.files?.[0])}
        />
      </div>

      {job.running && (
        <p className="mt-2 inline-flex items-center gap-1.5 text-xs text-slate-500" role="status">
          <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
          {job.message ?? 'Working…'}
        </p>
      )}
      {state?.file_missing && (
        <p className="mt-2 text-xs text-slate-500">The kept file is missing — find or attach it again.</p>
      )}
      {!available && !fetchEnabled && !job.running && (
        <p className="mt-2 text-xs text-slate-500">
          Switch on a PDF source in Settings → Plugins to have ALMa find it.
        </p>
      )}

      {(attempts.length > 0 || (available && stored)) && (
        <DisclosurePanel
          className="mt-3"
          title={available ? 'Where this PDF came from' : 'What was tried'}
          meta={
            available && stored ? (
              <span className="text-xs text-slate-500">
                {stored.origin === 'fetched' ? `From ${pdfSourceLabel(stored.source_id)}` : 'You added it'}
                {stored.version ? ` · ${stored.version.replace('Version', ' version')}` : ''}
              </span>
            ) : (
              <span className="text-xs text-slate-500">{attempts.length} source(s)</span>
            )
          }
        >
          <ul className="space-y-1 text-xs text-slate-600">
            {available && stored && <li>File: {stored.filename}</li>}
            {available && stored?.license && <li>Licence: {stored.license}</li>}
            {attempts.map((attempt) => (
              <PdfAttemptItem key={attempt.source_id} attempt={attempt} />
            ))}
          </ul>
        </DisclosurePanel>
      )}
    </section>
  )
}
