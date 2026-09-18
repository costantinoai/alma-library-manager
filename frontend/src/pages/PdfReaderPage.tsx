import { useCallback, useEffect, useRef, useState } from 'react'
import { FileSearch, Loader2, Paperclip, RotateCw } from 'lucide-react'

import {
  api,
  ApiError,
  fetchPaperPdf,
  paperPdfUrl,
  uploadPaperPdf,
  type PaperPdfState,
  type PdfFetchResult,
} from '@/api/client'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { SurfaceProvider } from '@/components/ui/surface'
import { usePdfJob } from '@/hooks/usePdfJob'
import { describeAttempt } from '@/lib/pdf'

type Phase = 'loading' | 'opening' | 'working' | 'not-found' | 'no-source' | 'error'

/**
 * The PDF handoff page (`#/read?paper=ID`), opened in a new tab by a paper
 * card's PDF link — so the tab exists from the tap itself, which phones
 * require. It becomes the PDF:
 *
 * - the paper already has one → replaced by the file at once;
 * - it has never been looked for → asks the enabled sources (an Activity
 *   job), showing each step, then opens the file;
 * - earlier looks failed → says what each source said, with Try again and
 *   Attach — never re-fetching on its own, so a restored tab stays quiet.
 */
export function PdfReaderPage({ paperId }: { paperId: string }) {
  const [phase, setPhase] = useState<Phase>('loading')
  const [title, setTitle] = useState('')
  const [pdf, setPdf] = useState<PaperPdfState | null>(null)
  const [error, setError] = useState('')
  const job = usePdfJob()
  const fileInput = useRef<HTMLInputElement>(null)
  const started = useRef(false)

  const open = useCallback(() => {
    setPhase('opening')
    window.location.replace(paperPdfUrl(paperId))
  }, [paperId])

  const load = useCallback(async () => {
    const details = await api.get<{ title: string; pdf: PaperPdfState }>(
      `/papers/${encodeURIComponent(paperId)}/details`,
    )
    setTitle(details.title)
    setPdf(details.pdf)
    return details.pdf
  }, [paperId])

  const find = useCallback(async () => {
    setPhase('working')
    try {
      const result = await job.run<PdfFetchResult>(() => fetchPaperPdf(paperId))
      if (result.found) {
        open()
        return
      }
      await load()
      setPhase('not-found')
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        setPhase('no-source')
        return
      }
      setError(err instanceof Error ? err.message : String(err))
      setPhase('error')
    }
  }, [job, load, open, paperId])

  const attach = useCallback(
    async (file: File | undefined) => {
      if (!file) return
      setPhase('working')
      try {
        await job.run(() => uploadPaperPdf(paperId, file))
        open()
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err))
        setPhase('error')
      }
    },
    [job, open, paperId],
  )

  useEffect(() => {
    if (started.current) return
    started.current = true
    load()
      .then((state) => {
        if (state.url) open()
        else if (state.attempts.length === 0) void find()
        else setPhase('not-found')
      })
      .catch((err: unknown) => {
        setError(err instanceof ApiError && err.status === 404 ? 'This paper is not in ALMa.' : String(err))
        setPhase('error')
      })
  }, [find, load, open])

  return (
    <div className="grid min-h-[100dvh] place-items-center p-4">
      <SurfaceProvider level={1}>
        <Card variant="elevated" className="w-full max-w-md space-y-4 p-6">
          <div className="flex items-center gap-3">
            <img src="/brand/alma-mark-source.svg" alt="" aria-hidden className="h-8 w-8 shrink-0" />
            <p className="min-w-0 text-sm font-medium leading-snug text-alma-900">{title || 'Paper PDF'}</p>
          </div>

          {(phase === 'loading' || phase === 'opening' || phase === 'working') && (
            <p className="inline-flex items-center gap-2 text-sm text-slate-600" role="status">
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
              {phase === 'opening' ? 'Opening the PDF…' : (job.message ?? 'Looking for the PDF…')}
            </p>
          )}

          {phase === 'not-found' && (
            <div className="space-y-2">
              <p className="text-sm text-slate-700">No PDF found for this paper yet.</p>
              {pdf && pdf.attempts.length > 0 && (
                <ul className="space-y-0.5 text-xs text-slate-500">
                  {pdf.attempts.map((attempt) => (
                    <li key={attempt.source_id}>{describeAttempt(attempt)}</li>
                  ))}
                </ul>
              )}
            </div>
          )}
          {phase === 'no-source' && (
            <p className="text-sm text-slate-700">
              No PDF source is switched on. Turn one on in Settings → Plugins, or attach the file yourself.
            </p>
          )}
          {phase === 'error' && <p className="text-sm text-slate-700">{error}</p>}

          {(phase === 'not-found' || phase === 'no-source' || phase === 'error') && (
            <div className="flex flex-wrap gap-2">
              {phase !== 'no-source' && (
                <Button size="sm" variant="outline" onClick={() => void find()}>
                  {phase === 'not-found' ? <RotateCw aria-hidden /> : <FileSearch aria-hidden />} Try again
                </Button>
              )}
              <Button size="sm" onClick={() => fileInput.current?.click()}>
                <Paperclip aria-hidden /> Attach a PDF…
              </Button>
              <input
                ref={fileInput}
                type="file"
                accept="application/pdf,.pdf"
                className="sr-only"
                aria-label="Choose a PDF file"
                onChange={(event) => void attach(event.target.files?.[0])}
              />
            </div>
          )}
        </Card>
      </SurfaceProvider>
    </div>
  )
}

export default PdfReaderPage
