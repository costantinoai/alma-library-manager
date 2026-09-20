import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { PdfImportTab } from './PdfImportTab'
import { resetPdfImportQueue } from '@/lib/pdfImportQueue'

const TOKEN = 'a'.repeat(32)

const importPdf = vi.fn()
const retryPdfImport = vi.fn()
const waitForJob = vi.fn()
const listPendingPdfUploads = vi.fn()
const discardPdfUpload = vi.fn()

vi.mock('@/api/client', () => ({
  importPdf: (...args: unknown[]) => importPdf(...args),
  retryPdfImport: (...args: unknown[]) => retryPdfImport(...args),
  waitForJob: (...args: unknown[]) => waitForJob(...args),
  listPendingPdfUploads: (...args: unknown[]) => listPendingPdfUploads(...args),
  discardPdfUpload: (...args: unknown[]) => discardPdfUpload(...args),
}))

/** The tab, mounted the way the Import dialog mounts it (each mount is fresh). */
function mountTab() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <PdfImportTab />
    </QueryClientProvider>,
  )
}

/** Drop one PDF on the zone's file input. */
function dropPdf(container: HTMLElement, name = 'scan_0001.pdf') {
  const input = container.querySelector('input[type="file"]') as HTMLInputElement
  const file = new File(['%PDF-1.7'], name, { type: 'application/pdf' })
  fireEvent.change(input, { target: { files: [file] } })
}

describe('PDF import rows survive the panel that started them', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    resetPdfImportQueue()
    importPdf.mockResolvedValue({ job_id: 'job-1', status: 'queued', activity_url: '/api/v1/activity/job-1' })
    waitForJob.mockResolvedValue({ outcome: 'unresolved', upload_id: TOKEN, filename: 'scan_0001.pdf' })
    listPendingPdfUploads.mockResolvedValue([])
    discardPdfUpload.mockResolvedValue({ status: 'discarded', upload_id: TOKEN })
  })

  it('keeps an unresolved upload and its typed hint across unmount and remount', async () => {
    const first = mountTab()
    dropPdf(first.container)

    expect(await screen.findByText('Could not tell which paper this is')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('DOI or title for scan_0001.pdf'), {
      target: { value: '10.5555/typed.1' },
    })

    // The dialog closes (or the user switches tabs): Radix unmounts the tab.
    first.unmount()
    expect(screen.queryByText('scan_0001.pdf')).not.toBeInTheDocument()

    mountTab()
    expect(await screen.findByText('scan_0001.pdf')).toBeInTheDocument()
    expect(screen.getByLabelText('DOI or title for scan_0001.pdf')).toHaveValue('10.5555/typed.1')

    // And the kept file is still addressable: Retry names the same upload.
    retryPdfImport.mockResolvedValue({ job_id: 'job-2', status: 'queued', activity_url: '' })
    waitForJob.mockResolvedValue({ outcome: 'attached', paper_id: 'p1', title: 'A paper' })
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await waitFor(() => expect(retryPdfImport).toHaveBeenCalledWith(TOKEN, { doi: '10.5555/typed.1' }))
    expect(await screen.findByText(/Attached to A paper/)).toBeInTheDocument()
  })

  it('reads unresolved uploads back from the server after a full reload', async () => {
    // Nothing in memory (a reloaded page), one file still staged server-side.
    listPendingPdfUploads.mockResolvedValue([
      { upload_id: TOKEN, filename: 'from_before.pdf', sha256: 'f'.repeat(64), bytes: 1024, staged_at: '2026-09-20T09:00:00' },
    ])
    mountTab()

    expect(await screen.findByText('from_before.pdf')).toBeInTheDocument()
    expect(screen.getByText(/Could not tell which paper this is/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Give up on from_before.pdf' })).toBeInTheDocument()
  })

  it('gives up on an unresolved upload and drops its row', async () => {
    listPendingPdfUploads.mockResolvedValue([
      { upload_id: TOKEN, filename: 'from_before.pdf', sha256: 'f'.repeat(64), bytes: 1024, staged_at: '2026-09-20T09:00:00' },
    ])
    mountTab()
    fireEvent.click(await screen.findByRole('button', { name: 'Give up on from_before.pdf' }))

    await waitFor(() => expect(discardPdfUpload).toHaveBeenCalledWith(TOKEN))
    // The refetch after a discard answers with nothing left.
    listPendingPdfUploads.mockResolvedValue([])
    await waitFor(() => expect(screen.queryByText('from_before.pdf')).not.toBeInTheDocument())
  })

  it('does not show a locally dropped file twice when the server list names it too', async () => {
    listPendingPdfUploads.mockResolvedValue([
      { upload_id: TOKEN, filename: 'scan_0001.pdf', sha256: 'f'.repeat(64), bytes: 8, staged_at: '2026-09-20T09:00:00' },
    ])
    const view = mountTab()
    dropPdf(view.container)

    await waitFor(() => expect(screen.getAllByText('scan_0001.pdf')).toHaveLength(1))
  })
})
