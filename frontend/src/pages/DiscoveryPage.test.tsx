import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { TooltipProvider } from '@/components/ui/tooltip'
import { DiscoveryPage } from './DiscoveryPage'

const listLenses = vi.fn()
const listLensRecommendations = vi.fn()
const explainRecommendation = vi.fn()

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  listLenses: (...args: unknown[]) => listLenses(...args),
  listLensRecommendations: (...args: unknown[]) => listLensRecommendations(...args),
  explainRecommendation: (...args: unknown[]) => explainRecommendation(...args),
  getDiscoverySettings: () => Promise.resolve({ schedule: { refresh_enabled: false, refresh_interval_hours: 6 } }),
  markLensSeen: () => Promise.resolve({}),
}))

vi.mock('@/components/discovery', () => ({
  LensManager: ({ lenses, onSelectLens }: { lenses: Array<{ id: string; name: string }>; onSelectLens: (id: string) => void }) => (
    <div>{lenses.map((lens) => <button key={lens.id} onClick={() => onSelectLens(lens.id)}>{lens.name}</button>)}</div>
  ),
  BranchExplorerPanel: () => null,
  LensWeightsPanel: () => null,
  PaperDetailPanel: () => null,
}))

vi.mock('@/components/shared', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/shared')>()),
  PaperCard: ({ paper, onExpandBreakdown }: { paper: { title: string }; onExpandBreakdown: () => void }) => (
    <div>{paper.title}<button onClick={onExpandBreakdown}>Why</button></div>
  ),
}))

vi.mock('@/components/onboarding', () => ({ PageTour: () => null, DISCOVERY_TOUR: [] }))
vi.mock('@/hooks/useDiscoveryImpressions', () => ({ useDiscoveryImpressions: () => () => undefined }))

const lenses = [
  { id: 'one', name: 'Lens One', context_type: 'library_global', last_refreshed_at: '2026-09-01T12:00:00Z' },
  { id: 'two', name: 'Lens Two', context_type: 'library_global', last_refreshed_at: null },
]

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <TooltipProvider><DiscoveryPage /></TooltipProvider>
    </QueryClientProvider>,
  )
}

describe('DiscoveryPage lens state', () => {
  beforeEach(() => {
    window.location.hash = '#/discovery?lens=one'
    listLenses.mockReset().mockResolvedValue(lenses)
    listLensRecommendations.mockReset().mockResolvedValue([])
    explainRecommendation.mockReset()
  })

  it('applies a lens deep link once and lets the reader switch lenses', async () => {
    renderPage()
    await screen.findByText('Active:')
    expect(listLensRecommendations).toHaveBeenCalledWith('one', expect.anything())
    expect(screen.getByText((text, element) => element?.tagName === 'SPAN' && /^Last refresh /.test(text))).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Lens Two' }))
    await waitFor(() => expect(listLensRecommendations).toHaveBeenCalledWith('two', expect.anything()))
    expect(screen.getByText('No refresh on record yet')).toBeInTheDocument()
  })

  it('shows a fetch error with retry instead of an empty deck', async () => {
    listLensRecommendations.mockRejectedValueOnce(new Error('offline')).mockResolvedValue([])
    renderPage()

    await screen.findByText('Could not load recommendations for this lens.')
    expect(screen.queryByText('No recommendations yet')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    await screen.findByText('No recommendations yet')
  })

  it('retries a failed score explanation on the next expansion', async () => {
    listLensRecommendations.mockResolvedValue([{
      id: 'rec-1', paper_id: 'paper-1', score: 50, created_at: '2026-09-01T12:00:00Z',
      paper: { id: 'paper-1', title: 'Candidate paper' },
    }])
    explainRecommendation.mockRejectedValueOnce(new Error('offline')).mockResolvedValue({ explanation: 'Matched branch seeds' })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined)
    renderPage()

    await screen.findByText('Candidate paper')
    fireEvent.click(screen.getByRole('button', { name: 'Why' }))
    await waitFor(() => expect(warn).toHaveBeenCalledTimes(1))
    fireEvent.click(screen.getByRole('button', { name: 'Why' }))
    await waitFor(() => expect(explainRecommendation).toHaveBeenCalledTimes(2))
    warn.mockRestore()
  })

  it('keeps year-only dates at year precision when sorting recent papers', async () => {
    listLensRecommendations.mockResolvedValue([
      { id: 'year-only', paper_id: 'year-only', score: 60, created_at: '2026-09-01T12:00:00Z', paper: { id: 'year-only', title: 'Year only', year: 2024 } },
      { id: 'dated', paper_id: 'dated', score: 50, created_at: '2026-09-01T12:00:00Z', paper: { id: 'dated', title: 'Dated', year: 2024, publication_date: '2024-01-01' } },
    ])
    renderPage()
    await screen.findByText('Year only')

    fireEvent.click(screen.getByRole('button', { name: 'Sort by recent' }))
    const titles = screen.getAllByRole('button', { name: 'Why' }).map((button) => button.parentElement?.textContent)
    expect(titles).toEqual(['DatedWhy', 'Year onlyWhy'])
  })
})
