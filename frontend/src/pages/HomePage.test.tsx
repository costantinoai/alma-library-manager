import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { HomePage } from './HomePage'
import type { HomeBrief } from '@/api/client'

const getHomeBrief = vi.fn()
const markHomeSeen = vi.fn().mockResolvedValue({ last_seen_at: '2026-07-25T00:00:00Z' })

vi.mock('@/api/client', () => ({
  getHomeBrief: (...args: unknown[]) => getHomeBrief(...args),
  markHomeSeen: (...args: unknown[]) => markHomeSeen(...args),
  addToLibrary: vi.fn(),
  dismissRecommendation: vi.fn(),
}))

vi.mock('@/hooks/useToast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  errorToast: vi.fn(),
}))

/** A healthy, quiet system: nothing arrived, nothing needs you, no suggestion. */
const QUIET: HomeBrief = {
  since: new Date(Date.now() - 3 * 3600_000).toISOString(),
  first_visit: false,
  last_seen_at: new Date(Date.now() - 3 * 3600_000).toISOString(),
  insight: null,
  recent_arrivals: [],
  reading_now: [],
  arrived: { feed_items: 0, alerts_fired: 0, recommendations: 0 },
  waiting: { reading: 0, imports_pending: 0, monitors_need_attention: 0 },
}

function renderHome() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <HomePage />
    </QueryClientProvider>,
  )
}

describe('HomePage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    markHomeSeen.mockResolvedValue({ last_seen_at: '2026-07-25T00:00:00Z' })
  })

  it('renders the brief and stamps the visit after it lands', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      arrived: { feed_items: 12, alerts_fired: 2, recommendations: 7 },
      waiting: { reading: 3, imports_pending: 0, monitors_need_attention: 0 },
    })
    renderHome()

    expect(await screen.findByText('Since you were last here')).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
    // The GET is pure; the visit is stamped separately, after render.
    await waitFor(() => expect(markHomeSeen).toHaveBeenCalledTimes(1))
  })

  it('says "here is where things stand" on a first visit', async () => {
    getHomeBrief.mockResolvedValue({ ...QUIET, first_visit: true, last_seen_at: null })
    renderHome()
    expect(await screen.findByText("Here's where things stand.")).toBeInTheDocument()
    expect(screen.getByText(/first visit/i)).toBeInTheDocument()
  })

  it('keeps zero counts in the row so it never reflows', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    await screen.findByText('Since you were last here')
    // All four figures are present even at zero — the row is stable and honest.
    expect(screen.getAllByText('0')).toHaveLength(4)
  })

  it('renders NOTHING for attention when the system is quiet', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    await screen.findByText('Since you were last here')
    // No "all good" card — a healthy system is silent.
    expect(screen.queryByText('Needs you')).not.toBeInTheDocument()
  })

  it('surfaces staged imports and stopped monitors as actions', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      waiting: { reading: 0, imports_pending: 2, monitors_need_attention: 1 },
    })
    renderHome()
    expect(await screen.findByText('Needs you')).toBeInTheDocument()
    expect(screen.getByText(/2 imported papers are waiting/i)).toBeInTheDocument()
    expect(screen.getByText(/1 monitor stopped/i)).toBeInTheDocument()
  })

  it('omits the insight module when there is no suggestion', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    await screen.findByText('Since you were last here')
    expect(screen.queryByText('One to look at')).not.toBeInTheDocument()
  })

  it('shows arrivals and the reading list when they have content', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      recent_arrivals: [
        { paper_id: 'p1', title: 'A newly arrived paper', authors: 'Ada L.', year: 2026 },
      ],
      reading_now: [{ paper_id: 'p2', title: 'Something I started', authors: 'Alan T.', year: 2025 }],
    })
    renderHome()
    expect(await screen.findByText('Newest in your Feed')).toBeInTheDocument()
    expect(screen.getByText('A newly arrived paper')).toBeInTheDocument()
    expect(screen.getByText('Still reading')).toBeInTheDocument()
    expect(screen.getByText('Something I started')).toBeInTheDocument()
  })
})
