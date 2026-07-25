import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { HomePage } from './HomePage'
import type { HomeBrief } from '@/api/client'

const getHomeBrief = vi.fn()

vi.mock('@/api/client', () => ({
  getHomeBrief: (...args: unknown[]) => getHomeBrief(...args),
}))

const QUIET: HomeBrief = {
  generated_at: new Date().toISOString(),
  day_start: new Date().toISOString(),
  timezone: 'Europe/Brussels',
  user_name: null,
  activity: {
    feed: {
      today: 0,
      carryover: 0,
      by_monitor_type: { authors: 0, journals: 0, other: 0 },
    },
    discovery: { today: 0, carryover: 0, lenses_today: 0 },
    alerts: { today: 0 },
  },
  highlights: [],
  reading: { total: 0, items: [] },
  attention: {
    imports_pending: 0,
    monitors_need_resolution: 0,
    author_decisions: 0,
    critical_health: 0,
  },
}

function renderHome() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
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
    window.location.hash = ''
  })

  it('renders a personal daily activity summary without a review mutation', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      user_name: 'Andrea Costantino',
      activity: {
        feed: {
          today: 12,
          carryover: 4,
          by_monitor_type: { authors: 7, journals: 3, other: 2 },
        },
        discovery: { today: 6, carryover: 2, lenses_today: 2 },
        alerts: { today: 1 },
      },
    })
    renderHome()

    expect(await screen.findByText(/Andrea/)).toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
    expect(screen.getByText('6')).toBeInTheDocument()
    expect(screen.getByText('1')).toBeInTheDocument()
    expect(screen.getByText(/4 in Feed/)).toBeInTheDocument()
    expect(screen.getByText(/2 in Discovery/)).toBeInTheDocument()
    expect(getHomeBrief).toHaveBeenCalledTimes(1)
  })

  it('renders source-balanced highlights with reasons, excerpts, and owner links', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      highlights: [
        {
          kind: 'feed_paper',
          period: 'today',
          paper: {
            id: 'feed-1',
            title: 'A monitored result',
            authors: 'Ada Lovelace',
            tldr: 'A compact explanation of the monitored result.',
          },
          reason: { kind: 'author', label: 'From followed author Ada Lovelace' },
          monitor_id: 'm1',
          monitor_type: 'author',
        },
        {
          kind: 'discovery_paper',
          period: 'last_7_days',
          paper: {
            id: 'disc-1',
            title: 'A discovery result',
            authors: 'Alan Turing',
            abstract: 'An abstract explaining why this result may matter.',
          },
          reason: { kind: 'lens', label: 'Top match from Methods' },
          lens_id: 'lens-1',
        },
      ],
    })
    renderHome()

    const feedLink = await screen.findByRole('link', { name: /A monitored result/ })
    const discoveryLink = screen.getByRole('link', { name: /A discovery result/ })
    expect(feedLink).toHaveAttribute('href', '#/feed?scope=inbox&monitor=m1&paper=feed-1')
    expect(discoveryLink).toHaveAttribute('href', '#/discovery?lens=lens-1&paper=disc-1')
    expect(screen.getByText('From followed author Ada Lovelace')).toBeInTheDocument()
    expect(screen.getByText('A compact explanation of the monitored result.')).toBeInTheDocument()
    expect(screen.getByText('Last 7 days')).toBeInTheDocument()
  })

  it('shows reading continuity and only nonzero attention rows', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      reading: {
        total: 1,
        items: [{ id: 'p1', title: 'Continue this paper', authors: 'Grace Hopper' }],
      },
      attention: {
        imports_pending: 2,
        monitors_need_resolution: 0,
        author_decisions: 1,
        critical_health: 0,
      },
    })
    renderHome()

    expect(await screen.findByText('Continue reading')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Continue this paper/ })).toHaveAttribute(
      'href',
      '#/library?tab=reading&paper=p1',
    )
    expect(screen.getByText(/2 imported papers need review/)).toBeInTheDocument()
    expect(screen.getByText(/1 author identity needs review/)).toBeInTheDocument()
    expect(screen.queryByText(/monitor needs relinking/)).not.toBeInTheDocument()
  })

  it('keeps a truthful quiet state and provides navigation-only workflow shortcuts', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    expect(await screen.findByText('Your daily brief')).toBeInTheDocument()
    expect(screen.getByText(/No noteworthy research arrived/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Import' })).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /Find papers/ }))
    expect(window.location.hash).toBe('#/discovery?action=find')
  })
})
