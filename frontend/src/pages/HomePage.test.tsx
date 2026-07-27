import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { HomePage } from './HomePage'
import type { HomeBrief } from '@/api/client'
import { HOME_SECTION_THEMES } from '@/lib/palette'

const getHomeBrief = vi.fn()
const applyPaperAction = vi.fn().mockResolvedValue({})
const getSignalLabQueue = vi.fn().mockResolvedValue({ available: false })
const answerSignalLabRound = vi.fn().mockResolvedValue({
  status: 'recorded',
  round_id: 1,
  skipped: false,
})
const getSignalLabSummary = vi.fn().mockResolvedValue({
  active: true,
  rounds: {
    today: 0,
    total: 0,
    answered: 0,
    skipped: 0,
    unique_queries: 0,
    duplicate_queries: 0,
  },
  fit: {
    ready: false,
    fresh: false,
    source_rounds: 0,
    fitted_queries: 0,
    fitted_observations: 0,
    pending_rounds: 0,
    utility_preferences: 0,
    metric_constraints: 0,
  },
  coverage: { regions_observed: 0, regions_total: 0, edges_observed: 0, edges_total: 0 },
  effects: { upward: [], downward: [], regions_moving: 0, boundary_overrides: 0 },
})

// PARTIAL mock: Home renders real primitives whose children reach for other
// client exports. A whole-module mock silently blanks every one of them, so
// keep the originals and override only what this test drives.
vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  getHomeBrief: (...args: unknown[]) => getHomeBrief(...args),
  applyPaperAction: (...args: unknown[]) => applyPaperAction(...args),
  getSignalLabQueue: (...args: unknown[]) => getSignalLabQueue(...args),
  getSignalLabSummary: (...args: unknown[]) => getSignalLabSummary(...args),
  answerSignalLabRound: (...args: unknown[]) => answerSignalLabRound(...args),
  listCollections: () => Promise.resolve([]),
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
    trend: [],
  },
  status: [],
  highlights: [],
  reading: { total: 0, items: [] },
  inbox: { total: 0, items: [] },
  attention: {
    imports_pending: 0,
    monitors_need_resolution: 0,
    author_decisions: 0,
    critical_health: 0,
    inbox_unresolved: 0,
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
    getSignalLabQueue.mockResolvedValue({ available: false })
    localStorage.removeItem('alma.signal-lab.dismissed-day')
    sessionStorage.clear()
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
        trend: [],
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

  it('renders the backend status contract without reading the removed connections field', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      status: [
        {
          key: 'feed',
          label: 'Feed',
          state: 'ok',
          severity: 'ok',
          metric: '12 monitors · 2h ago',
          detail: 'Feed last completed successfully.',
          tier: 'always',
          checked_at: '2026-07-26T10:00:00+00:00',
          href: '#/feed',
        },
        {
          key: 'alerts',
          label: 'Alerts',
          state: 'failed',
          severity: 'critical',
          metric: 'delivery failed',
          detail: 'The last delivery failed.',
          tier: 'always',
          checked_at: '2026-07-26T09:00:00+00:00',
          href: '#/alerts?tab=history',
        },
      ],
    })

    renderHome()

    const feed = await screen.findByRole('link', { name: /Feed: working/ })
    const alerts = screen.getByRole('link', { name: /Alerts: failing/ })
    expect(feed).toHaveAttribute('href', '#/feed')
    expect(alerts).toHaveAttribute('href', '#/alerts?tab=history')
    expect(feed).toHaveAttribute(
      'title',
      expect.stringContaining('12 monitors · 2h ago'),
    )
    expect(screen.queryByText('12 monitors · 2h ago')).not.toBeInTheDocument()
    expect(screen.queryByText('Slack')).not.toBeInTheDocument()
    expect(screen.queryByText(/is failing/)).not.toBeInTheDocument()
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
    expect(feedLink.closest('.group')).toHaveClass(...HOME_SECTION_THEMES.feed.noteSurface.split(' '))
    expect(discoveryLink.closest('.group')).toHaveClass(
      ...HOME_SECTION_THEMES.discovery.noteSurface.split(' '),
    )
    expect(screen.getByText('From followed author Ada Lovelace')).toBeInTheDocument()
    expect(screen.getByText('A compact explanation of the monitored result.')).toBeInTheDocument()
    expect(screen.getByText('Last 7 days')).toBeInTheDocument()
  })

  it('renders and advances a 12-round game deck with fitted effects and trivia', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    getSignalLabQueue.mockResolvedValue({
      available: true,
      game_id: 'triplet_best_worst',
      question: 'Which would you read first — and which would you skip?',
      options: ['best', 'worst'],
      rounds: Array.from({ length: 12 }, (_, index) => ({
        token: `signed-${index}`,
        papers: [
          { id: `p${index}a`, title: `Paper ${index}A`, summary: 'A' },
          { id: `p${index}b`, title: `Paper ${index}B`, summary: 'B' },
          { id: `p${index}c`, title: `Paper ${index}C`, summary: 'C' },
        ],
      })),
    })
    getSignalLabSummary.mockResolvedValue({
      active: true,
      rounds: {
        today: 3,
        total: 21,
        answered: 15,
        skipped: 6,
        unique_queries: 14,
        duplicate_queries: 1,
      },
      fit: {
        ready: true,
        fresh: false,
        source_rounds: 15,
        fitted_queries: 14,
        fitted_observations: 14,
        pending_rounds: 6,
        utility_preferences: 42,
        metric_constraints: 4,
      },
      coverage: { regions_observed: 6, regions_total: 32, edges_observed: 2, edges_total: 58 },
      effects: {
        upward: [{ region_id: 1, label: 'Methods', value: 0.25 }],
        downward: [{ region_id: 2, label: 'Theory', value: -0.15 }],
        regions_moving: 2,
        boundary_overrides: 1,
      },
    })

    renderHome()

    // The progress readout counts trials DONE, so a fresh deck reads 0 / 12.
    expect(await screen.findByText('0 / 12')).toBeInTheDocument()
    expect(screen.getByText(/Methods \+25%/)).toBeInTheDocument()
    expect(screen.getByText(/Theory −15%/)).toBeInTheDocument()
    expect(screen.getByText(/14 obs/)).toBeInTheDocument()
    expect(screen.getByText(/6\/32 regions/)).toBeInTheDocument()

    // Two named verdicts per paper, not two sequential taps on one control:
    // the round only records once both halves of the pair are given.
    fireEvent.click(
      screen.getByRole('button', { name: '“Paper 0A” is your most favourite of the three' }),
    )
    expect(screen.getByText('Now pick the other one')).toBeInTheDocument()
    fireEvent.click(
      screen.getByRole('button', { name: '“Paper 0B” is your least favourite of the three' }),
    )

    await waitFor(() =>
      expect(answerSignalLabRound).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          token: 'signed-0',
          answer: { best: 'p0a', worst: 'p0b' },
        }),
      ),
    )
    expect(await screen.findByText('1 / 12')).toBeInTheDocument()
  })

  it('scores a Discovery highlight and explains why every highlight is there', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      highlights: [
        {
          kind: 'discovery_paper',
          period: 'today',
          paper: { id: 'disc-1', title: 'A scored match' },
          reason: { kind: 'lens', label: 'Top match from Methods' },
          lens_id: 'lens-1',
          lens_name: 'Methods',
          score: 77,
        },
        {
          kind: 'feed_paper',
          period: 'today',
          paper: { id: 'feed-1', title: 'An unscored monitor hit' },
          reason: { kind: 'author', label: 'From followed author Ada Lovelace' },
          monitor_id: 'm1',
          monitor_type: 'author',
        },
      ],
    })
    renderHome()

    // The scored highlight shows its number; the unscored one shows none
    // rather than a fabricated placeholder.
    expect(await screen.findByText('77')).toBeInTheDocument()
    const explainers = screen.getAllByRole('button', { name: 'Why this paper is here' })
    expect(explainers).toHaveLength(2)

    fireEvent.click(explainers[0])
    expect(await screen.findByText(/scores 77 of 100 against that lens/)).toBeInTheDocument()
  })

  it('keeps the Inbox shelf stable and fills it only when papers are waiting', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    const { unmount } = renderHome()
    expect(await screen.findByText(/Your workspace is quiet/)).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Inbox' })).toBeInTheDocument()
    expect(screen.getByText(/waiting for a decision/)).toBeInTheDocument()
    unmount()

    getHomeBrief.mockResolvedValue({
      ...QUIET,
      inbox: {
        total: 2,
        items: [
          { id: 'i1', title: 'Sent from my phone', authors: 'Ada Lovelace' },
          { id: 'i2', title: 'Another capture' },
        ],
      },
    })
    renderHome()

    expect(await screen.findByRole('heading', { name: 'Inbox' })).toBeInTheDocument()
    expect(screen.getByText(/waiting for a decision/)).toBeInTheDocument()
    expect(screen.getByText('Sent from my phone')).toBeInTheDocument()
  })

  it('triages an Inbox paper in place, and the X defers without an opinion', async () => {
    // Home is the Inbox's owning surface, so triage happens here — the one
    // deliberate exception to Home being navigation-only.
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      inbox: {
        total: 1,
        items: [{ id: 'i1', title: 'Sent from my phone', authors: 'Ada Lovelace' }],
      },
    })
    renderHome()
    expect(await screen.findByRole('heading', { name: 'Inbox' })).toBeInTheDocument()

    // The X is `defer`, NOT `dismiss`: leaving the Inbox is the absence of a
    // verdict, so it must never travel as the global hide.
    // Accessible name comes from `dismissTitle` (ActionButton sets aria-label
    // from title), so assert on the wording the user actually hears.
    fireEvent.click(screen.getByRole('button', { name: /Clear from Inbox/i }))
    // `mutate` dispatches asynchronously — wait for the call rather than
    // racing react-query's scheduler.
    await waitFor(() =>
      expect(applyPaperAction).toHaveBeenCalledWith('i1', 'defer', { surface: 'inbox' }),
    )
  })

  it('shows reading continuity, and attention chips for nonzero kinds only', async () => {
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
        inbox_unresolved: 0,
      },
    })
    renderHome()

    expect(
      await screen.findByRole('heading', { name: 'Reading list' }),
    ).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /Continue this paper/ })).toHaveAttribute(
      'href',
      '#/library?tab=reading&paper=p1',
    )
    // Home shows dot + owner only. Count/remedy live in the hover title.
    const imports = screen.getByText('Imports').closest('a')
    const authors = screen.getByText('Author identities').closest('a')
    expect(imports).toHaveAttribute('title', expect.stringContaining('2 to review'))
    expect(authors).toHaveAttribute('title', expect.stringContaining('1 to review'))
    expect(screen.queryByText('2 to review')).not.toBeInTheDocument()
    expect(screen.queryByText('Feed monitors')).not.toBeInTheDocument()
    expect(screen.queryByText('Health')).not.toBeInTheDocument()
  })

  // Collapsed sections show whole rows of the MEASURED grid. Under jsdom no
  // width is ever reported, so the grid renders at its 3-column fallback →
  // 2 rows = 6 tiles.
  it('caps the reading list at whole rows and expands the rest in place', async () => {
    const items = Array.from({ length: 9 }, (_, index) => ({
      id: `r${index}`,
      title: `Reading paper ${index}`,
    }))
    getHomeBrief.mockResolvedValue({ ...QUIET, reading: { total: 9, items } })
    renderHome()

    expect(await screen.findByText('Reading paper 5')).toBeInTheDocument()
    expect(screen.queryByText('Reading paper 6')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Show 3 more' }))
    expect(screen.getByText('Reading paper 8')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Show \d+ more/ })).not.toBeInTheDocument()
  })

  it('keeps a truthful quiet state and opens Find & add on the desk, not elsewhere', async () => {
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    expect(await screen.findByText('Your daily brief')).toBeInTheDocument()
    expect(screen.getByText(/Your workspace is quiet/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Import' })).not.toBeInTheDocument()

    // Adding a paper you already know about is a desk action: the masthead
    // carries the search itself, folded, instead of a button that bounced the
    // user to Discovery to do the same thing (2026-07-27).
    const fold = screen.getByText('Find & add a paper').closest('details')
    expect(fold).not.toBeNull()
    expect(fold).not.toHaveAttribute('open')
    expect(screen.queryByRole('button', { name: /Find papers/ })).not.toBeInTheDocument()
    expect(window.location.hash).not.toContain('action=find')
  })

  it('opens Find & add from an ?action=find deep link', async () => {
    window.location.hash = '#/home?action=find'
    getHomeBrief.mockResolvedValue(QUIET)
    renderHome()
    expect(await screen.findByText('Your daily brief')).toBeInTheDocument()

    // The deep link opens the fold in place and then scrubs `action` out of the
    // URL, so a reload doesn't re-open it forever.
    expect(screen.getByText('Find & add a paper').closest('details')).toHaveAttribute('open')
    expect(window.location.hash).not.toContain('action=find')
  })

  it('can recover when the backend becomes ready after the first brief request', async () => {
    getHomeBrief
      .mockRejectedValueOnce(new Error('backend starting'))
      .mockResolvedValueOnce(QUIET)
    renderHome()

    expect(
      await screen.findByText("Couldn't load your daily brief."),
    ).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))

    expect(await screen.findByText('Your daily brief')).toBeInTheDocument()
  })
})
