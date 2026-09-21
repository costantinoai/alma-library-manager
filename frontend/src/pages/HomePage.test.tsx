import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { HomePage } from './HomePage'
import type { HomeBrief } from '@/api/client'
import { HOME_SECTION_THEMES } from '@/lib/palette'

const getHomeBrief = vi.fn()
const applyPaperAction = vi.fn().mockResolvedValue({})
const getCaptureAttentionMessages = vi.fn().mockResolvedValue([])
const applyCaptureMessageAction = vi.fn().mockResolvedValue({ action: 'archive', status: 'archived' })
vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  getHomeBrief: (...args: unknown[]) => getHomeBrief(...args),
  applyPaperAction: (...args: unknown[]) => applyPaperAction(...args),
  getCaptureAttentionMessages: (...args: unknown[]) => getCaptureAttentionMessages(...args),
  applyCaptureMessageAction: (...args: unknown[]) => applyCaptureMessageAction(...args),
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

  it('opens failed capture records on Home and exposes archive and retry actions', async () => {
    getHomeBrief.mockResolvedValue({
      ...QUIET,
      attention: { ...QUIET.attention, inbox_unresolved: 1 },
    })
    getCaptureAttentionMessages.mockResolvedValue([
      {
        id: 'message-1',
        channel: 'slack',
        external_id: '1.2',
        received_at: '2026-09-20T10:00:00+00:00',
        raw_text: 'Can you save the paper Andrea mentioned?',
        extracted: {},
        outcome: 'unresolved',
        error: 'No DOI, arXiv id, OpenAlex id or link found in the message.',
        created_at: '2026-09-20T10:01:00+00:00',
        archived_at: null,
        retry_input: null,
        updated_at: null,
      },
    ])
    renderHome()

    fireEvent.click(await screen.findByRole('button', { name: /Captures: 1 not identified/i }))
    expect(await screen.findByRole('heading', { name: 'Capture review' })).toBeInTheDocument()
    expect(
      await screen.findByText('Can you save the paper Andrea mentioned?'),
    ).toBeInTheDocument()
    expect(window.location.hash).toBe('#/home?captures=attention')

    fireEvent.change(screen.getByLabelText('Try another paper link'), {
      target: { value: 'https://doi.org/10.1/example' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Try capture' }))
    await waitFor(() =>
      expect(applyCaptureMessageAction).toHaveBeenCalledWith('message-1', {
        action: 'retry',
        replacement_link: 'https://doi.org/10.1/example',
      }),
    )

    fireEvent.click(screen.getByRole('button', { name: 'Archive' }))
    await waitFor(() =>
      expect(applyCaptureMessageAction).toHaveBeenCalledWith('message-1', {
        action: 'archive',
      }),
    )
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
    expect(await screen.findByText(/Your (morning|afternoon|evening|late) brief/)).toBeInTheDocument()
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
    expect(await screen.findByText(/Your (morning|afternoon|evening|late) brief/)).toBeInTheDocument()

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

    expect(await screen.findByText(/Your (morning|afternoon|evening|late) brief/)).toBeInTheDocument()
  })
})
