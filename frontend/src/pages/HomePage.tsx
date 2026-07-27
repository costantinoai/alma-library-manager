import { useEffect, useRef, useState } from 'react'
/**
 * Home — a read-only daily research desk.
 *
 * The page reports today's stable activity, preserves older carryover until
 * Feed/Discovery themselves are reviewed, and hands every item to the surface
 * that owns it. It never stamps review state.
 *
 * ONE deliberate exception: the Inbox (D13). Home is the Inbox's owning
 * surface — there is no other page for it — so triage happens here. Every
 * other section stays navigation-only.
 */
import {
  AlertTriangle,
  ArrowRight,
  BookMarked,
  BookOpen,
  FileSearch,
  Inbox,
  Moon,
  Send,
  Sparkles,
  Sun,
  Sunrise,
  Sunset,
} from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  applyPaperAction,
  getApiErrorMessage,
  getHomeBrief,
  type HomeBrief,
  type HomeHighlight,
  type HomePaper,
  type OnboardingPaperAction,
} from '@/api/client'
import { AttentionChips } from '@/components/home/AttentionChips'
import { HomeStatusRail } from '@/components/home/HomeStatusRail'
import { InflowStrip } from '@/components/home/InflowStrip'
import { PaperActionBar } from '@/components/discovery/PaperActionBar'
import { PageTour, HOME_TOUR } from '@/components/onboarding'
import { IdentityChip, MetricTile, PaperTile, PaperTileGrid } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { OnlineSearchTab } from '@/components/OnlineSearchTab'
import { MetaLine, PageIntro } from '@/components/ui/page-intro'
import { Meter } from '@/components/ui/meter'
import { PageSection } from '@/components/ui/page-section'
import { SignalLabSheet } from '@/components/home/SignalLabSheet'
import { ErrorState } from '@/components/ui/ErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { Surface } from '@/components/ui/surface'
import { StatusBadge } from '@/components/ui/status-badge'
import { errorToast, toast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo, useHashRoute } from '@/lib/hashRoute'
import {
  CAPTURE_CHANNEL_LABEL,
  HOME_SECTION_THEMES,
  MONITOR_MIX_FILL,
  type HomeSectionThemeKey,
} from '@/lib/palette'
import { cn, formatRelativeShort } from '@/lib/utils'

/**
 * The four parts of a working day, by local hour.
 *
 * Four, not three: 01:00 is not "evening", and a tool you open at midnight
 * saying "Good evening" is the tell that nobody looked. The bands are the ones
 * spoken English actually uses — night runs from 22:00 to 05:00 and owns both
 * ends of the clock, which is why this is a function and not a `<` ladder.
 */
type DayPart = 'morning' | 'afternoon' | 'evening' | 'night'

function dayPart(hour: number): DayPart {
  if (hour >= 22 || hour < 5) return 'night'
  if (hour < 12) return 'morning'
  if (hour < 18) return 'afternoon'
  return 'evening'
}

/**
 * Greetings per part of the day — several, so the page does not say the same
 * eight words every time you open it, and one of them is always the plain
 * "Good morning, X" so the rotation never feels like a gimmick.
 *
 * `{name}` is the reader's first name.
 */
const GREETINGS: Record<DayPart, string[]> = {
  morning: [
    'Good morning, {name}',
    'Morning, {name}',
    'A fresh page, {name}',
    'Coffee and papers, {name}',
  ],
  afternoon: [
    'Good afternoon, {name}',
    'Afternoon, {name}',
    'Back to it, {name}',
    'Mid-day reading, {name}',
  ],
  evening: [
    'Good evening, {name}',
    'Evening, {name}',
    'Winding down, {name}',
    'One more paper, {name}',
  ],
  night: [
    'Still up, {name}',
    'Late shift, {name}',
    'Burning the midnight oil, {name}',
    'Good night, {name}',
  ],
}

/**
 * A reader who never gave a name gets a TITLE, not a greeting — and no
 * rotation: "Still up" without a name is the app talking to nobody, and a
 * masthead that changes its words for an anonymous reader is just noise. It
 * still follows the clock, because "Your morning brief" opened at 23:00 would
 * be the same lie the old three-band split told.
 */
const ANONYMOUS_GREETINGS: Record<DayPart, string> = {
  morning: 'Your morning brief',
  afternoon: 'Your afternoon brief',
  evening: 'Your evening brief',
  night: 'Your late brief',
}

/** Days since the epoch — the rotation cursor. Stable for the whole day, so a
 *  reload or a background refetch cannot reshuffle the greeting under you, and
 *  a new one still arrives tomorrow. */
function dayIndex(now: Date): number {
  return Math.floor(
    new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime() / 86_400_000,
  )
}

function greeting(name: string | null, now: Date = new Date()): string {
  const part = dayPart(now.getHours())
  const firstName = name?.trim().split(/\s+/)[0] || null
  if (!firstName) return ANONYMOUS_GREETINGS[part]
  const pool = GREETINGS[part]
  // Offset by the part as well as the day, so the four bands of one day do not
  // all land on the same index of their pools.
  const line = pool[(dayIndex(now) + part.length) % pool.length]
  return line.replace('{name}', firstName)
}

/** The masthead glyph, following the same clock as the words beside it. It is
 *  the only page whose medallion changes through the day — which is the point:
 *  this block is a greeting, not a section heading. */
function greetingIcon(now: Date = new Date()): typeof Sun {
  const part = dayPart(now.getHours())
  if (part === 'morning') return Sunrise
  if (part === 'afternoon') return Sun
  if (part === 'evening') return Sunset
  return Moon
}

function localDate(): string {
  return new Intl.DateTimeFormat(undefined, {
    weekday: 'long',
    month: 'long',
    day: 'numeric',
  }).format(new Date())
}

function paperByline(paper: HomePaper): string {
  return [paper.authors, paper.journal, paper.year].filter(Boolean).join(' · ')
}

function excerpt(paper: HomePaper): string | null {
  return paper.tldr?.trim() || paper.abstract?.trim() || null
}

function highlightHref(highlight: HomeHighlight): string {
  if (highlight.kind === 'discovery_paper') {
    return buildHashRoute('discovery', {
      lens: highlight.lens_id,
      paper: highlight.paper.id,
    })
  }
  if (highlight.kind === 'source_update' && highlight.source?.type === 'author') {
    return buildHashRoute('feed', {
      author: highlight.source.author_id,
      paper: highlight.paper.id,
    })
  }
  const monitorType =
    highlight.kind === 'source_update' ? highlight.source?.type : highlight.monitor_type
  const monitorId =
    highlight.kind === 'source_update' ? highlight.source?.id : highlight.monitor_id
  return buildHashRoute('feed', {
    scope: monitorType === 'venue' ? 'journals' : 'inbox',
    monitor: monitorId,
    paper: highlight.paper.id,
  })
}

/**
 * The long-form "why am I seeing this" for a highlight.
 *
 * Home selects deterministically (one Feed paper, one Discovery match, one
 * active source, then the next-best matches), so the explanation can state the
 * actual rule rather than gesture at an algorithm.
 */
function highlightExplanation(highlight: HomeHighlight): string {
  const window =
    highlight.period === 'today' ? 'arrived today' : 'arrived in the last seven days'
  if (highlight.kind === 'discovery_paper') {
    const lens = highlight.lens_name || 'a Discovery lens'
    const score =
      highlight.score != null
        ? ` It scores ${Math.round(highlight.score)} of 100 against that lens.`
        : ''
    return `Discovery matched this paper to ${lens} — it ${window} and ranked near the top of that lens.${score}`
  }
  if (highlight.kind === 'source_update') {
    const label = highlight.source?.label || 'a source you follow'
    const count = highlight.source?.paper_count ?? 0
    return `${label} published ${count} ${count === 1 ? 'paper' : 'papers'} recently; this one stands for that batch. It ${window}.`
  }
  return `One of your Feed monitors matched this paper — it ${window}. Feed picks are chosen by what you monitor and when they appeared, so they carry no relevance score.`
}

/**
 * Rows a collapsed paper section shows. The item count itself is measured
 * (see `PaperTileGrid`), so this is two full rows at whatever width the
 * window happens to be.
 */
const COLLAPSED_ROWS = 2

/** When a paper entered the reading workflow — the tile's footer line. */
function addedReason(paper: HomePaper): string | undefined {
  return paper.added_at ? `Added ${formatRelativeShort(paper.added_at)}` : undefined
}

/** A paper section: measured tile grid, whole rows, in-place expansion. */
function PaperSection({
  papers,
  hrefFor,
  reasonFor = addedReason,
  noteTheme,
}: {
  papers: HomePaper[]
  hrefFor: (paper: HomePaper) => string
  reasonFor?: (paper: HomePaper) => string | undefined
  noteTheme: HomeSectionThemeKey
}) {
  return (
    <PaperTileGrid
      items={papers}
      getKey={(paper) => paper.id}
      collapsedRows={COLLAPSED_ROWS}
      renderTile={(paper) => (
        <PaperTile
          href={hrefFor(paper)}
          title={paper.title}
          byline={paperByline(paper)}
          excerpt={excerpt(paper)}
          reason={reasonFor?.(paper)}
          noteTheme={noteTheme}
        />
      )}
    />
  )
}

/**
 * How today's Feed intake splits across monitor kinds.
 *
 * A count of 12 says nothing about whether your reading day is author-driven
 * or journal-driven; this does, in the space of one rail. Rendered only when
 * something actually arrived — a breakdown of zero is furniture.
 */
function MonitorMix({ mix }: { mix: HomeBrief['activity']['feed']['by_monitor_type'] }) {
  const entries = [
    { key: 'authors' as const, label: 'authors', value: mix.authors },
    { key: 'journals' as const, label: 'journals', value: mix.journals },
    { key: 'other' as const, label: 'other', value: mix.other },
  ].filter((entry) => entry.value > 0)
  if (entries.length === 0) return null

  return (
    <div className="space-y-1.5">
      <Meter
        size="xs"
        segments={entries.map((entry) => ({
          value: entry.value,
          fillClassName: MONITOR_MIX_FILL[entry.key],
        }))}
        label={`Today's Feed papers by monitor: ${entries
          .map((entry) => `${entry.value} ${entry.label}`)
          .join(', ')}`}
      />
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500">
        {entries.map((entry) => (
          <span key={entry.key} className="flex items-center gap-1.5">
            <span
              className={cn('h-1.5 w-1.5 rounded-full', MONITOR_MIX_FILL[entry.key])}
              aria-hidden
            />
            {entry.value} {entry.label}
          </span>
        ))}
      </div>
    </div>
  )
}

/**
 * What each triage verb DID, in the past tense of the button that fired it.
 *
 * Deliberately states the outcome rather than "Saved!" — the whole point of
 * `defer` is that it records no opinion, and a user who cannot tell it apart
 * from `dislike` will avoid using it.
 */
const TRIAGE_CONFIRMATION: Partial<Record<OnboardingPaperAction, string>> = {
  add: 'Saved to your library',
  like: 'Liked and saved to your library',
  love: 'Loved and saved to your library',
  dislike: 'Noted — you will see less like this',
  defer: 'Cleared from your Inbox, kept in your corpus',
}

/**
 * The Inbox tile's eyebrow: which transport this arrived on, and when.
 *
 * The chip wears the INBOX note's hue, not the channel's own
 * (`CAPTURE_CHANNEL_CHIP`): on a coloured sticky note a categorical hue reads
 * as a clash rather than as information, and the channel is already named in
 * words. The channel palette still owns the chip everywhere the host surface is
 * plain paper.
 */
function CaptureEyebrow({ paper }: { paper: HomePaper }) {
  const channel = (paper.capture_channel || '').toLowerCase()
  const when = paper.captured_at ?? paper.added_at
  return (
    <>
      {channel && (
        <IdentityChip
          icon={Send}
          chipClassName={HOME_SECTION_THEMES.inbox.noteChip}
          title="The channel you sent this from"
        >
          {CAPTURE_CHANNEL_LABEL[channel] ?? channel}
        </IdentityChip>
      )}
      {when && (
        <span className="text-[11px] text-slate-500">
          Captured {formatRelativeShort(when)}
        </span>
      )}
    </>
  )
}

export function HomePage() {
  const queryClient = useQueryClient()
  const route = useHashRoute()
  const routeQuery = route.params.get('query')?.trim() ?? ''
  const routeAction = route.params.get('action')?.trim() ?? ''
  const briefQuery = useQuery({
    queryKey: ['home-brief'],
    queryFn: () => getHomeBrief(),
    staleTime: 30_000,
  })

  // Find & add lives on the desk (user call 2026-07-27), not on Discovery:
  // adding a paper you already know about is a starting move, and Discovery
  // should be about the lens and its suggestions and nothing else. Folded by
  // default; the masthead's "Find papers" button and `?action=find` open it.
  const [findAddOpen, setFindAddOpen] = useState(false)
  const findAddRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (routeAction !== 'find' && !routeQuery) return
    setFindAddOpen(true)
    window.setTimeout(() => {
      findAddRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }, 50)
    const next = new URLSearchParams(route.params)
    next.delete('action')
    window.history.replaceState(null, '', buildHashRoute('home', Object.fromEntries(next)))
  }, [route.params, routeAction, routeQuery])

  // Inbox triage (D13). Every verb goes through the ONE canonical route,
  // `POST /papers/{id}/action`, with `surface: 'inbox'` so feedback provenance
  // says where the user actually acted. Refetching the brief is what makes the
  // tile leave the section: every action moves the paper off `status='inbox'`.
  const triage = useMutation({
    mutationFn: ({ paperId, action }: { paperId: string; action: OnboardingPaperAction }) =>
      applyPaperAction(paperId, action, { surface: 'inbox' }),
    onSuccess: (_result, { action }) => {
      toast({ title: TRIAGE_CONFIRMATION[action] ?? 'Done' })
    },
    // Without this, a rejected write (a 503 while a background job holds the
    // writer gate is the realistic one) leaves the tile sitting exactly where
    // it was, which is indistinguishable from a dead button. Failures are
    // loud, per "no silent failures".
    onError: (error) => {
      errorToast(
        "Couldn't apply that",
        `${getApiErrorMessage(error)} The paper is still in your Inbox — try again.`,
      )
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['home-brief'] }),
  })
  const act = (paperId: string, action: OnboardingPaperAction) => () =>
    triage.mutate({ paperId, action })

  if (briefQuery.isLoading) {
    return (
      <div className="mx-auto max-w-5xl space-y-8 py-2">
        <Skeleton className="h-14 w-80" />
        <div className="grid gap-3 sm:grid-cols-3">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
        <Skeleton className="h-72" />
      </div>
    )
  }
  if (briefQuery.isError || !briefQuery.data) {
    return (
      <ErrorState
        message="Couldn't load your daily brief."
        actionLabel="Try again"
        actionPending={briefQuery.isFetching}
        onAction={() => void briefQuery.refetch()}
      />
    )
  }

  const brief = briefQuery.data
  const { feed, discovery, alerts } = brief.activity
  const attentionTotal = Object.values(brief.attention).reduce((sum, value) => sum + value, 0)
  const carryoverTotal = feed.carryover + discovery.carryover

  return (
    <div className="mx-auto max-w-5xl space-y-8 py-2 2xl:max-w-6xl">
      {/* THE BLOTTER — one raised panel carrying your whole SITUATION: who you
          are today, whether the machinery is running, what arrived, and what
          needs a decision. The research sections below sit directly on the
          desk as loose sheets, so the page reads as three real depths (desk →
          blotter → its recessed wells) instead of one flat field of cards.
          Grouping is semantic, not decorative: status lives on the blotter,
          papers live on the desk. */}
      <Card className="space-y-5 p-5 sm:p-6">
        {/* The one page whose intro IS its content: the greeting is the lede,
            and the gold rule closes the masthead (masthead trim only — never a
            section divider). Rendered `bare` because this band supplies the
            paper.

            The band is one rung LIGHTER than the blotter it sits on (`Surface`,
            relational — never a hand-picked cream) and bleeds to the card's
            edges, so the masthead is visibly a nameplate rather than the first
            of the blotter's sections. The gold rule closes it from inside, so
            the lighter paper stops exactly where the trim does. */}
        <Surface
          bordered={false}
          className="-mx-5 -mt-5 rounded-t-sm px-5 pt-5 sm:-mx-6 sm:-mt-6 sm:px-6 sm:pt-6"
        >
          <PageIntro
            bare
            masthead
            rule
            // The gold medallion + gold lede are Home's identity hue doing what
            // it does on every other page's masthead — here it also separates a
            // greeting from the section headings under it.
            icon={greetingIcon()}
            lede={greeting(brief.user_name)}
            meta={
              <MetaLine
                items={[
                  localDate(),
                  <span>updated {formatRelativeShort(brief.generated_at)}</span>,
                ]}
              />
            }
            tour={<PageTour pageKey="home" steps={HOME_TOUR} />}
          />
        </Surface>

        <PageSection
          id="home-activity"
          title="Today in ALMa"
          icon={Sunrise}
          // The strip mixes two scopes on purpose, and each label says which:
          // three "today" figures, then two standing queues. A desk needs both —
          // what arrived, and what is still on it.
          description="What arrived since your local midnight, and what is waiting."
        >
          {/* An editorial scoreboard, not a grid of cards: bare figures split by
              hairlines, with the week's shape as the final cell. Six bordered
              tiles read as six competing objects and made the eye cross a
              border per number; the sparkline floating in its own plate beside
              the heading read as a separate widget rather than part of the
              same reading. */}
          <div
            data-tour="home-brief"
            className="flex flex-wrap items-start gap-y-5 divide-edge-1 sm:divide-x"
          >
            <MetricTile
              variant="bare"
              className="basis-1/2 px-0 sm:basis-auto sm:flex-1 sm:pr-5"
              label="new Feed papers"
              value={feed.today}
              // The monitor split is spelled once, by the ribbon under the row —
              // repeating it here as text was the same fact twice.
              hint="from your sources"
              tone={feed.today > 0 ? 'accent' : 'neutral'}
              onClick={() => navigateTo('feed')}
            />
            <MetricTile
              variant="bare"
              className="basis-1/2 sm:basis-auto sm:flex-1 sm:px-5"
              label="new suggestions"
              value={discovery.today}
              hint={`today, across ${discovery.lenses_today} ${discovery.lenses_today === 1 ? 'lens' : 'lenses'}`}
              tone={discovery.today > 0 ? 'accent' : 'neutral'}
              onClick={() => navigateTo('discovery')}
            />
            <MetricTile
              variant="bare"
              className="basis-1/2 sm:basis-auto sm:flex-1 sm:px-5"
              label="alerts delivered"
              value={alerts.today}
              hint="successful digests"
              tone={alerts.today > 0 ? 'info' : 'neutral'}
              onClick={() => navigateTo('alerts', { tab: 'history' })}
            />
            {/* Two standing queues rather than today-figures. Both are already
                on the payload — Home was reporting them only as section pills,
                which you cannot see until you scroll to the section. */}
            <MetricTile
              variant="bare"
              className="basis-1/2 sm:basis-auto sm:flex-1 sm:px-5"
              label="waiting in Inbox"
              value={brief.inbox.total}
              hint="sent by you"
              tone={brief.inbox.total > 0 ? 'accent' : 'neutral'}
              // Home OWNS the Inbox, so there is nowhere to navigate to: the
              // tile scrolls to the section it counts. Only when there IS one —
              // a click that silently does nothing is worse than no affordance.
              onClick={
                brief.inbox.total > 0
                  ? () =>
                      document
                        .getElementById('home-inbox')
                        ?.scrollIntoView({ behavior: 'smooth', block: 'start' })
                  : undefined
              }
            />
            <MetricTile
              variant="bare"
              className="basis-1/2 sm:basis-auto sm:flex-1 sm:px-5"
              label="on your reading list"
              value={brief.reading.total}
              hint="currently reading"
              tone="neutral"
              onClick={() => navigateTo('library', { tab: 'reading' })}
            />
            <InflowStrip
              trend={brief.activity.trend}
              className="basis-full sm:basis-auto sm:flex-1 sm:pl-5"
            />
          </div>
          <MonitorMix mix={feed.by_monitor_type} />
          {/* The status line sits UNDER the figures because that is what it is
              FOR: it says why they read the way they do — which machinery
              produced them and how recently — and then what wants a decision.
              Above the numbers it was preamble; here it is the footnote the
              numbers need. */}
          <div
            data-tour="home-status"
            className="flex flex-wrap items-center gap-x-4 gap-y-1.5 border-t border-edge-1 pt-3"
          >
            <HomeStatusRail status={brief.status} />
            {/* One hairline between the two halves: everything left of it is
                machinery ALMa runs, everything right of it wants a decision
                from you. Only drawn when both sides exist. */}
            {attentionTotal > 0 && brief.status.length > 0 && (
              <span className="h-3 w-px bg-control-edge" aria-hidden />
            )}
            <AttentionChips attention={brief.attention} />
          </div>
          {carryoverTotal > 0 && (
            <p className="flex flex-wrap items-center gap-x-2 text-xs text-slate-500">
              <BookOpen className="h-3.5 w-3.5 text-alma-folio" />
              <span>
                Still waiting from earlier:
                {' '}
                <a
                  href={buildHashRoute('feed')}
                  className={cn(
                    'font-medium hover:underline',
                    HOME_SECTION_THEMES.feed.title,
                  )}
                >
                  {feed.carryover} in Feed
                </a>
                {' · '}
                <a
                  href={buildHashRoute('discovery')}
                  className={cn(
                    'font-medium hover:underline',
                    HOME_SECTION_THEMES.discovery.title,
                  )}
                >
                  {discovery.carryover} in Discovery
                </a>
              </span>
            </p>
          )}
        </PageSection>

      </Card>

      {/* Find & add — the manual way into the corpus: you already know the
          paper, so no lens is involved. Directly under the blotter, first of
          the loose sheets, because it is an ACTION and everything below it
          only reports. It replaced a masthead button that did nothing but
          navigate to Discovery to run the same search (2026-07-27). */}
      <DisclosurePanel
        ref={findAddRef}
        icon={FileSearch}
        title="Find & add a paper"
        description="Search any source and add a paper by hand — when you already know what you're looking for."
        open={findAddOpen}
        onOpenChange={setFindAddOpen}
      >
        <OnlineSearchTab initialQuery={routeQuery} autoRun={!!routeQuery} resultPreviewLimit={5} />
      </DisclosurePanel>

      {/* Signal Lab (task 58, D20) — the day's response sheet. Sits above
          Inbox as a distinct printed artifact, not another card; renders
          nothing until the substrate and at least 10 rounds exist. */}
      <SignalLabSheet />

      {/* D13 Inbox — papers you sent yourself from another device, awaiting
          triage. Home IS the Inbox's surface, so there is no "open elsewhere"
          action. It leads the research sections, ahead of even "Picked for
          you": what you chose to send yourself outranks anything ALMa chose
          for you. An empty Inbox keeps its header bar and a `0` pill, folded
          shut (2026-07-27) — the desk's shelves are always where you left
          them; only their contents change. */}
      <PageSection
        id="home-inbox"
        data-tour="home-inbox"
        collapsible
        title="Inbox"
        icon={Inbox}
        categoryTheme={HOME_SECTION_THEMES.inbox}
        count={brief.inbox.total}
        description="Papers you sent yourself, waiting for a decision."
        emptyState={
          <p className="text-sm text-slate-500">
            Nothing waiting. Papers you send yourself from another device land here for triage.
          </p>
        }
      >
        {/* The same measured tile grid every other section on this page
            uses, so the Inbox scans as one more shelf on the desk rather
            than a different kind of list. The tiles carry triage controls
            because Home OWNS this decision — there is no other page to open
            an Inbox paper on. */}
        <PaperTileGrid
          items={brief.inbox.items}
          getKey={(paper) => paper.id}
          collapsedRows={COLLAPSED_ROWS}
          renderTile={(paper) => (
            <PaperTile
              href={buildHashRoute('library', { paper: paper.id })}
              title={paper.title}
              byline={paperByline(paper)}
              excerpt={excerpt(paper)}
              noteTheme="inbox"
              eyebrow={<CaptureEyebrow paper={paper} />}
              actions={
                <PaperActionBar
                  compact
                  showLabels={false}
                  disabled={triage.isPending}
                  onAdd={act(paper.id, 'add')}
                  onLike={act(paper.id, 'like')}
                  onLove={act(paper.id, 'love')}
                  onDislike={act(paper.id, 'dislike')}
                  // The X. `defer` returns the paper to `tracked` writing no
                  // rating and no feedback event — "I saw it, no action for
                  // it", NOT a judgement. Relabelled so the button never
                  // reads as the global hide `dismiss` performs elsewhere.
                  onDismiss={act(paper.id, 'defer')}
                  dismissLabel="Not now"
                  dismissTitle="Clear from Inbox — stays in your corpus, records no opinion"
                />
              }
            />
          )}
        />
      </PageSection>

      <PageSection
        id="home-highlights"
        data-tour="home-picked"
        collapsible
        title="Picked for you"
        icon={Sparkles}
        categoryTheme={HOME_SECTION_THEMES.picked}
        count={brief.highlights.length}
        description="New research from your monitored sources and Discovery, with the reason it surfaced."
        emptyState={
          <p className="text-sm text-slate-500">
            No new research arrived in the last seven days. Your reading list and source pages remain available below.
          </p>
        }
      >
        <PaperTileGrid
          items={brief.highlights}
          getKey={(highlight) =>
            `${highlight.kind}-${highlight.paper.id}-${highlight.source?.id ?? highlight.lens_id ?? ''}`
          }
          // Exactly one row of the same measured grid the sections below
          // use — a shortlist, not a truncated list, so no "Show more".
          collapsedRows={1}
          expandable={false}
          renderTile={(highlight) => {
            const noteTheme = highlight.kind === 'discovery_paper' ? 'discovery' : 'feed'
            return (
            <PaperTile
              href={highlightHref(highlight)}
              title={highlight.paper.title}
              byline={paperByline(highlight.paper)}
              excerpt={excerpt(highlight.paper)}
              noteTheme={noteTheme}
              // The period chip takes the note's own hue rather than `accent`:
              // a folio pill on a magenta or cyan sheet fights it. "Today" vs
              // "Last 7 days" is still carried by the words, and by the chip's
              // weight against the quieter older-window one.
              eyebrow={
                highlight.period === 'today' ? (
                  <IdentityChip
                    size="sm"
                    chipClassName={HOME_SECTION_THEMES[noteTheme].noteChip}
                  >
                    Today
                  </IdentityChip>
                ) : (
                  <StatusBadge tone="neutral" size="sm">
                    Last 7 days
                  </StatusBadge>
                )
              }
              score={highlight.score ?? null}
              reason={highlight.reason.label}
              explanation={highlightExplanation(highlight)}
            />
            )
          }}
        />
      </PageSection>

      <PageSection
        id="home-reading"
        collapsible
        title="Reading list"
        icon={BookMarked}
        categoryTheme={HOME_SECTION_THEMES.reading}
        count={brief.reading.total}
        description="Latest added, newest first."
        action={
          <Button size="sm" variant="ghost" onClick={() => navigateTo('library', { tab: 'reading' })}>
            Open reading list
            <ArrowRight className="h-4 w-4" />
          </Button>
        }
        emptyState={
          <p className="text-sm text-slate-500">
            Nothing queued. Papers you add to the reading list show up here, newest first.
          </p>
        }
      >
        <PaperSection
          papers={brief.reading.items}
          hrefFor={(paper) => buildHashRoute('library', { tab: 'reading', paper: paper.id })}
          noteTheme="reading"
        />
      </PageSection>

      {attentionTotal === 0 && brief.reading.total === 0 && brief.inbox.total === 0 && brief.highlights.length === 0 && (
        <p className="flex items-center gap-2 text-sm text-slate-500">
          <AlertTriangle className="h-4 w-4" />
          Your workspace is quiet. Start by finding a paper or following an author.
        </p>
      )}
    </div>
  )
}
