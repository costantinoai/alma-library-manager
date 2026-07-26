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
  Send,
  Sparkles,
  Sunrise,
  UserPlus,
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
import { ConnectionRail } from '@/components/home/ConnectionRail'
import { InflowStrip } from '@/components/home/InflowStrip'
import { PaperActionBar } from '@/components/discovery/PaperActionBar'
import { IdentityChip, MetricTile, PaperTile, PaperTileGrid } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { BrandRule } from '@/components/ui/brand-rule'
import { Card } from '@/components/ui/card'
import { Meter } from '@/components/ui/meter'
import { PageSection } from '@/components/ui/page-section'
import { CalibrationCard } from '@/components/home/CalibrationCard'
import { ErrorState } from '@/components/ui/ErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusBadge } from '@/components/ui/status-badge'
import { errorToast, toast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import {
  CAPTURE_CHANNEL_CHIP,
  CAPTURE_CHANNEL_CHIP_FALLBACK,
  CAPTURE_CHANNEL_LABEL,
  MONITOR_MIX_FILL,
} from '@/lib/palette'
import { cn, formatRelativeShort } from '@/lib/utils'

function greeting(name: string | null): string {
  if (!name) return 'Your daily brief'
  const firstName = name.trim().split(/\s+/)[0] || name
  const hour = new Date().getHours()
  if (hour < 12) return `Good morning, ${firstName}`
  if (hour < 18) return `Good afternoon, ${firstName}`
  return `Good evening, ${firstName}`
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
}: {
  papers: HomePaper[]
  hrefFor: (paper: HomePaper) => string
  reasonFor?: (paper: HomePaper) => string | undefined
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

/** The Inbox tile's eyebrow: which transport this arrived on, and when. */
function CaptureEyebrow({ paper }: { paper: HomePaper }) {
  const channel = (paper.capture_channel || '').toLowerCase()
  const when = paper.captured_at ?? paper.added_at
  return (
    <>
      {channel && (
        <IdentityChip
          icon={Send}
          chipClassName={CAPTURE_CHANNEL_CHIP[channel] ?? CAPTURE_CHANNEL_CHIP_FALLBACK}
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
  const briefQuery = useQuery({
    queryKey: ['home-brief'],
    queryFn: () => getHomeBrief(),
    staleTime: 30_000,
  })

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
        <header className="space-y-3">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div>
              <h1 className="font-brand text-2xl font-semibold text-alma-800 sm:text-[1.75rem]">
                {greeting(brief.user_name)}
              </h1>
              <p className="mt-1 text-sm text-slate-500">
                {localDate()} · updated {formatRelativeShort(brief.generated_at)}
              </p>
            </div>
            <div className="flex flex-wrap gap-2" aria-label="Start a workflow">
              <Button size="sm" onClick={() => navigateTo('discovery', { action: 'find' })}>
                <FileSearch className="h-4 w-4" />
                Find papers
              </Button>
              <Button size="sm" onClick={() => navigateTo('authors', { action: 'follow' })}>
                <UserPlus className="h-4 w-4" />
                Follow author
              </Button>
            </div>
          </div>
          {/* The wordmark's own gold rule closes the masthead. */}
          <BrandRule center="diamond" />
        </header>

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
          <div className="flex flex-wrap items-start gap-y-5 divide-edge-1 sm:divide-x">
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
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 border-t border-edge-1 pt-3">
            <ConnectionRail connections={brief.connections} />
            {/* One hairline between the two halves: everything left of it is
                machinery ALMa runs, everything right of it wants a decision
                from you. Only drawn when both sides exist. */}
            {attentionTotal > 0 && brief.connections.length > 0 && (
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
                <a href={buildHashRoute('feed')} className="font-medium text-alma-folio hover:underline">
                  {feed.carryover} in Feed
                </a>
                {' · '}
                <a href={buildHashRoute('discovery')} className="font-medium text-alma-folio hover:underline">
                  {discovery.carryover} in Discovery
                </a>
              </span>
            </p>
          )}
        </PageSection>

      </Card>

      {/* Signal Lab calibration — one round per visit (task 54, D20).
          Sits above the Inbox: a 10-second question, answered before the desk
          work starts. Renders nothing until the corpus substrate exists. */}
      <CalibrationCard />

      {/* D13 Inbox — papers you sent yourself from another device, awaiting
          triage. Home IS the Inbox's surface, so there is no "open elsewhere"
          action. It leads the research sections, ahead of even "Picked for
          you": what you chose to send yourself outranks anything ALMa chose
          for you. Renders only when non-empty — a capture queue notifies, it
          never nags (I-22). */}
      {brief.inbox.total > 0 && (
        <PageSection
          id="home-inbox"
          collapsible
          title="Inbox"
          icon={Inbox}
          count={brief.inbox.total}
          description="Papers you sent yourself, waiting for a decision."
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
      )}

      <PageSection
        id="home-highlights"
          collapsible
        title="Picked for you"
        icon={Sparkles}
        count={brief.highlights.length}
        description="New research from your monitored sources and Discovery, with the reason it surfaced."
      >
        {brief.highlights.length > 0 ? (
          <PaperTileGrid
            items={brief.highlights}
            getKey={(highlight) =>
              `${highlight.kind}-${highlight.paper.id}-${highlight.source?.id ?? highlight.lens_id ?? ''}`
            }
            // Exactly one row of the same measured grid the sections below
            // use — a shortlist, not a truncated list, so no "Show more".
            collapsedRows={1}
            expandable={false}
            renderTile={(highlight) => (
              <PaperTile
                href={highlightHref(highlight)}
                title={highlight.paper.title}
                byline={paperByline(highlight.paper)}
                excerpt={excerpt(highlight.paper)}
                eyebrow={
                  <StatusBadge
                    tone={highlight.period === 'today' ? 'accent' : 'neutral'}
                    size="sm"
                  >
                    {highlight.period === 'today' ? 'Today' : 'Last 7 days'}
                  </StatusBadge>
                }
                score={highlight.score ?? null}
                reason={highlight.reason.label}
                explanation={highlightExplanation(highlight)}
              />
            )}
          />
        ) : (
          <Card className="p-5">
            <p className="text-sm text-slate-500">
              No new research arrived in the last seven days. Your reading list and source pages remain available below.
            </p>
          </Card>
        )}
      </PageSection>

      {brief.reading.total > 0 && (
        <PageSection
          id="home-reading"
          collapsible
          title="Reading list"
          icon={BookMarked}
          count={brief.reading.total}
          description="Latest added, newest first."
          action={
            <Button size="sm" variant="ghost" onClick={() => navigateTo('library', { tab: 'reading' })}>
              Open reading list
              <ArrowRight className="h-4 w-4" />
            </Button>
          }
        >
          <PaperSection
            papers={brief.reading.items}
            hrefFor={(paper) => buildHashRoute('library', { tab: 'reading', paper: paper.id })}
          />
        </PageSection>
      )}

      {attentionTotal === 0 && brief.reading.total === 0 && brief.inbox.total === 0 && brief.highlights.length === 0 && (
        <p className="flex items-center gap-2 text-sm text-slate-500">
          <AlertTriangle className="h-4 w-4" />
          Your workspace is quiet. Start by finding a paper or following an author.
        </p>
      )}
    </div>
  )
}
