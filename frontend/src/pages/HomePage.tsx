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
  BookOpen,
  FileSearch,
  FileUp,
  HeartPulse,
  Inbox,
  Radio,
  UserPlus,
  Users,
} from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  applyPaperAction,
  getHomeBrief,
  type HomeHighlight,
  type HomePaper,
  type OnboardingPaperAction,
} from '@/api/client'
import { MetricTile, PaperCard, PaperTile, PaperTileGrid } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { PageSection } from '@/components/ui/page-section'
import { ErrorState } from '@/components/ui/ErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusBadge } from '@/components/ui/status-badge'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import { formatRelativeShort } from '@/lib/utils'

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

/** `HomePaper` spells absent fields `null`; `PaperCardPaper` spells them
 *  `undefined`. Convert explicitly rather than casting, so a genuinely missing
 *  field stays visibly missing instead of being asserted away. */
function toCardPaper(paper: HomePaper) {
  return {
    id: paper.id,
    title: paper.title,
    authors: paper.authors ?? undefined,
    year: paper.year ?? null,
    journal: paper.journal ?? undefined,
    abstract: paper.abstract ?? undefined,
    tldr: paper.tldr ?? undefined,
    url: paper.url ?? undefined,
    doi: paper.doi ?? undefined,
    status: paper.status ?? undefined,
  }
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

interface AttentionRowProps {
  icon: React.ComponentType<{ className?: string }>
  label: string
  href: string
}

function AttentionRow({ icon: Icon, label, href }: AttentionRowProps) {
  return (
    <a
      href={href}
      className="flex items-center justify-between gap-3 border-b border-edge-1 px-4 py-3 text-sm last:border-b-0 hover:bg-control-quiet focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-alma-folio"
    >
      <span className="flex min-w-0 items-center gap-2.5 text-alma-800">
        <Icon className="h-4 w-4 shrink-0 text-warning-700" />
        <span>{label}</span>
      </span>
      <ArrowRight className="h-4 w-4 shrink-0 text-slate-400" />
    </a>
  )
}

export function HomePage() {
  const queryClient = useQueryClient()
  const briefQuery = useQuery({
    queryKey: ['home-brief'],
    queryFn: () => getHomeBrief(),
    staleTime: 30_000,
  })

  // Inbox triage (D13). `source_surface: 'inbox'` so feedback provenance says
  // where the user actually acted. Refetching the brief is what makes the card
  // leave the section: every action moves the paper off `status='inbox'`.
  const triage = useMutation({
    mutationFn: ({ paperId, action }: { paperId: string; action: OnboardingPaperAction }) =>
      applyPaperAction(paperId, action, { surface: 'inbox' }),
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
    <div className="mx-auto max-w-5xl space-y-9 py-2 2xl:max-w-6xl">
      <header className="space-y-4">
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
      </header>

      <PageSection
        id="home-activity"
        title="Today in ALMa"
        description="Activity since your local midnight."
      >
        <div className="grid gap-3 sm:grid-cols-3">
          <MetricTile
            label="new Feed papers"
            value={feed.today}
            hint={`${feed.by_monitor_type.authors} authors · ${feed.by_monitor_type.journals} journals · ${feed.by_monitor_type.other} other`}
            tone={feed.today > 0 ? 'accent' : 'neutral'}
            onClick={() => navigateTo('feed')}
          />
          <MetricTile
            label="new suggestions"
            value={discovery.today}
            hint={`across ${discovery.lenses_today} ${discovery.lenses_today === 1 ? 'lens' : 'lenses'}`}
            tone={discovery.today > 0 ? 'accent' : 'neutral'}
            onClick={() => navigateTo('discovery')}
          />
          <MetricTile
            label="alerts delivered"
            value={alerts.today}
            hint="successful digest deliveries"
            tone={alerts.today > 0 ? 'info' : 'neutral'}
            onClick={() => navigateTo('alerts', { tab: 'history' })}
          />
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

      <PageSection
        id="home-highlights"
        title="Picked for you"
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

      {/* D13 Inbox — papers you sent yourself from another device, awaiting
          triage. Home IS the Inbox's surface, so there is no "open elsewhere"
          action. Placed above the reading list because it is the section that
          wants a decision. Renders only when non-empty: a capture queue
          notifies, it never nags (I-22). */}
      {brief.inbox.total > 0 && (
        <PageSection
          id="home-inbox"
          title="Inbox"
          description={`Sent from your capture channels — ${brief.inbox.total} ${brief.inbox.total === 1 ? 'paper' : 'papers'} waiting for a decision.`}
        >
          <div className="space-y-3">
            {brief.inbox.items.map((paper) => (
              <PaperCard
                key={paper.id}
                paper={toCardPaper(paper)}
                size="compact"
                actionDisabled={triage.isPending}
                onAdd={act(paper.id, 'add')}
                onLike={act(paper.id, 'like')}
                onLove={act(paper.id, 'love')}
                onDislike={act(paper.id, 'dislike')}
                // The X. `defer` returns the paper to `tracked` writing no
                // rating and no feedback event — "I saw it, no action for it",
                // NOT a judgement. Relabelled so the button never reads as the
                // global hide that `dismiss` performs elsewhere.
                onDismiss={act(paper.id, 'defer')}
                dismissLabel="Not now"
                dismissTitle="Remove from Inbox — stays in your corpus, records no opinion"
              />
            ))}
          </div>
        </PageSection>
      )}

      {brief.reading.total > 0 && (
        <PageSection
          id="home-reading"
          title="Reading list"
          description={`Latest added — ${brief.reading.total} ${brief.reading.total === 1 ? 'paper' : 'papers'} in all.`}
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

      {attentionTotal > 0 && (
        <PageSection
          id="home-attention"
          title="Needs attention"
          description="Only decisions or blockers that need you."
        >
          <Card className="overflow-hidden">
            {brief.attention.imports_pending > 0 && (
              <AttentionRow
                icon={FileUp}
                label={`${brief.attention.imports_pending} imported ${brief.attention.imports_pending === 1 ? 'paper needs' : 'papers need'} review`}
                href={buildHashRoute('library', { tab: 'imports' })}
              />
            )}
            {brief.attention.monitors_need_resolution > 0 && (
              <AttentionRow
                icon={Radio}
                label={`${brief.attention.monitors_need_resolution} ${brief.attention.monitors_need_resolution === 1 ? 'monitor needs' : 'monitors need'} relinking`}
                href={buildHashRoute('settings', { anchor: 'feed-monitors' })}
              />
            )}
            {brief.attention.author_decisions > 0 && (
              <AttentionRow
                icon={Users}
                label={`${brief.attention.author_decisions} author ${brief.attention.author_decisions === 1 ? 'identity needs' : 'identities need'} review`}
                href={buildHashRoute('authors', { focus: 'needs-attention' })}
              />
            )}
            {brief.attention.inbox_unresolved > 0 && (
              <AttentionRow
                icon={Inbox}
                label={`${brief.attention.inbox_unresolved} captured ${brief.attention.inbox_unresolved === 1 ? 'message' : 'messages'} couldn't be identified`}
                href={buildHashRoute('settings', { anchor: 'channels' })}
              />
            )}
            {brief.attention.critical_health > 0 && (
              <AttentionRow
                icon={HeartPulse}
                label={`${brief.attention.critical_health} critical health ${brief.attention.critical_health === 1 ? 'issue needs' : 'issues need'} action`}
                href={buildHashRoute('health')}
              />
            )}
          </Card>
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
