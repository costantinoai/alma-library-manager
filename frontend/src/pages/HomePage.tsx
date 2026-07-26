/**
 * Home — a read-only daily research desk.
 *
 * The page reports today's stable activity, preserves older carryover until
 * Feed/Discovery themselves are reviewed, and hands every item to the surface
 * that owns it. It never stamps review state or duplicates paper actions.
 */
import {
  AlertTriangle,
  ArrowRight,
  BookOpen,
  FileSearch,
  FileUp,
  HeartPulse,
  Radio,
  UserPlus,
  Users,
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'

import {
  getHomeBrief,
  type HomeHighlight,
  type HomePaper,
} from '@/api/client'
import { MetricTile } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
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

function HighlightRow({ highlight }: { highlight: HomeHighlight }) {
  const summary = excerpt(highlight.paper)
  return (
    <a
      href={highlightHref(highlight)}
      className="group block border-b border-edge-1 px-4 py-4 transition-colors last:border-b-0 hover:bg-control-quiet focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-alma-folio"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="mb-1.5 flex flex-wrap items-center gap-2">
            <StatusBadge tone={highlight.period === 'today' ? 'accent' : 'neutral'} size="sm">
              {highlight.period === 'today' ? 'Today' : 'Last 7 days'}
            </StatusBadge>
            <span className="text-xs font-medium text-alma-folio">
              {highlight.reason.label}
            </span>
          </div>
          <h3 className="font-brand text-base font-semibold leading-snug text-alma-800 transition-colors group-hover:text-alma-folio">
            {highlight.paper.title}
          </h3>
          {paperByline(highlight.paper) && (
            <p className="mt-1 truncate text-xs text-slate-500">
              {paperByline(highlight.paper)}
            </p>
          )}
          {summary && (
            <p className="mt-2 line-clamp-2 text-sm leading-relaxed text-slate-600">
              {summary}
            </p>
          )}
        </div>
        <ArrowRight className="mt-1 h-4 w-4 shrink-0 text-slate-400 transition-transform group-hover:translate-x-0.5 group-hover:text-alma-folio" />
      </div>
    </a>
  )
}

function PaperRow({ paper }: { paper: HomePaper }) {
  return (
    <a
      href={buildHashRoute('library', { tab: 'reading', paper: paper.id })}
      className="group flex items-center justify-between gap-4 border-b border-edge-1 px-4 py-3 last:border-b-0 hover:bg-control-quiet focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-alma-folio"
    >
      <span className="min-w-0">
        <span className="block truncate text-sm font-medium text-alma-800 group-hover:text-alma-folio">
          {paper.title}
        </span>
        {paperByline(paper) && (
          <span className="mt-0.5 block truncate text-xs text-slate-500">
            {paperByline(paper)}
          </span>
        )}
      </span>
      <ArrowRight className="h-4 w-4 shrink-0 text-slate-400 group-hover:text-alma-folio" />
    </a>
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
  const briefQuery = useQuery({
    queryKey: ['home-brief'],
    queryFn: () => getHomeBrief(),
    staleTime: 30_000,
  })

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
    <div className="mx-auto max-w-5xl space-y-9 py-2">
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

      <section className="space-y-3" aria-labelledby="home-activity">
        <div>
          <h2 id="home-activity" className="font-brand text-lg font-semibold text-alma-800">
            Today in ALMa
          </h2>
          <p className="text-sm text-slate-500">Activity since your local midnight.</p>
        </div>
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
      </section>

      <section className="space-y-3" aria-labelledby="home-highlights">
        <div>
          <h2 id="home-highlights" className="font-brand text-lg font-semibold text-alma-800">
            Worth your attention
          </h2>
          <p className="text-sm text-slate-500">
            A balanced selection from monitored research and Discovery.
          </p>
        </div>
        {brief.highlights.length > 0 ? (
          <Card className="overflow-hidden">
            {brief.highlights.map((highlight) => (
              <HighlightRow
                key={`${highlight.kind}-${highlight.paper.id}-${highlight.source?.id ?? highlight.lens_id ?? ''}`}
                highlight={highlight}
              />
            ))}
          </Card>
        ) : (
          <Card className="p-5">
            <p className="text-sm text-slate-500">
              No noteworthy research arrived in the last seven days. Your reading list and source pages remain available below.
            </p>
          </Card>
        )}
      </section>

      {brief.reading.total > 0 && (
        <section className="space-y-3" aria-labelledby="home-reading">
          <div className="flex items-end justify-between gap-3">
            <div>
              <h2 id="home-reading" className="font-brand text-lg font-semibold text-alma-800">
                Continue reading
              </h2>
              <p className="text-sm text-slate-500">
                {brief.reading.total} {brief.reading.total === 1 ? 'paper' : 'papers'} on your reading list.
              </p>
            </div>
            <Button size="sm" variant="ghost" onClick={() => navigateTo('library', { tab: 'reading' })}>
              Reading list
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
          <Card className="overflow-hidden">
            {brief.reading.items.map((paper) => (
              <PaperRow key={paper.id} paper={paper} />
            ))}
          </Card>
        </section>
      )}

      {attentionTotal > 0 && (
        <section className="space-y-3" aria-labelledby="home-attention">
          <div>
            <h2 id="home-attention" className="font-brand text-lg font-semibold text-alma-800">
              Needs attention
            </h2>
            <p className="text-sm text-slate-500">Only decisions or blockers that need you.</p>
          </div>
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
            {brief.attention.critical_health > 0 && (
              <AttentionRow
                icon={HeartPulse}
                label={`${brief.attention.critical_health} critical health ${brief.attention.critical_health === 1 ? 'issue needs' : 'issues need'} action`}
                href={buildHashRoute('health')}
              />
            )}
          </Card>
        </section>
      )}

      {attentionTotal === 0 && brief.reading.total === 0 && brief.highlights.length === 0 && (
        <p className="flex items-center gap-2 text-sm text-slate-500">
          <AlertTriangle className="h-4 w-4" />
          Your workspace is quiet. Start by finding a paper or following an author.
        </p>
      )}
    </div>
  )
}
