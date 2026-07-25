/**
 * HomePage — the landing page (task 47 Phase 6).
 *
 * Design thesis: a **note left on your desk overnight**, not a dashboard.
 * Dashboards imply monitoring; this page implies "here's what came in while
 * you were away, and here's what needs you". It answers those two questions
 * and then gets out of the way.
 *
 * Three modules, nothing else:
 *   1. THE BRIEF — the signature. What arrived since your last visit, set as a
 *      row of ledger figures in display type rather than metric tiles. Tiles
 *      are what every other page in this app uses; a landing page that looks
 *      like the Insights grid would read as templated. Zero counts stay in the
 *      row but recede, so the eye lands on what actually changed, and the row
 *      never reflows between visits. Each figure is a link to its surface.
 *   2. NEEDS YOU — actionable rows only. Renders NOTHING when everything is
 *      quiet: no "all good" card (a healthy system should be silent).
 *   3. ONE TO LOOK AT — the top unacted suggestion, rendered through the same
 *      PaperCard as Feed and Discovery so its actions are the real ones.
 *
 * The whole page costs ONE request: `GET /home/brief` carries the counts and
 * the suggestion. After it renders we fire `POST /home/seen`, so the brief you
 * are reading always describes the window you actually missed — a GET that
 * stamped the visit would let a refresh silently eat it.
 */
import { useEffect, useRef } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowRight, FileUp, Loader2, Radio } from 'lucide-react'

import {
  addToLibrary,
  dismissRecommendation,
  getHomeBrief,
  markHomeSeen,
  type HomeBrief,
} from '@/api/client'
import { PaperCard } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { ErrorState } from '@/components/ui/ErrorState'
import { RevealItem, RevealList } from '@/components/ui/reveal'
import { Skeleton } from '@/components/ui/skeleton'
import { useToast, errorToast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn } from '@/lib/utils'

/** One column of the brief ledger: a figure, its label, and where it goes. */
interface BriefFigure {
  value: number
  label: string
  sub: string
  href: string
}

/** Humanised window: "since Tuesday", "since yesterday", "since this morning". */
function windowLabel(since: string): string {
  const then = new Date(since)
  if (Number.isNaN(then.getTime())) return 'since your last visit'
  const hours = (Date.now() - then.getTime()) / 36e5
  if (hours < 1) return 'in the last hour'
  if (hours < 12) return 'since earlier today'
  if (hours < 36) return 'since yesterday'
  if (hours < 24 * 7) {
    return `since ${then.toLocaleDateString(undefined, { weekday: 'long' })}`
  }
  return `since ${then.toLocaleDateString(undefined, { month: 'long', day: 'numeric' })}`
}

/**
 * The ledger. A figure sits over a hairline with its label beneath — the
 * hairline IS the link affordance, going folio-blue on hover, so the row reads
 * as a set of doors rather than a set of statistics.
 */
function BriefLedger({ figures }: { figures: BriefFigure[] }) {
  return (
    <RevealList className="grid grid-cols-2 gap-x-6 gap-y-5 sm:grid-cols-4 sm:gap-x-8">
      {figures.map((f, i) => (
        <RevealItem key={f.label} index={i} stagger={0.06}>
          <a
            href={f.href}
            className="group block focus-visible:outline-none"
            aria-label={`${f.value} ${f.label} ${f.sub}`}
          >
            <span
              className={cn(
                'block font-brand text-[2.5rem] leading-none tabular-nums transition-colors sm:text-[3.25rem]',
                // A zero is still true and still holds its column, but it must
                // not compete with the number that changed.
                f.value === 0
                  ? 'text-slate-300'
                  : 'text-alma-800 group-hover:text-alma-folio',
              )}
            >
              {f.value}
            </span>
            <span
              className={cn(
                'mt-2 block border-t pt-2 text-xs leading-snug transition-colors',
                f.value === 0
                  ? 'border-edge-1 text-slate-400'
                  : 'border-edge-2 text-slate-600 group-hover:border-alma-folio group-hover:text-alma-folio',
              )}
            >
              <span className="font-medium">{f.label}</span>
              <span className="block text-slate-400 group-hover:text-alma-folio/70">{f.sub}</span>
            </span>
          </a>
        </RevealItem>
      ))}
    </RevealList>
  )
}

/** One actionable row in "Needs you". */
function AttentionRow({
  icon: Icon,
  text,
  actionLabel,
  onAction,
}: {
  icon: React.ComponentType<{ className?: string }>
  text: string
  actionLabel: string
  onAction: () => void
}) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-edge-1 py-2.5 last:border-b-0">
      <span className="flex min-w-0 items-center gap-2.5 text-sm text-alma-800">
        <Icon className="h-4 w-4 shrink-0 text-alma-folio" />
        <span className="truncate">{text}</span>
      </span>
      <Button size="sm" variant="outline" className="shrink-0" onClick={onAction}>
        {actionLabel}
        <ArrowRight className="h-3.5 w-3.5" />
      </Button>
    </div>
  )
}

export function HomePage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const briefQuery = useQuery({
    queryKey: ['home-brief'],
    queryFn: getHomeBrief,
    staleTime: 30_000,
  })
  const brief: HomeBrief | undefined = briefQuery.data

  // Stamp the visit ONCE, after the brief has rendered. The ref guards against
  // React 18 double-invocation in dev and against a refetch re-stamping.
  const stamped = useRef(false)
  const seenMutation = useMutation({ mutationFn: markHomeSeen })
  useEffect(() => {
    if (!brief || stamped.current) return
    stamped.current = true
    seenMutation.mutate()
    // Intentionally fire-and-forget: a failed stamp just means the next brief
    // reports a slightly wider window, which is harmless and honest.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [brief])

  const saveMutation = useMutation({
    mutationFn: (paperId: string) => addToLibrary(paperId),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['home-brief'], ['library'])
      toast({ title: 'Saved to Library' })
    },
    onError: () => errorToast('Could not save', 'Please try again.'),
  })
  const dismissMutation = useMutation({
    mutationFn: (recId: string) => dismissRecommendation(recId),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['home-brief'])
    },
    onError: () => errorToast('Could not dismiss', 'Please try again.'),
  })

  if (briefQuery.isLoading) {
    return (
      <div className="space-y-8">
        <Skeleton className="h-10 w-72" />
        <Skeleton className="h-28 w-full" />
      </div>
    )
  }
  if (briefQuery.isError || !brief) {
    return <ErrorState message="Couldn't load your brief." />
  }

  const { arrived, waiting, insight } = brief
  const figures: BriefFigure[] = [
    {
      value: arrived.feed_items,
      label: 'new papers',
      sub: 'in Feed',
      href: buildHashRoute('feed'),
    },
    {
      value: arrived.recommendations,
      label: 'suggestions',
      sub: 'from Discovery',
      href: buildHashRoute('discovery'),
    },
    {
      value: arrived.alerts_fired,
      label: 'alerts',
      sub: 'delivered',
      href: buildHashRoute('alerts'),
    },
    {
      value: waiting.reading,
      label: 'to read',
      sub: 'in your list',
      href: buildHashRoute('library', { tab: 'reading' }),
    },
  ]

  const hasAttention = waiting.imports_pending > 0 || waiting.monitors_need_attention > 0

  return (
    <div className="mx-auto max-w-4xl space-y-10 py-2">
      {/* ── 1. The brief ─────────────────────────────────────────────────── */}
      <section className="space-y-6">
        <div>
          <h1 className="font-brand text-2xl font-semibold text-alma-800 sm:text-[1.75rem]">
            {brief.first_visit ? "Here's where things stand." : 'Since you were last here'}
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            {brief.first_visit
              ? 'Your first visit — showing the last 60 days.'
              : `What arrived ${windowLabel(brief.since)}.`}
          </p>
        </div>
        <BriefLedger figures={figures} />
      </section>

      {/* ── 2. Needs you — silent when there's nothing to do ─────────────── */}
      {hasAttention && (
        <section className="space-y-2">
          <h2 className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">
            Needs you
          </h2>
          <Card className="px-4 py-1">
            {waiting.imports_pending > 0 && (
              <AttentionRow
                icon={FileUp}
                text={`${waiting.imports_pending} imported ${
                  waiting.imports_pending === 1 ? 'paper is' : 'papers are'
                } waiting to be matched`}
                actionLabel="Review"
                onAction={() => navigateTo('library', { tab: 'imports' })}
              />
            )}
            {waiting.monitors_need_attention > 0 && (
              <AttentionRow
                icon={Radio}
                text={`${waiting.monitors_need_attention} ${
                  waiting.monitors_need_attention === 1 ? 'monitor' : 'monitors'
                } stopped and need re-linking`}
                actionLabel="Fix"
                onAction={() => navigateTo('settings', { anchor: 'feed-monitors' })}
              />
            )}
          </Card>
        </section>
      )}

      {/* ── 3. One to look at — absent when there's no suggestion ────────── */}
      {insight && (
        <section className="space-y-2">
          <div className="flex items-baseline justify-between gap-3">
            <h2 className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">
              One to look at
            </h2>
            <button
              type="button"
              onClick={() => navigateTo('discovery')}
              className="text-xs text-slate-500 transition-colors hover:text-alma-folio"
            >
              See all suggestions →
            </button>
          </div>
          <PaperCard
            paper={{
              id: insight.paper_id,
              title: insight.title,
              authors: insight.authors ?? '',
              year: insight.year ?? null,
              journal: insight.journal ?? undefined,
              url: insight.url ?? undefined,
              doi: insight.doi ?? undefined,
            }}
            score={insight.score ?? undefined}
            onAdd={() => saveMutation.mutate(insight.paper_id)}
            onDismiss={() => dismissMutation.mutate(insight.id)}
          />
          {(saveMutation.isPending || dismissMutation.isPending) && (
            <p className="flex items-center gap-1.5 text-xs text-slate-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Working…
            </p>
          )}
        </section>
      )}
    </div>
  )
}
