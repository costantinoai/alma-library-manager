/**
 * Health → Activity — what the system has actually been DOING lately.
 *
 * Re-homed from the retired Insights → Diagnostics tab (task 47 Phase 5).
 * Health already owns "is my data healthy and what repairs it"; operational
 * telemetry belongs in the same place rather than in an analytics page, so
 * the Health popups' "Open in Activity" deep link now lands here.
 *
 * Three sections, all operational, none of them a passive trend chart:
 *   1. Background operations (last 24 h) — the deep-link target
 *      (`#/health?tab=activity&focus=failed`): WHAT failed, when, and why.
 *   2. Evaluation scorecards — product-level quality grades.
 *   3. Recent refreshes — the latest Feed / Discovery runs + a way in.
 *
 * The five trend charts that used to sit around these (Intake, Discovery
 * Action, Alert Delivery, Followed Author Growth, Feedback Activity) were
 * deleted rather than moved: they plotted history nobody acted on, and the
 * numbers that DO drive action already live on the Health cards above.
 */
import { useEffect, useRef } from 'react'
import { Activity, CheckCircle2, ChevronDown, Clock3, Gauge } from 'lucide-react'

import type { DiagnosticsOperationalSection } from '@/api/client'
import type {
  InsightsDiagnosticsSections,
  SectionState,
} from '@/components/insights/useDiagnosticsSections'
import { SectionHeader } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusBadge } from '@/components/ui/status-badge'
import { scoreStatusTone } from '@/components/ui/status-badge-tones'
import { SubPanel } from '@/components/ui/sub-panel'
import { navigateTo, parseHashRoute } from '@/lib/hashRoute'
import { formatTimestamp } from '@/lib/utils'

/**
 * Renders a per-card skeleton or inline error while a section's first
 * response is in flight; once `data` lands the gate gets out of the way.
 */
function SectionGate<T>({
  section,
  skeletonHeight = 220,
  children,
  errorLabel = 'Failed to load this section.',
}: {
  section: SectionState<T>
  skeletonHeight?: number
  children: (data: T) => React.ReactNode
  errorLabel?: string
}) {
  if (section.loading && !section.data) {
    return <Skeleton style={{ height: skeletonHeight }} className="w-full" />
  }
  if (section.error && !section.data) {
    return <ErrorState message={errorLabel} />
  }
  if (!section.data) return null
  return <>{children(section.data)}</>
}

/** One row in "Recent refreshes". */
function RefreshEntryRow({
  status,
  finishedAt,
  detail,
}: {
  status: string
  finishedAt?: string | null
  detail: string
}) {
  return (
    <div className="rounded-sm border border-[var(--color-border)] p-3 text-sm">
      <div className="flex items-center justify-between gap-3">
        <span className="font-medium text-alma-800">{status}</span>
        <span className="text-xs text-slate-500">{formatTimestamp(finishedAt)}</span>
      </div>
      <p className="mt-1 text-xs text-slate-500">{detail}</p>
    </div>
  )
}

/**
 * Background operations, last 24 h. The Health "Background jobs" popup
 * deep-links here (`#/health?tab=activity&focus=failed`) and that click has to
 * land on the failures THEMSELVES — what failed, when, why — not on a count
 * with no body.
 */
function FailedOperationsCard({
  operational,
}: {
  operational: SectionState<DiagnosticsOperationalSection>
}) {
  const focusFailed = parseHashRoute().params.get('focus') === 'failed'
  const ref = useRef<HTMLDivElement | null>(null)
  const count = operational.data?.summary?.recent_failed_operations_24h ?? 0

  useEffect(() => {
    if (focusFailed && !operational.loading && ref.current) {
      ref.current.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [focusFailed, operational.loading])

  return (
    <div ref={ref} className="scroll-mt-6">
      <Card className={focusFailed ? 'ring-2 ring-alma-folio' : undefined}>
        <SectionHeader
          icon={Activity}
          accent={count > 0 ? 'text-critical-600' : 'text-success-600'}
          title="Background operations"
          description="Scheduler job outcomes over the last 24 hours — maintenance, hydration, embedding and refresh runs."
        />
        <CardContent>
          <SectionGate section={operational} skeletonHeight={120}>
            {(data) => {
              const rows = data.failed_operations ?? []
              if (rows.length === 0) {
                return (
                  <p className="flex items-center gap-2 text-sm text-slate-600">
                    <CheckCircle2 className="h-4 w-4 text-success-600" />
                    No failed operations in the last 24 hours.
                  </p>
                )
              }
              // A row expands only when it carries a step-log tail or an error
              // distinct from its message — so the hint below can't promise
              // detail that isn't there.
              const rowHasDetail = (op: (typeof rows)[number]): boolean => {
                const tail = op.log_tail ?? []
                return tail.length > 0 || !!(op.error && op.message && op.error !== op.message)
              }
              const anyExpandable = rows.some(rowHasDetail)
              return (
                <div className="space-y-3">
                  <div className="divide-y divide-[var(--color-border)] rounded-sm border border-[var(--color-border)] bg-surface-2">
                    {rows.map((op) => {
                      const summaryLine = op.error || op.message
                      const tail = op.log_tail ?? []
                      const hasDetail = rowHasDetail(op)
                      return (
                        <Collapsible key={op.job_id}>
                          {/* Progressive disclosure: the row states what + when;
                              the WHY expands on demand. */}
                          <CollapsibleTrigger
                            className="group w-full px-3 py-2.5 text-left"
                            disabled={!hasDetail}
                          >
                            <span className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
                              <span className="flex min-w-0 items-center gap-2">
                                <span className="h-2 w-2 shrink-0 rounded-full bg-critical-500" />
                                <span className="truncate font-mono text-xs font-medium text-alma-800">
                                  {op.operation_key || op.job_id}
                                </span>
                                {op.trigger_source ? (
                                  <StatusBadge tone="neutral" size="sm">
                                    {op.trigger_source}
                                  </StatusBadge>
                                ) : null}
                              </span>
                              <span className="flex shrink-0 items-center gap-1.5 text-xs text-slate-500">
                                {formatTimestamp(op.finished_at)}
                                {hasDetail ? (
                                  <ChevronDown className="h-3.5 w-3.5 transition-transform group-data-[state=open]:rotate-180" />
                                ) : null}
                              </span>
                            </span>
                            {summaryLine ? (
                              <span
                                title={summaryLine}
                                className="mt-1 block truncate pl-4 text-xs leading-relaxed text-slate-500"
                              >
                                {summaryLine}
                              </span>
                            ) : null}
                          </CollapsibleTrigger>
                          {hasDetail ? (
                            <CollapsibleContent>
                              <div className="space-y-2 px-3 pb-3 pl-7">
                                {op.error && op.message && op.error !== op.message ? (
                                  <p className="text-xs leading-relaxed text-critical-700">
                                    {op.error}
                                  </p>
                                ) : null}
                                {tail.length > 0 ? (
                                  <SubPanel padded={false} className="px-2.5 py-2">
                                    <ol className="space-y-1">
                                      {tail.map((line, i) => (
                                        <li
                                          key={i}
                                          className="flex items-baseline gap-2 text-[11px] leading-relaxed"
                                        >
                                          <span className="shrink-0 font-mono text-slate-400">
                                            {formatTimestamp(line.timestamp)?.split(', ')[1] ?? ''}
                                          </span>
                                          <span
                                            className={
                                              (line.level ?? '').toLowerCase() === 'error'
                                                ? 'text-critical-700'
                                                : 'text-slate-600'
                                            }
                                          >
                                            {line.step ? (
                                              <span className="font-medium">{line.step}: </span>
                                            ) : null}
                                            {line.message}
                                          </span>
                                        </li>
                                      ))}
                                    </ol>
                                  </SubPanel>
                                ) : (
                                  <p className="text-xs italic text-slate-500">
                                    The run recorded no step log before failing.
                                  </p>
                                )}
                              </div>
                            </CollapsibleContent>
                          ) : null}
                        </Collapsible>
                      )
                    })}
                  </div>
                  <p className="text-xs text-slate-500">
                    {anyExpandable ? 'Click a row for its error and step log; complete' : 'Complete'}{' '}
                    run history lives in the Activity panel (top bar).
                  </p>
                </div>
              )
            }}
          </SectionGate>
        </CardContent>
      </Card>
    </div>
  )
}

export function ActivitySection({ sections }: { sections: InsightsDiagnosticsSections }) {
  const { operational, evaluation, feed, discovery } = sections

  return (
    <div className="space-y-4">
      <FailedOperationsCard operational={operational} />

      {/* Evaluation scorecards — descriptive product-quality grades only. The
          operational_health scorecard is filtered out: system degradation is
          the Health cards' job right above this, and showing it twice invites
          two different numbers for one question. */}
      <Card>
        <SectionHeader
          icon={Gauge}
          accent="text-alma-600"
          title="Evaluation scorecards"
          description="Product-level quality across intake, discovery, branch behavior, and reading workflow."
        />
        <CardContent>
          <SectionGate section={evaluation} skeletonHeight={220}>
            {(data) => {
              const cards = data.scorecards.filter((c) => c.id !== 'operational_health')
              return cards.length === 0 ? (
                <EmptyState title="No evaluation scorecards available yet" />
              ) : (
                <div className="grid gap-3 md:grid-cols-2">
                  {cards.map((card) => (
                    <div key={card.id} className="rounded-sm border border-[var(--color-border)] p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="font-medium text-alma-800">{card.label}</p>
                          <p className="text-xs text-slate-500">{card.summary}</p>
                        </div>
                        {/* A score badge only when there IS a graded score;
                            otherwise an honest "N/A" (empty population) or
                            "Observed" (measures-only). */}
                        <StatusBadge tone={scoreStatusTone(card.status)}>
                          {card.status === 'insufficient_data'
                            ? 'N/A'
                            : card.score === null || card.score === undefined
                              ? 'Observed'
                              : `${card.score}/100`}
                        </StatusBadge>
                      </div>
                      <p className="mt-3 text-sm text-slate-600">{card.detail}</p>
                      {card.measures && card.measures.length > 0 ? (
                        <dl className="mt-3 space-y-1.5 border-t border-[var(--color-border)] pt-3">
                          {card.measures.map((m) => (
                            <div
                              key={m.key}
                              className="flex items-baseline justify-between gap-3 text-sm"
                            >
                              <dt className="text-slate-600">{m.label}</dt>
                              <dd className="font-medium text-alma-800">
                                {m.sufficient ? (
                                  <>
                                    {m.value}
                                    {m.unit}
                                    <span className="ml-1 text-xs font-normal text-slate-400">
                                      (n={m.sample_size})
                                    </span>
                                  </>
                                ) : (
                                  <span className="text-xs font-normal text-slate-400">
                                    insufficient data (n={m.sample_size})
                                  </span>
                                )}
                              </dd>
                            </div>
                          ))}
                        </dl>
                      ) : null}
                      {typeof card.sample_size === 'number' && !card.measures ? (
                        <p className="mt-2 text-xs text-slate-400">
                          Based on {card.sample_size.toLocaleString()} observations.
                        </p>
                      ) : null}
                    </div>
                  ))}
                </div>
              )
            }}
          </SectionGate>
        </CardContent>
      </Card>

      {/* Recent refreshes — the last runs plus a door into the owning page. */}
      <Card>
        <SectionHeader
          icon={Clock3}
          accent="text-accent"
          title="Recent refreshes"
          description="Latest Feed and Discovery refresh outcomes, with direct actions into the owning pages."
        />
        <CardContent>
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="font-medium text-alma-800">Feed</p>
                <Button size="sm" variant="outline" onClick={() => navigateTo('feed')}>
                  Open Feed
                </Button>
              </div>
              <SectionGate section={feed} skeletonHeight={120}>
                {(data) =>
                  (data.recent_refreshes ?? []).length === 0 ? (
                    <p className="text-sm text-slate-400">No recent Feed refreshes.</p>
                  ) : (
                    <>
                      {data.recent_refreshes.map((refresh) => (
                        <RefreshEntryRow
                          key={refresh.job_id}
                          status={refresh.status}
                          finishedAt={refresh.finished_at}
                          detail={`${refresh.items_created} new items from ${refresh.monitors_total} monitors (${refresh.monitors_degraded} degraded)`}
                        />
                      ))}
                    </>
                  )
                }
              </SectionGate>
            </div>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="font-medium text-alma-800">Discovery</p>
                <Button size="sm" variant="outline" onClick={() => navigateTo('discovery')}>
                  Open Discovery
                </Button>
              </div>
              <SectionGate section={discovery} skeletonHeight={120}>
                {(data) =>
                  (data.recent_refreshes ?? []).length === 0 ? (
                    <p className="text-sm text-slate-400">No recent Discovery refreshes.</p>
                  ) : (
                    <>
                      {data.recent_refreshes.map((refresh) => (
                        <RefreshEntryRow
                          key={refresh.job_id}
                          status={refresh.status}
                          finishedAt={refresh.finished_at}
                          detail={`${refresh.new_recommendations} new recommendations, ${refresh.total_recommendations} total retained`}
                        />
                      ))}
                    </>
                  )
                }
              </SectionGate>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
