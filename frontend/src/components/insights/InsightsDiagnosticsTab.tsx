import { useMemo } from 'react'
import {
  Brain,
  ChartLine,
  Clock3,
  Compass,
  Gauge,
  GitBranch,
  Radio,
  Sparkles,
  TrendingUp,
  UserRound,
  Waves,
} from 'lucide-react'
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type {
  DiagnosticsAiSection,
  DiagnosticsAlertsSection,
  DiagnosticsAuthorsSection,
  DiagnosticsDiscoverySection,
  DiagnosticsEvaluationSection,
  DiagnosticsFeedSection,
  DiagnosticsFeedbackSection,
  DiagnosticsOperationalSection,
} from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { Skeleton } from '@/components/ui/skeleton'
import { StatusBadge, scoreStatusTone } from '@/components/ui/status-badge'
import { MetricTile, SectionHeader } from '@/components/shared'
import { navigateTo } from '@/lib/hashRoute'
import { formatTimestamp } from '@/lib/utils'

// ── Trend / palette types ---------------------------------------------------

interface Palette {
  blue: string
  purple: string
  green: string
  amber: string
  cyan: string
  pink: string
  indigo: string
  orange: string
  red: string
  slate: string
}
interface TooltipStyle {
  contentStyle: React.CSSProperties
}


// ── Per-section state contract ---------------------------------------------
//
// Each card in this tab is fed by exactly one materialised view on the
// backend. The page passes us each section's load state separately so
// fast sections paint while slow sections still show a skeleton.
//
// `loading` is true while the section's first response is in flight.
// `stale` is true when the cached payload is being served while a
// background rebuild runs — we surface that as a "Refreshing…" pill,
// not as a blocking spinner.

export interface SectionState<T> {
  data?: T
  loading: boolean
  error: boolean
  stale?: boolean
}

export interface InsightsDiagnosticsSections {
  feed: SectionState<DiagnosticsFeedSection>
  discovery: SectionState<DiagnosticsDiscoverySection>
  ai: SectionState<DiagnosticsAiSection>
  authors: SectionState<DiagnosticsAuthorsSection>
  alerts: SectionState<DiagnosticsAlertsSection>
  feedback: SectionState<DiagnosticsFeedbackSection>
  operational: SectionState<DiagnosticsOperationalSection>
  evaluation: SectionState<DiagnosticsEvaluationSection>
}

export interface InsightsDiagnosticsTabProps {
  sections: InsightsDiagnosticsSections

  // Chart palette (shared with Overview/Reports)
  colors: Palette
  tooltipStyle: TooltipStyle
}

// ── Local primitives -------------------------------------------------------

/**
 * SectionGate — renders a per-card skeleton or inline error while the
 * section's first response is in flight. Once `data` lands the gate
 * gets out of the way and renders its children. Treat the children as
 * authoritative consumers of the section payload; they should assume
 * `data` is defined inside the gate.
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
    return (
      <Card>
        <CardContent className="py-6">
          <ErrorState message={errorLabel} />
        </CardContent>
      </Card>
    )
  }
  if (!section.data) {
    return null
  }
  return <>{children(section.data)}</>
}

/** Action chip for branch tuning. Loops the four identical variants instead
 *  of duplicating the loading/disabled logic four times.
 */
/** One row in the "Recent refreshes" card. */
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

// ── Tone derivation helpers -------------------------------------------------

function alertUsefulnessTone(score: number): 'good' | 'attention' | 'critical' {
  if (score >= 75) return 'good'
  if (score >= 50) return 'attention'
  return 'critical'
}
function branchQualityTone(state: string): 'good' | 'attention' | 'critical' {
  if (state === 'strong') return 'good'
  if (state === 'cool') return 'critical'
  return 'attention'
}
function branchDeltaTone(delta: number): 'good' | 'attention' | 'critical' {
  if (delta >= 0.08) return 'good'
  if (delta <= -0.08) return 'critical'
  return 'attention'
}

// ── Main component ----------------------------------------------------------

export function InsightsDiagnosticsTab({
  sections,
  colors,
  tooltipStyle,
}: InsightsDiagnosticsTabProps) {
  // `operational` is intentionally not destructured — operational health moved
  // to the Health page's Status tab; this tab is analytics only (I-27). The
  // evaluation section still carries operational_health / recommended_actions /
  // automation_opportunities, but those are deliberately NOT rendered here for
  // the same reason (see the Evaluation Scorecards block below).
  const { feed, discovery, ai, authors, alerts, feedback, evaluation } = sections

  // Headline tiles depend on feed + discovery.
  const feedSummary = feed.data?.summary
  const discoverySummary = discovery.data?.summary
  const latestFeedRefresh = feed.data?.recent_refreshes?.[0]
  const latestDiscoveryRefresh = discovery.data?.recent_refreshes?.[0]
  const sourceRequestsTotal = useMemo(
    () =>
      (discovery.data?.source_diagnostics ?? []).reduce(
        (sum, source) => sum + (source.requests ?? 0),
        0,
      ),
    [discovery.data?.source_diagnostics],
  )

  const branchSourceBadges = useMemo(
    () =>
      (discovery.data?.branch_quality ?? []).map((branch) => ({
        key: branch.branch_id ?? branch.branch_label,
        mix: branch.source_mix,
      })),
    [discovery.data?.branch_quality],
  )

  const windowDays = 30

  return (
    <div className="space-y-6">
      {/* ── Headline metrics ── */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {feed.loading && !feed.data ? (
          <>
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
          </>
        ) : (
          <>
            <MetricTile
              label="Feed monitors"
              value={feedSummary?.total_monitors ?? 0}
              hint={`${feedSummary?.degraded_monitors ?? 0} degraded`}
              tone={
                (feedSummary?.degraded_monitors ?? 0) > 0 ? 'warning' : 'neutral'
              }
            />
            <MetricTile
              label="Latest feed intake"
              value={latestFeedRefresh?.items_created ?? 0}
              hint={
                latestFeedRefresh
                  ? `${latestFeedRefresh.monitors_total} monitors in last refresh`
                  : 'No recent feed refresh'
              }
            />
          </>
        )}
        {discovery.loading && !discovery.data ? (
          <>
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
          </>
        ) : (
          <>
            <MetricTile
              label="Unseen discovery recs"
              value={discoverySummary?.active_unseen ?? 0}
              hint={`${discoverySummary?.total ?? 0} total recommendations`}
            />
            <MetricTile
              label="Source requests"
              value={sourceRequestsTotal}
              hint={
                latestDiscoveryRefresh
                  ? `${latestDiscoveryRefresh.new_recommendations} new recs in last refresh`
                  : 'No recent discovery refresh'
              }
            />
          </>
        )}
      </div>

      {/* ── Evaluation Scorecards ── */}
      {/* I-27: descriptive product-quality scores only. The operational_health
          scorecard, the Recommended-Actions repair list, and Automation
          Opportunities are intentionally NOT rendered here — operational
          diagnosis + repair + automation setup live on the Health / Alerts
          pages; Insights stays read-only analytics (D7). */}
      <div className="space-y-6">
        <Card>
          <SectionHeader
            icon={Gauge}
            accent="text-alma-600"
            title="Evaluation Scorecards"
            description="Product-level quality across intake, discovery, branch behavior, and reading workflow."
          />
          <CardContent>
            <SectionGate section={evaluation} skeletonHeight={220}>
              {(data) => {
                // I-27: drop the operational-health scorecard (system degradation
                // is the Health page's job); keep the descriptive quality cards.
                const cards = data.scorecards.filter((c) => c.id !== 'operational_health')
                return cards.length === 0 ? (
                  <EmptyState title="No evaluation scorecards available yet" />
                ) : (
                  <div className="grid gap-3 md:grid-cols-2">
                    {cards.map((card) => (
                      <div
                        key={card.id}
                        className="rounded-sm border border-[var(--color-border)] p-4"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="font-medium text-alma-800">{card.label}</p>
                            <p className="text-xs text-slate-500">{card.summary}</p>
                          </div>
                          {/* I-23/I-26: a score badge only when there is a graded
                              score; otherwise an honest "N/A" (empty population)
                              or "Observed" (measures-only) chip. */}
                          <StatusBadge tone={scoreStatusTone(card.status)}>
                            {card.status === 'insufficient_data'
                              ? 'N/A'
                              : card.score === null || card.score === undefined
                                ? 'Observed'
                                : `${card.score}/100`}
                          </StatusBadge>
                        </div>
                        <p className="mt-3 text-sm text-slate-600">{card.detail}</p>
                        {/* I-23: AI card surfaces separate observed diagnostics,
                            each with its own sample — no single composite grade. */}
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
      </div>

      {/* ── AI and Similarity Health ── (AI Recommendations moved to Health → Status) */}
      <div className="grid gap-6">
        <Card>
          <SectionHeader
            icon={Brain}
            accent="text-accent"
            title="AI and Similarity Health"
            description="Whether embeddings and scholarly similarity are materially helping retrieval instead of just being enabled."
          />
          <CardContent className="space-y-4">
            <SectionGate section={ai} skeletonHeight={200}>
              {(data) => (
                <>
                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <MetricTile
                      label="Coverage"
                      value={`${data.summary.embedding_coverage_pct}%`}
                      hint={`${data.summary.up_to_date_embeddings}/${data.summary.total_papers} up to date`}
                    />
                    <MetricTile
                      label="Compressed"
                      value={`${Math.round((data.summary.compressed_similarity_rate ?? 0) * 100)}%`}
                      hint="Recent recs in weak semantic ranges"
                    />
                    <MetricTile
                      label="Hybrid use"
                      value={`${Math.round((data.summary.hybrid_text_rate ?? 0) * 100)}%`}
                      hint="Recommendations using both semantic and lexical signals"
                    />
                  </div>

                  <div className="grid gap-4 xl:grid-cols-2">
                    <div className="rounded-sm border border-[var(--color-border)] p-4">
                      <p className="font-medium text-alma-800">Similarity profile</p>
                      <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-2">
                        <span>Avg text similarity: {data.summary.avg_text_similarity}</span>
                        <span>Avg semantic raw: {data.summary.avg_semantic_raw}</span>
                        <span>Avg semantic support: {data.summary.avg_semantic_support_raw}</span>
                        <span>Avg lexical term raw: {data.summary.avg_lexical_term_raw}</span>
                        <span>
                          Candidate embeddings ready:{' '}
                          {Math.round((data.summary.embedding_candidate_ready_rate ?? 0) * 100)}%
                        </span>
                        <span>
                          Low-similarity rate:{' '}
                          {Math.round((data.summary.low_similarity_rate ?? 0) * 100)}%
                        </span>
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <Badge variant="outline">
                          semantic {Math.round((data.summary.semantic_only_rate ?? 0) * 100)}%
                        </Badge>
                        <Badge variant="outline">
                          lexical {Math.round((data.summary.lexical_only_rate ?? 0) * 100)}%
                        </Badge>
                        <Badge variant="outline">
                          hybrid {Math.round((data.summary.hybrid_text_rate ?? 0) * 100)}%
                        </Badge>
                        <Badge variant="outline">
                          dims {data.summary.dominant_embedding_dimension || 'n/a'} /{' '}
                          {data.summary.embedding_dimension_variants} variants
                        </Badge>
                      </div>
                    </div>
                  </div>
                </>
              )}
            </SectionGate>
          </CardContent>
        </Card>

      </div>

      {/* ── Authors + Alerts ── */}
      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <SectionHeader
            icon={UserRound}
            accent="text-success-600"
            title="Authors Monitoring"
            description="Coverage and health of tracked researchers versus provenance-only authors."
          />
          <CardContent className="space-y-4">
            <SectionGate section={authors} skeletonHeight={260}>
              {(data) => (
                <>
                  <div className="grid grid-cols-2 gap-3">
                    <MetricTile label="Tracked authors" value={data.summary.tracked_authors ?? 0} />
                    <MetricTile
                      label="Provenance-only"
                      value={data.summary.provenance_only_authors ?? 0}
                    />
                    <MetricTile
                      label="Ready monitors"
                      value={data.summary.ready_tracked ?? 0}
                      tone="success"
                    />
                    <MetricTile
                      label="Bridge gaps"
                      value={data.summary.bridge_gap_count ?? 0}
                      tone={(data.summary.bridge_gap_count ?? 0) > 0 ? 'warning' : 'neutral'}
                    />
                    <MetricTile
                      label="Background corpus papers"
                      value={data.summary.background_corpus_papers ?? 0}
                    />
                  </div>
                  {/* Degraded-author detail moved to Health → Status. */}
                  {(data.suggestions ?? []).length > 0 && (
                    <div className="space-y-2">
                      <p className="text-sm font-medium text-slate-700">Suggested expansion</p>
                      <div className="flex flex-wrap gap-2">
                        {data.suggestions.slice(0, 4).map((suggestion) => (
                          <Badge key={suggestion.key} variant="outline">
                            {suggestion.name} · {suggestion.suggestion_type}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </SectionGate>
            <div className="flex justify-end">
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  navigateTo('authors', { followed: true })
                }}
              >
                Authors
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={Radio}
            accent="text-warning-600"
            title="Alert Quality"
            description="Delivery reliability and usefulness of recent alert runs."
          />
          <CardContent className="space-y-4">
            <SectionGate section={alerts} skeletonHeight={260}>
              {(data) => (
                <>
                  <div className="grid grid-cols-2 gap-3">
                    <MetricTile label="Enabled alerts" value={data.summary.enabled_alerts ?? 0} />
                    <MetricTile
                      label="Active in 30d"
                      value={data.summary.active_alerts_30d ?? 0}
                    />
                    <MetricTile
                      label="Sent runs"
                      value={data.summary.sent_runs_30d ?? 0}
                      tone="success"
                    />
                    <MetricTile
                      label="Avg papers / sent"
                      value={data.summary.avg_papers_per_sent ?? 0}
                      tone="warning"
                    />
                  </div>
                  {data.long_horizon?.summary ? (
                    <div className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div>
                          <p className="font-medium text-alma-800">90-day usefulness baseline</p>
                          <p className="text-xs text-slate-500">
                            Longer-horizon alert usefulness helps separate transient noise from durable delivery quality.
                          </p>
                        </div>
                        <StatusBadge
                          tone={scoreStatusTone(
                            alertUsefulnessTone(data.long_horizon.summary.usefulness_score ?? 0),
                          )}
                        >
                          {data.long_horizon.summary.usefulness_score}/100
                        </StatusBadge>
                      </div>
                      <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-3">
                        <span>90d sent: {data.long_horizon.summary.sent_runs}</span>
                        <span>90d failed: {data.long_horizon.summary.failed_runs}</span>
                        <span>
                          Delta vs 30d:{' '}
                          {data.long_horizon.summary.delta_vs_30d >= 0 ? '+' : ''}
                          {data.long_horizon.summary.delta_vs_30d.toFixed(0)}
                        </span>
                      </div>
                    </div>
                  ) : null}
                  {(data.top_alerts ?? []).length > 0 ? (
                    <div className="space-y-3">
                      {data.top_alerts.map((alert) => (
                        <div
                          key={`${alert.alert_id ?? alert.alert_name}`}
                          className="rounded-sm border border-[var(--color-border)] p-3"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <p className="font-medium text-alma-800">{alert.alert_name}</p>
                              <p className="mt-1 text-xs text-slate-500">
                                {alert.sent_runs} sent · {alert.empty_runs} empty · {alert.failed_runs} failed
                              </p>
                            </div>
                            <StatusBadge
                              tone={scoreStatusTone(alertUsefulnessTone(alert.usefulness_score))}
                            >
                              {alert.usefulness_score}/100
                            </StatusBadge>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-slate-400">No alert-quality data available yet.</p>
                  )}
                </>
              )}
            </SectionGate>
            <div className="flex justify-end">
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  navigateTo('alerts')
                }}
              >
                Alerts
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* ── Feedback Learning ── (Operational Health moved to Health → Status) */}
      <div className="grid gap-6">
        <Card>
          <SectionHeader
            icon={Sparkles}
            accent="text-accent"
            title="Feedback Learning"
            description="Interaction depth and preference-learning coverage across sources, topics, and authors."
          />
          <CardContent className="space-y-4">
            <SectionGate section={feedback} skeletonHeight={260}>
              {(data) => (
                <>
                  <div className="grid grid-cols-2 gap-3">
                    <MetricTile label="Week interactions" value={data.summary.week_interactions ?? 0} />
                    <MetricTile label="Streak days" value={data.summary.streak_days ?? 0} />
                    <MetricTile label="Positive topics" value={data.summary.topic_coverage ?? 0} />
                    <MetricTile label="Sources touched" value={data.summary.source_diversity_7d ?? 0} />
                    <MetricTile
                      label="Background papers used"
                      value={data.summary.background_corpus_papers ?? 0}
                    />
                    <MetricTile
                      label="Background authors used"
                      value={data.summary.background_corpus_authors ?? 0}
                    />
                  </div>
                  {(data.top_topics ?? []).length > 0 && (
                    <div className="space-y-2">
                      <p className="text-sm font-medium text-slate-700">Top topics</p>
                      <div className="flex flex-wrap gap-2">
                        {data.top_topics.slice(0, 5).map((topic, index) => (
                          <Badge key={`${topic.topic ?? topic.name ?? index}`} variant="outline">
                            {topic.topic ?? topic.name ?? 'Unknown topic'}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                  {(data.top_authors ?? []).length > 0 && (
                    <div className="space-y-2">
                      <p className="text-sm font-medium text-slate-700">Top authors</p>
                      <div className="flex flex-wrap gap-2">
                        {data.top_authors.slice(0, 5).map((author, index) => (
                          <Badge key={`${author.author ?? author.name ?? index}`} variant="outline">
                            {author.author ?? author.name ?? 'Unknown author'}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                  {(data.next_actions ?? []).length > 0 && (
                    <div className="space-y-2">
                      <p className="text-sm font-medium text-slate-700">Next actions</p>
                      <ul className="space-y-1 text-sm text-slate-500">
                        {data.next_actions.map((item) => (
                          <li key={item}>· {item}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </>
              )}
            </SectionGate>
            <div className="flex justify-end">
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  navigateTo('discovery')
                }}
              >
                Discovery
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* ── Feed & Discovery trends ── */}
      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <SectionHeader
            icon={ChartLine}
            accent="text-success-600"
            title="Feed Trend"
            description={`Daily monitor intake over the last ${windowDays} days.`}
          />
          <CardContent>
            <SectionGate section={feed} skeletonHeight={220}>
              {(data) =>
                (data.feed_refresh_trend ?? []).length === 0 ? (
                  <EmptyState title="No Feed refresh trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.feed_refresh_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11 }} allowDecimals={false} />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        tick={{ fontSize: 11 }}
                        allowDecimals={false}
                      />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        yAxisId="left"
                        dataKey="items_created"
                        name="Items created"
                        fill={colors.green}
                        radius={[4, 4, 0, 0]}
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="papers_found"
                        name="Papers found"
                        stroke={colors.blue}
                        strokeWidth={2}
                        dot={false}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={Compass}
            accent="text-accent"
            title="Discovery Refresh Trend"
            description={`Daily recommendation refresh output over the last ${windowDays} days.`}
          />
          <CardContent>
            <SectionGate section={discovery} skeletonHeight={220}>
              {(data) =>
                (data.discovery_refresh_trend ?? []).length === 0 ? (
                  <EmptyState title="No Discovery refresh trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.discovery_refresh_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11 }} allowDecimals={false} />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        tick={{ fontSize: 11 }}
                        allowDecimals={false}
                      />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        yAxisId="left"
                        dataKey="new_recommendations"
                        name="New recs"
                        fill={colors.purple}
                        radius={[4, 4, 0, 0]}
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="total_recommendations"
                        name="Total retained"
                        stroke={colors.indigo}
                        strokeWidth={2}
                        dot={false}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={TrendingUp}
            accent="text-accent"
            title="Discovery Action Trend"
            description="Daily recommendation outcomes across seen, saved, likes, and dismissals."
          />
          <CardContent>
            <SectionGate section={discovery} skeletonHeight={220}>
              {(data) =>
                (data.recommendation_action_trend ?? []).length === 0 ? (
                  <EmptyState title="No recommendation-action trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.recommendation_action_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        dataKey="liked"
                        name="Likes"
                        stackId="actions"
                        fill={colors.green}
                        radius={[4, 4, 0, 0]}
                      />
                      <Bar dataKey="saved" name="Saves" stackId="actions" fill={colors.blue} />
                      <Bar
                        dataKey="dismissed"
                        name="Dismissals"
                        stackId="actions"
                        fill={colors.red}
                      />
                      <Line
                        type="monotone"
                        dataKey="seen"
                        name="Seen"
                        stroke={colors.slate}
                        strokeWidth={2}
                        dot={false}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={Radio}
            accent="text-warning-600"
            title="Alert Delivery Trend"
            description={`Delivery history across sent, empty, skipped, and failed alert runs.${
              (alerts.data?.alert_history_weekly_90d?.length ?? 0) > 0
                ? ' Weekly 90-day baselines are also tracked for longer-horizon evaluation.'
                : ''
            }`}
          />
          <CardContent>
            <SectionGate section={alerts} skeletonHeight={220}>
              {(data) =>
                (data.alert_history_trend ?? []).length === 0 ? (
                  <EmptyState title="No alert-history trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.alert_history_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        dataKey="sent"
                        name="Sent"
                        stackId="alert-history"
                        fill={colors.green}
                        radius={[4, 4, 0, 0]}
                      />
                      <Bar
                        dataKey="empty"
                        name="Empty"
                        stackId="alert-history"
                        fill={colors.slate}
                      />
                      <Bar
                        dataKey="skipped"
                        name="Skipped"
                        stackId="alert-history"
                        fill={colors.amber}
                      />
                      <Bar
                        dataKey="failed"
                        name="Failed"
                        stackId="alert-history"
                        fill={colors.red}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>
      </div>

      {/* ── Author growth + Feedback-learning trends ── */}
      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <SectionHeader
            icon={UserRound}
            accent="text-success-500"
            title="Followed Author Growth"
            description="Daily expansion of the monitored-author corpus."
          />
          <CardContent>
            <SectionGate section={authors} skeletonHeight={220}>
              {(data) =>
                (data.author_follow_trend ?? []).length === 0 ? (
                  <EmptyState title="No followed-author trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.author_follow_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        dataKey="follows"
                        name="Follows"
                        fill={colors.green}
                        radius={[4, 4, 0, 0]}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={Waves}
            accent="text-accent"
            title="Feedback Activity"
            description="Daily learning volume across feed actions, topic tuning, and ratings."
          />
          <CardContent>
            <SectionGate section={feedback} skeletonHeight={220}>
              {(data) =>
                (data.feedback_learning_trend ?? []).length === 0 ? (
                  <EmptyState title="No feedback-learning trend yet" />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <ComposedChart data={data.feedback_learning_trend ?? []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                      <YAxis yAxisId="left" tick={{ fontSize: 11 }} allowDecimals={false} />
                      <YAxis
                        yAxisId="right"
                        orientation="right"
                        tick={{ fontSize: 11 }}
                        allowDecimals={false}
                      />
                      <Tooltip {...tooltipStyle} />
                      <Legend />
                      <Bar
                        yAxisId="left"
                        dataKey="feed_actions"
                        name="Feed actions"
                        stackId="signal"
                        fill={colors.blue}
                        radius={[4, 4, 0, 0]}
                      />
                      <Bar
                        yAxisId="left"
                        dataKey="topic_tunes"
                        name="Topic tunes"
                        stackId="signal"
                        fill={colors.amber}
                      />
                      <Bar
                        yAxisId="left"
                        dataKey="ratings"
                        name="Ratings"
                        stackId="signal"
                        fill={colors.purple}
                      />
                      <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="interactions"
                        name="Total interactions"
                        stroke={colors.green}
                        strokeWidth={2}
                        dot={false}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>
      </div>

      {/* ── Source & Branch Quality ── */}
      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <SectionHeader
            icon={Compass}
            accent="text-accent"
            title="Discovery Source Quality"
            description="Recommendation outcomes by source family. High-dismiss groups are candidates for tuning."
          />
          <CardContent>
            <SectionGate section={discovery} skeletonHeight={260}>
              {(data) =>
                (data.source_quality ?? []).length === 0 ? (
                  <EmptyState title="No discovery source quality data available" />
                ) : (
                  <div className="space-y-3">
                    {data.source_quality.map((source) => (
                      <div
                        key={`${source.source_type}-${source.source_api}`}
                        className="rounded-sm border border-[var(--color-border)] p-3"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <p className="font-medium text-alma-800">{source.source_type}</p>
                            <p className="text-xs text-slate-500">{source.source_api}</p>
                          </div>
                          <Badge variant="secondary">{source.count} recs</Badge>
                        </div>
                        <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-2">
                          <span>Avg score: {source.avg_score.toFixed(2)}</span>
                          <span>Engagement: {(source.engagement_rate * 100).toFixed(0)}%</span>
                          <span>Liked: {source.liked}</span>
                          <span>Dismissed: {source.dismissed}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )
              }
            </SectionGate>
          </CardContent>
        </Card>

        <Card>
          <SectionHeader
            icon={GitBranch}
            accent="text-accent"
            title="Branch Quality"
            description="Branch outcomes, source mix, and tuning guidance derived from recommendation behavior."
          />
          <CardContent>
            <SectionGate section={discovery} skeletonHeight={260}>
              {(data) => (
                <>
                  {data.cold_start_topic_validation && data.cold_start_topic_validation.total_runs > 0 ? (
                    <div className="mb-4 rounded-sm border border-[var(--color-border)] bg-surface-2 p-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div>
                          <p className="font-medium text-alma-800">Topic cold-start validation</p>
                          <p className="text-xs text-slate-500">
                            Whether topic lenses can still retrieve externally when local seeds are sparse.
                          </p>
                        </div>
                        <Badge variant="outline">
                          {data.cold_start_topic_validation.validated_runs}/{data.cold_start_topic_validation.total_runs} validated
                        </Badge>
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {Object.entries(data.cold_start_topic_validation.state_counts ?? {}).map(([state, count]) => (
                          <Badge key={state} variant="outline" className="capitalize">
                            {state.replace(/_/g, ' ')} · {count}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {(data.branch_quality ?? []).length === 0 ? (
                    <EmptyState title="No branch quality data available yet" />
                  ) : (
                    <div className="space-y-3">
                      {data.branch_quality.map((branch) => (
                        <div
                          key={branch.branch_id ?? branch.branch_label}
                          className="rounded-sm border border-[var(--color-border)] p-3"
                        >
                          <div className="flex flex-wrap items-start justify-between gap-3">
                            <div>
                              <p className="font-medium text-alma-800">{branch.branch_label}</p>
                              <p className="text-xs text-slate-500">
                                {branch.branch_id ?? 'no branch id'}
                              </p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge variant="secondary">{branch.count} recs</Badge>
                              <StatusBadge
                                tone={scoreStatusTone(branchQualityTone(branch.quality_state))}
                              >
                                {branch.quality_state}
                              </StatusBadge>
                            </div>
                          </div>
                          <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-2">
                            <span>Avg score: {branch.avg_score.toFixed(2)}</span>
                            <span>Engagement: {(branch.engagement_rate * 100).toFixed(0)}%</span>
                            <span>Positive: {(branch.positive_rate * 100).toFixed(0)}%</span>
                            <span>Dismissed: {(branch.dismiss_rate * 100).toFixed(0)}%</span>
                            <span>Recent share: {(branch.recent_share * 100).toFixed(0)}%</span>
                            <span>
                              Mode: {branch.dominant_mode} ({branch.core_count} core /{' '}
                              {branch.explore_count} explore)
                            </span>
                            <span>Sources: {branch.unique_sources}</span>
                            <span>
                              Saved: {branch.saved} · Liked: {branch.liked}
                            </span>
                          </div>
                          {branch.source_mix.length > 0 && (
                            <div className="mt-3 flex flex-wrap gap-2">
                              {branch.source_mix.map((source) => (
                                <Badge
                                  key={`${
                                    branchSourceBadges.find((b) => b.key === (branch.branch_id ?? branch.branch_label))?.key
                                  }-${source.source_type}`}
                                  variant="outline"
                                  className="text-[11px]"
                                >
                                  {source.source_type} · {source.count}
                                </Badge>
                              ))}
                            </div>
                          )}
                          <div className="mt-3 rounded-md border border-[var(--color-border)] bg-surface-2 px-2.5 py-2 text-xs text-slate-600">
                            {branch.tuning_hint}
                          </div>
                          {/* I-27: branch TUNING (pin/boost/mute/reset) is a
                              mutation and lives on its owning Discovery surface
                              (Branch Studio, which already pins/boosts/mutes per
                              lens). Read-only Insights links there instead of
                              mutating from analytics. */}
                          <div className="mt-3 flex flex-wrap justify-end gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => {
                                navigateTo('discovery')
                              }}
                            >
                              Tune in Discovery
                            </Button>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}
            </SectionGate>
          </CardContent>
        </Card>
      </div>

      {/* ── Branch Trends ── */}
      <Card>
        <SectionHeader
          icon={TrendingUp}
          accent="text-accent"
          title="Branch Trends"
          description="Recent 14-day branch movement. Compare current-week positive rate against the prior week before tuning."
        />
        <CardContent>
          <SectionGate section={discovery} skeletonHeight={260}>
            {(data) =>
              (data.branch_trends ?? []).length === 0 ? (
                <EmptyState title="No branch trend data available yet" />
              ) : (
                <div className="grid gap-4 xl:grid-cols-2">
                  {data.branch_trends.map((branch) => (
                    <div
                      key={`trend-${branch.branch_id ?? branch.branch_label}`}
                      className="rounded-sm border border-[var(--color-border)] p-4"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="font-medium text-alma-800">{branch.branch_label}</p>
                          <p className="text-xs text-slate-500">
                            7d positive {(branch.recent_7d_positive_rate * 100).toFixed(0)}% vs{' '}
                            {(branch.prior_7d_positive_rate * 100).toFixed(0)}%
                          </p>
                        </div>
                        <StatusBadge
                          tone={scoreStatusTone(branchDeltaTone(branch.delta_positive_rate))}
                        >
                          {branch.delta_positive_rate >= 0 ? '+' : ''}
                          {(branch.delta_positive_rate * 100).toFixed(0)} pts
                        </StatusBadge>
                      </div>
                      <div className="mt-3 grid gap-2 text-xs text-slate-500 sm:grid-cols-2">
                        <span>Recent 7d volume: {branch.recent_7d_total}</span>
                        <span>Prior 7d volume: {branch.prior_7d_total}</span>
                      </div>
                      <div className="mt-3 h-44">
                        <ResponsiveContainer width="100%" height="100%">
                          <ComposedChart data={branch.daily}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                            <XAxis dataKey="date" tick={{ fontSize: 11 }} interval="preserveStartEnd" minTickGap={30} />
                            <YAxis yAxisId="left" tick={{ fontSize: 11 }} allowDecimals={false} />
                            <YAxis
                              yAxisId="right"
                              orientation="right"
                              tick={{ fontSize: 11 }}
                              domain={[0, 1]}
                            />
                            <Tooltip {...tooltipStyle} />
                            <Bar
                              yAxisId="left"
                              dataKey="total"
                              fill={colors.blue}
                              radius={[4, 4, 0, 0]}
                            />
                            <Line
                              yAxisId="right"
                              type="monotone"
                              dataKey="positive_rate"
                              stroke={colors.green}
                              strokeWidth={2}
                              dot={false}
                            />
                          </ComposedChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  ))}
                </div>
              )
            }
          </SectionGate>
        </CardContent>
      </Card>

      {/* ── Recent Refreshes ── */}
      <Card>
        <SectionHeader
          icon={Clock3}
          accent="text-accent"
          title="Recent Refreshes"
          description="Latest Feed and Discovery refresh outcomes with direct actions into the owning pages."
        />
        <CardContent>
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="font-medium text-alma-800">Feed</p>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    navigateTo('feed')
                  }}
                >
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
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      navigateTo('discovery')
                    }}
                  >
                    Open Discovery
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      navigateTo('settings')
                    }}
                  >
                    Settings
                  </Button>
                </div>
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
