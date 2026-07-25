import { useMemo, useState } from 'react'
import {
  BarChart3,
  BookOpen,
  Building2,
  Database,
  FolderOpen,
  Globe,
  Heart,
  Library,
  Newspaper,
  Quote,
  Sparkles,
  Tag,
  UserPlus,
  Users,
} from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { AIStatus, InsightsData, InsightsDrilldownFilter } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/ui/status-badge'
import { Card, CardContent } from '@/components/ui/card'
import { EmptyState } from '@/components/ui/empty-state'
import { Progress } from '@/components/ui/progress'
import { ActionCardHeader, MetricTile, SectionHeader } from '@/components/shared'
import {
  InsightsPaperDrilldown,
  type DrilldownTarget,
} from '@/components/insights/InsightsPaperDrilldown'
import {
  SeriesToggleGroup,
} from '@/components/insights/ChartSeriesToggle'
import {
  TIMELINE_SERIES,
  useSeriesVisibility,
} from '@/components/insights/chartSeries'
import { VenueHoverCard } from '@/components/shared/VenueHoverCard'
import { usePaperVenueFollow } from '@/hooks/usePaperVenueFollow'
import { formatNumber, truncate } from '@/lib/utils'

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

interface InsightsOverviewTabProps {
  data: InsightsData
  aiStatus?: AIStatus
  colors: Palette
  tooltipStyle: TooltipStyle
}

function EmptyChart({ message }: { message: string }) {
  return (
    <div className="flex h-[250px] items-center justify-center">
      <EmptyState title={message} />
    </div>
  )
}

// Single-line y-axis tick for horizontal bar charts. Recharts' default tick
// wraps category labels on spaces inside the axis width — long topic /
// journal / institution names became two clipped lines of mush. A raw
// <text> node never wraps; label length is controlled via truncate() and
// the full name is restored by the tooltip.
function SingleLineTick({ x, y, payload }: { x?: number; y?: number; payload?: { value?: string } }) {
  return (
    <text x={x} y={y} dy={4} textAnchor="end" fontSize={11} fill="#152642">
      {payload?.value ?? ''}
    </text>
  )
}

export function InsightsOverviewTab({
  data,
  aiStatus,
  colors,
  tooltipStyle,
}: InsightsOverviewTabProps) {
  const {
    summary,
    publications_by_year,
    countries,
    top_institutions,
    cluster_topics,
    top_journals,
    recommendations,
    embeddings,
    library,
  } = data

  // I-18: charts that overlay a count + a per-paper average (Publications
  // Timeline, Top Journals) let the reader view volume and impact independently
  // via the SHARED toggle primitive (keeps at least one series on).
  // Median is the default impact line; the mean ships but starts off, so the
  // chart opens on the typical year rather than the skewed one.
  const timeline = useSeriesVisibility(['papers', 'median_citations', 'avg_citations'], {
    papers: true,
    median_citations: true,
    avg_citations: false,
  })
  // I-19: paper-list drilldown opened from a chart bar or summary tile (null = closed).
  const [drilldown, setDrilldown] = useState<DrilldownTarget | null>(null)

  // One helper so every bar drills through the SAME shared route/dialog. recharts
  // passes the datum on the bar `onClick`; we read the un-truncated `drillValue`
  // (or the raw field) so the filter matches the real Library value.
  const openDrilldown = (filterType: InsightsDrilldownFilter, value: unknown, label: string) => {
    const v = value == null ? '' : String(value)
    if (v) setDrilldown({ filterType, filterValue: v, scope: 'library', title: label })
  }

  // Cluster vocabulary is the Overview's topic source (47-E). `top_topics`
  // (the OpenAlex taxonomy) stays in the payload for paper drilldown rows.
  const clusterTopics = cluster_topics ?? []
  const embeddingModels = aiStatus?.embeddings?.models ?? []

  // Widest bar in the journals list — the row bars are proportional to the
  // top journal, not to an absolute scale.
  const journalMax = useMemo(
    () => Math.max(1, ...top_journals.map((j) => Number(j.count) || 0)),
    [top_journals],
  )
  // Journals are followable straight from this table (Phase 2.4): the same
  // hook + monitors cache the paper cards use, so follow-state can't disagree.
  const venueFollow = usePaperVenueFollow()

  return (
    <div className="space-y-6">
      {/* I-20: every figure on this tab is scoped to the saved Library and is an
          all-time aggregate. State it once so no number is read as corpus-wide. */}
      <p className="text-xs text-slate-400">
        All figures cover your <span className="font-medium text-slate-500">saved Library</span> (all-time).
        Click any summary tile to list the library, or a chart bar to see the papers behind it.
      </p>

      {/* ── Summary Cards ── */}
      {/* Every summary number is computed over the same saved-Library population,
          so each tile drills through to that library via the shared `all`
          drilldown (closes the I-19 per-tile remainder). The Publications tile
          leads with the outlier-robust MEDIAN citations, mean second (I-18). */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
        <MetricTile
          label="Publications"
          value={summary.total_publications}
          icon={BookOpen}
          iconColor={colors.blue}
          hint={`median ${summary.median_citations_per_paper} · mean ${summary.avg_citations_per_paper} cit/paper`}
          onClick={() => openDrilldown('all', 'all', 'All library papers')}
        />
        <MetricTile
          label="Citations"
          value={summary.total_citations}
          icon={Quote}
          iconColor={colors.amber}
          onClick={() => openDrilldown('all', 'all', 'Library papers by citations')}
        />
        <MetricTile
          label="Authors"
          value={summary.total_authors}
          icon={Users}
          iconColor={colors.green}
          hint={`avg ${summary.avg_papers_per_author} papers/author`}
          onClick={() => openDrilldown('all', 'all', 'All library papers')}
        />
        <MetricTile
          label="Countries"
          value={summary.total_countries}
          icon={Globe}
          iconColor={colors.purple}
          onClick={() => openDrilldown('all', 'all', 'All library papers')}
        />
        <MetricTile
          label="Topics"
          value={summary.total_topics}
          icon={Tag}
          iconColor={colors.cyan}
          onClick={() => openDrilldown('all', 'all', 'All library papers')}
        />
      </div>

      {/* ── Publications Timeline ── */}
      {/* I-18: papers (volume) and avg-citations (impact) are incompatible units;
          the shared toggle lets each be read on its own axis instead of forcing
          a dual-axis read. Avg-citations is mechanically lower for recent years,
          so it's NOT the default emphasis — both start on, reader can isolate. */}
      <Card>
        <ActionCardHeader
          icon={BarChart3}
          accent="text-alma-700"
          title="Publications Timeline"
          action={
            <SeriesToggleGroup
              specs={TIMELINE_SERIES}
              visible={timeline.visible}
              onToggle={timeline.toggle}
            />
          }
        />
        <CardContent>
          {publications_by_year.length === 0 ? (
            <EmptyChart message="No publication year data available" />
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={publications_by_year}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                <XAxis dataKey="year" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                <YAxis yAxisId="left" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                {(timeline.visible.avg_citations || timeline.visible.median_citations) && (
                  <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                )}
                <Tooltip {...tooltipStyle} content={<TimelineTooltip />} />
                <Legend />
                {timeline.visible.papers && (
                  <Bar
                    yAxisId="left"
                    dataKey="count"
                    name="Papers"
                    fill={colors.blue}
                    radius={[2, 2, 0, 0]}
                    cursor="pointer"
                    onClick={(d: { year?: number | string }) =>
                      openDrilldown('year', d?.year, `Papers from ${d?.year}`)
                    }
                  />
                )}
                {/* Seminal markers: papers in the library-wide top citation
                    decile, drawn as dots ABOVE the bar so a year's standout
                    work is visible without reading the tooltip. Click drills
                    into that year (sorted by citations, so they lead). */}
                {timeline.visible.papers && (
                  <Scatter
                    yAxisId="left"
                    dataKey="count"
                    name="Top-decile papers"
                    fill={colors.amber}
                    shape={<SeminalMarker />}
                    cursor="pointer"
                    onClick={(d: { year?: number | string }) =>
                      openDrilldown('year', d?.year, `Papers from ${d?.year}`)
                    }
                  />
                )}
                {timeline.visible.median_citations && (
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="median_citations"
                    name="Median Citations"
                    stroke={colors.amber}
                    strokeWidth={2}
                    dot={false}
                  />
                )}
                {timeline.visible.avg_citations && (
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="avg_citations"
                    name="Mean Citations"
                    stroke={colors.slate ?? '#94a3b8'}
                    strokeWidth={1.5}
                    strokeDasharray="4 3"
                    dot={false}
                  />
                )}
              </ComposedChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* ── Geography + Topics ── */}
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <SectionHeader icon={Globe} accent="text-alma-folio" title="Geographic Distribution" />
          <CardContent>
            {countries.length === 0 ? (
              <EmptyChart message="No institution data available" />
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(250, countries.length * 28)}>
                <BarChart data={countries} layout="vertical" margin={{ left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <YAxis dataKey="country_code" type="category" width={40} tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <Tooltip {...tooltipStyle} />
                  <Bar
                    dataKey="count"
                    name="Publications"
                    fill={colors.green}
                    radius={[0, 2, 2, 0]}
                    cursor="pointer"
                    onClick={(d: { country_code?: string }) =>
                      openDrilldown('country', d?.country_code, `Papers from ${d?.country_code}`)
                    }
                  />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* 47-E: "Topics" means YOUR library's own structure — the c-TF-IDF
            labels over its embedding clusters — not OpenAlex's global taxonomy
            applied to your papers. Those two answer different questions, and
            showing the taxonomy under this heading would quietly substitute one
            for the other. When clusters aren't computed yet we SAY so and point
            at the setting that produces them, rather than falling back. */}
        <Card>
          <SectionHeader
            icon={Tag}
            accent="text-alma-folio"
            title="Your topics"
            description="Clusters of your library, labelled by the terms that distinguish them."
          />
          <CardContent>
            {clusterTopics.length === 0 ? (
              <EmptyState
                icon={Sparkles}
                title="Topics appear once embeddings are computed"
                description="These are your library's own clusters, not a generic taxonomy — they need SPECTER2 vectors. Turn embeddings on in Settings → AI."
              />
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(250, clusterTopics.length * 28)}>
                <BarChart
                  data={clusterTopics.map((t) => ({
                    ...t,
                    term: truncate(t.term, 30),
                    drillValue: String(t.cluster_id),
                  }))}
                  layout="vertical"
                  margin={{ left: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <YAxis dataKey="term" type="category" width={200} tick={<SingleLineTick />} stroke="#D9CBAF" />
                  {/* Cluster labels are long and the axis truncates them; the
                      tooltip restores the FULL label on hover. */}
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value: number) => [value, 'Papers']}
                    labelFormatter={(label: string) => {
                      const t = clusterTopics.find((x) => truncate(x.term, 30) === label)
                      return t ? t.term : label
                    }}
                  />
                  <Bar
                    dataKey="count"
                    name="Papers"
                    fill={colors.cyan}
                    radius={[0, 2, 2, 0]}
                    cursor="pointer"
                    onClick={(d: { drillValue?: string; term?: string }) =>
                      openDrilldown('cluster', d?.drillValue, `Papers in: ${d?.term}`)
                    }
                  />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      </div>

      {/* ── Journals + Institutions ── */}
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <SectionHeader
            icon={Newspaper}
            accent="text-alma-700"
            title="Top Journals"
            description="Where your library comes from — and the ones you can follow."
          />
          <CardContent>
            {top_journals.length === 0 ? (
              <EmptyChart message="No journal data available" />
            ) : (
              // A ranked list, not a bar chart: the useful acts here are "see
              // the papers" and "follow this journal", and a chart affords
              // neither. The bar lives INSIDE each row, so volume still reads
              // at a glance without costing a second axis.
              <ul className="space-y-0.5">
                {top_journals.map((j) => {
                  const share = journalMax > 0 ? (Number(j.count) || 0) / journalMax : 0
                  const followed = venueFollow.isVenueFollowed(j.journal)
                  return (
                    <li key={j.journal} className="group relative">
                      <button
                        type="button"
                        onClick={() =>
                          openDrilldown('journal', j.journal, `Papers in journal: ${j.journal}`)
                        }
                        className="flex w-full items-center gap-3 rounded-sm px-2 py-1.5 text-left transition-colors hover:bg-surface-2"
                      >
                        <span className="min-w-0 flex-1">
                          <span className="flex items-baseline justify-between gap-2">
                            <VenueHoverCard
                              journal={j.journal}
                              isFollowed={followed}
                              followPending={venueFollow.pendingVenueName === j.journal}
                              onFollow={venueFollow.followVenue}
                            >
                              <span className="truncate text-sm text-alma-800">{j.journal}</span>
                            </VenueHoverCard>
                            <span className="shrink-0 text-xs tabular-nums text-slate-500">
                              {j.count}
                              <span className="ml-2 text-slate-400">
                                {Number(j.avg_citations).toFixed(0)} avg cit
                              </span>
                            </span>
                          </span>
                          <span className="mt-1 block h-1 w-full overflow-hidden rounded-full bg-surface-2">
                            <span
                              className="block h-1 rounded-full bg-alma-500"
                              style={{ width: `${Math.max(2, share * 100)}%` }}
                            />
                          </span>
                        </span>
                        {followed ? (
                          <StatusBadge tone="positive" size="sm" className="shrink-0">
                            Following
                          </StatusBadge>
                        ) : (
                          // A quiet nudge, only where following would clearly
                          // pay off (3+ saved papers) — never on every row.
                          Number(j.count) >= 3 && (
                            <StatusBadge
                              tone="neutral"
                              size="sm"
                              className="shrink-0 opacity-0 transition-opacity group-hover:opacity-100"
                            >
                              Follow?
                            </StatusBadge>
                          )
                        )}
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <SectionHeader icon={Building2} accent="text-alma-folio" title="Top Institutions" />
          <CardContent>
            {top_institutions.length === 0 ? (
              <EmptyChart message="No institution data available" />
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(250, top_institutions.length * 28)}>
                <BarChart
                  data={top_institutions.map((i) => ({
                    ...i,
                    label: truncate(i.institution_name, 30),
                    drillValue: i.institution_name,
                  }))}
                  layout="vertical"
                  margin={{ left: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <YAxis dataKey="label" type="category" width={200} tick={<SingleLineTick />} stroke="#D9CBAF" />
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value: number) => [value, 'Publications']}
                    labelFormatter={(label: string) => {
                      const inst = top_institutions.find(
                        (i) => truncate(i.institution_name, 30) === label,
                      )
                      return inst ? `${inst.institution_name} (${inst.country_code})` : label
                    }}
                  />
                  <Bar
                    dataKey="count"
                    name="Publications"
                    fill={colors.green}
                    radius={[0, 2, 2, 0]}
                    cursor="pointer"
                    onClick={(d: { drillValue?: string }) =>
                      openDrilldown('institution', d?.drillValue, `Papers from: ${d?.drillValue}`)
                    }
                  />
                </BarChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>
      </div>

      {/* ── Recommendations + Library ── */}
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <SectionHeader icon={Sparkles} accent="text-gold-500" title="Recommendation Insights" />
          <CardContent>
            {recommendations.total === 0 ? (
              <EmptyChart message="No recommendations yet" />
            ) : (
              // Two-column grid (CSS grid with min-w-0 on both columns)
              // replaces the v2 `flex items-start gap-6` +
              // `ResponsiveContainer width="50%"` recipe — the v2 version
              // forced the pie into a fixed half-width and squeezed the
              // stat list to nothing on narrow viewports. CSS grid does
              // the right thing automatically.
              <div className="grid gap-6 sm:grid-cols-2">
                {(recommendations.by_lens ?? []).length > 0 && (
                  <div className="min-w-0">
                    {/* Sorted horizontal bars, not a pie: lens shares are a
                        ranking, and bars make magnitudes comparable at a
                        glance (same recipe as Top Topics). */}
                    <ResponsiveContainer
                      width="100%"
                      height={Math.max(160, (recommendations.by_lens ?? []).length * 34)}
                    >
                      <BarChart
                        data={[...(recommendations.by_lens ?? [])]
                          .sort(
                            (a: { count: number }, b: { count: number }) => b.count - a.count,
                          )
                          .map((s: { lens_id: string; count: number }) => ({
                            name: s.lens_id === 'unknown' ? 'Global' : s.lens_id,
                            count: s.count,
                          }))}
                        layout="vertical"
                        margin={{ left: 10 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                        <XAxis
                          type="number"
                          tick={{ fontSize: 12, fill: '#152642' }}
                          stroke="#D9CBAF"
                          allowDecimals={false}
                        />
                        <YAxis
                          dataKey="name"
                          type="category"
                          width={110}
                          tick={{ fontSize: 11, fill: '#152642' }}
                          stroke="#D9CBAF"
                        />
                        <Tooltip
                          {...tooltipStyle}
                          formatter={(value: number) => [value, 'Recommendations']}
                        />
                        <Bar dataKey="count" name="Recommendations" fill={colors.purple} radius={[0, 2, 2, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                )}
                <div className="min-w-0 space-y-2 pt-4">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Total</span>
                    <span className="font-medium tabular-nums text-alma-800">{recommendations.total}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Seen</span>
                    <span className="font-medium tabular-nums text-alma-800">{recommendations.seen}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Liked</span>
                    <span className="font-medium tabular-nums text-success-700">{recommendations.liked}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Dismissed</span>
                    <span className="font-medium tabular-nums text-critical-700">{recommendations.dismissed}</span>
                  </div>
                  <div className="border-t border-[var(--color-border)] pt-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-slate-500">Engagement</span>
                      <span className="font-brand font-semibold tabular-nums text-alma-800">
                        {(recommendations.engagement_rate * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                  {(recommendations.by_lens ?? []).length > 0 && (
                    <div className="space-y-1 border-t border-[var(--color-border)] pt-2">
                      {(recommendations.by_lens ?? []).map(
                        (s: { lens_id: string; count: number; avg_score?: number }) => (
                          <div key={s.lens_id} className="flex min-w-0 items-center gap-2 text-xs">
                            <span className="min-w-0 flex-1 truncate text-slate-500">
                              {s.lens_id === 'unknown' ? 'Global' : s.lens_id}
                            </span>
                            <span className="shrink-0 font-medium tabular-nums text-alma-800">{s.count}</span>
                            {s.avg_score != null && (
                              <span className="shrink-0 text-slate-400 tabular-nums">
                                avg {(s.avg_score * 100).toFixed(0)}%
                              </span>
                            )}
                          </div>
                        ),
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <SectionHeader icon={Library} accent="text-alma-800" title="Library & Vectors" />
          <CardContent className="space-y-5">
            <div className="space-y-3">
              <h4 className="font-brand text-sm font-semibold text-alma-800">Library</h4>
              <div className="grid grid-cols-2 gap-3">
                {[
                  { icon: Heart, color: '#1E5B86', label: 'Saved Papers', value: library.total_saved },
                  { icon: FolderOpen, color: '#0F1E36', label: 'Collections', value: library.total_collections },
                  { icon: UserPlus, color: '#C49A45', label: 'Followed Authors', value: library.total_followed_authors },
                  { icon: Tag, color: '#A77E36', label: 'Avg Rating', value: library.avg_rating > 0 ? `${library.avg_rating}/5` : '—' },
                ].map((tile) => (
                  <MetricTile
                    key={tile.label}
                    icon={tile.icon}
                    iconColor={tile.color}
                    label={tile.label}
                    value={tile.value}
                  />
                ))}
              </div>
            </div>

            <div className="space-y-3">
              <h4 className="font-brand text-sm font-semibold text-alma-800">Vector Embeddings</h4>
              <div className="rounded-sm border border-[var(--color-border)] bg-surface-1 p-4 shadow-paper-sm">
                <div className="flex items-center gap-3">
                  <Database className="h-5 w-5 text-alma-folio" />
                  <div className="flex-1">
                    <div className="flex items-baseline justify-between">
                      <p className="font-brand font-semibold text-alma-800 tabular-nums">
                        {formatNumber(embeddings.total_vectors)} vectors
                      </p>
                      <Badge variant="secondary" className="text-xs">
                        {embeddings.coverage_pct}% coverage
                      </Badge>
                    </div>
                    <p className="mt-1 text-xs text-slate-400">Model: {embeddings.model}</p>
                    <Progress
                      value={Math.min(embeddings.coverage_pct, 100)}
                      className="mt-2 h-1.5 [&>div]:bg-alma-folio"
                    />
                    {embeddingModels.length > 0 ? (
                      <div className="mt-3 space-y-1">
                        {embeddingModels.map((row) => (
                          <div
                            key={row.model}
                            className="grid grid-cols-[1fr_auto_auto_auto] items-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-2 px-2 py-1.5 text-xs"
                          >
                            <span className="truncate font-mono text-alma-800">{row.model}</span>
                            <span className="text-slate-500 tabular-nums">{formatNumber(row.vectors)} vectors</span>
                            <span className="text-slate-500 tabular-nums">{row.stale ?? 0} stale</span>
                            <StatusBadge
                              tone={row.active ? 'accent' : 'neutral'}
                              size="sm"
                              className="justify-self-end"
                            >
                              {row.active ? 'active' : `${row.coverage_pct.toFixed(1)}%`}
                            </StatusBadge>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <InsightsPaperDrilldown target={drilldown} onClose={() => setDrilldown(null)} />
    </div>
  )
}

/**
 * A year's top-decile ("seminal") papers, drawn as small dots stacked just
 * above the bar. Renders nothing when the year has none, so the row stays
 * quiet rather than showing an empty slot per year.
 */
function SeminalMarker(props: {
  cx?: number
  cy?: number
  payload?: { seminal_count?: number }
}) {
  const { cx, cy, payload } = props
  const n = Number(payload?.seminal_count ?? 0)
  if (!n || cx == null || cy == null) return null
  const dots = Math.min(n, 3)
  return (
    <g>
      {Array.from({ length: dots }).map((_, i) => (
        <circle key={i} cx={cx} cy={cy - 8 - i * 5} r={2} fill="var(--color-gold-400)" />
      ))}
    </g>
  )
}

/**
 * Timeline tooltip. Beyond the plotted series it NAMES the year's most-cited
 * paper — the fact a reader actually wants when a bar catches their eye — and
 * states how many that year sit in the library-wide top citation decile.
 */
function TimelineTooltip(props: {
  active?: boolean
  label?: string | number
  payload?: Array<{ payload?: Record<string, unknown> }>
}) {
  const { active, label, payload } = props
  if (!active || !payload?.length) return null
  const row = (payload[0]?.payload ?? {}) as {
    count?: number
    median_citations?: number
    avg_citations?: number
    seminal_count?: number
    top_paper_title?: string
    top_paper_citations?: number
  }
  return (
    <div className="max-w-xs rounded-sm border border-[var(--color-border)] bg-surface-3 px-3 py-2 text-xs shadow-paper-md">
      <p className="font-semibold text-alma-800">{label}</p>
      <p className="mt-0.5 text-slate-600">
        {row.count ?? 0} paper{row.count === 1 ? '' : 's'} · median{' '}
        {Number(row.median_citations ?? 0).toFixed(1)} citations
        {typeof row.avg_citations === 'number' ? ` · mean ${row.avg_citations.toFixed(1)}` : ''}
      </p>
      {row.seminal_count ? (
        <p className="mt-0.5 text-gold-700">
          {row.seminal_count} in your top citation decile
        </p>
      ) : null}
      {row.top_paper_title ? (
        <p className="mt-1.5 border-t border-[var(--color-border)] pt-1.5 text-slate-500">
          <span className="text-slate-400">Most cited:</span>{' '}
          <span className="text-alma-800">{row.top_paper_title}</span>
          {row.top_paper_citations ? ` (${row.top_paper_citations})` : ''}
        </p>
      ) : null}
    </div>
  )
}
