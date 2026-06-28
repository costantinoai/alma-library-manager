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
  Cell,
  ComposedChart,
  Legend,
  Line,
  Pie,
  PieChart,
  ResponsiveContainer,
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
  PAPERS_AVG_CIT_SERIES,
  SeriesToggleGroup,
  useSeriesVisibility,
} from '@/components/insights/ChartSeriesToggle'
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
  pieColors: string[]
  tooltipStyle: TooltipStyle
}

function EmptyChart({ message }: { message: string }) {
  return (
    <div className="flex h-[250px] items-center justify-center">
      <EmptyState title={message} />
    </div>
  )
}

export function InsightsOverviewTab({
  data,
  aiStatus,
  colors,
  pieColors,
  tooltipStyle,
}: InsightsOverviewTabProps) {
  const {
    summary,
    publications_by_year,
    countries,
    top_institutions,
    top_topics,
    top_journals,
    recommendations,
    embeddings,
    library,
  } = data

  // I-18: charts that overlay a count + a per-paper average (Publications
  // Timeline, Top Journals) let the reader view volume and impact independently
  // via the SHARED toggle primitive (keeps at least one series on).
  const timeline = useSeriesVisibility(['papers', 'avg_citations'])
  const journals = useSeriesVisibility(['papers', 'avg_citations'])
  // I-19: paper-list drilldown opened from a chart bar or summary tile (null = closed).
  const [drilldown, setDrilldown] = useState<DrilldownTarget | null>(null)

  // One helper so every bar drills through the SAME shared route/dialog. recharts
  // passes the datum on the bar `onClick`; we read the un-truncated `drillValue`
  // (or the raw field) so the filter matches the real Library value.
  const openDrilldown = (filterType: InsightsDrilldownFilter, value: unknown, label: string) => {
    const v = value == null ? '' : String(value)
    if (v) setDrilldown({ filterType, filterValue: v, scope: 'library', title: label })
  }

  const topJournalsData = useMemo(
    () => top_journals.map((j) => ({ ...j, journal: truncate(j.journal, 30), drillValue: j.journal })),
    [top_journals],
  )
  const embeddingModels = aiStatus?.embeddings?.models ?? []

  const visibleJournalMax = useMemo(() => {
    const values: number[] = []
    if (journals.visible.papers) values.push(...top_journals.map((j) => Number(j.count) || 0))
    if (journals.visible.avg_citations)
      values.push(...top_journals.map((j) => Number(j.avg_citations) || 0))
    if (values.length === 0) return 1
    return Math.max(...values, 1)
  }, [top_journals, journals.visible])

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
              specs={PAPERS_AVG_CIT_SERIES}
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
                {timeline.visible.avg_citations && (
                  <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                )}
                <Tooltip {...tooltipStyle} />
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
                {timeline.visible.avg_citations && (
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="avg_citations"
                    name="Avg Citations"
                    stroke={colors.amber}
                    strokeWidth={2}
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

        <Card>
          <SectionHeader icon={Tag} accent="text-alma-folio" title="Top Topics" />
          <CardContent>
            {top_topics.length === 0 ? (
              <EmptyChart message="No topic data available" />
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(250, top_topics.length * 28)}>
                <BarChart
                  data={top_topics.map((t) => ({ ...t, term: truncate(t.term, 25), drillValue: t.term }))}
                  layout="vertical"
                  margin={{ left: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <YAxis dataKey="term" type="category" width={140} tick={{ fontSize: 11, fill: '#152642' }} stroke="#D9CBAF" />
                  {/* Topic names are long and the axis truncates them; the tooltip
                      restores the FULL name on hover (mirrors the Institutions chart). */}
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value: number) => [value, 'Papers']}
                    labelFormatter={(label: string) => {
                      const t = top_topics.find((x) => truncate(x.term, 25) === label)
                      return t ? t.term : label
                    }}
                  />
                  <Bar
                    dataKey="count"
                    name="Papers"
                    fill={colors.cyan}
                    radius={[0, 2, 2, 0]}
                    cursor="pointer"
                    onClick={(d: { drillValue?: string }) =>
                      openDrilldown('topic', d?.drillValue, `Papers in topic: ${d?.drillValue}`)
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
          <ActionCardHeader
            icon={Newspaper}
            accent="text-alma-700"
            title="Top Journals"
            action={
              <SeriesToggleGroup
                specs={PAPERS_AVG_CIT_SERIES}
                visible={journals.visible}
                onToggle={journals.toggle}
              />
            }
          />
          <CardContent>
            {top_journals.length === 0 ? (
              <EmptyChart message="No journal data available" />
            ) : (
              <ResponsiveContainer width="100%" height={Math.max(250, top_journals.length * 32)}>
                <BarChart data={topJournalsData} layout="vertical" margin={{ left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis
                    type="number"
                    tick={{ fontSize: 12, fill: '#152642' }}
                    stroke="#D9CBAF"
                    domain={[0, Math.ceil(visibleJournalMax * 1.1)]}
                  />
                  <YAxis dataKey="journal" type="category" width={150} tick={{ fontSize: 11, fill: '#152642' }} stroke="#D9CBAF" />
                  <Tooltip {...tooltipStyle} />
                  <Legend />
                  <Bar
                    dataKey="count"
                    name="Papers"
                    fill={colors.blue}
                    radius={[0, 2, 2, 0]}
                    hide={!journals.visible.papers}
                    cursor="pointer"
                    onClick={(d: { drillValue?: string }) =>
                      openDrilldown('journal', d?.drillValue, `Papers in journal: ${d?.drillValue}`)
                    }
                  />
                  <Bar
                    dataKey="avg_citations"
                    name="Avg Citations"
                    fill={colors.amber}
                    radius={[0, 2, 2, 0]}
                    hide={!journals.visible.avg_citations}
                  />
                </BarChart>
              </ResponsiveContainer>
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
                    label: truncate(i.institution_name, 25),
                    drillValue: i.institution_name,
                  }))}
                  layout="vertical"
                  margin={{ left: 10 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#152642' }} stroke="#D9CBAF" />
                  <YAxis dataKey="label" type="category" width={150} tick={{ fontSize: 11, fill: '#152642' }} stroke="#D9CBAF" />
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value: number) => [value, 'Publications']}
                    labelFormatter={(label: string) => {
                      const inst = top_institutions.find(
                        (i) => truncate(i.institution_name, 25) === label,
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
                    <ResponsiveContainer width="100%" height={220}>
                      <PieChart>
                        <Pie
                          data={(recommendations.by_lens ?? []).map(
                            (s: { lens_id: string; count: number }) => ({
                              name: s.lens_id === 'unknown' ? 'Global' : s.lens_id,
                              value: s.count,
                            }),
                          )}
                          cx="50%"
                          cy="50%"
                          innerRadius={50}
                          outerRadius={80}
                          paddingAngle={3}
                          dataKey="value"
                        >
                          {(recommendations.by_lens ?? []).map((_: unknown, i: number) => (
                            <Cell key={i} fill={pieColors[i % pieColors.length]} />
                          ))}
                        </Pie>
                        <Tooltip {...tooltipStyle} />
                      </PieChart>
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
                        (
                          s: { lens_id: string; count: number; avg_score?: number },
                          i: number,
                        ) => (
                          <div key={s.lens_id} className="flex min-w-0 items-center gap-2 text-xs">
                            <span
                              className="inline-block h-2.5 w-2.5 shrink-0 rounded-full ring-1 ring-[var(--color-border)]"
                              style={{ backgroundColor: pieColors[i % pieColors.length] }}
                            />
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
