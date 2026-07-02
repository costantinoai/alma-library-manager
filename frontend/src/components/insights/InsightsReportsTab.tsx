import { useMemo } from 'react'
import {
  ArrowUpDown,
  FileText,
  FolderOpen,
  Loader2,
  TrendingUp,
} from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { ColumnDef } from '@tanstack/react-table'

import type {
  CollectionIntelligenceData,
  SignalImpactData,
  TopicDriftData,
  WeeklyBriefData,
} from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { DataTable } from '@/components/ui/data-table'
import { ActionCardHeader, MetricTile } from '@/components/shared'
import { truncate } from '@/lib/utils'
import { formatPercent } from '@/lib/format'

interface Palette {
  slate: string
  green: string
  red: string
}

interface TooltipStyle {
  contentStyle: React.CSSProperties
}

interface InsightsReportsTabProps {
  weeklyBrief?: WeeklyBriefData
  weeklyLoading: boolean
  collectionIntel?: CollectionIntelligenceData
  collectionLoading: boolean
  topicDriftData?: TopicDriftData
  driftLoading: boolean
  signalImpactData?: SignalImpactData
  impactLoading: boolean
  onGenerate: (report: 'weekly' | 'collections' | 'drift' | 'impact') => void
  colors: Palette
  tooltipStyle: TooltipStyle
}

type CollectionRow = CollectionIntelligenceData['collections'][number]
type SignalRow = SignalImpactData['signals'][number]

export function InsightsReportsTab({
  weeklyBrief,
  weeklyLoading,
  collectionIntel,
  collectionLoading,
  topicDriftData,
  driftLoading,
  signalImpactData,
  impactLoading,
  onGenerate,
  colors,
  tooltipStyle,
}: InsightsReportsTabProps) {
  const collectionColumns = useMemo<ColumnDef<CollectionRow>[]>(() => [
    {
      id: 'name',
      accessorKey: 'name',
      header: 'Collection',
      size: 220,
      // Flex row with colour dot + name — handle truncation via `min-w-0`
      // on the name span so the dot stays visible.
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <div className="flex min-w-0 items-center gap-2">
          <span
            className="inline-block h-2.5 w-2.5 shrink-0 rounded-full"
            style={{ backgroundColor: row.original.color || colors.slate }}
          />
          <span className="min-w-0 flex-1 truncate font-medium text-alma-800" title={row.original.name}>
            {row.original.name}
          </span>
        </div>
      ),
    },
    {
      id: 'paper_count',
      accessorKey: 'paper_count',
      header: 'Papers',
      size: 90,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => <span className="block text-right tabular-nums text-slate-700">{row.original.paper_count}</span>,
    },
    {
      id: 'avg_citations',
      accessorKey: 'avg_citations',
      header: 'Avg Cit.',
      size: 100,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => <span className="block text-right tabular-nums text-slate-700">{row.original.avg_citations.toFixed(1)}</span>,
    },
    {
      id: 'avg_rating',
      accessorKey: 'avg_rating',
      header: 'Avg Rating',
      size: 110,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span className="block text-right tabular-nums text-slate-700">
          {row.original.avg_rating > 0 ? row.original.avg_rating.toFixed(1) : '—'}
        </span>
      ),
    },
    {
      id: 'year_range',
      header: 'Years',
      size: 110,
      enableSorting: false,
      cell: ({ row }) => {
        const { min, max } = row.original.year_range
        return (
          <span className="text-xs text-slate-500">
            {min && max ? `${min}–${max}` : '—'}
          </span>
        )
      },
    },
    {
      // I-29: a real diversity figure (normalized topic evenness 0..1) plus the
      // raw distinct-topic count — replaces the old len(top5) that maxed at 5.
      id: 'topic_diversity',
      accessorKey: 'topic_diversity',
      header: 'Diversity',
      size: 110,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span
          className="block text-right tabular-nums text-slate-700"
          title={`Topic evenness ${formatPercent(row.original.topic_diversity, 0)} across ${row.original.distinct_topics} distinct topics`}
        >
          {row.original.distinct_topics > 1 ? formatPercent(row.original.topic_diversity, 0) : '—'}
          <span className="ml-1 text-xs text-slate-400">/ {row.original.distinct_topics}</span>
        </span>
      ),
    },
    {
      id: 'top_topics',
      header: 'Top Topics',
      size: 240,
      enableSorting: false,
      meta: { cellOverflow: 'wrap' },
      cell: ({ row }) => (
        <div className="flex flex-wrap gap-1">
          {row.original.top_topics.slice(0, 3).map((t) => (
            <Badge key={t.topic} variant="secondary" className="text-xs" title={t.topic}>
              {truncate(t.topic, 20)}
            </Badge>
          ))}
        </div>
      ),
    },
  ], [colors])

  const signalColumns = useMemo<ColumnDef<SignalRow>[]>(() => [
    {
      id: 'signal',
      accessorKey: 'signal',
      header: 'Signal',
      size: 180,
      cell: ({ row }) => <span className="font-medium text-alma-800" title={row.original.signal}>{row.original.signal}</span>,
    },
    {
      id: 'liked_avg',
      accessorKey: 'liked_avg',
      header: 'Liked Avg',
      size: 110,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => <span className="block text-right tabular-nums text-success-700">{row.original.liked_avg.toFixed(3)}</span>,
    },
    {
      id: 'dismissed_avg',
      accessorKey: 'dismissed_avg',
      header: 'Dismissed Avg',
      size: 130,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => <span className="block text-right tabular-nums text-critical-700">{row.original.dismissed_avg.toFixed(3)}</span>,
    },
    {
      id: 'delta',
      accessorKey: 'delta',
      header: 'Delta',
      size: 100,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span
          className="block text-right tabular-nums font-medium"
          style={{
            color:
              row.original.delta > 0
                ? colors.green
                : row.original.delta < 0
                  ? colors.red
                  : colors.slate,
          }}
        >
          {row.original.delta > 0 ? '+' : ''}
          {row.original.delta.toFixed(3)}
        </span>
      ),
    },
    {
      // I-31: sample sizes behind the comparison, so a delta is read with its N.
      id: 'evidence',
      header: 'n (liked/dism.)',
      size: 120,
      enableSorting: false,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span className="block text-right tabular-nums text-slate-500">
          {row.original.liked_n} / {row.original.dismissed_n}
        </span>
      ),
    },
    {
      // I-31: 95% CI on the difference of means — the honest uncertainty band.
      id: 'ci',
      header: '95% CI (Δ)',
      size: 140,
      enableSorting: false,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <span className="block text-right tabular-nums text-xs text-slate-500">
          [{row.original.ci_low.toFixed(3)}, {row.original.ci_high.toFixed(3)}]
        </span>
      ),
    },
    {
      // I-31: only positive/negative when the cohort is powered AND the CI
      // excludes zero (direction !== inconclusive); otherwise "inconclusive".
      id: 'impact',
      accessorKey: 'impact',
      header: 'Association',
      size: 130,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => (
        <StatusBadge
          tone={
            row.original.impact === 'positive'
              ? 'positive'
              : row.original.impact === 'negative'
                ? 'negative'
                : 'neutral'
          }
        >
          {row.original.impact === 'neutral' ? 'inconclusive' : row.original.impact}
        </StatusBadge>
      ),
    },
  ], [colors])

  return (
    <div className="space-y-6">
      {/* ── Weekly Brief ── */}
      <Card>
        <ActionCardHeader
          icon={FileText}
          accent="text-accent"
          title="Weekly Brief"
          description="Summary of your library activity over the past week"
          action={
            <Button
              size="sm"
              variant="outline"
              disabled={weeklyLoading}
              onClick={() => onGenerate('weekly')}
            >
              {weeklyLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Generate'}
            </Button>
          }
        />
        {weeklyBrief && (
          <CardContent className="space-y-4">
            <div className="mb-2 text-xs text-slate-400">
              {weeklyBrief.period.from} &mdash; {weeklyBrief.period.to}
            </div>
            <div className="grid grid-cols-3 gap-3">
              <MetricTile label="New Papers" value={weeklyBrief.new_papers} />
              <MetricTile label="Total Library" value={weeklyBrief.total_library} />
              <MetricTile label="Rated This Week" value={weeklyBrief.rated_this_week} />
            </div>
            {weeklyBrief.trending_topics.length > 0 && (
              <div>
                <h4 className="mb-2 text-sm font-semibold text-slate-700">Trending Topics</h4>
                <div className="flex flex-wrap gap-2">
                  {weeklyBrief.trending_topics.map((t) => (
                    <Badge key={t.topic} variant="secondary">
                      {t.topic} ({t.papers})
                    </Badge>
                  ))}
                </div>
              </div>
            )}
            {weeklyBrief.active_authors.length > 0 && (
              <div>
                <h4 className="mb-2 text-sm font-semibold text-slate-700">Active Authors</h4>
                <div className="flex flex-wrap gap-2">
                  {weeklyBrief.active_authors.map((a) => (
                    <Badge key={a.name} variant="outline">
                      {a.name} ({a.new_papers} new)
                    </Badge>
                  ))}
                </div>
              </div>
            )}
            <div className="rounded-sm border border-[var(--color-border)] p-3">
              <h4 className="mb-2 text-sm font-semibold text-slate-700">Recommendation Engagement</h4>
              <div className="flex gap-4 text-sm">
                <span className="text-slate-500">
                  Total: <span className="font-medium text-alma-800">{weeklyBrief.recommendations.total}</span>
                </span>
                <span className="text-slate-500">
                  Liked: <span className="font-medium text-success-700 tabular-nums">{weeklyBrief.recommendations.liked}</span>
                </span>
                <span className="text-slate-500">
                  Dismissed: <span className="font-medium text-critical-700 tabular-nums">{weeklyBrief.recommendations.dismissed}</span>
                </span>
              </div>
            </div>
          </CardContent>
        )}
      </Card>

      {/* ── Collection Intelligence ── */}
      <Card>
        <ActionCardHeader
          icon={FolderOpen}
          accent="text-accent"
          title="Collection Intelligence"
          description="Detailed analytics for each of your collections"
          action={
            <Button
              size="sm"
              variant="outline"
              disabled={collectionLoading}
              onClick={() => onGenerate('collections')}
            >
              {collectionLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Generate'}
            </Button>
          }
        />
        {collectionIntel && (
          <CardContent>
            {collectionIntel.collections.length === 0 ? (
              <p className="text-sm text-slate-400">No collections found.</p>
            ) : (
              <DataTable<CollectionRow>
                data={collectionIntel.collections}
                columns={collectionColumns}
                storageKey="insights.collection-intelligence"
                getRowId={(row) => row.id}
              />
            )}
          </CardContent>
        )}
      </Card>

      {/* ── Topic Drift ── */}
      <Card>
        <ActionCardHeader
          icon={TrendingUp}
          accent="text-accent"
          title="Topic Drift"
          description="How your research interests have shifted over time"
          action={
            <Button
              size="sm"
              variant="outline"
              disabled={driftLoading}
              onClick={() => onGenerate('drift')}
            >
              {driftLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Generate'}
            </Button>
          }
        />
        {topicDriftData && (
          <CardContent className="space-y-4">
            {/* I-30: explicit windows with paper counts; prevalence not raw count
                so a busy window can't dominate by volume; insufficient state. */}
            <div className="grid gap-4 lg:grid-cols-3">
              {topicDriftData.windows.map((w) => (
                <div key={w.label} className="rounded-sm border border-[var(--color-border)] p-3">
                  <div className="mb-1 flex items-center justify-between gap-2">
                    <h4 className="text-sm font-semibold text-slate-700 capitalize">{w.label}</h4>
                    {!w.sufficient && <StatusBadge tone="neutral" size="sm">thin</StatusBadge>}
                  </div>
                  <p className="mb-2 text-xs text-slate-400">
                    {w.from_year}&ndash;{w.to_year} · {w.paper_count} papers
                  </p>
                  <div className="space-y-1">
                    {w.top_topics.map((t) => (
                      <div key={t.topic} className="flex items-center justify-between text-sm">
                        <span className="mr-2 truncate text-slate-600" title={t.topic}>{truncate(t.topic, 25)}</span>
                        <span className="shrink-0 text-xs text-slate-400 tabular-nums">
                          {formatPercent(t.prevalence, 0)}
                        </span>
                      </div>
                    ))}
                    {w.top_topics.length === 0 && (
                      <p className="text-xs text-slate-400">No data</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
            {topicDriftData.insufficient ? (
              <p className="text-sm text-slate-400">
                {topicDriftData.note || 'Not enough data to identify a reliable trend.'}
              </p>
            ) : (
              <div className="flex flex-wrap gap-4">
                {topicDriftData.emerging_topics.length > 0 && (
                  <div>
                    <h4 className="mb-2 text-[11px] font-semibold uppercase tracking-[0.08em] text-success-700">Emerging</h4>
                    <div className="flex flex-wrap gap-1.5">
                      {topicDriftData.emerging_topics.map((t) => (
                        <StatusBadge
                          key={t.topic}
                          tone="positive"
                          title={`${formatPercent(t.early_prevalence, 0)} → ${formatPercent(t.recent_prevalence, 0)}`}
                        >
                          {truncate(t.topic, 28)} +{formatPercent(t.delta, 0)}
                        </StatusBadge>
                      ))}
                    </div>
                  </div>
                )}
                {topicDriftData.fading_topics.length > 0 && (
                  <div>
                    <h4 className="mb-2 text-[11px] font-semibold uppercase tracking-[0.08em] text-critical-700">Fading</h4>
                    <div className="flex flex-wrap gap-1.5">
                      {topicDriftData.fading_topics.map((t) => (
                        <StatusBadge
                          key={t.topic}
                          tone="negative"
                          title={`${formatPercent(t.early_prevalence, 0)} → ${formatPercent(t.recent_prevalence, 0)}`}
                        >
                          {truncate(t.topic, 28)} {formatPercent(t.delta, 0)}
                        </StatusBadge>
                      ))}
                    </div>
                  </div>
                )}
                {topicDriftData.emerging_topics.length === 0 &&
                  topicDriftData.fading_topics.length === 0 && (
                    <p className="text-sm text-slate-400">
                      No topic moved enough between windows to call a trend.
                    </p>
                  )}
              </div>
            )}
          </CardContent>
        )}
      </Card>

      {/* ── Signal Impact ── */}
      <Card>
        <ActionCardHeader
          icon={ArrowUpDown}
          accent="text-warning-500"
          title="Signal Impact"
          description="Which scoring signals differentiate liked from dismissed papers"
          action={
            <Button
              size="sm"
              variant="outline"
              disabled={impactLoading}
              onClick={() => onGenerate('impact')}
            >
              {impactLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Generate'}
            </Button>
          }
        />
        {signalImpactData && (
          <CardContent className="space-y-4">
            <div className="mb-3 flex flex-wrap items-center gap-4 text-sm">
              <span className="text-slate-500">
                Positive cohort: <span className="font-medium text-success-700 tabular-nums">{signalImpactData.liked_count}</span>
              </span>
              <span className="text-slate-500">
                Negative cohort: <span className="font-medium text-critical-700 tabular-nums">{signalImpactData.dismissed_count}</span>
              </span>
              {!signalImpactData.sufficient && (
                <StatusBadge tone="neutral">underpowered cohort</StatusBadge>
              )}
            </div>
            {/* I-31: name it honestly — association, not causation. */}
            <p className="text-xs text-slate-400">{signalImpactData.note}</p>
            {signalImpactData.signals.length === 0 ? (
              <p className="text-sm text-slate-400">Not enough data to compare signals.</p>
            ) : (
              <>
                <DataTable<SignalRow>
                  data={signalImpactData.signals}
                  columns={signalColumns}
                  storageKey="insights.signal-impact"
                  getRowId={(row) => row.signal}
                />
                <ResponsiveContainer width="100%" height={250}>
                  <BarChart data={signalImpactData.signals} margin={{ left: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E9DCBC" />
                    <XAxis dataKey="signal" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip {...tooltipStyle} />
                    <Legend />
                    <Bar dataKey="liked_avg" name="Liked Avg" fill={colors.green} radius={[4, 4, 0, 0]} />
                    <Bar dataKey="dismissed_avg" name="Dismissed Avg" fill={colors.red} radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </>
            )}
          </CardContent>
        )}
      </Card>
    </div>
  )
}
