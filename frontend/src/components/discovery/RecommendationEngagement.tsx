/**
 * Recommendation engagement — is Discovery earning its keep?
 *
 * This lived on the Insights/Analytics Overview, which is the wrong shelf: it
 * describes how DISCOVERY is performing, not what the library contains, and
 * a reader asking "are these suggestions any good?" was sent to a different
 * page to find out (task 47 Phase 3/4 — "move + delete together").
 *
 * It reads the shared `['insights']` query, so opening it here costs nothing
 * extra when Analytics has already been viewed, and the numbers can never
 * disagree between the two surfaces.
 */
import { useQuery } from '@tanstack/react-query'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { api, type InsightsData } from '@/api/client'
import { COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'
import { Card, CardContent } from '@/components/ui/card'
import { EmptyState } from '@/components/ui/empty-state'
import { SectionHeader } from '@/components/shared'
import { Skeleton } from '@/components/ui/skeleton'
import { Sparkles } from 'lucide-react'

/** Map an internal lens id to something a reader recognises. */
function lensLabel(lensId: string, names: Map<string, string>): string {
  if (lensId === 'unknown') return 'Global'
  return names.get(lensId) ?? lensId
}

export function RecommendationEngagement({
  lensNames,
}: {
  /** id → display name, so the per-lens rows read as lenses, not as UUIDs. */
  lensNames?: Map<string, string>
}) {
  const { data, isLoading } = useQuery({
    queryKey: ['insights'],
    queryFn: () => api.get<InsightsData>('/insights'),
    staleTime: 60_000,
    retry: 1,
  })

  if (isLoading && !data) return <Skeleton className="h-48 w-full" />
  const recommendations = data?.recommendations
  if (!recommendations) return null

  const names = lensNames ?? new Map<string, string>()
  const byLens = [...(recommendations.by_lens ?? [])].sort((a, b) => b.count - a.count)

  return (
    <Card>
      <SectionHeader
        icon={Sparkles}
        accent="text-gold-500"
        title="Recommendation engagement"
        description="How many suggestions you've seen, kept, and passed on — across every lens."
      />
      <CardContent>
        {recommendations.total === 0 ? (
          <EmptyState title="No recommendations yet" description="Refresh a lens to start." />
        ) : (
          <div className="grid gap-6 sm:grid-cols-2">
            {byLens.length > 0 && (
              <div className="min-w-0">
                {/* Sorted bars, not a pie: lens shares are a ranking, and bars
                    make magnitudes comparable at a glance. */}
                <ResponsiveContainer width="100%" height={Math.max(160, byLens.length * 34)}>
                  <BarChart
                    data={byLens.map((s) => ({
                      name: lensLabel(s.lens_id, names),
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
                      {...TOOLTIP_STYLE}
                      formatter={(value: number) => [value, 'Recommendations']}
                    />
                    <Bar
                      dataKey="count"
                      name="Recommendations"
                      fill={COLORS.purple}
                      radius={[0, 2, 2, 0]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
            <div className="min-w-0 space-y-2 pt-4">
              <div className="flex justify-between text-sm">
                <span className="text-slate-500">Total</span>
                <span className="font-medium tabular-nums text-alma-800">
                  {recommendations.total}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-slate-500">Seen</span>
                <span className="font-medium tabular-nums text-alma-800">
                  {recommendations.seen}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-slate-500">Liked</span>
                <span className="font-medium tabular-nums text-success-700">
                  {recommendations.liked}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-slate-500">Dismissed</span>
                <span className="font-medium tabular-nums text-critical-700">
                  {recommendations.dismissed}
                </span>
              </div>
              <div className="border-t border-[var(--color-border)] pt-2">
                <div className="flex justify-between text-sm">
                  <span className="text-slate-500">Engagement</span>
                  <span className="font-brand font-semibold tabular-nums text-alma-800">
                    {(recommendations.engagement_rate * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
              {byLens.length > 0 && (
                <div className="space-y-1 border-t border-[var(--color-border)] pt-2">
                  {byLens.map((s) => (
                    <div key={s.lens_id} className="flex min-w-0 items-center gap-2 text-xs">
                      <span className="min-w-0 flex-1 truncate text-slate-500">
                        {lensLabel(s.lens_id, names)}
                      </span>
                      <span className="shrink-0 font-medium tabular-nums text-alma-800">
                        {s.count}
                      </span>
                      {s.avg_score != null && (
                        <span className="shrink-0 tabular-nums text-slate-400">
                          avg {(s.avg_score * 100).toFixed(0)}%
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
