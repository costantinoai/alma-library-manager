import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { InsightsOverviewTab } from '@/components/insights/InsightsOverviewTab'
import { InsightsReportsTab } from '@/components/insights/InsightsReportsTab'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { LoadingState } from '@/components/ui/LoadingState'
import { ErrorState } from '@/components/ui/ErrorState'
import {
  api,
  type InsightsData,
  type AIStatus,
  getWeeklyBrief,
  getTopicDrift,
  getSignalImpact,
} from '@/api/client'
import { COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'
import { buildHashRoute, useHashRoute } from '@/lib/hashRoute'

// Library › Analytics — the "understand your data" surface, absorbed from the
// retired Insights page (task 47 Phase 4, decision 47-C). Sections: Overview
// (corpus stats) + Reports. The Map section moved to the top-level Map page
// (task 50 M3, 50-A) — a `?section=map` deep link redirects there so old
// bookmarks keep working. Driven by the `?section=` param so `#/insights?…`
// redirects land on the right section.
const SECTIONS = ['overview', 'reports'] as const

export function AnalyticsTab() {
  const route = useHashRoute()
  const routeSection = route.params.get('section')?.trim() ?? 'overview'
  const [section, setSection] = useState<string>(
    (SECTIONS as readonly string[]).includes(routeSection) ? routeSection : 'overview',
  )
  const [activeReport, setActiveReport] = useState<string | null>(null)

  useEffect(() => {
    // Task 50 M3: the Map section left this tab — send its deep links to the
    // top-level Map page instead of silently landing on Overview.
    if (routeSection === 'map') {
      window.location.hash = buildHashRoute('map')
      return
    }
    setSection((SECTIONS as readonly string[]).includes(routeSection) ? routeSection : 'overview')
  }, [routeSection])

  const { data, isLoading, isError } = useQuery({
    queryKey: ['insights'],
    queryFn: () => api.get<InsightsData>('/insights'),
    staleTime: 60_000,
    retry: 1,
  })
  const { data: aiStatus } = useQuery({
    queryKey: ['ai-status'],
    queryFn: () => api.get<AIStatus>('/ai/status'),
    staleTime: 30_000,
  })

  const { data: weeklyBrief, isLoading: weeklyLoading } = useQuery({
    queryKey: ['report-weekly'],
    queryFn: getWeeklyBrief,
    staleTime: 120_000,
    enabled: activeReport === 'weekly',
  })
  const { data: topicDriftData, isLoading: driftLoading } = useQuery({
    queryKey: ['report-drift'],
    queryFn: getTopicDrift,
    staleTime: 120_000,
    enabled: activeReport === 'drift',
  })
  const { data: signalImpactData, isLoading: impactLoading } = useQuery({
    queryKey: ['report-impact'],
    queryFn: getSignalImpact,
    staleTime: 120_000,
    enabled: activeReport === 'impact',
  })

  const showStatsSkeleton = isLoading && !data
  const showStatsError = isError && !data
  const isRefreshing = Boolean(data?.stale || data?.rebuilding)

  return (
    <div className="space-y-6">
      <Tabs
        value={section}
        onValueChange={(value) => {
          setSection(value)
          window.location.hash = buildHashRoute('library', { tab: 'analytics', section: value })
        }}
        className="w-full"
      >
        <div className="flex items-center justify-between gap-4">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="reports">Reports</TabsTrigger>
          </TabsList>
          {isRefreshing ? (
            <span
              className="inline-flex items-center gap-1.5 rounded-full border border-control-edge bg-control-quiet px-2.5 py-1 text-xs text-alma-700"
              title="Analytics are being recomputed in the background. This view is from the previous snapshot."
            >
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-alma-folio" aria-hidden />
              Refreshing…
            </span>
          ) : null}
        </div>
        <TabsContent value="overview" className="mt-4 space-y-6">
          {showStatsSkeleton ? (
            <LoadingState message="Loading analytics..." />
          ) : showStatsError ? (
            <ErrorState message="Failed to load analytics data." />
          ) : data ? (
            <InsightsOverviewTab data={data} aiStatus={aiStatus} colors={COLORS} tooltipStyle={TOOLTIP_STYLE} />
          ) : null}
        </TabsContent>
        <TabsContent value="reports" className="mt-4">
          <InsightsReportsTab
            weeklyBrief={weeklyBrief}
            weeklyLoading={weeklyLoading}
            topicDriftData={topicDriftData}
            driftLoading={driftLoading}
            signalImpactData={signalImpactData}
            impactLoading={impactLoading}
            onGenerate={(report) => setActiveReport(report)}
            colors={COLORS}
            tooltipStyle={TOOLTIP_STYLE}
          />
        </TabsContent>
      </Tabs>
    </div>
  )
}
