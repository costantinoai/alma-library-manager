import { useAsyncRefresh } from '@/hooks/useAsyncRefresh'
import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { InsightsOverviewTab } from '@/components/insights/InsightsOverviewTab'
import { InsightsReportsTab } from '@/components/insights/InsightsReportsTab'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { LoadingState } from '@/components/ui/LoadingState'
import { Button } from '@/components/ui/button'
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
      window.location.hash = buildHashRoute('discovery')
      return
    }
    setSection((SECTIONS as readonly string[]).includes(routeSection) ? routeSection : 'overview')
  }, [routeSection])

  const { data, isLoading, isError } = useQuery({
    queryKey: ['insights'],
    enabled: section === 'overview',
    queryFn: () => api.get<InsightsData | null>('/insights'),
    staleTime: 60_000,
    retry: 1,
  })
  const { building, buildError, refresh } = useAsyncRefresh({
    queryKey: ['insights'],
    enabled: section === 'overview',
    request: force => api.post<{ job_id: string | null }>(`/insights/refresh?force=${force}`),
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

  const statsError = buildError || (isError ? 'Failed to load analytics data.' : null)
  const showStatsSkeleton = !data && !statsError && (isLoading || building)

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
          {section === 'overview' && (
            <Button variant="outline" size="sm" disabled={building} onClick={refresh}>
              {building ? 'Refreshing…' : 'Refresh analytics'}
            </Button>
          )}
        </div>
        <TabsContent value="overview" className="mt-4 space-y-6">
          {statsError && (
            <ErrorState message={statsError} actionLabel="Retry" onAction={refresh} actionPending={building} />
          )}
          {building && data && <p role="status" className="text-sm text-slate-500">Refreshing analytics. Showing the previous snapshot.</p>}
          {showStatsSkeleton ? (
            <LoadingState message="Building analytics…" />
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
