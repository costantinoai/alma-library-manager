import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { InsightsActivity } from '@/components/insights/InsightsActivity'
import { InsightsGraphTab } from '@/components/insights/InsightsGraphTab'
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
  getCollectionIntelligence,
  getTopicDrift,
  getSignalImpact,
} from '@/api/client'
import { COLORS, PIE_COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'
import { buildHashRoute, useHashRoute } from '@/lib/hashRoute'

// ── Main Page ──
//
// Insights is scoped to *understanding your data*: Stats (corpus overview),
// Graph (paper map), Activity (subsystem trends + quality over time), Reports.
// Operational *health* — what's degraded / failing — lives on the Health page's
// Status tab; Insights → Activity is the analytics half of the old Diagnostics.

const INSIGHTS_TABS = ['stats', 'graph', 'activity', 'reports'] as const

export function InsightsPage() {
  const route = useHashRoute()
  const routeTab = route.params.get('tab')?.trim() ?? 'stats'
  const [activeTab, setActiveTab] = useState<string>(
    (INSIGHTS_TABS as readonly string[]).includes(routeTab) ? routeTab : 'stats',
  )

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

  const [activeReport, setActiveReport] = useState<string | null>(null)

  useEffect(() => {
    setActiveTab((INSIGHTS_TABS as readonly string[]).includes(routeTab) ? routeTab : 'stats')
  }, [routeTab])

  const { data: weeklyBrief, isLoading: weeklyLoading } = useQuery({
    queryKey: ['report-weekly'],
    queryFn: getWeeklyBrief,
    staleTime: 120_000,
    enabled: activeReport === 'weekly',
  })

  const { data: collectionIntel, isLoading: collectionLoading } = useQuery({
    queryKey: ['report-collections'],
    queryFn: getCollectionIntelligence,
    staleTime: 120_000,
    enabled: activeReport === 'collections',
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

  // The Stats tab depends on `/insights`; it shows its own skeleton on first
  // load. After the first build the MV cache returns instantly; on data
  // changes the response carries `stale=true` while a background rebuild runs,
  // surfaced as a small "Refreshing…" pill rather than a full-page block.
  const showStatsSkeleton = isLoading && !data
  const showStatsError = isError && !data
  const isRefreshing = Boolean(data?.stale || data?.rebuilding)

  return (
    <div className="space-y-6">
      <Tabs
        value={activeTab}
        onValueChange={(value) => {
          setActiveTab(value)
          window.location.hash = buildHashRoute('insights', { tab: value })
        }}
        className="w-full"
      >
        <div className="flex items-center justify-between gap-4">
          <TabsList>
            <TabsTrigger value="stats">Stats</TabsTrigger>
            <TabsTrigger value="graph">Graph</TabsTrigger>
            <TabsTrigger value="activity">Activity</TabsTrigger>
            <TabsTrigger value="reports">Reports</TabsTrigger>
          </TabsList>
          {isRefreshing ? (
            <span
              className="inline-flex items-center gap-1.5 rounded-full border border-alma-200 bg-alma-50 px-2.5 py-1 text-xs text-alma-700"
              title="Insights are being recomputed in the background. The current view is from the previous snapshot."
            >
              <span className="h-1.5 w-1.5 rounded-full bg-alma-folio animate-pulse" aria-hidden />
              Refreshing…
            </span>
          ) : null}
        </div>
        <TabsContent value="stats" className="space-y-6 mt-4">
          {showStatsSkeleton ? (
            <LoadingState message="Loading insights..." />
          ) : showStatsError ? (
            <ErrorState message="Failed to load insights data." />
          ) : data ? (
            <InsightsOverviewTab
              data={data}
              aiStatus={aiStatus}
              colors={COLORS}
              pieColors={PIE_COLORS}
              tooltipStyle={TOOLTIP_STYLE}
            />
          ) : null}
        </TabsContent>
        <TabsContent value="graph" className="mt-4">
          <InsightsGraphTab
            embeddingsReady={!!aiStatus?.capability_tiers?.tier1_embeddings?.ready}
          />
        </TabsContent>
        <TabsContent value="activity" className="mt-4">
          <InsightsActivity />
        </TabsContent>
        <TabsContent value="reports" className="mt-4">
          <InsightsReportsTab
            weeklyBrief={weeklyBrief}
            weeklyLoading={weeklyLoading}
            collectionIntel={collectionIntel}
            collectionLoading={collectionLoading}
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
