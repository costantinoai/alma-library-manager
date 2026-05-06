import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { InsightsDiagnosticsTab } from '@/components/insights/InsightsDiagnosticsTab'
import { InsightsGraphTab } from '@/components/insights/InsightsGraphTab'
import { InsightsOverviewTab } from '@/components/insights/InsightsOverviewTab'
import { InsightsReportsTab } from '@/components/insights/InsightsReportsTab'
import { type SavedDrilldown } from '@/components/insights/InsightsRecommendedActionsCard'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { LoadingState } from '@/components/ui/LoadingState'
import { ErrorState } from '@/components/ui/ErrorState'
import {
  api, type InsightsData, type AIStatus,
  getWeeklyBrief, getCollectionIntelligence, getTopicDrift, getSignalImpact,
  applyInsightsBranchAction, getInsightsDiagnostics, type InsightsDiagnostics,
} from '@/api/client'
import { useToast, errorToast } from '@/hooks/useToast'
import { buildHashRoute, useHashRoute } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'

// ── Colors ──

// Chart palette pinned to the ALMa v2 brand (navy anchor, teal accent,
// pale-blue + parchment supporting tones, gold + status semantic tones).
// Keeps Insights charts tonally consistent with the rest of the product.
// Order chosen so the 8-slot pie/legend stays distinguishable.
const COLORS = {
  blue:   '#0F1E36', // alma-800 (brand navy)
  purple: '#152642', // alma-700 (deeper navy)
  green:  '#1E5B86', // alma-folio (Folio binding blue — replaces v2 teal)
  amber:  '#C49A45', // gold-400 (brand gold)
  cyan:   '#6F98BB', // pale-500 (mid pale-blue)
  pink:   '#C2A86B', // parchment-500 (warm parchment)
  indigo: '#344E7C', // alma-500 (mid navy)
  orange: '#A77E36', // gold-500 (warm trim)
  red:    '#f43f5e', // critical (semantic token)
  slate:  '#64748b',
}

const PIE_COLORS = [COLORS.blue, COLORS.green, COLORS.amber, COLORS.cyan, COLORS.indigo, COLORS.pink, COLORS.purple, COLORS.orange]

const TOOLTIP_STYLE = {
  contentStyle: {
    background: '#0F1E36', // alma-800 (brand navy)
    border: '1px solid #C49A45', // gold trim — editorial card edge
    borderRadius: 2,
    color: '#FFF9F0',      // alma-cream
    fontSize: 13,
    padding: '6px 10px',
  },
  itemStyle: { color: '#FFF9F0' },
  labelStyle: { color: '#C49A45', fontWeight: 600, marginBottom: 4 },
}

// ── Helpers ──

function loadSavedDrilldowns(): SavedDrilldown[] {
  if (typeof window === 'undefined') return []
  try {
    const raw = window.localStorage.getItem('alma.insights.savedDrilldowns')
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

// ── Main Page ──

export function InsightsPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const route = useHashRoute()
  const routeTab = route.params.get('tab')?.trim() ?? 'diagnostics'
  const [savedDrilldowns, setSavedDrilldowns] = useState<SavedDrilldown[]>(() => loadSavedDrilldowns())
  const [activeTab, setActiveTab] = useState<string>(
    ['diagnostics', 'stats', 'graph', 'reports'].includes(routeTab) ? routeTab : 'diagnostics',
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
    const next = ['diagnostics', 'stats', 'graph', 'reports'].includes(routeTab)
      ? routeTab
      : 'diagnostics'
    setActiveTab(next)
  }, [routeTab])

  useEffect(() => {
    try {
      window.localStorage.setItem('alma.insights.savedDrilldowns', JSON.stringify(savedDrilldowns.slice(0, 8)))
    } catch {
      // Ignore localStorage failures.
    }
  }, [savedDrilldowns])

  const { data: diagnostics, isLoading: diagnosticsLoading, isError: diagnosticsError } = useQuery({
    queryKey: ['insights-diagnostics'],
    queryFn: getInsightsDiagnostics,
    staleTime: 60_000,
    retry: 1,
    enabled: activeTab === 'diagnostics',
  })

  const branchActionMutation = useMutation({
    mutationFn: ({ branchId, action }: { branchId: string; action: 'pin' | 'boost' | 'mute' | 'reset' | 'cool' }) =>
      applyInsightsBranchAction({ branch_id: branchId, action }),
    onSuccess: async (_result, variables) => {
      await invalidateQueries(queryClient, ['insights-diagnostics'], ['lenses'], ['lens-branches'])
      toast({
        title: 'Branch controls updated',
        description: `Applied '${variables.action}' to the branch across matching lenses.`,
      })
    },
    onError: (error) => {
      errorToast('Could not update branch controls')
    },
  })

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

  // The page shell (tabs, diagnostics, graph, reports) renders without
  // waiting for `/insights`. Only the Stats tab depends on `data`, and
  // it shows its own skeleton during the very first load. After the
  // first build the materialised-view cache returns instantly on every
  // subsequent visit; on subsequent data changes the response carries
  // `stale=true` while a background rebuild runs, which we surface as a
  // small "Refreshing…" pill rather than as a full-page block.
  const showStatsSkeleton = isLoading && !data
  const showStatsError = isError && !data
  const isRefreshing = Boolean(data?.stale || data?.rebuilding)

  const diagnosticsData: InsightsDiagnostics | null = diagnostics ?? null
  const diagnosticsFeedSummary = diagnosticsData?.feed.summary
  const diagnosticsDiscoverySummary = diagnosticsData?.discovery.summary
  const diagnosticsSourceRequests = (diagnosticsData?.discovery.source_diagnostics ?? []).reduce(
    (sum, source) => sum + (source.requests ?? 0),
    0,
  )
  const diagnosticsBranchTrends = diagnosticsData?.discovery.branch_trends ?? []
  const latestFeedRefresh = diagnosticsData?.feed.recent_refreshes?.[0]
  const latestDiscoveryRefresh = diagnosticsData?.discovery.recent_refreshes?.[0]
  const diagnosticsScorecards = diagnosticsData?.evaluation.scorecards ?? []
  const diagnosticsActions = diagnosticsData?.evaluation.recommended_actions ?? []
  const automationOpportunities = diagnosticsData?.evaluation.automation_opportunities ?? []
  const diagnosticsTrends = diagnosticsData?.trends
  const feedRefreshTrend = diagnosticsTrends?.feed_refresh_daily ?? []
  const discoveryRefreshTrend = diagnosticsTrends?.discovery_refresh_daily ?? []
  const recommendationActionTrend = diagnosticsTrends?.recommendation_actions_daily ?? []
  const alertHistoryTrend = diagnosticsTrends?.alert_history_daily ?? []
  const alertHistoryWeeklyTrend = diagnosticsTrends?.alert_history_weekly_90d ?? []
  const diagnosticsAuthors = diagnosticsData?.authors
  const diagnosticsAlerts = diagnosticsData?.alerts
  const diagnosticsFeedbackLearning = diagnosticsData?.feedback_learning
  const diagnosticsAI = diagnosticsData?.ai
  const diagnosticsOperational = diagnosticsData?.operational
  const coldStartValidation = diagnosticsData?.discovery.cold_start_topic_validation
  const authorFollowTrend = diagnosticsTrends?.author_follows_daily ?? []
  const feedbackLearningTrend = diagnosticsTrends?.feedback_learning_daily ?? []

  const saveDrilldown = (item: SavedDrilldown) => {
    setSavedDrilldowns((prev) => {
      const next = [item, ...prev.filter((entry) => entry.id !== item.id)]
      return next.slice(0, 8)
    })
    toast({ title: 'Drill-down saved', description: item.title })
  }

  const removeSavedDrilldown = (id: string) => {
    setSavedDrilldowns((prev) => prev.filter((item) => item.id !== id))
  }

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
            <TabsTrigger value="diagnostics">Diagnostics</TabsTrigger>
            <TabsTrigger value="stats">Stats</TabsTrigger>
            <TabsTrigger value="graph">Graph</TabsTrigger>
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
        <TabsContent value="diagnostics" className="mt-4 space-y-6">
          <InsightsDiagnosticsTab
            loading={diagnosticsLoading}
            error={diagnosticsError}
            diagnostics={diagnosticsData}
            feedSummary={diagnosticsFeedSummary}
            discoverySummary={diagnosticsDiscoverySummary}
            latestFeedRefresh={latestFeedRefresh}
            latestDiscoveryRefresh={latestDiscoveryRefresh}
            sourceRequestsTotal={diagnosticsSourceRequests}
            scorecards={diagnosticsScorecards}
            recommendedActions={diagnosticsActions}
            automationOpportunities={automationOpportunities}
            ai={diagnosticsAI}
            authors={diagnosticsAuthors}
            alerts={diagnosticsAlerts}
            feedbackLearning={diagnosticsFeedbackLearning}
            operational={diagnosticsOperational}
            coldStartValidation={coldStartValidation}
            branchTrends={diagnosticsBranchTrends}
            trendWindowDays={diagnosticsTrends?.window_days}
            feedRefreshTrend={feedRefreshTrend}
            discoveryRefreshTrend={discoveryRefreshTrend}
            recommendationActionTrend={recommendationActionTrend}
            alertHistoryTrend={alertHistoryTrend}
            alertHistoryWeeklyTrend={alertHistoryWeeklyTrend}
            authorFollowTrend={authorFollowTrend}
            feedbackLearningTrend={feedbackLearningTrend}
            savedDrilldowns={savedDrilldowns}
            onSaveDrilldown={saveDrilldown}
            onRemoveSavedDrilldown={removeSavedDrilldown}
            onBranchAction={(variables) => branchActionMutation.mutate(variables)}
            branchActionPending={branchActionMutation.isPending}
            branchActionVariables={branchActionMutation.variables}
            colors={COLORS}
            tooltipStyle={TOOLTIP_STYLE}
          />
        </TabsContent>
        <TabsContent value="graph" className="mt-4">
          <InsightsGraphTab
            embeddingsReady={!!aiStatus?.capability_tiers?.tier1_embeddings?.ready}
          />
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
