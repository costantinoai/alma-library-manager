/**
 * SystemDiagnostics — the 8 subsystem scorecards (feed / discovery / ai /
 * authors / alerts / feedback / operational / evaluation), now homed on the
 * Health page. Previously the Insights "Diagnostics" tab; folded into Health
 * so all system/operational state lives in one place and Insights stays about
 * the library's content (Stats / Graph / Reports).
 *
 * Self-contained: it owns the per-section queries, the branch-action mutation,
 * and the saved-drilldown persistence, then renders the existing presentational
 * InsightsDiagnosticsTab unchanged.
 */
import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  InsightsDiagnosticsTab,
  type InsightsDiagnosticsSections,
  type SectionState,
} from '@/components/insights/InsightsDiagnosticsTab'
import { type SavedDrilldown } from '@/components/insights/InsightsRecommendedActionsCard'
import { COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'
import {
  applyInsightsBranchAction,
  getDiagnosticsSection,
  type DiagnosticsAiSection,
  type DiagnosticsAlertsSection,
  type DiagnosticsAuthorsSection,
  type DiagnosticsDiscoverySection,
  type DiagnosticsEvaluationSection,
  type DiagnosticsFeedSection,
  type DiagnosticsFeedbackSection,
  type DiagnosticsOperationalSection,
} from '@/api/client'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

const SAVED_DRILLDOWNS_KEY = 'alma.insights.savedDrilldowns'

function loadSavedDrilldowns(): SavedDrilldown[] {
  if (typeof window === 'undefined') return []
  try {
    const raw = window.localStorage.getItem(SAVED_DRILLDOWNS_KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function toSectionState<T extends { stale?: boolean }>(query: {
  data?: T
  isLoading: boolean
  isError: boolean
}): SectionState<T> {
  return {
    data: query.data,
    loading: query.isLoading,
    error: query.isError,
    stale: query.data?.stale ?? false,
  }
}

export function SystemDiagnostics() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [savedDrilldowns, setSavedDrilldowns] = useState<SavedDrilldown[]>(() =>
    loadSavedDrilldowns(),
  )

  useEffect(() => {
    try {
      window.localStorage.setItem(
        SAVED_DRILLDOWNS_KEY,
        JSON.stringify(savedDrilldowns.slice(0, 8)),
      )
    } catch {
      // Ignore localStorage failures.
    }
  }, [savedDrilldowns])

  // One materialised view per card; they stream in independently. Always
  // enabled — System Diagnostics is a permanent section of the Health page.
  const diagFeedQuery = useQuery({
    queryKey: ['insights-diag', 'feed'],
    queryFn: () => getDiagnosticsSection('feed'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagDiscoveryQuery = useQuery({
    queryKey: ['insights-diag', 'discovery'],
    queryFn: () => getDiagnosticsSection('discovery'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagAiQuery = useQuery({
    queryKey: ['insights-diag', 'ai'],
    queryFn: () => getDiagnosticsSection('ai'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagAuthorsQuery = useQuery({
    queryKey: ['insights-diag', 'authors'],
    queryFn: () => getDiagnosticsSection('authors'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagAlertsQuery = useQuery({
    queryKey: ['insights-diag', 'alerts'],
    queryFn: () => getDiagnosticsSection('alerts'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagFeedbackQuery = useQuery({
    queryKey: ['insights-diag', 'feedback'],
    queryFn: () => getDiagnosticsSection('feedback'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagOperationalQuery = useQuery({
    queryKey: ['insights-diag', 'operational'],
    queryFn: () => getDiagnosticsSection('operational'),
    staleTime: 60_000,
    retry: 1,
  })
  const diagEvaluationQuery = useQuery({
    queryKey: ['insights-diag', 'evaluation'],
    queryFn: () => getDiagnosticsSection('evaluation'),
    staleTime: 60_000,
    retry: 1,
  })

  const sections: InsightsDiagnosticsSections = {
    feed: toSectionState<DiagnosticsFeedSection>(diagFeedQuery),
    discovery: toSectionState<DiagnosticsDiscoverySection>(diagDiscoveryQuery),
    ai: toSectionState<DiagnosticsAiSection>(diagAiQuery),
    authors: toSectionState<DiagnosticsAuthorsSection>(diagAuthorsQuery),
    alerts: toSectionState<DiagnosticsAlertsSection>(diagAlertsQuery),
    feedback: toSectionState<DiagnosticsFeedbackSection>(diagFeedbackQuery),
    operational: toSectionState<DiagnosticsOperationalSection>(diagOperationalQuery),
    evaluation: toSectionState<DiagnosticsEvaluationSection>(diagEvaluationQuery),
  }

  const branchActionMutation = useMutation({
    mutationFn: ({
      branchId,
      action,
    }: {
      branchId: string
      action: 'pin' | 'boost' | 'mute' | 'reset' | 'cool'
    }) => applyInsightsBranchAction({ branch_id: branchId, action }),
    onSuccess: async (_result, variables) => {
      await invalidateQueries(
        queryClient,
        ['insights-diag', 'discovery'],
        ['insights-diag', 'evaluation'],
        ['insights-diagnostics'],
        ['lenses'],
        ['lens-branches'],
      )
      toast({
        title: 'Branch controls updated',
        description: `Applied '${variables.action}' to the branch across matching lenses.`,
      })
    },
    onError: () => errorToast('Could not update branch controls'),
  })

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
    <InsightsDiagnosticsTab
      sections={sections}
      savedDrilldowns={savedDrilldowns}
      onSaveDrilldown={saveDrilldown}
      onRemoveSavedDrilldown={removeSavedDrilldown}
      onBranchAction={(variables) => branchActionMutation.mutate(variables)}
      branchActionPending={branchActionMutation.isPending}
      branchActionVariables={branchActionMutation.variables}
      colors={COLORS}
      tooltipStyle={TOOLTIP_STYLE}
    />
  )
}
