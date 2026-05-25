/**
 * InsightsActivity — the subsystem scorecards (feed / discovery / ai / authors /
 * alerts / feedback / evaluation): trends, distributions, and quality metrics
 * over time. The analytics half of the old "Diagnostics" tab; lives under
 * **Insights** (Stats / Graph / Activity / Reports). Actionable operational
 * *health* lives on the Health page's Status tab.
 *
 * Reads the shared `useDiagnosticsSections` hook; owns the branch-action
 * mutation + saved-drilldown persistence; renders the presentational
 * InsightsDiagnosticsTab.
 */
import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'

import { InsightsDiagnosticsTab } from '@/components/insights/InsightsDiagnosticsTab'
import { useDiagnosticsSections } from '@/components/insights/useDiagnosticsSections'
import { type SavedDrilldown } from '@/components/insights/InsightsRecommendedActionsCard'
import { COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'
import { applyInsightsBranchAction } from '@/api/client'
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

export function InsightsActivity() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const sections = useDiagnosticsSections()
  const [savedDrilldowns, setSavedDrilldowns] = useState<SavedDrilldown[]>(() =>
    loadSavedDrilldowns(),
  )

  useEffect(() => {
    try {
      window.localStorage.setItem(SAVED_DRILLDOWNS_KEY, JSON.stringify(savedDrilldowns.slice(0, 8)))
    } catch {
      // Ignore localStorage failures.
    }
  }, [savedDrilldowns])

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
    setSavedDrilldowns((prev) => [item, ...prev.filter((entry) => entry.id !== item.id)].slice(0, 8))
    toast({ title: 'Drill-down saved', description: item.title })
  }
  const removeSavedDrilldown = (id: string) =>
    setSavedDrilldowns((prev) => prev.filter((item) => item.id !== id))

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
