import type { InsightsDiagnostics } from '@/api/client'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, scoreStatusTone } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'

export type SavedDrilldown = {
  id: string
  title: string
  page: string
  params?: Record<string, string>
}

type InsightsRecommendedActionsCardProps = {
  actions: InsightsDiagnostics['evaluation']['recommended_actions']
  savedDrilldowns: SavedDrilldown[]
  onSaveDrilldown: (item: SavedDrilldown) => void
  onRemoveSavedDrilldown: (id: string) => void
}

export function InsightsRecommendedActionsCard({
  actions,
  savedDrilldowns,
  onSaveDrilldown,
  onRemoveSavedDrilldown,
}: InsightsRecommendedActionsCardProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Recommended Actions</CardTitle>
        <p className="text-sm text-slate-500">
          Highest-value corrections and integration moves based on current diagnostics.
        </p>
      </CardHeader>
      <CardContent className="space-y-3">
        {actions.length === 0 ? (
          <p className="text-sm text-slate-400">No recommended actions right now.</p>
        ) : (
          actions.map((action) => (
            <div key={action.id} className="rounded-sm border border-[var(--color-border)] p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="font-medium text-alma-800">{action.title}</p>
                  <p className="mt-1 text-sm text-slate-500">{action.detail}</p>
                </div>
                <StatusBadge tone={scoreStatusTone(action.priority === 'high' ? 'critical' : action.priority === 'medium' ? 'attention' : 'good')}>
                  {action.priority}
                </StatusBadge>
              </div>
              <div className="mt-3 flex justify-end gap-2">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => onSaveDrilldown({
                    id: action.id,
                    title: action.title,
                    page: action.page,
                    params: action.params,
                  })}
                >
                  Save
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    navigateTo(
                      action.page as Parameters<typeof buildHashRoute>[0],
                      action.params ?? {},
                    )
                  }}
                >
                  Open
                </Button>
              </div>
            </div>
          ))
        )}
        <div className="border-t border-slate-200 pt-3">
          <p className="text-sm font-medium text-slate-700">Saved drill-downs</p>
          <div className="mt-3 space-y-2">
            {savedDrilldowns.length === 0 ? (
              <p className="text-sm text-slate-400">Save useful routes here to revisit them quickly.</p>
            ) : (
              savedDrilldowns.map((item) => (
                <div key={item.id} className="flex items-center justify-between gap-2 rounded-sm border border-[var(--color-border)] p-2">
                  <span className="truncate text-sm text-slate-700">{item.title}</span>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => {
                        navigateTo(
                          item.page as Parameters<typeof buildHashRoute>[0],
                          item.params ?? {},
                        )
                      }}
                    >
                      Open
                    </Button>
                    <Button size="sm" variant="ghost" onClick={() => onRemoveSavedDrilldown(item.id)}>
                      Remove
                    </Button>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
