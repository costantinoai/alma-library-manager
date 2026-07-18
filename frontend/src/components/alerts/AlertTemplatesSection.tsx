import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, BellRing, FolderOpen, GitBranch, Loader2, Rss, UserRound, Workflow } from 'lucide-react'

import { applyAlertTemplate, getAlertTemplates, type AlertAutomationTemplate } from '@/api/client'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { CATEGORY_ICON_COLORS, CATEGORY_ICON_FALLBACK_COLOR } from '@/lib/palette'

function templateIcon(category: string) {
  // Icon shape is per-category; color comes from the centralized palette (44.5).
  const cls = `h-4 w-4 ${CATEGORY_ICON_COLORS[category] ?? CATEGORY_ICON_FALLBACK_COLOR}`
  if (category === 'author') return <UserRound className={cls} />
  if (category === 'collection') return <FolderOpen className={cls} />
  if (category === 'feed_monitor') return <Rss className={cls} />
  if (category === 'branch') return <GitBranch className={cls} />
  return <Workflow className={cls} />
}

export function AlertTemplatesSection() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const templatesQuery = useQuery({
    queryKey: ['alert-templates'],
    queryFn: getAlertTemplates,
    retry: 1,
    staleTime: 60_000,
  })

  const createFromTemplateMutation = useMutation({
    // Single atomic endpoint: the backend recomputes the template from its
    // key and creates rule + digest in one transaction (no orphan rule when
    // the second insert fails, no client-forged payloads).
    mutationFn: (template: AlertAutomationTemplate) => applyAlertTemplate(template.key),
    onSuccess: async (applied) => {
      await invalidateQueries(queryClient, ['alert-rules'], ['alerts'], ['alert-templates'])
      toast({
        title: 'Automation created',
        description: `${applied.template_title} is now active as ${applied.alert.name}.`,
      })
    },
    onError: () => {
      errorToast('Could not create automation')
    },
  })

  const templates = templatesQuery.data ?? []

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <BellRing className="h-5 w-5 text-warning-600" />
          <div>
            <CardTitle>Suggested Automations</CardTitle>
            <p className="text-sm text-slate-500">
              One-click alerts derived from productive monitors, monitored authors, curated collections, strong branches, and library workflow pressure.
            </p>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {templatesQuery.isLoading ? (
          <div className="flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading alert suggestions...
          </div>
        ) : templatesQuery.isError ? (
          <div className="flex items-center gap-2 text-sm text-warning-700">
            <AlertTriangle className="h-4 w-4" /> Could not load alert suggestions.
          </div>
        ) : templates.length === 0 ? (
          <EmptyState
            icon={BellRing}
            title="No alert suggestions yet"
            description="Use Feed, Discovery, and Library a bit more so ALMa can propose good automation hooks."
          />
        ) : (
          <div className="grid gap-3 xl:grid-cols-2">
            {templates.map((template) => (
              <div key={template.key} className="rounded-sm border border-[var(--color-border)] p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2">
                      {templateIcon(template.category)}
                      <p className="font-medium text-alma-800">{template.title}</p>
                    </div>
                    <p className="text-sm text-slate-500">{template.description}</p>
                  </div>
                  <Badge variant="secondary">{template.category.replace(/_/g, ' ')}</Badge>
                </div>
                {template.rationale && (
                  <p className="mt-3 rounded-md bg-surface-2 px-3 py-2 text-xs text-slate-600">{template.rationale}</p>
                )}
                <div className="mt-3 flex flex-wrap gap-2">
                  {Object.entries(template.metrics).map(([key, value]) => (
                    <Badge key={`${template.key}-${key}`} variant="outline" className="text-[11px]">
                      {key.replace(/_/g, ' ')}: {String(value)}
                    </Badge>
                  ))}
                </div>
                <div className="mt-4 flex items-center justify-between gap-3">
                  <div className="text-xs text-slate-500">
                    {template.alert.schedule} via {template.alert.channels.join(', ') || 'no channels'}
                  </div>
                  <Button
                    size="sm"
                    onClick={() => createFromTemplateMutation.mutate(template)}
                    disabled={createFromTemplateMutation.isPending}
                  >
                    {createFromTemplateMutation.isPending &&
                    createFromTemplateMutation.variables?.key === template.key ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : null}
                    Create Automation
                  </Button>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
