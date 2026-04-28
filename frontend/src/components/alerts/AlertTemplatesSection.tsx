import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, BellRing, FolderOpen, GitBranch, Loader2, Rss, UserRound, Workflow } from 'lucide-react'

import { api, getAlertTemplates, type Alert, type AlertAutomationTemplate, type AlertRule } from '@/api/client'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

function templateIcon(category: string) {
  if (category === 'author') return <UserRound className="h-4 w-4 text-indigo-600" />
  if (category === 'collection') return <FolderOpen className="h-4 w-4 text-violet-600" />
  if (category === 'feed_monitor') return <Rss className="h-4 w-4 text-emerald-600" />
  if (category === 'branch') return <GitBranch className="h-4 w-4 text-sky-600" />
  return <Workflow className="h-4 w-4 text-amber-600" />
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
    mutationFn: async (template: AlertAutomationTemplate) => {
      const rule = await api.post<AlertRule>('/alerts/rules', template.rule)
      const alert = await api.post<Alert>('/alerts/', {
        ...template.alert,
        rule_ids: [rule.id],
      })
      return { template, rule, alert }
    },
    onSuccess: async ({ template, alert }) => {
      await invalidateQueries(queryClient, ['alert-rules'], ['alerts'], ['alert-templates'])
      toast({
        title: 'Automation created',
        description: `${template.title} is now active as ${alert.name}.`,
      })
    },
    onError: (error) => {
      errorToast('Could not create automation')
    },
  })

  const templates = templatesQuery.data ?? []

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <BellRing className="h-5 w-5 text-amber-600" />
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
          <div className="flex items-center gap-2 text-sm text-amber-700">
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
                  <p className="mt-3 rounded-md bg-parchment-50 px-3 py-2 text-xs text-slate-600">{template.rationale}</p>
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
                    {createFromTemplateMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
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
