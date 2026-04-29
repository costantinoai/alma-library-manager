import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus,
  Trash2,
  Edit3,
  Play,
  Loader2,
  AlertCircle,
  Hash,
  Send,
  Zap,
} from 'lucide-react'
import {
  api,
  evaluateAlert,
  type AlertRule,
  type Alert,
  type AlertEvaluationResult,
} from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { LoadingState } from '@/components/ui/LoadingState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate } from '@/lib/utils'

export function AlertsDeliverySection() {
  const [createOpen, setCreateOpen] = useState(false)
  const [editingAlert, setEditingAlert] = useState<Alert | null>(null)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [evalResultOpen, setEvalResultOpen] = useState(false)
  const [evalResult, setEvalResult] = useState<AlertEvaluationResult | null>(null)

  const [formName, setFormName] = useState('')
  const [formChannels, setFormChannels] = useState<string[]>(['slack'])
  const [formSchedule, setFormSchedule] = useState<string>('manual')
  const [formScheduleTime, setFormScheduleTime] = useState('09:00')
  const [formScheduleDay, setFormScheduleDay] = useState('monday')
  const [formEnabled, setFormEnabled] = useState(true)

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const alertsQuery = useQuery({
    queryKey: ['alerts'],
    queryFn: () => api.get<Alert[]>('/alerts/'),
    retry: 1,
  })

  const rulesQuery = useQuery({
    queryKey: ['alert-rules'],
    queryFn: () => api.get<AlertRule[]>('/alerts/rules'),
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: (body: {
      name: string
      channels: string[]
      schedule: string
      schedule_config?: Record<string, unknown>
      format: string
      enabled: boolean
    }) => api.post<Alert>('/alerts/', body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alerts'])
      setCreateOpen(false)
      resetForm()
      toast({ title: 'Created', description: 'Digest created successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create alert.')
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({
      id,
      body,
    }: {
      id: string
      body: {
        name: string
        channels: string[]
        schedule: string
        schedule_config?: Record<string, unknown>
        format: string
        enabled: boolean
      }
    }) => api.put<Alert>(`/alerts/${id}`, body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alerts'])
      setEditingAlert(null)
      resetForm()
      toast({ title: 'Updated', description: 'Digest updated.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to update alert.')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/alerts/${id}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alerts'])
      setDeleteId(null)
      toast({ title: 'Deleted', description: 'Digest deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete alert.')
    },
  })

  const evaluateMutation = useMutation({
    // `evaluateAlert` posts to the envelope endpoint and waits for the
    // background job (Slack send included) to complete -- so the modal
    // sees the same shape it always saw, and the user can also watch the
    // job progress live in the Activity tab.
    mutationFn: (id: string) => evaluateAlert(id),
    onSuccess: async (data) => {
      setEvalResult(data)
      setEvalResultOpen(true)
      await invalidateQueries(queryClient, ['alerts'], ['alert-history'])
      toast({ title: 'Digest evaluated', description: 'Digest evaluation completed.' })
    },
    onError: (err) => {
      errorToast(
        'Error',
        err instanceof Error ? err.message : 'Failed to evaluate alert.',
      )
    },
  })

  const dryRunMutation = useMutation({
    mutationFn: (id: string) => api.post<AlertEvaluationResult>(`/alerts/${id}/dry-run`),
    onSuccess: (data) => {
      setEvalResult(data)
      setEvalResultOpen(true)
      toast({ title: 'Dry Run Complete', description: 'See results below.' })
    },
    onError: () => {
      errorToast('Error', 'Dry run failed.')
    },
  })

  const assignRuleMutation = useMutation({
    mutationFn: ({ alertId, ruleId }: { alertId: string; ruleId: string }) =>
      api.post(`/alerts/${alertId}/rules`, { rule_ids: [ruleId] }),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alerts'])
      toast({ title: 'Assigned', description: 'Rule assigned to alert.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to assign rule.')
    },
  })

  const unassignRuleMutation = useMutation({
    mutationFn: ({ alertId, ruleId }: { alertId: string; ruleId: string }) =>
      api.delete(`/alerts/${alertId}/rules/${ruleId}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alerts'])
      toast({ title: 'Removed', description: 'Rule unassigned from alert.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to unassign rule.')
    },
  })

  const alerts = alertsQuery.data ?? []
  const allRules = rulesQuery.data ?? []

  function resetForm() {
    setFormName('')
    setFormChannels(['slack'])
    setFormSchedule('manual')
    setFormScheduleTime('09:00')
    setFormScheduleDay('monday')
    setFormEnabled(true)
  }

  function openEdit(alert: Alert) {
    setEditingAlert(alert)
    setFormName(alert.name)
    setFormChannels(alert.channels)
    setFormSchedule(alert.schedule)
    const cfg = alert.schedule_config ?? {}
    setFormScheduleTime(typeof cfg.time === 'string' ? cfg.time : '09:00')
    setFormScheduleDay(typeof cfg.day === 'string' ? cfg.day : 'monday')
    setFormEnabled(alert.enabled)
  }

  function buildScheduleConfig(): Record<string, unknown> | undefined {
    if (formSchedule === 'daily') {
      return { time: formScheduleTime }
    }
    if (formSchedule === 'weekly') {
      return { day: formScheduleDay, time: formScheduleTime }
    }
    return undefined
  }

  function handleCreate() {
    if (!formName.trim()) return
    createMutation.mutate({
      name: formName.trim(),
      channels: formChannels,
      schedule: formSchedule,
      schedule_config: buildScheduleConfig(),
      format: 'text',
      enabled: formEnabled,
    })
  }

  function handleUpdate() {
    if (!editingAlert || !formName.trim()) return
    updateMutation.mutate({
      id: editingAlert.id,
      body: {
        name: formName.trim(),
        channels: formChannels,
        schedule: formSchedule,
        schedule_config: buildScheduleConfig(),
        format: 'text',
        enabled: formEnabled,
      },
    })
  }

  const alertFormContent = (
    <div className="space-y-4 py-4">
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Name</label>
        <Input
          placeholder="e.g., Weekly Digest"
          value={formName}
          onChange={(e) => setFormName(e.target.value)}
        />
      </div>
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Channels</label>
        <label className="flex items-center gap-2">
          <Checkbox
            checked={formChannels.includes('slack')}
            onCheckedChange={(checked) =>
              setFormChannels(
                checked === true
                  ? [...formChannels, 'slack']
                  : formChannels.filter((c) => c !== 'slack'),
              )
            }
          />
          <span className="text-sm text-slate-700">Slack</span>
        </label>
      </div>
      <div className="space-y-2">
        <label className="text-sm font-medium text-slate-700">Schedule</label>
        <Select value={formSchedule} onValueChange={setFormSchedule}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="manual">Manual</SelectItem>
            <SelectItem value="daily">Daily</SelectItem>
            <SelectItem value="weekly">Weekly</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {formSchedule !== 'manual' && (
        <div className="space-y-2">
          {formSchedule === 'weekly' && (
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Day</label>
              <Select value={formScheduleDay} onValueChange={setFormScheduleDay}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="monday">Monday</SelectItem>
                  <SelectItem value="tuesday">Tuesday</SelectItem>
                  <SelectItem value="wednesday">Wednesday</SelectItem>
                  <SelectItem value="thursday">Thursday</SelectItem>
                  <SelectItem value="friday">Friday</SelectItem>
                  <SelectItem value="saturday">Saturday</SelectItem>
                  <SelectItem value="sunday">Sunday</SelectItem>
                </SelectContent>
              </Select>
            </div>
          )}
          <div className="space-y-2">
            <label className="text-sm font-medium text-slate-700">Time (UTC)</label>
            <Input
              type="time"
              value={formScheduleTime}
              onChange={(e) => setFormScheduleTime(e.target.value)}
            />
          </div>
        </div>
      )}
      <label className="flex items-center gap-2">
        <Checkbox
          checked={formEnabled}
          onCheckedChange={(checked) => setFormEnabled(checked === true)}
        />
        <span className="text-sm text-slate-700">Enabled</span>
      </label>
    </div>
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-alma-800">Digests</h2>
        <Button onClick={() => { resetForm(); setCreateOpen(true) }}>
          <Plus className="h-4 w-4" />
          New Digest
        </Button>
      </div>

      {alertsQuery.isLoading ? (
        <LoadingState />
      ) : alertsQuery.isError ? (
        <ErrorState message="Failed to load digests." />
      ) : alerts.length === 0 ? (
        <div className="py-12 text-center">
          <Send className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm text-slate-500">No digests configured yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Create a digest, then assign rules to it.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {alerts.map((alert) => {
            const assignedRuleIds = (alert.rules ?? []).map((r) => r.id)
            const availableRules = allRules.filter((r) => !assignedRuleIds.includes(r.id))
            return (
              <Card key={alert.id} className="transition-shadow hover:shadow-md">
                <CardContent className="p-5">
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <h3 className="font-medium text-alma-800">{alert.name}</h3>
                        {!alert.enabled && (
                          <Badge variant="secondary">Disabled</Badge>
                        )}
                      </div>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <Badge variant="outline">{alert.schedule}</Badge>
                        {typeof alert.schedule_config?.day === 'string' && (
                          <Badge variant="outline">{alert.schedule_config.day}</Badge>
                        )}
                        {typeof alert.schedule_config?.time === 'string' && (
                          <Badge variant="outline">{alert.schedule_config.time} UTC</Badge>
                        )}
                        {alert.channels.map((ch) => (
                          <Badge key={ch} variant="outline">
                            <Hash className="mr-1 h-3 w-3" />
                            {ch}
                          </Badge>
                        ))}
                      </div>
                      {alert.rules && alert.rules.length > 0 && (
                        <div className="mt-2 flex flex-wrap items-center gap-1">
                          <span className="text-xs text-slate-400">Rules:</span>
                          {alert.rules.map((rule) => (
                            <Badge
                              key={rule.id}
                              variant="default"
                              className="cursor-pointer"
                              onClick={() =>
                                unassignRuleMutation.mutate({ alertId: alert.id, ruleId: rule.id })
                              }
                              title="Click to unassign"
                            >
                              {rule.name} x
                            </Badge>
                          ))}
                        </div>
                      )}
                      {availableRules.length > 0 && (
                        <div className="mt-2">
                          <Select
                            value=""
                            onValueChange={(value) => {
                              if (value) {
                                assignRuleMutation.mutate({ alertId: alert.id, ruleId: value })
                              }
                            }}
                          >
                            <SelectTrigger className="h-8 text-xs">
                              <SelectValue placeholder="+ Assign rule..." />
                            </SelectTrigger>
                            <SelectContent>
                              {availableRules.map((r) => (
                                <SelectItem key={r.id} value={r.id}>
                                  {r.name}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                      )}
                      {alert.last_evaluated_at && (
                        <p className="mt-1.5 text-xs text-slate-400">
                          Last evaluated: {formatDate(alert.last_evaluated_at)}
                        </p>
                      )}
                    </div>
                    <div className="flex items-center gap-1">
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => evaluateMutation.mutate(alert.id)}
                        disabled={evaluateMutation.isPending}
                        title="Evaluate Now"
                      >
                        {evaluateMutation.isPending && evaluateMutation.variables === alert.id ? (
                          <Loader2 className="h-4 w-4 animate-spin text-green-500" />
                        ) : (
                          <Play className="h-4 w-4 text-green-500" />
                        )}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => dryRunMutation.mutate(alert.id)}
                        disabled={dryRunMutation.isPending}
                        title="Dry Run"
                      >
                        {dryRunMutation.isPending && dryRunMutation.variables === alert.id ? (
                          <Loader2 className="h-4 w-4 animate-spin text-alma-500" />
                        ) : (
                          <Zap className="h-4 w-4 text-alma-500" />
                        )}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => openEdit(alert)}
                        title="Edit"
                      >
                        <Edit3 className="h-4 w-4 text-slate-500" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => setDeleteId(alert.id)}
                        title="Delete"
                      >
                        <Trash2 className="h-4 w-4 text-red-400" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}

      {/* Create Digest Dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Digest</DialogTitle>
            <DialogDescription>
              Configure a new digest delivery.
            </DialogDescription>
          </DialogHeader>
          {alertFormContent}
          {createMutation.isError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
              <AlertCircle className="h-4 w-4 text-red-500" />
              <span className="text-sm text-red-700">Failed to create alert.</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button
              onClick={handleCreate}
              disabled={!formName.trim() || createMutation.isPending}
            >
              {createMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create Digest
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Digest Dialog */}
      <Dialog open={!!editingAlert} onOpenChange={(open) => { if (!open) { setEditingAlert(null); resetForm() } }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Digest</DialogTitle>
            <DialogDescription>
              Update the digest configuration.
            </DialogDescription>
          </DialogHeader>
          {alertFormContent}
          {updateMutation.isError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
              <AlertCircle className="h-4 w-4 text-red-500" />
              <span className="text-sm text-red-700">Failed to update alert.</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => { setEditingAlert(null); resetForm() }}>Cancel</Button>
            <Button
              onClick={handleUpdate}
              disabled={!formName.trim() || updateMutation.isPending}
            >
              {updateMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Save Changes
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Digest Dialog */}
      <Dialog open={!!deleteId} onOpenChange={(open) => !open && setDeleteId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Digest</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this digest?
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteId(null)}>Cancel</Button>
            <Button
              variant="destructive"
              onClick={() => deleteId && deleteMutation.mutate(deleteId)}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Evaluation Results Dialog */}
      <Dialog open={evalResultOpen} onOpenChange={setEvalResultOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Evaluation Results</DialogTitle>
            <DialogDescription>
              {evalResult?.dry_run ? 'Dry run results (nothing sent)' : 'Evaluation results'}
            </DialogDescription>
          </DialogHeader>
          {evalResult && (
            <div className="space-y-3 py-2">
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">Papers Found</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.papers_found}</p>
                </div>
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">New Papers</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.papers_new}</p>
                </div>
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">Papers Sent</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.papers_sent}</p>
                </div>
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">Failed Sends</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.papers_failed ?? 0}</p>
                </div>
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">Matched Rules</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.matched_rules ?? 0}</p>
                </div>
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="text-xs text-slate-500">Channels</p>
                  <p className="text-lg font-bold text-alma-800">{evalResult.channels.join(', ') || 'None'}</p>
                </div>
              </div>
              {evalResult.channel_results && (
                <div className="rounded-lg bg-parchment-50 p-3">
                  <p className="mb-2 text-xs font-medium text-slate-500">Channel Results</p>
                  <div className="space-y-1">
                    {Object.entries(evalResult.channel_results).map(([channel, result]) => (
                      <div key={channel} className="text-xs text-slate-700">
                        <span className="font-medium">{channel}</span>: {result.status}
                        {result.error ? ` (${result.error})` : ''}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {evalResult.papers && evalResult.papers.length > 0 && (
                <div className="max-h-48 overflow-y-auto rounded-lg bg-parchment-50 p-3">
                  <p className="mb-2 text-xs font-medium text-slate-500">Papers:</p>
                  <pre className="whitespace-pre-wrap text-xs text-slate-700">
                    {JSON.stringify(evalResult.papers, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setEvalResultOpen(false)}>Close</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
