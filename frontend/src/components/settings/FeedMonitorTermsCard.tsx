import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Plus, RefreshCw, Save, Search, Tag, Trash2, UserRound } from 'lucide-react'

import {
  createFeedMonitor,
  deleteFeedMonitor,
  listFeedMonitors,
  refreshFeedMonitor,
  updateFeedMonitor,
  type FeedMonitor,
} from '@/api/client'
import { AsyncButton, SettingsCard, SettingsSection } from '@/components/settings/primitives'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { EmptyState } from '@/components/ui/empty-state'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { StatusBadge, monitorHealthTone } from '@/components/ui/status-badge'
import { useToast, errorToast } from '@/hooks/useToast'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatMonitorTypeLabel, formatTimestamp } from '@/lib/utils'

type FeedMonitorCreateType = 'query' | 'topic'

function monitorQuery(monitor: FeedMonitor): string {
  const raw = monitor.config?.query
  return typeof raw === 'string' ? raw : monitor.monitor_key
}

function createMonitorPlaceholder(type: FeedMonitorCreateType): string {
  if (type === 'query') {
    return 'e.g. (manifold OR topology) AND representations NOT images'
  }
  return 'e.g. protein design'
}

function createMonitorHelp(type: FeedMonitorCreateType): string {
  if (type === 'query') {
    return 'Keyword monitors use strict boolean logic over title and abstract only.'
  }
  return 'Topic monitors cast a broader retrieval net, then Feed applies stricter matching before insertion.'
}

function monitorOwnerPage(monitor: FeedMonitor): 'authors' | 'feed' {
  return monitor.monitor_type === 'author' ? 'authors' : 'feed'
}

function MonitorRow({ monitor }: { monitor: FeedMonitor }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const editableDefinition = monitor.monitor_type !== 'author'
  const ownerPage = monitorOwnerPage(monitor)
  const [label, setLabel] = useState(monitor.label)
  const [query, setQuery] = useState(monitorQuery(monitor))
  const [enabled, setEnabled] = useState(Boolean(monitor.enabled))

  useEffect(() => {
    setLabel(monitor.label)
    setQuery(monitorQuery(monitor))
    setEnabled(Boolean(monitor.enabled))
  }, [monitor.id, monitor.label, monitor.monitor_key, monitor.config, monitor.enabled])

  const dirty =
    enabled !== Boolean(monitor.enabled) ||
    (editableDefinition &&
      (label.trim() !== monitor.label.trim() || query.trim() !== monitorQuery(monitor).trim()))

  const saveMutation = useMutation({
    mutationFn: () => {
      const payload: { enabled?: boolean; label?: string; query?: string } = {}
      if (enabled !== Boolean(monitor.enabled)) payload.enabled = enabled
      if (editableDefinition) {
        if (label.trim() !== monitor.label.trim()) payload.label = label.trim()
        if (query.trim() !== monitorQuery(monitor).trim()) payload.query = query.trim()
      }
      return updateFeedMonitor(monitor.id, payload)
    },
    onSuccess: async (updated) => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({
        title: 'Monitor updated',
        description: updated.enabled
          ? 'Feed monitor is active.'
          : 'Feed monitor is paused and will be skipped on refresh.',
      })
    },
    onError: () => errorToast('Could not update monitor'),
  })

  const refreshMutation = useMutation({
    mutationFn: () => refreshFeedMonitor(monitor.id),
    onSuccess: async (result) => {
      await invalidateQueries(
        queryClient,
        ['feed-monitors'],
        ['feed-inbox'],
        ['activity-operations'],
      )
      const status = String(result?.status ?? result?.operation?.status ?? '')
      toast({
        title:
          status === 'already_running'
            ? 'Monitor refresh already running'
            : status === 'completed' || status === 'noop'
              ? 'Monitor refreshed'
              : 'Monitor refresh queued',
        description:
          result?.message ||
          (status === 'completed' || status === 'noop'
            ? 'Recent matching papers were checked.'
            : 'Track the refresh in Activity. Feed data will update automatically when it finishes.'),
      })
    },
    onError: () => errorToast('Could not refresh monitor'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteFeedMonitor(monitor.id),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Monitor removed', description: 'The saved Feed rule was deleted.' })
    },
    onError: () => errorToast('Could not delete monitor'),
  })

  const busy = saveMutation.isPending || refreshMutation.isPending || deleteMutation.isPending
  const result = monitor.last_result ?? {}
  const papersFound = typeof result.papers_found === 'number' ? result.papers_found : null
  const itemsCreated = typeof result.items_created === 'number' ? result.items_created : null
  const monitoredQuery = monitorQuery(monitor)

  return (
    <div className="rounded-sm border border-[var(--color-border)] p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{formatMonitorTypeLabel(monitor.monitor_type)} Monitor</Badge>
            <StatusBadge tone={monitorHealthTone(monitor.health)}>{monitor.health}</StatusBadge>
            <label className="inline-flex items-center gap-2 rounded-full border border-[var(--color-border)] px-2.5 py-1 text-[11px] font-medium text-slate-700">
              <Checkbox
                checked={enabled}
                disabled={busy}
                onCheckedChange={(checked) => setEnabled(checked === true)}
              />
              {enabled ? 'active' : 'paused'}
            </label>
          </div>

          {editableDefinition ? (
            <div className="grid gap-3">
              <Input
                value={label}
                onChange={(event) => setLabel(event.target.value)}
                placeholder="Display label"
                disabled={busy}
              />
              <textarea
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                rows={monitor.monitor_type === 'query' ? 3 : 2}
                disabled={busy}
                className="min-h-[72px] rounded-sm border border-[var(--color-border)] bg-alma-paper px-3 py-2 text-sm text-slate-700 shadow-paper-inset-cool outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                placeholder={
                  monitor.monitor_type === 'query'
                    ? 'Examples: manifold AND representations\n(protein OR antibody) AND design NOT vaccine'
                    : 'Describe the topic to monitor'
                }
              />
            </div>
          ) : (
            <div className="rounded-md border border-[var(--color-border)] bg-parchment-50 px-3 py-2 text-sm text-slate-700">
              <div className="flex flex-wrap items-center gap-2 font-medium text-alma-800">
                <UserRound className="h-4 w-4 text-slate-500" />
                <span>{monitor.author_name || monitor.label}</span>
              </div>
              <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500">
                {monitor.openalex_id && <span>OpenAlex: {monitor.openalex_id}</span>}
                {monitor.scholar_id && <span>Scholar: {monitor.scholar_id}</span>}
                {!monitor.openalex_id && (
                  <span>
                    Author monitor stays visible here so it can be paused even before identifier
                    repair is complete.
                  </span>
                )}
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-wrap gap-2">
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            icon={<RefreshCw className="h-3.5 w-3.5" />}
            pending={refreshMutation.isPending}
            disabled={busy && !refreshMutation.isPending}
            onClick={() => refreshMutation.mutate()}
          >
            Refresh
          </AsyncButton>
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            onClick={() => navigateTo(ownerPage)}
          >
            Open owner
          </AsyncButton>
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            icon={<Save className="h-3.5 w-3.5" />}
            pending={saveMutation.isPending}
            disabled={busy || !dirty || (editableDefinition && !query.trim())}
            onClick={() => saveMutation.mutate()}
          >
            Save
          </AsyncButton>
          {editableDefinition && (
            <AsyncButton
              type="button"
              size="sm"
              variant="outline"
              icon={<Trash2 className="h-3.5 w-3.5" />}
              pending={deleteMutation.isPending}
              disabled={busy && !deleteMutation.isPending}
              onClick={() => deleteMutation.mutate()}
            >
              Delete
            </AsyncButton>
          )}
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2 text-xs text-slate-500">
        {editableDefinition && monitoredQuery && <span>Rule: {monitoredQuery}</span>}
        {monitor.updated_at && <span>Updated: {formatTimestamp(monitor.updated_at)}</span>}
        {monitor.last_checked_at && <span>Checked: {formatTimestamp(monitor.last_checked_at)}</span>}
        {monitor.last_success_at && (
          <span>Last success: {formatTimestamp(monitor.last_success_at)}</span>
        )}
        {papersFound != null && <span>Papers: {papersFound}</span>}
        {itemsCreated != null && <span>New items: {itemsCreated}</span>}
      </div>

      {(monitor.last_error || monitor.health_reason) && (
        <p className="mt-2 text-xs text-amber-800">{monitor.last_error || monitor.health_reason}</p>
      )}
    </div>
  )
}

export function FeedMonitorTermsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [newType, setNewType] = useState<FeedMonitorCreateType>('query')
  const [newLabel, setNewLabel] = useState('')
  const [newQuery, setNewQuery] = useState('')

  const monitorsQuery = useQuery({
    queryKey: ['feed-monitors', 'settings'],
    queryFn: listFeedMonitors,
    staleTime: 30_000,
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: () =>
      createFeedMonitor({
        monitor_type: newType,
        label: newLabel.trim() || undefined,
        query: newQuery.trim(),
      }),
    onSuccess: async () => {
      setNewLabel('')
      setNewQuery('')
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({
        title: newType === 'query' ? 'Keyword monitor added' : 'Topic monitor added',
        description: 'The new Feed rule is now active.',
      })
    },
    onError: () => errorToast('Could not create monitor'),
  })

  const monitors = monitorsQuery.data ?? []
  const authorMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'author'),
    [monitors],
  )
  const keywordMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'query'),
    [monitors],
  )
  const topicMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'topic'),
    [monitors],
  )
  const otherMonitors = useMemo(
    () => monitors.filter((monitor) => !['author', 'query', 'topic'].includes(monitor.monitor_type)),
    [monitors],
  )
  const disabledCount = monitors.filter((monitor) => !monitor.enabled).length
  const degradedCount = monitors.filter((monitor) => monitor.health === 'degraded').length

  const headerStats = (
    <>
      <StatusBadge tone="neutral" size="sm">
        {monitors.length} total
      </StatusBadge>
      <StatusBadge tone="neutral" size="sm">
        {authorMonitors.length} authors
      </StatusBadge>
      <StatusBadge tone="neutral" size="sm">
        {topicMonitors.length} topics
      </StatusBadge>
      <StatusBadge tone="neutral" size="sm">
        {keywordMonitors.length} keywords
      </StatusBadge>
      {disabledCount > 0 && (
        <StatusBadge tone="neutral" size="sm">
          {disabledCount} paused
        </StatusBadge>
      )}
      {degradedCount > 0 && (
        <StatusBadge tone="warning" size="sm">
          {degradedCount} degraded
        </StatusBadge>
      )}
    </>
  )

  return (
    <SettingsCard
      icon={Search}
      title="Feed Monitor Controls"
      description="Feed stays deterministic only if the monitor layer is explicit and easy to tune. Authors are owned by the Authors page, but you can pause or refresh them here."
      action={headerStats}
      roomy
    >
      <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50/70 p-4">
        <div className="grid gap-3 lg:grid-cols-[170px_minmax(0,1fr)_minmax(0,0.9fr)_auto]">
          <Select
            value={newType}
            onValueChange={(value) => setNewType(value as FeedMonitorCreateType)}
            disabled={createMutation.isPending}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="query">Keyword Monitor</SelectItem>
              <SelectItem value="topic">Topic Monitor</SelectItem>
            </SelectContent>
          </Select>
          <Input
            value={newQuery}
            onChange={(event) => setNewQuery(event.target.value)}
            placeholder={createMonitorPlaceholder(newType)}
            disabled={createMutation.isPending}
          />
          <Input
            value={newLabel}
            onChange={(event) => setNewLabel(event.target.value)}
            placeholder="Display label"
            disabled={createMutation.isPending}
          />
          <AsyncButton
            type="button"
            icon={<Plus className="h-4 w-4" />}
            pending={createMutation.isPending}
            disabled={!newQuery.trim()}
            onClick={() => createMutation.mutate()}
          >
            Add Monitor
          </AsyncButton>
        </div>
        <p className="mt-3 text-xs text-slate-500">{createMonitorHelp(newType)}</p>
      </div>

      {monitorsQuery.isLoading ? (
        <p className="text-sm text-slate-500">Loading Feed monitors...</p>
      ) : monitorsQuery.isError ? (
        <Alert variant="negative">
          <AlertDescription>Failed to load Feed monitors.</AlertDescription>
        </Alert>
      ) : monitors.length === 0 ? (
        <EmptyState title="No Feed monitors yet" description="Add a topic or keyword monitor above." />
      ) : (
        <div className="space-y-5">
          {authorMonitors.length > 0 && (
            <SettingsSection
              defaultOpen={false}
              title={
                <span className="flex items-center gap-2">
                  <UserRound className="h-4 w-4 text-slate-500" />
                  Author Monitors
                </span>
              }
              trailing={
                <StatusBadge tone="neutral" size="sm">
                  {authorMonitors.length}
                </StatusBadge>
              }
            >
              <div className="space-y-3">
                {authorMonitors.map((monitor) => (
                  <MonitorRow key={monitor.id} monitor={monitor} />
                ))}
              </div>
            </SettingsSection>
          )}
          {topicMonitors.length > 0 && (
            <MonitorSection icon={<Tag className="h-4 w-4 text-slate-500" />} title="Topic Monitors">
              {topicMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
          {keywordMonitors.length > 0 && (
            <MonitorSection icon={<Search className="h-4 w-4 text-slate-500" />} title="Keyword Monitors">
              {keywordMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
          {otherMonitors.length > 0 && (
            <MonitorSection icon={<Tag className="h-4 w-4 text-slate-500" />} title="Other Monitor Types">
              {otherMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
        </div>
      )}
    </SettingsCard>
  )
}

function MonitorSection({
  icon,
  title,
  children,
}: {
  icon: React.ReactNode
  title: string
  children: React.ReactNode
}) {
  return (
    <section className="space-y-3">
      <div className="flex items-center gap-2">
        {icon}
        <h3 className="text-sm font-semibold text-alma-800">{title}</h3>
      </div>
      <div className="space-y-3">{children}</div>
    </section>
  )
}
