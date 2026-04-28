import { useMutation, useQuery, useQueryClient, type UseMutationResult } from '@tanstack/react-query'
import { Activity, AlertTriangle, CheckCircle2, Cpu, Settings2 } from 'lucide-react'

import {
  api,
  clearDiscoverySimilarityCache,
  evaluateAlert,
  getDiscoverySettings,
  getInsightsDiagnostics,
  queueAuthorHistoryBackfill,
  refreshFeedInbox,
  refreshFeedMonitor,
  repairAuthor,
  runGraphReferenceBackfill,
  testPluginConnection,
  updateDiscoverySettings,
} from '@/api/client'
import { AsyncButton, SettingsCard, StatTile } from '@/components/settings/primitives'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { StatusBadge, severityTone, type StatusBadgeTone } from '@/components/ui/status-badge'
import { useToast, errorToast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'

type TargetAction = string
type RemediationTarget = {
  id?: string
  label?: string
  action?: TargetAction
  author_id?: string
  monitor_id?: string
  source?: string
  alert_id?: string
  plugin_name?: string
}

// Each remediation action knows how to extract its argument from the target,
// which mutation to fire, and how to label the resulting button. Keeping
// this declarative collapses the previous 10-branch switch into one map.
type RemediationEntry = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mutation: UseMutationResult<any, unknown, any, unknown>
  getArg: (target: RemediationTarget) => string | undefined
  label: (target: RemediationTarget) => string
  argless?: boolean
  icon?: React.ReactNode
}

export function OperationalStatusCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const diagnosticsQuery = useQuery({
    queryKey: ['insights-diagnostics', 'settings-operational'],
    queryFn: getInsightsDiagnostics,
    staleTime: 60_000,
    retry: 1,
  })

  const invalidateOperationalQueries = (...extraKeys: readonly unknown[][]) => {
    void invalidateQueries(
      queryClient,
      ['insights-diagnostics'],
      ['ai-status'],
      ['activity-operations'],
      ...extraKeys,
    )
  }

  const refreshFeedMutation = useMutation({
    mutationFn: refreshFeedInbox,
    onSuccess: (result) => {
      const status = String(result?.status ?? result?.operation?.status ?? '')
      invalidateOperationalQueries(['feed-monitors'], ['feed-inbox'])
      toast({
        title:
          status === 'already_running'
            ? 'Feed refresh already running'
            : status === 'completed' || status === 'noop'
              ? 'Feed refreshed'
              : 'Feed refresh queued',
        description:
          result?.message ||
          (status === 'completed' || status === 'noop'
            ? 'Active monitors were refreshed.'
            : 'Active monitors are being refreshed now.'),
      })
    },
    onError: () => errorToast('Feed refresh failed'),
  })

  const refreshDiscoveryMutation = useMutation({
    mutationFn: () =>
      api.post<{ status?: string; job_id?: string; message?: string }>(
        '/discovery/refresh?background=true',
      ),
    onSuccess: (result) => {
      invalidateOperationalQueries()
      toast({
        title: 'Discovery refresh queued',
        description: result?.message || 'Recommendations are being refreshed now.',
      })
    },
    onError: () => errorToast('Discovery refresh failed'),
  })

  const rebuildGraphsMutation = useMutation({
    mutationFn: () =>
      api.post<{ status?: string; job_id?: string; message?: string }>(
        '/graphs/rebuild?background=true',
      ),
    onSuccess: (result) => {
      invalidateOperationalQueries()
      toast({
        title: 'Graph rebuild queued',
        description: result?.message || 'Graph caches are rebuilding in the background.',
      })
    },
    onError: () => errorToast('Graph rebuild failed'),
  })

  const referenceBackfillMutation = useMutation({
    mutationFn: runGraphReferenceBackfill,
    onSuccess: () => {
      invalidateOperationalQueries()
      toast({
        title: 'Graph backfill queued',
        description: 'Missing reference edges are being backfilled now.',
      })
    },
    onError: () => errorToast('Graph backfill failed'),
  })

  const computeEmbeddingsMutation = useMutation({
    mutationFn: () =>
      api.post<{ status?: string; job_id?: string; message?: string }>('/ai/compute-embeddings'),
    onSuccess: (result) => {
      invalidateOperationalQueries()
      toast({
        title: 'Embedding job started',
        description: result?.message || 'Embedding computation is running in the background.',
      })
    },
    onError: () => errorToast('Embedding job failed'),
  })

  const computeStaleEmbeddingsMutation = useMutation({
    mutationFn: () =>
      api.post<{ status?: string; job_id?: string; message?: string }>(
        '/ai/compute-embeddings?scope=stale',
      ),
    onSuccess: (result) => {
      invalidateOperationalQueries()
      toast({
        title: 'Stale embedding refresh queued',
        description: result?.message || 'Stale publication embeddings are being recomputed.',
      })
    },
    onError: () => errorToast('Stale embedding refresh failed'),
  })

  const clearSimilarityCacheMutation = useMutation({
    mutationFn: clearDiscoverySimilarityCache,
    onSuccess: (result) => {
      invalidateOperationalQueries()
      toast({
        title: 'Similarity cache cleared',
        description: `${result.deleted ?? 0} cached similarity results removed.`,
      })
    },
    onError: () => errorToast('Could not clear similarity cache'),
  })

  const repairAuthorMutation = useMutation({
    mutationFn: (authorId: string) => repairAuthor(authorId),
    onSuccess: (result) => {
      invalidateOperationalQueries(['authors'])
      const status = String(result.status ?? '')
      if (status === 'queued' || status === 'running' || status === 'already_running') {
        toast({
          title:
            status === 'already_running' ? 'Author repair already running' : 'Author repair queued',
          description: result.message || 'Track progress in Activity.',
        })
        return
      }
      toast({
        title: 'Author repaired',
        description: result.refreshed
          ? 'Identifiers repaired and author refresh ran.'
          : 'Identifier repair completed.',
      })
    },
    onError: () => errorToast('Author repair failed'),
  })

  const refreshMonitorMutation = useMutation({
    mutationFn: (monitorId: string) => refreshFeedMonitor(monitorId),
    onSuccess: (result) => {
      invalidateOperationalQueries(['feed-monitors'], ['feed-inbox'])
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
            ? 'Targeted monitor refresh completed.'
            : 'Targeted monitor refresh is running in Activity.'),
      })
    },
    onError: () => errorToast('Monitor refresh failed'),
  })

  const historyBackfillMutation = useMutation({
    mutationFn: (authorId: string) => queueAuthorHistoryBackfill(authorId),
    onSuccess: (result) => {
      invalidateOperationalQueries(['authors'])
      toast({
        title: 'Historical backfill queued',
        description: result?.job_id
          ? `Job ${result.job_id} queued.`
          : 'Historical corpus refresh has been queued.',
      })
    },
    onError: () => errorToast('Historical backfill failed'),
  })

  const enableSourceMutation = useMutation({
    mutationFn: async (sourceName: string) => {
      const current = await getDiscoverySettings()
      const currentSources = current.sources as unknown as Record<
        string,
        { enabled: boolean; weight: number }
      >
      const next = {
        ...current,
        sources: {
          ...current.sources,
          [sourceName]: {
            ...(currentSources[sourceName] ?? { enabled: false, weight: 1 }),
            enabled: true,
          },
        },
      }
      return updateDiscoverySettings(next)
    },
    onSuccess: () => {
      invalidateOperationalQueries(['discovery-settings'])
      toast({ title: 'Source enabled', description: 'Discovery source has been re-enabled.' })
    },
    onError: () => errorToast('Source enable failed'),
  })

  const evaluateAlertMutation = useMutation({
    mutationFn: (alertId: string) => evaluateAlert(alertId),
    onSuccess: () => {
      invalidateOperationalQueries(['alerts'])
      toast({
        title: 'Alert evaluated',
        description: 'The alert has been re-run to verify delivery and matches.',
      })
    },
    onError: () => errorToast('Alert evaluation failed'),
  })

  const testPluginMutation = useMutation({
    mutationFn: (pluginName: string) => testPluginConnection(pluginName),
    onSuccess: (result, pluginName) => {
      invalidateOperationalQueries()
      toast({
        title: `Plugin test ${result.success ? 'passed' : 'failed'}`,
        description: result.message || pluginName,
        variant: result.success ? 'default' : 'destructive',
      })
    },
    onError: () => errorToast('Plugin test failed'),
  })

  const remediationActions: Record<string, RemediationEntry> = {
    repair_author: {
      mutation: repairAuthorMutation,
      getArg: (target) => target.author_id,
      label: (target) => `Repair ${target.label ?? 'author'}`,
    },
    backfill_author: {
      mutation: historyBackfillMutation,
      getArg: (target) => target.author_id,
      label: (target) => `Backfill ${target.label ?? 'author'}`,
    },
    refresh_monitor: {
      mutation: refreshMonitorMutation,
      getArg: (target) => target.monitor_id,
      label: (target) => `Refresh ${target.label ?? 'monitor'}`,
    },
    enable_source: {
      mutation: enableSourceMutation,
      getArg: (target) => target.source,
      label: (target) => `Enable ${target.label ?? 'source'}`,
    },
    evaluate_alert: {
      mutation: evaluateAlertMutation,
      getArg: (target) => target.alert_id,
      label: (target) => `Re-run ${target.label ?? 'alert'}`,
    },
    test_plugin: {
      mutation: testPluginMutation,
      getArg: (target) => target.plugin_name,
      label: (target) => `Test ${target.label ?? 'plugin'}`,
    },
    compute_embeddings: {
      mutation: computeEmbeddingsMutation,
      getArg: () => 'compute_embeddings',
      label: (target) => `AI Compute ${target.label ?? 'embeddings'}`,
      argless: true,
      icon: <Cpu className="mr-1 h-4 w-4" />,
    },
    compute_stale_embeddings: {
      mutation: computeStaleEmbeddingsMutation,
      getArg: () => 'compute_stale_embeddings',
      label: (target) => `AI Refresh ${target.label ?? 'embeddings'}`,
      argless: true,
      icon: <Cpu className="mr-1 h-4 w-4" />,
    },
    clear_similarity_cache: {
      mutation: clearSimilarityCacheMutation,
      getArg: () => 'clear_similarity_cache',
      label: (target) => `Clear ${target.label ?? 'similarity cache'}`,
      argless: true,
    },
  }

  if (diagnosticsQuery.isLoading) {
    return (
      <SettingsCard icon={Settings2} title="Operational Status">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="h-2 w-2 animate-pulse rounded-full bg-slate-300" />
          Loading current product health...
        </div>
      </SettingsCard>
    )
  }

  if (diagnosticsQuery.isError || !diagnosticsQuery.data) {
    const errorMessage =
      diagnosticsQuery.error instanceof Error ? diagnosticsQuery.error.message : null
    return (
      <SettingsCard icon={Settings2} title="Operational Status">
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            Could not load operational diagnostics.
            {errorMessage ? <span className="mt-1 block text-xs">{errorMessage}</span> : null}
          </AlertDescription>
        </Alert>
      </SettingsCard>
    )
  }

  const operational = diagnosticsQuery.data.operational
  const summary = operational.summary
  const states = operational.states ?? []

  return (
    <SettingsCard
      icon={Settings2}
      title="Operational Status"
      description="Current degraded states across retrieval, alerts, AI, and background operations."
    >
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatTile label="Active issues" value={summary.issues_total} />
        <StatTile label="Critical" value={summary.critical_count} tone="negative" />
        <StatTile label="Warnings" value={summary.warning_count} tone="warning" />
        <StatTile label="Healthy checks" value={summary.healthy_checks} tone="positive" />
      </div>

      <div className="flex flex-wrap gap-2">
        <StatusBadge tone={summary.embeddings_ready ? 'positive' : 'negative'}>
          Embeddings {summary.embeddings_ready ? 'ready' : 'degraded'}
        </StatusBadge>
        <StatusBadge tone={summary.slack_configured ? 'positive' : 'warning'}>
          Slack {summary.slack_configured ? 'configured' : 'missing'}
        </StatusBadge>
        {operational.disabled_sources.length > 0 && (
          <Badge variant="outline">
            Disabled sources: {operational.disabled_sources.join(', ')}
          </Badge>
        )}
        {summary.recent_failed_operations_24h > 0 && (
          <Badge variant="outline">Failed ops 24h: {summary.recent_failed_operations_24h}</Badge>
        )}
      </div>

      <div className="grid gap-3 xl:grid-cols-4">
        <RemediationTile
          title="AI remediation"
          body={
            !summary.embeddings_ready
              ? 'Compute embeddings to restore semantic retrieval and ranking.'
              : summary.warning_count > 0
                ? 'Refresh stale vectors or clear cached similarity results when diagnostics report compressed similarity.'
                : 'Embedding layer is healthy.'
          }
        >
          <AsyncButton
            size="sm"
            variant="outline"
            pending={computeStaleEmbeddingsMutation.isPending}
            onClick={() => computeStaleEmbeddingsMutation.mutate()}
          >
            Refresh stale
          </AsyncButton>
          <AsyncButton
            size="sm"
            variant="outline"
            pending={clearSimilarityCacheMutation.isPending}
            onClick={() => clearSimilarityCacheMutation.mutate()}
          >
            Clear cache
          </AsyncButton>
          <AsyncButton size="sm" variant="outline" onClick={() => navigateTo('settings')}>
            Settings
          </AsyncButton>
        </RemediationTile>

        <RemediationTile
          title="Graph remediation"
          body={
            summary.recent_failed_operations_24h > 0
              ? 'Rebuild or backfill citation edges when graph-dependent discovery is thin.'
              : 'Graph maintenance is currently stable.'
          }
        >
          <AsyncButton
            size="sm"
            variant="outline"
            pending={referenceBackfillMutation.isPending}
            onClick={() => referenceBackfillMutation.mutate()}
          >
            Backfill
          </AsyncButton>
        </RemediationTile>

        <RemediationTile
          title="Source remediation"
          body={
            operational.disabled_sources.length > 0
              ? `Re-enable disabled sources: ${operational.disabled_sources.join(', ')}.`
              : 'All configured discovery sources are enabled.'
          }
        >
          <AsyncButton size="sm" variant="outline" onClick={() => navigateTo('settings')}>
            Sources
          </AsyncButton>
        </RemediationTile>

        <RemediationTile
          title="Alert remediation"
          body={
            summary.slack_configured
              ? 'Alert delivery channel is configured.'
              : 'Configure Slack to turn alert rules into real delivery.'
          }
        >
          <AsyncButton size="sm" variant="outline" onClick={() => navigateTo('alerts')}>
            Alerts
          </AsyncButton>
        </RemediationTile>
      </div>

      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        <AsyncButton
          size="sm"
          variant="outline"
          pending={refreshFeedMutation.isPending}
          onClick={() => refreshFeedMutation.mutate()}
        >
          Refresh Feed
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          pending={refreshDiscoveryMutation.isPending}
          onClick={() => refreshDiscoveryMutation.mutate()}
        >
          Refresh Discovery
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          pending={rebuildGraphsMutation.isPending}
          onClick={() => rebuildGraphsMutation.mutate()}
        >
          Rebuild Graphs
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          pending={referenceBackfillMutation.isPending}
          onClick={() => referenceBackfillMutation.mutate()}
        >
          Backfill References
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          icon={<Cpu className="h-4 w-4" />}
          pending={computeEmbeddingsMutation.isPending}
          disabled={summary.embeddings_ready}
          onClick={() => computeEmbeddingsMutation.mutate()}
        >
          AI Compute Embeddings
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          icon={<Cpu className="h-4 w-4" />}
          pending={computeStaleEmbeddingsMutation.isPending}
          onClick={() => computeStaleEmbeddingsMutation.mutate()}
        >
          AI Refresh Stale Embeddings
        </AsyncButton>
        <AsyncButton
          size="sm"
          variant="outline"
          pending={clearSimilarityCacheMutation.isPending}
          onClick={() => clearSimilarityCacheMutation.mutate()}
        >
          Clear Similarity Cache
        </AsyncButton>
      </div>

      {states.length === 0 ? (
        <Alert variant="success">
          <CheckCircle2 className="h-4 w-4" />
          <AlertDescription>No active degraded states right now.</AlertDescription>
        </Alert>
      ) : (
        <div className="space-y-3">
          {states.slice(0, 6).map((state) => {
            const tone: StatusBadgeTone = severityTone(state.severity)
            return (
              <div key={state.id} className="rounded-sm border border-[var(--color-border)] p-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="font-medium text-alma-800">{state.label}</p>
                    <p className="mt-1 text-sm text-slate-500">{state.detail}</p>
                  </div>
                  <StatusBadge tone={tone}>{state.severity}</StatusBadge>
                </div>
                <div className="mt-3 flex justify-end">
                  <AsyncButton
                    size="sm"
                    variant="outline"
                    icon={<Activity className="h-4 w-4" />}
                    onClick={() => {
                      navigateTo(
                        state.page as Parameters<typeof buildHashRoute>[0],
                        state.params ?? {},
                      )
                    }}
                  >
                    Open Owner
                  </AsyncButton>
                </div>
                {(state.targets ?? []).length > 0 && (
                  <div className="mt-3 flex flex-wrap gap-2">
                    {state.targets?.map((target) => {
                      const action = target.action as string | undefined
                      if (!action) return null
                      const handler = remediationActions[action]
                      if (!handler) return null
                      const arg = handler.getArg(target as RemediationTarget)
                      if (!arg) return null
                      const pending =
                        handler.mutation.isPending &&
                        (handler.argless || handler.mutation.variables === arg)
                      return (
                        <AsyncButton
                          key={`${state.id}-${target.id ?? arg}`}
                          size="sm"
                          variant="secondary"
                          icon={handler.icon}
                          pending={pending}
                          onClick={() => {
                            if (handler.argless) handler.mutation.mutate(undefined)
                            else handler.mutation.mutate(arg)
                          }}
                        >
                          {handler.label(target as RemediationTarget)}
                        </AsyncButton>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}

      <div className="flex justify-end">
        <AsyncButton
          variant="outline"
          size="sm"
          onClick={() => navigateTo('insights', { tab: 'diagnostics' })}
        >
          Open Diagnostics
        </AsyncButton>
      </div>
    </SettingsCard>
  )
}

function RemediationTile({
  title,
  body,
  children,
}: {
  title: string
  body: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className="rounded-sm border border-[var(--color-border)] p-3">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{title}</p>
      <p className="mt-2 text-sm text-slate-700">{body}</p>
      <div className="mt-3 flex flex-wrap gap-2">{children}</div>
    </div>
  )
}
