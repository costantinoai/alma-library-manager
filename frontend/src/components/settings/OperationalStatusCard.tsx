/**
 * OperationalStatusCard — the "degraded right now" list for the Health page's
 * System status band: each active degraded state (stale author, unhealthy
 * monitor, disabled source, failing alert, embedding gap…) with a one-click
 * remediation and a jump to its owner page.
 *
 * Styled to match the rest of the Health page (content-elev card, chrome-elev
 * rows) — NOT a settings card. The old summary stat tiles, remediation-tile
 * grid, and global maintenance button-wall were dropped: the vitals scoreboard
 * already shows the counts, and those bulk actions now live in the repair cards
 * (Compute embeddings, …) or on their owner pages (Refresh feed, Rebuild graphs).
 */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient, type UseMutationResult } from '@tanstack/react-query'
import { Activity, AlertTriangle, CheckCircle2, Cpu } from 'lucide-react'

import {
  api,
  clearDiscoverySimilarityCache,
  evaluateAlert,
  getDiscoverySettings,
  getInsightsDiagnostics,
  queueAuthorHistoryBackfill,
  refreshFeedMonitor,
  repairAuthor,
  testPluginConnection,
  updateDiscoverySettings,
} from '@/api/client'
import { AsyncButton } from '@/components/settings/primitives'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { StatusRow } from '@/components/health/StatusRow'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { StatusBadge, severityTone } from '@/components/ui/status-badge'
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
// this declarative collapses what was a 10-branch switch into one map.
type RemediationEntry = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  mutation: UseMutationResult<any, unknown, any, unknown>
  getArg: (target: RemediationTarget) => string | undefined
  label: (target: RemediationTarget) => string
  argless?: boolean
  icon?: React.ReactNode
}

const CARD =
  'space-y-3 rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-4 shadow-paper'

type OperationalState = {
  id: string
  label: string
  severity: string
  detail: string
  page: string
  params?: Record<string, string>
  targets?: Array<{
    id: string
    label: string
    kind: string
    action: string
    author_id?: string | null
    monitor_id?: string | null
    source?: string | null
    alert_id?: string | null
    plugin_name?: string | null
  }>
}

export function OperationalStatusCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  // The selected degraded issue, shown in a detail popup with its remediation.
  const [openState, setOpenState] = useState<OperationalState | null>(null)
  const diagnosticsQuery = useQuery({
    queryKey: ['insights-diagnostics', 'health-operational'],
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
        title: `Plugin test ${result.ok ? 'passed' : 'failed'}`,
        description: result.message || pluginName,
        variant: result.ok ? 'default' : 'destructive',
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
      <div className={CARD}>
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <div className="h-2 w-2 animate-pulse rounded-full bg-slate-300" />
          Loading operational issues…
        </div>
      </div>
    )
  }

  if (diagnosticsQuery.isError || !diagnosticsQuery.data) {
    const errorMessage =
      diagnosticsQuery.error instanceof Error ? diagnosticsQuery.error.message : null
    return (
      <div className={CARD}>
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            Could not load operational diagnostics.
            {errorMessage ? <span className="mt-1 block text-xs">{errorMessage}</span> : null}
          </AlertDescription>
        </Alert>
      </div>
    )
  }

  const states = diagnosticsQuery.data.operational.states ?? []

  if (states.length === 0) {
    return (
      <div className={CARD}>
        <Alert variant="success">
          <CheckCircle2 className="h-4 w-4" />
          <AlertDescription>No active degraded states right now.</AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className={CARD}>
      <div className="flex items-center justify-between gap-2">
        <h3 className="text-sm font-medium text-alma-800">Degraded right now</h3>
        <StatusBadge tone="warning" size="sm">
          {states.length}
        </StatusBadge>
      </div>

      {/* Same StatusRow language as the subsystem + repair cards above; the
          per-issue remediation moves into a detail popup. */}
      <div className="space-y-1.5">
        {states.map((state) => {
          const fixable = (state.targets ?? []).some((t) => t.action && remediationActions[t.action])
          return (
            <StatusRow
              key={state.id}
              severity={state.severity}
              label={state.label}
              metric={
                <span className="shrink-0 text-xs text-slate-500">
                  {fixable ? 'fix available' : 'review'}
                </span>
              }
              onOpen={() => setOpenState(state)}
            />
          )
        })}
      </div>

      {/* Detail + one-click remediation for the selected issue. */}
      <Dialog open={openState != null} onOpenChange={(o) => !o && setOpenState(null)}>
        <DialogContent className="max-w-lg bg-alma-chrome">
          {openState ? (
            <>
              <DialogHeader>
                <DialogTitle className="flex flex-wrap items-center gap-2 text-alma-900">
                  {openState.label}
                  <StatusBadge tone={severityTone(openState.severity)} size="sm" className="capitalize">
                    {openState.severity}
                  </StatusBadge>
                </DialogTitle>
                <DialogDescription className="text-slate-600">{openState.detail}</DialogDescription>
              </DialogHeader>
              <div className="flex flex-wrap items-center justify-end gap-2">
                {(openState.targets ?? []).map((target) => {
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
                      key={`${openState.id}-${target.id ?? arg}`}
                      size="sm"
                      variant="outline"
                      className="border-alma-200 text-alma-700 hover:bg-alma-50"
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
                <AsyncButton
                  size="sm"
                  variant="ghost"
                  icon={<Activity className="h-4 w-4" />}
                  className="text-alma-700 hover:bg-alma-50"
                  onClick={() => {
                    navigateTo(
                      openState.page as Parameters<typeof buildHashRoute>[0],
                      openState.params ?? {},
                    )
                  }}
                >
                  Open owner
                </AsyncButton>
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
