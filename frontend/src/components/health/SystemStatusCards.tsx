/**
 * SystemStatusCards — the Health page's **System status** band, consolidated.
 *
 * Previously this was three surfaces that all answered "is the running system
 * OK?" and could disagree: the vitals scoreboard tiles, a subsystem status-row
 * card, and a separate "degraded right now" list. They are now ONE grid of
 * square per-component cards, each fed from the SAME canonical source
 * (`useDiagnosticsSections`): name, a status pill, a one-line description, the
 * issue metric, and the one-click remediation buttons we had before (moved into
 * a per-component detail popup). The colored vitals ribbon above stays as the
 * at-a-glance severity strip; everything actionable lives in these cards.
 *
 * Status + count come from each subsystem's diagnostics section (so we keep the
 * 429/​rate-limit source signal, degraded-monitor counts, etc.); the actionable
 * remediation targets come from `operational.states`. No charts here —
 * subsystem trends live in Insights → Activity.
 */
import { useMemo, useState } from 'react'
import {
  useMutation,
  useQueryClient,
  type UseMutationResult,
} from '@tanstack/react-query'
import {
  Activity,
  AlertTriangle,
  Bell,
  Cpu,
  Globe,
  Plug,
  Rss,
  Users,
  type LucideIcon,
} from 'lucide-react'
import { motion, useReducedMotion } from 'framer-motion'

import {
  api,
  clearDiscoverySimilarityCache,
  evaluateAlert,
  getDiscoverySettings,
  queueAuthorHistoryBackfill,
  refreshFeedMonitor,
  repairAuthor,
  testPluginConnection,
  updateDiscoverySettings,
} from '@/api/client'
import { useDiagnosticsSections } from '@/components/insights/useDiagnosticsSections'
import { AsyncButton } from '@/components/settings/primitives'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { StatusBadge } from '@/components/ui/status-badge'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { useToast, errorToast } from '@/hooks/useToast'
import { buildHashRoute, navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn } from '@/lib/utils'
import { dimensionBadgeTone, severityLabel, severityRank } from './healthFormat'

type Severity = 'ok' | 'warning' | 'critical' | 'info'

// One degraded state as shaped by the operational diagnostics section. The
// nested target carries the remediation action + its argument.
interface OperationalTarget {
  id: string
  label: string
  kind: string
  action: string
  author_id?: string | null
  monitor_id?: string | null
  source?: string | null
  alert_id?: string | null
  plugin_name?: string | null
}
interface OperationalState {
  id: string
  label: string
  severity: string
  detail: string
  page: string
  params?: Record<string, string>
  targets?: OperationalTarget[]
}
// A review-only line (no one-click fix) — e.g. a rate-limited upstream source.
interface ReviewItem {
  id: string
  primary: string
  secondary?: string
}

interface SystemComponent {
  id: string
  name: string
  icon: LucideIcon
  description: string
  severity: Severity
  /** Right-aligned metric, e.g. "2 degraded" / "healthy" / "rate-limited". */
  metric: string
  states: OperationalState[]
  reviewItems: ReviewItem[]
  ownerPage: string
  ownerParams?: Record<string, string>
}

// Loosely-typed diagnostics rows — the section payloads are dynamic JSON; we
// read only the fields we render.
interface MonitorRow {
  health?: string
}
interface AuthorRow {
  author_name?: string
  health_reason?: string
  last_error?: string
}
interface SourceRow {
  source?: string
  http_errors?: number
  transport_errors?: number
  last_error?: string
}

const worstSeverity = (states: OperationalState[], floor: Severity): Severity => {
  const ranked = [floor, ...states.map((s) => (s.severity as Severity) ?? 'warning')]
  return ranked.reduce((worst, s) => (severityRank(s) < severityRank(worst) ? s : worst), 'ok')
}

// Spell raw HTTP codes into a glance-readable reason (kept from the old card).
const humanizeSourceError = (s: SourceRow): string => {
  const raw = (s.last_error ?? '').trim()
  if (/\b429\b/.test(raw))
    return 'Rate-limited (HTTP 429) — the source is throttling us. ALMa backs off and retries; a verified API key raises the limit.'
  if (/\b50\d\b/.test(raw)) return `Source server error (${raw}) — usually transient; ALMa retries automatically.`
  if (/\b40[13]\b/.test(raw)) return `Access rejected (${raw}) — check the API key for this source.`
  if (raw) return raw
  const total = (s.http_errors ?? 0) + (s.transport_errors ?? 0)
  return `${s.http_errors ?? 0} HTTP / ${s.transport_errors ?? 0} transport error${total === 1 ? '' : 's'}`
}

// Which component a degraded state belongs to — by remediation kind, falling
// back to its id when a state has no actionable target (e.g. slack_unconfigured).
function componentOfState(s: OperationalState): string {
  const kinds = new Set((s.targets ?? []).map((t) => t.kind))
  if (kinds.has('monitor')) return 'monitors'
  if (kinds.has('author')) return 'authors'
  if (kinds.has('ai')) return 'ai'
  if (kinds.has('plugin')) return 'plugins'
  if (kinds.has('alert')) return 'alerts'
  if (kinds.has('source')) return 'sources'
  const id = s.id.toLowerCase()
  if (id.includes('slack') || id.includes('alert')) return 'alerts'
  if (id.includes('source')) return 'sources'
  if (id.includes('embedding') || id.includes('similarity') || id.includes('vector')) return 'ai'
  if (id.includes('author')) return 'authors'
  if (id.includes('monitor')) return 'monitors'
  return 'jobs'
}

// Severity → subtle left spine. Only attention cards get a tinted spine; a
// healthy card stays calm with the neutral border (brand: saturated tones in an
// off-white card read as alarms, so we reserve them for the triage signal).
const SPINE: Record<Severity, string> = {
  critical: 'border-l-2 border-l-rose-500',
  warning: 'border-l-2 border-l-amber-500',
  info: 'border-l-2 border-l-alma-folio',
  ok: '',
}

export function SystemStatusCards() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const reducedMotion = useReducedMotion()
  const sections = useDiagnosticsSections()
  const [openComp, setOpenComp] = useState<SystemComponent | null>(null)

  const invalidateOperational = (...extraKeys: readonly unknown[][]) => {
    void invalidateQueries(
      queryClient,
      ['insights-diag'],
      ['ai-status'],
      ['activity-operations'],
      ...extraKeys,
    )
  }

  // ── Remediation mutations (carried over from the old OperationalStatusCard).
  const computeStaleEmbeddingsMutation = useMutation({
    mutationFn: () => api.post('/ai/compute-embeddings?scope=stale'),
    onSuccess: () => {
      invalidateOperational()
      toast({ title: 'Stale embedding refresh queued', description: 'Recomputing in the background.' })
    },
    onError: () => errorToast('Stale embedding refresh failed'),
  })
  const clearSimilarityCacheMutation = useMutation({
    mutationFn: clearDiscoverySimilarityCache,
    onSuccess: (result) => {
      invalidateOperational()
      toast({ title: 'Similarity cache cleared', description: `${result.deleted ?? 0} cached results removed.` })
    },
    onError: () => errorToast('Could not clear similarity cache'),
  })
  const repairAuthorMutation = useMutation({
    mutationFn: (authorId: string) => repairAuthor(authorId),
    onSuccess: () => {
      invalidateOperational(['authors'])
      toast({ title: 'Author repair queued', description: 'Track progress in Activity.' })
    },
    onError: () => errorToast('Author repair failed'),
  })
  const historyBackfillMutation = useMutation({
    mutationFn: (authorId: string) => queueAuthorHistoryBackfill(authorId),
    onSuccess: (result) => {
      invalidateOperational(['authors'])
      toast({
        title: 'Historical backfill queued',
        description: result?.job_id ? `Job ${result.job_id} queued.` : 'Queued.',
      })
    },
    onError: () => errorToast('Historical backfill failed'),
  })
  const refreshMonitorMutation = useMutation({
    mutationFn: (monitorId: string) => refreshFeedMonitor(monitorId),
    onSuccess: () => {
      invalidateOperational(['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Monitor refresh queued', description: 'Running in Activity.' })
    },
    onError: () => errorToast('Monitor refresh failed'),
  })
  const enableSourceMutation = useMutation({
    mutationFn: async (sourceName: string) => {
      const current = await getDiscoverySettings()
      const currentSources = current.sources as unknown as Record<string, { enabled: boolean; weight: number }>
      return updateDiscoverySettings({
        ...current,
        sources: {
          ...current.sources,
          [sourceName]: { ...(currentSources[sourceName] ?? { enabled: false, weight: 1 }), enabled: true },
        },
      })
    },
    onSuccess: () => {
      invalidateOperational(['discovery-settings'])
      toast({ title: 'Source enabled', description: 'Discovery source re-enabled.' })
    },
    onError: () => errorToast('Source enable failed'),
  })
  const evaluateAlertMutation = useMutation({
    mutationFn: (alertId: string) => evaluateAlert(alertId),
    onSuccess: () => {
      invalidateOperational(['alerts'])
      toast({ title: 'Alert evaluated', description: 'Re-run to verify delivery and matches.' })
    },
    onError: () => errorToast('Alert evaluation failed'),
  })
  const testPluginMutation = useMutation({
    mutationFn: (pluginName: string) => testPluginConnection(pluginName),
    onSuccess: (result, pluginName) => {
      invalidateOperational()
      toast({
        title: `Plugin test ${result.ok ? 'passed' : 'failed'}`,
        description: result.message || pluginName,
        variant: result.ok ? 'default' : 'destructive',
      })
    },
    onError: () => errorToast('Plugin test failed'),
  })

  // action → how to extract its argument, which mutation to fire, its label.
  type RemediationEntry = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mutation: UseMutationResult<any, unknown, any, unknown>
    getArg: (t: OperationalTarget) => string | undefined
    label: (t: OperationalTarget) => string
    argless?: boolean
    icon?: React.ReactNode
  }
  const remediation: Record<string, RemediationEntry> = {
    repair_author: { mutation: repairAuthorMutation, getArg: (t) => t.author_id ?? undefined, label: (t) => `Repair ${t.label ?? 'author'}` },
    backfill_author: { mutation: historyBackfillMutation, getArg: (t) => t.author_id ?? undefined, label: (t) => `Backfill ${t.label ?? 'author'}` },
    refresh_monitor: { mutation: refreshMonitorMutation, getArg: (t) => t.monitor_id ?? undefined, label: (t) => `Refresh ${t.label ?? 'monitor'}` },
    enable_source: { mutation: enableSourceMutation, getArg: (t) => t.source ?? undefined, label: (t) => `Enable ${t.label ?? 'source'}` },
    evaluate_alert: { mutation: evaluateAlertMutation, getArg: (t) => t.alert_id ?? undefined, label: (t) => `Re-run ${t.label ?? 'alert'}` },
    test_plugin: { mutation: testPluginMutation, getArg: (t) => t.plugin_name ?? undefined, label: (t) => `Test ${t.label ?? 'plugin'}` },
    compute_stale_embeddings: { mutation: computeStaleEmbeddingsMutation, getArg: () => 'compute_stale_embeddings', label: (t) => `AI Refresh ${t.label ?? 'embeddings'}`, argless: true, icon: <Cpu className="mr-1 h-4 w-4" /> },
    clear_similarity_cache: { mutation: clearSimilarityCacheMutation, getArg: () => 'clear_similarity_cache', label: (t) => `Clear ${t.label ?? 'similarity cache'}`, argless: true },
  }

  // ── Build the component model from the canonical diagnostics sections.
  const components = useMemo<SystemComponent[]>(() => {
    const op = sections.operational.data
    const summary = op?.summary
    const states = (op?.states ?? []) as unknown as OperationalState[]
    const grouped = new Map<string, OperationalState[]>()
    for (const s of states) {
      const c = componentOfState(s)
      grouped.set(c, [...(grouped.get(c) ?? []), s])
    }
    const at = (id: string) => grouped.get(id) ?? []

    const monitorsDegraded = ((sections.feed.data?.monitors ?? []) as unknown as MonitorRow[]).filter(
      (m) => m.health && m.health !== 'ready' && m.health !== 'disabled',
    ).length
    const degradedAuthors = (sections.authors.data?.degraded ?? []) as unknown as AuthorRow[]
    const badSources = ((sections.discovery.data?.source_diagnostics ?? []) as unknown as SourceRow[]).filter(
      (s) => (s.http_errors ?? 0) > 0 || (s.transport_errors ?? 0) > 0,
    )
    const disabledSources = summary?.disabled_sources ?? op?.disabled_sources?.length ?? 0
    const unhealthyPlugins = summary?.unhealthy_plugins ?? 0
    const configuredPlugins = (op?.plugins ?? []).filter((p) => p.is_configured).length
    const failedJobs = summary?.recent_failed_operations_24h ?? 0

    // One builder so a card's severity and its metric ALWAYS agree — no
    // "warning" pill sitting above an "all healthy" metric. `count` is the
    // subsystem's own tally (degraded monitors, source errors…); the operational
    // `states` carry the remediation and can raise severity on their own.
    const mk = (
      c: Omit<SystemComponent, 'severity' | 'metric'>,
      opts: { count: number; countLabel: string; healthyLabel: string; attentionLabel?: string },
    ): SystemComponent => {
      const severity = worstSeverity(c.states, opts.count > 0 ? 'warning' : 'ok')
      const metric =
        severity === 'ok'
          ? opts.healthyLabel
          : opts.count > 0
            ? `${opts.count} ${opts.countLabel}`
            : opts.attentionLabel ?? 'needs attention'
      return { ...c, severity, metric }
    }

    const list: SystemComponent[] = [
      mk(
        {
          id: 'monitors',
          name: 'Feed monitors',
          icon: Rss,
          description: 'The followed-author + search monitors that pull new papers into the Feed.',
          states: at('monitors'),
          reviewItems: [],
          ownerPage: 'authors',
          ownerParams: { followed: 'true' },
        },
        { count: monitorsDegraded, countLabel: 'degraded', healthyLabel: 'all healthy' },
      ),
      mk(
        {
          id: 'sources',
          name: 'Upstream sources',
          icon: Globe,
          description: 'OpenAlex, Crossref & Semantic Scholar — the APIs that resolve and enrich papers.',
          states: at('sources'),
          reviewItems: badSources.map((s, i) => ({
            id: String(i),
            primary: s.source || 'Source',
            secondary: humanizeSourceError(s),
          })),
          ownerPage: 'settings',
          ownerParams: { section: 'connections' },
        },
        {
          count: badSources.length,
          countLabel: 'with errors',
          healthyLabel: disabledSources > 0 ? `${disabledSources} disabled` : 'all reachable',
        },
      ),
      mk(
        {
          id: 'ai',
          name: 'AI & embeddings',
          icon: Cpu,
          // Coverage backlog is data-health — it lives in the ribbon caption and
          // the repair cards below; this card is the provider's OPERATIONAL state.
          description: 'The embedding provider behind Discovery similarity and the paper map.',
          states: at('ai'),
          reviewItems: [],
          ownerPage: 'settings',
          ownerParams: { section: 'ai' },
        },
        { count: 0, countLabel: 'issues', healthyLabel: 'operational', attentionLabel: 'needs attention' },
      ),
      mk(
        {
          id: 'authors',
          name: 'Tracked authors',
          icon: Users,
          description: 'Followed authors whose identity bridge or historical corpus needs maintenance.',
          states: at('authors'),
          reviewItems: degradedAuthors.slice(0, 12).map((a, i) => ({
            id: String(i),
            primary: a.author_name || 'Author',
            secondary: a.last_error || a.health_reason,
          })),
          ownerPage: 'authors',
          ownerParams: { followed: 'true' },
        },
        {
          count: degradedAuthors.length,
          countLabel: 'degraded',
          healthyLabel: 'all healthy',
          attentionLabel: 'maintenance due',
        },
      ),
    ]

    // Conditional components — only shown when relevant, so the grid stays a
    // signal, not a wall of "n/a" cards.
    if (at('alerts').length > 0) {
      list.push(
        mk(
          {
            id: 'alerts',
            name: 'Alerts',
            icon: Bell,
            description: 'Scheduled digests and their delivery channel (Slack).',
            states: at('alerts'),
            reviewItems: [],
            ownerPage: 'settings',
            ownerParams: { section: 'channels' },
          },
          { count: 0, countLabel: 'issues', healthyLabel: 'delivering', attentionLabel: 'delivery degraded' },
        ),
      )
    }
    if (configuredPlugins > 0 || at('plugins').length > 0) {
      list.push(
        mk(
          {
            id: 'plugins',
            name: 'Plugins',
            icon: Plug,
            description: 'Configured integrations and their last connection test.',
            states: at('plugins'),
            reviewItems: [],
            ownerPage: 'settings',
            ownerParams: { section: 'plugins' },
          },
          { count: unhealthyPlugins, countLabel: 'unhealthy', healthyLabel: `${configuredPlugins} configured` },
        ),
      )
    }
    if (failedJobs > 0 || at('jobs').length > 0) {
      list.push(
        mk(
          {
            id: 'jobs',
            name: 'Background jobs',
            icon: Activity,
            description: 'Maintenance, hydration and embedding jobs run on the scheduler.',
            states: at('jobs'),
            reviewItems: [],
            ownerPage: 'activity',
          },
          { count: failedJobs, countLabel: 'failed (24h)', healthyLabel: 'all healthy' },
        ),
      )
    }

    // Worst-first so a critical/degraded card leads the grid.
    return list.sort((a, b) => severityRank(a.severity) - severityRank(b.severity))
  }, [sections.operational.data, sections.feed.data, sections.authors.data, sections.discovery.data])

  if (sections.operational.loading) {
    return (
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="h-32 animate-pulse rounded-sm bg-alma-content-elev" />
        ))}
      </div>
    )
  }
  if (sections.operational.error) {
    return (
      <Alert variant="warning">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>Could not load operational diagnostics.</AlertDescription>
      </Alert>
    )
  }

  const hasIssues = (c: SystemComponent) => c.states.length > 0 || c.reviewItems.length > 0

  return (
    <>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {components.map((c, i) => {
          const Icon = c.icon
          const attention = c.severity !== 'ok'
          const actionable = hasIssues(c)
          return (
            <motion.div
              key={c.id}
              initial={reducedMotion ? false : { opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.35, ease: 'easeOut', delay: 0.04 * i }}
              className={cn(
                'flex flex-col rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-4 shadow-paper-sm',
                SPINE[c.severity],
              )}
            >
              {/* Header — icon + name + status pill */}
              <div className="flex items-start justify-between gap-2">
                <div className="flex min-w-0 items-center gap-2">
                  <span
                    className={cn(
                      'grid h-7 w-7 shrink-0 place-items-center rounded-sm',
                      attention ? 'bg-alma-100 text-alma-700' : 'bg-alma-50 text-alma-600',
                    )}
                  >
                    <Icon className="h-4 w-4" />
                  </span>
                  <span className="truncate text-sm font-medium text-alma-900">{c.name}</span>
                </div>
                <StatusBadge tone={dimensionBadgeTone(c.severity)} size="sm" className="shrink-0 capitalize">
                  {severityLabel(c.severity)}
                </StatusBadge>
              </div>

              {/* Description */}
              <p className="mt-2 flex-1 text-xs leading-relaxed text-slate-500">{c.description}</p>

              {/* Footer — metric + action */}
              <div className="mt-3 flex items-center justify-between gap-2">
                <span className="truncate text-xs tabular-nums text-slate-600">{c.metric}</span>
                {actionable ? (
                  <AsyncButton
                    size="sm"
                    variant="outline"
                    className="shrink-0 border-alma-200 text-alma-700 hover:bg-alma-50"
                    onClick={() => setOpenComp(c)}
                  >
                    Review &amp; fix
                  </AsyncButton>
                ) : null}
              </div>
            </motion.div>
          )
        })}
      </div>

      {/* Per-component detail + one-click remediation. */}
      <Dialog open={openComp != null} onOpenChange={(o) => !o && setOpenComp(null)}>
        <DialogContent className="max-h-[80vh] max-w-2xl overflow-y-auto bg-alma-chrome">
          {openComp ? (
            <>
              <DialogHeader>
                <DialogTitle className="flex flex-wrap items-center gap-2 text-alma-900">
                  {openComp.name}
                  <StatusBadge tone={dimensionBadgeTone(openComp.severity)} size="sm" className="capitalize">
                    {severityLabel(openComp.severity)}
                  </StatusBadge>
                </DialogTitle>
                <DialogDescription className="text-slate-600">{openComp.description}</DialogDescription>
              </DialogHeader>

              <div className="space-y-2">
                {/* Actionable degraded states with their one-click remediation. */}
                {openComp.states.map((state) => (
                  <div
                    key={state.id}
                    className="rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-3"
                  >
                    <p className="text-sm font-medium text-alma-800">{state.label}</p>
                    {state.detail ? <p className="mt-1 text-xs text-slate-500">{state.detail}</p> : null}
                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      {(state.targets ?? []).map((target) => {
                        const handler = remediation[target.action]
                        if (!handler) return null
                        const arg = handler.getArg(target)
                        if (!arg) return null
                        const pending =
                          handler.mutation.isPending && (handler.argless || handler.mutation.variables === arg)
                        return (
                          <AsyncButton
                            key={`${state.id}-${target.id ?? arg}`}
                            size="sm"
                            variant="outline"
                            className="border-alma-200 text-alma-700 hover:bg-alma-50"
                            icon={handler.icon}
                            pending={pending}
                            onClick={() => (handler.argless ? handler.mutation.mutate(undefined) : handler.mutation.mutate(arg))}
                          >
                            {handler.label(target)}
                          </AsyncButton>
                        )
                      })}
                    </div>
                  </div>
                ))}

                {/* Review-only items (no one-click fix) — e.g. rate-limited sources. */}
                {openComp.reviewItems.map((item) => (
                  <div
                    key={`review-${item.id}`}
                    className="rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-3"
                  >
                    <p className="text-sm font-medium text-alma-800">{item.primary}</p>
                    {item.secondary ? <p className="mt-1 text-xs text-slate-500">{item.secondary}</p> : null}
                  </div>
                ))}
              </div>

              <div className="flex justify-end">
                <AsyncButton
                  size="sm"
                  variant="ghost"
                  icon={<Activity className="h-4 w-4" />}
                  className="text-alma-700 hover:bg-alma-50"
                  onClick={() =>
                    navigateTo(
                      openComp.ownerPage as Parameters<typeof buildHashRoute>[0],
                      openComp.ownerParams ?? {},
                    )
                  }
                >
                  Open owner
                </AsyncButton>
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>
    </>
  )
}
