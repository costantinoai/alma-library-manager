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
 * 429/rate-limit source signal, degraded-monitor counts, etc.); the actionable
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
  CheckCircle2,
  Cpu,
  Globe,
  Plug,
  Rss,
  Users,
  type LucideIcon,
} from 'lucide-react'
import { motion, useReducedMotion } from 'framer-motion'

import {
  ApiError,
  api,
  clearDiscoverySimilarityCache,
  evaluateAlert,
  getApiErrorMessage,
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
  /** Second-line metric, e.g. "2 with errors" / "maintenance due" / "operational". */
  metric: string
  /** Optional measurement caveat shown in the popup (e.g. the diagnostics window). */
  note?: string
  states: OperationalState[]
  reviewItems: ReviewItem[]
  /** Count of review rows beyond the capped `reviewItems` slice ("+N more"). */
  reviewOverflow?: number
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
  requests?: number
  operations?: number
  status_counts?: Record<string, number>
  last_error?: string
}

const worstSeverity = (states: OperationalState[], floor: Severity): Severity => {
  const ranked = [floor, ...states.map((s) => (s.severity as Severity) ?? 'warning')]
  return ranked.reduce((worst, s) => (severityRank(s) < severityRank(worst) ? s : worst), 'ok')
}

// HTTP status code → short reason, for spelling out the error mix.
const CODE_REASON: Record<string, string> = {
  '400': 'bad request',
  '401': 'auth rejected',
  '403': 'forbidden',
  '404': 'not found',
  '429': 'rate-limited',
  '500': 'server error',
  '502': 'bad gateway',
  '503': 'unavailable',
  '504': 'timeout',
}

// A specific, time-scoped reason for a source's recent HTTP failures, built from
// the actual status mix — NOT a generic present-tense "is throttling us". Source
// diagnostics are aggregated across the last feed + discovery refreshes, so this
// describes what happened "across recent refreshes", not a live probe of "now".
const humanizeSourceError = (s: SourceRow): string => {
  const sc = s.status_counts ?? {}
  const breakdown = Object.entries(sc)
    .filter(([code]) => Number(code) >= 400)
    .sort((a, b) => b[1] - a[1])
    .map(([code, n]) => `${n}× ${code}${CODE_REASON[code] ? ` (${CODE_REASON[code]})` : ''}`)
    .join(', ')
  const httpErr = s.http_errors ?? 0
  const transportErr = s.transport_errors ?? 0
  const reqs = s.requests ?? httpErr + transportErr
  const ops = s.operations ?? 0
  const window = ops > 0 ? ` across the last ${ops} refresh${ops === 1 ? '' : 'es'}` : ' recently'
  const lead = breakdown
    ? `${breakdown} — ${httpErr} of ${reqs} requests failed${window}.`
    : `${httpErr} HTTP / ${transportErr} transport error${httpErr + transportErr === 1 ? '' : 's'}${window}.`
  const why = (sc['429'] ?? 0) > 0 ? ' ALMa backs off and retries; a verified API key raises the 429 limit.' : ''
  return lead + why
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

// Severity → status-dot color. The dot is the at-a-glance status on each chip —
// the controlled bit of semantic color (like the ribbon), not decoration.
const DOT: Record<Severity, string> = {
  critical: 'bg-critical-500',
  warning: 'bg-warning-500',
  info: 'bg-alma-folio',
  ok: 'bg-success-500',
}

// Per-component plain-English "what healthy means / how it's configured", shown
// in the popup when a component has no issues so the user understands the green.
const HEALTHY_NOTE: Record<string, string> = {
  monitors: 'All feed monitors are refreshing cleanly — new papers are arriving on schedule.',
  sources: 'OpenAlex, Crossref and Semantic Scholar are reachable and responding without errors.',
  ai: 'An embedding provider is configured and operational — Discovery similarity and the paper map are powered.',
  authors: 'Every tracked author has a clean identity bridge and an up-to-date historical corpus.',
  alerts: 'Scheduled digests are configured and delivering.',
  plugins: 'All configured integrations passed their last connection test.',
  jobs: 'No background jobs have failed in the last 24 hours.',
}

export function SystemStatusCards() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const reducedMotion = useReducedMotion()
  const sections = useDiagnosticsSections()
  // Track the OPEN component by id, not a captured snapshot — the popup then
  // reads from the live `components` memo below, so a successful remediation
  // (which invalidates + refetches diagnostics) updates the open popup's states
  // / counts in place instead of showing the stale click-time snapshot.
  const [openId, setOpenId] = useState<string | null>(null)

  const invalidateOperational = (...extraKeys: readonly unknown[][]) => {
    void invalidateQueries(
      queryClient,
      ['insights-diag'],
      ['ai-status'],
      ['activity-operations'],
      ...extraKeys,
    )
  }

  // A remediation target lifted from the diagnostics snapshot can be a PHANTOM:
  // that snapshot is a stale-while-revalidate cache, so a monitor/author it still
  // lists may already be gone (author unfollowed, or the cache predates a
  // cleanup). The backend answers 404. Clicking "fix" on a phantom then dead-ended
  // on a scary "…failed" toast while the row sat there. So treat a 404 as "already
  // gone": close the popup, re-pull diagnostics (which kicks the SWR rebuild that
  // drops the phantom), and say so plainly. Returns true when it handled a 404 so
  // the caller skips its generic error toast.
  const handlePhantomTarget = (err: unknown, noun: string): boolean => {
    if (err instanceof ApiError && err.status === 404) {
      invalidateOperational()
      setOpenId(null)
      toast({
        title: `${noun} no longer exists`,
        description: 'It was already gone — cleared from health.',
      })
      return true
    }
    return false
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
    onError: (err) => {
      if (!handlePhantomTarget(err, 'Author')) errorToast('Author repair failed', getApiErrorMessage(err))
    },
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
    onError: (err) => {
      if (!handlePhantomTarget(err, 'Author')) errorToast('Historical backfill failed', getApiErrorMessage(err))
    },
  })
  const refreshMonitorMutation = useMutation({
    mutationFn: (monitorId: string) => refreshFeedMonitor(monitorId),
    onSuccess: () => {
      invalidateOperational(['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Monitor refresh queued', description: 'Running in Activity.' })
    },
    onError: (err) => {
      if (!handlePhantomTarget(err, 'Monitor')) errorToast('Monitor refresh failed', getApiErrorMessage(err))
    },
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
          ownerParams: { focus: 'needs-attention' },
        },
        { count: monitorsDegraded, countLabel: 'degraded', healthyLabel: 'all healthy' },
      ),
      mk(
        {
          id: 'sources',
          name: 'Upstream sources',
          icon: Globe,
          description: 'OpenAlex, Crossref & Semantic Scholar — the APIs that resolve and enrich papers.',
          note: 'HTTP behaviour aggregated over recent feed + discovery refreshes — a rolling window, not a live probe of right now.',
          states: at('sources'),
          reviewItems: badSources.map((s, i) => ({
            id: String(i),
            primary: s.source || 'Source',
            secondary: humanizeSourceError(s),
          })),
          ownerPage: 'settings',
          ownerParams: { anchor: 'external-apis' },
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
          ownerParams: { anchor: 'ai-config' },
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
          reviewOverflow: Math.max(0, degradedAuthors.length - 12),
          ownerPage: 'authors',
          ownerParams: { focus: 'needs-attention' },
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
            ownerPage: 'alerts',
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
            // Name the configured plugins so "1 configured" isn't a mystery.
            // Unhealthy ones surface as actionable states (with a Test button)
            // above, so here we list the healthy / not-yet-tested ones.
            reviewItems: (op?.plugins ?? [])
              .filter((p) => p.is_configured && p.is_healthy !== false)
              .map((p, i) => ({
                id: String(i),
                primary: p.display_name || p.name,
                secondary: p.is_healthy === true ? 'Configured · healthy' : 'Configured · not yet tested',
              })),
            ownerPage: 'settings',
            ownerParams: { anchor: 'channels' },
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
            ownerPage: 'insights',
            ownerParams: { tab: 'activity' },
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
      <div className="flex flex-wrap gap-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="h-9 w-36 animate-pulse rounded-sm bg-surface-2" />
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

  // Derive the open component from the LIVE list so remediations reflect in place.
  const openComp = openId != null ? components.find((c) => c.id === openId) ?? null : null
  const hasIssues = (c: SystemComponent) => c.states.length > 0 || c.reviewItems.length > 0
  const HeaderIcon = openComp?.icon
  // Name the destination so the CTA isn't a generic "Open owner".
  const OWNER_LABEL: Record<string, string> = {
    authors: 'Open in Authors',
    settings: 'Open in Settings',
    insights: 'Open in Activity',
    alerts: 'Open in Alerts',
    feed: 'Open in Feed',
    discovery: 'Open in Discovery',
    library: 'Open in Library',
  }

  return (
    <>
      {/* One-line strip — every component is a clickable chip; the colored dot
          is the at-a-glance status, full detail opens in the centered popup. */}
      <div className="flex flex-wrap gap-2">
        {components.map((c, i) => {
          const Icon = c.icon
          return (
            <motion.button
              key={c.id}
              type="button"
              onClick={() => setOpenId(c.id)}
              initial={reducedMotion ? false : { opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3, ease: 'easeOut', delay: 0.03 * i }}
              aria-label={`${c.name}: ${severityLabel(c.severity)} — ${c.metric}`}
              className="group flex min-w-[150px] flex-1 items-start gap-2 rounded-sm border border-[var(--color-border)] bg-surface-2 px-3 py-2 text-left transition-colors hover:border-alma-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio"
            >
              <Icon className="mt-0.5 h-4 w-4 shrink-0 text-alma-500" />
              <span className="min-w-0 flex-1">
                {/* Line 1 — component name + status dot. */}
                <span className="flex items-center gap-1.5">
                  <span className={cn('h-2 w-2 shrink-0 rounded-full', DOT[c.severity])} />
                  <span className="truncate text-sm font-medium text-alma-800">{c.name}</span>
                </span>
                {/* Line 2 — the metric. */}
                <span className="mt-0.5 block truncate text-xs tabular-nums text-slate-500">{c.metric}</span>
              </span>
            </motion.button>
          )
        })}
      </div>

      {/* Centered per-component detail — what's healthy / how it's configured,
          or the degraded issues + one-click remediation. One Dialog, no route. */}
      <Dialog open={openComp != null} onOpenChange={(o) => !o && setOpenId(null)}>
        <DialogContent className="max-w-lg bg-surface-1">
          {openComp ? (
            <>
              <DialogHeader>
                <DialogTitle className="flex flex-wrap items-center gap-2 text-alma-900">
                  {HeaderIcon ? <HeaderIcon className="h-5 w-5 text-alma-600" /> : null}
                  {openComp.name}
                  <StatusBadge tone={dimensionBadgeTone(openComp.severity)} size="sm" className="capitalize">
                    {severityLabel(openComp.severity)}
                  </StatusBadge>
                </DialogTitle>
                <DialogDescription className="text-slate-600">{openComp.description}</DialogDescription>
              </DialogHeader>

              {openComp.note ? <p className="-mt-1 text-xs italic text-slate-500">{openComp.note}</p> : null}

              {hasIssues(openComp) ? (
                <div className="space-y-2">
                  {/* Degraded states with their one-click remediation. */}
                  {openComp.states.map((state) => (
                    <div
                      key={state.id}
                      className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-3"
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
                      className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-3"
                    >
                      <p className="text-sm font-medium text-alma-800">{item.primary}</p>
                      {item.secondary ? <p className="mt-1 text-xs text-slate-500">{item.secondary}</p> : null}
                    </div>
                  ))}
                  {openComp.reviewOverflow ? (
                    <p className="px-1 text-xs text-slate-500">
                      +{openComp.reviewOverflow} more — open {OWNER_LABEL[openComp.ownerPage]?.replace('Open in ', '') ?? 'the owner'} to see all.
                    </p>
                  ) : null}
                </div>
              ) : (
                /* Healthy — explain in plain English what "healthy" means here. */
                <Alert variant="success">
                  <CheckCircle2 className="h-4 w-4" />
                  <AlertDescription>{HEALTHY_NOTE[openComp.id] ?? `${openComp.name} is healthy.`}</AlertDescription>
                </Alert>
              )}

              <div className="flex justify-end">
                <AsyncButton
                  size="sm"
                  variant="outline"
                  icon={<Activity className="h-4 w-4" />}
                  onClick={() => {
                    const page = openComp.ownerPage
                    setOpenId(null)
                    navigateTo(
                      page as Parameters<typeof buildHashRoute>[0],
                      openComp.ownerParams ?? {},
                    )
                  }}
                >
                  {OWNER_LABEL[openComp.ownerPage] ?? 'Open owner'}
                </AsyncButton>
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>
    </>
  )
}
