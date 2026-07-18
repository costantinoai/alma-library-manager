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
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
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
import { formatPaperDate } from '@/lib/format'
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
  /** Overrides the static HEALTHY_NOTE when the healthy text depends on data
   *  (e.g. AI with no provider configured). */
  healthyNote?: string
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
  label?: string | null
  author_name?: string | null
  health?: string
  health_reason?: string | null
  last_error?: string | null
  last_checked_at?: string | null
}
interface AuthorRow {
  author_name?: string
  health_reason?: string
  last_error?: string
  last_checked_at?: string | null
}
// One identity-resolution attention row (authors section `identity_attention`),
// from the same canonical builder the Authors page needs-attention list uses.
interface IdentityRow {
  author_name?: string | null
  status?: string
  reason?: string | null
  updated_at?: string | null
}
// One followed author's historical-corpus state (authors section `corpus_health`).
interface CorpusRow {
  author_name?: string | null
  state?: string
  detail?: string | null
  last_success_at?: string | null
}
interface SourceRow {
  source?: string
  http_errors?: number
  transport_errors?: number
  requests?: number
  operations?: number
  retries?: number
  status_counts?: Record<string, number>
  top_endpoints?: { path?: string; count?: number }[]
  last_error?: string
  /** ISO timestamps of the ops that touched this source (aggregation stamps). */
  first_seen?: string | null
  last_seen?: string | null
  /** Newest op in which this source had errors — the staleness signal. */
  last_error_at?: string | null
}

const worstSeverity = (states: OperationalState[], floor: Severity): Severity => {
  const ranked = [floor, ...states.map((s) => (s.severity as Severity) ?? 'warning')]
  return ranked.reduce((worst, s) => (severityRank(s) < severityRank(worst) ? s : worst), 'ok')
}

// ── Detail-popup ledger model ────────────────────────────────────────────────
// One row per remediation TARGET (an author, a monitor, a source…), its cause
// line joined from the review list by name. Previously the same entity showed
// up TWICE in the popup — a "Repair Alice Smith" button in a state card, and an
// unexplained "Alice Smith" review card further down — with no link between
// action and reason.
interface IssueRow {
  key: string
  name: string
  severity: Severity
  causes: string[]
  targets: OperationalTarget[]
}
const sameEntity = (a: string, b: string): boolean => {
  const x = a.trim().toLowerCase()
  const y = b.trim().toLowerCase()
  return x === y || x.startsWith(y) || y.startsWith(x)
}
function buildIssueRows(comp: SystemComponent): {
  rows: IssueRow[]
  broadcast: OperationalState[]
  review: ReviewItem[]
} {
  const consumed = new Set<string>()
  const rows: IssueRow[] = []
  const broadcast: OperationalState[] = [] // states with no per-entity target (e.g. slack_unconfigured)
  for (const s of comp.states) {
    const targets = s.targets ?? []
    if (!targets.length) {
      broadcast.push(s)
      continue
    }
    // Group a target's multiple actions (repair + backfill) into ONE row.
    const byLabel = new Map<string, OperationalTarget[]>()
    for (const t of targets) byLabel.set(t.label, [...(byLabel.get(t.label) ?? []), t])
    for (const [label, ts] of byLabel) {
      const causes: string[] = []
      for (const r of comp.reviewItems) {
        if (!consumed.has(r.id) && r.secondary && sameEntity(r.primary, label)) {
          causes.push(r.secondary)
          consumed.add(r.id)
        }
      }
      if (!causes.length && s.detail) causes.push(s.detail)
      rows.push({
        key: `${s.id}-${label}`,
        name: label,
        severity: (s.severity as Severity) ?? 'warning',
        causes: causes.slice(0, 2),
        targets: ts,
      })
    }
  }
  return { rows, broadcast, review: comp.reviewItems.filter((r) => !consumed.has(r.id)) }
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

// Source-specific "what to do about 429s". Only Semantic Scholar has API keys;
// Crossref's faster polite pool is unlocked by a contact email, not a key; and
// arXiv has neither — its throttle is purely pace-based, so the only honest
// advice is "ALMa paces and retries". A single generic "get an API key" line
// here would be false for every source but Semantic Scholar.
const RATE_LIMIT_ADVICE: Record<string, string> = {
  semantic_scholar:
    'ALMa backs off and retries; a Semantic Scholar API key (Settings → External APIs) moves requests off the shared anonymous pool.',
  crossref:
    "ALMa backs off and retries; a contact email (Settings → External APIs) moves requests to Crossref's faster polite pool.",
  unpaywall:
    'ALMa backs off and retries; Unpaywall requires a contact email (Settings → External APIs).',
  arxiv:
    'ALMa paces arXiv calls (~1 per 3 s) and retries — arXiv has no API keys, so throttling clears on its own.',
}
const DEFAULT_RATE_LIMIT_ADVICE = 'ALMa backs off and retries.'

// Machine health_reason codes → plain English. Raw tokens like
// "missing_openalex_id_for_scholar_monitor" told the user nothing about why a
// monitor/author was flagged; unknown codes fall through verbatim.
const REASON_LABEL: Record<string, string> = {
  missing_openalex_id_for_scholar_monitor:
    'Scholar monitor is missing its OpenAlex bridge id — refreshes cannot resolve this author',
  missing_author_id: 'monitor lost its link to the author record',
  missing_author_monitor: 'no feed monitor is mirroring this followed author',
  operation_failed: 'the last refresh attempt failed',
}
const humanizeReason = (reason?: string | null): string | undefined =>
  reason ? REASON_LABEL[reason] ?? reason : undefined

// Corpus backfill state → what it means for the "maintenance due" flag.
const CORPUS_STATE_LABEL: Record<string, string> = {
  stale: 'backfill is stale — new papers since the last successful run',
  thin: 'backfill looks thin — fewer papers than this author has published',
  failed: 'last backfill failed',
  unverified: 'backfill never verified — coverage unknown',
  pending: 'backfill queued — runs when the system is idle',
  running: 'backfill running now',
}

// Secondary line for a degraded monitor/author row: cause + age, never a bare
// machine token. `last_error` (an exception string) only adds signal when the
// reason itself is a failure.
const describeDegraded = (r: {
  health_reason?: string | null
  last_error?: string | null
  last_checked_at?: string | null
}): string => {
  const reason = humanizeReason(r.health_reason) ?? 'degraded'
  const err = r.health_reason === 'operation_failed' && r.last_error ? ` — ${r.last_error}` : ''
  const checked = r.last_checked_at ? ` · last checked ${formatPaperDate(r.last_checked_at)}` : ''
  return `${reason}${err}${checked}`
}

// A source whose newest error is older than this is history, not a live
// problem: the diagnostics window is count-based (the last 45 feed + 45
// discovery refreshes, however old), so on a low-traffic instance one old
// blip would otherwise keep the card "warning" indefinitely. Stale rows stay
// visible in the popup, clearly dated, but don't color the card.
const STALE_ERROR_DAYS = 14
const isFreshError = (s: SourceRow): boolean => {
  if (!s.last_error_at) return true // no timestamp → don't silently downgrade
  const age = Date.now() - new Date(s.last_error_at).getTime()
  return Number.isNaN(age) ? true : age <= STALE_ERROR_DAYS * 86_400_000
}

// A specific, time-scoped reason for a source's HTTP failures, built from the
// actual status mix — NOT a generic present-tense "is throttling us". The ops
// counted here are the recent refreshes that TOUCHED this source (they can all
// be weeks old), so the window is date-stamped rather than called "the last N".
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
  const first = formatPaperDate(s.first_seen ?? undefined)
  const last = formatPaperDate(s.last_seen ?? undefined)
  const span = first && last ? ` (${first === last ? last : `${first} – ${last}`})` : ''
  const window = ops > 0 ? ` across ${ops} refresh${ops === 1 ? '' : 'es'}${span}` : ' recently'
  // Requests are counted per ATTEMPT — a retried call adds a row per try — so
  // say so when retries are in the mix instead of overstating distinct failures.
  const retries = s.retries ?? 0
  const attempts = retries > 0 ? `${httpErr} of ${reqs} attempts failed (${retries} were retries)` : `${httpErr} of ${reqs} requests failed`
  const lead = breakdown
    ? `${breakdown} — ${attempts}${window}.`
    : `${httpErr} HTTP / ${transportErr} transport error${httpErr + transportErr === 1 ? '' : 's'}${window}.`
  const topEndpoint = (s.top_endpoints ?? []).find((e) => e.path)
  const where = topEndpoint?.path ? ` Mostly ${topEndpoint.path}.` : ''
  const throttled = (sc['429'] ?? 0) > 0 ? ` ${RATE_LIMIT_ADVICE[s.source ?? ''] ?? DEFAULT_RATE_LIMIT_ADVICE}` : ''
  const badRequest = (sc['400'] ?? 0) > 0
    ? ' HTTP 400s mean ALMa sent a malformed request — an app bug, not upstream throttling.'
    : ''
  // The status breakdown already names HTTP errors; last_error only adds
  // information for transport failures (timeouts, DNS…), which have no code.
  const transport = transportErr > 0 && s.last_error ? ` Last transport error: ${s.last_error}.` : ''
  return lead + where + throttled + badRequest + transport
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
  sources: 'Recent feed and discovery refreshes completed without upstream HTTP errors (Semantic Scholar, Crossref, arXiv and the other enrichment APIs).',
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
  const enableSource = async (sourceName: string) => {
    const current = await getDiscoverySettings()
    const currentSources = current.sources as unknown as Record<string, { enabled: boolean; weight: number }>
    return updateDiscoverySettings({
      ...current,
      sources: {
        ...current.sources,
        [sourceName]: { ...(currentSources[sourceName] ?? { enabled: false, weight: 1 }), enabled: true },
      },
    })
  }
  const enableSourceMutation = useMutation({
    mutationFn: enableSource,
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

  // action → how to extract its argument, which mutation to fire, its verb.
  // Verbs are SHORT — the dialog renders one row per entity with the name on
  // the row, so "Repair Alice Smith" would say the name twice.
  type RemediationEntry = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    mutation: UseMutationResult<any, unknown, any, unknown>
    getArg: (t: OperationalTarget) => string | undefined
    verb: string
    argless?: boolean
    icon?: React.ReactNode
  }
  const remediation: Record<string, RemediationEntry> = {
    repair_author: { mutation: repairAuthorMutation, getArg: (t) => t.author_id ?? undefined, verb: 'Repair' },
    backfill_author: { mutation: historyBackfillMutation, getArg: (t) => t.author_id ?? undefined, verb: 'Backfill' },
    refresh_monitor: { mutation: refreshMonitorMutation, getArg: (t) => t.monitor_id ?? undefined, verb: 'Refresh' },
    enable_source: { mutation: enableSourceMutation, getArg: (t) => t.source ?? undefined, verb: 'Enable' },
    evaluate_alert: { mutation: evaluateAlertMutation, getArg: (t) => t.alert_id ?? undefined, verb: 'Re-run' },
    test_plugin: { mutation: testPluginMutation, getArg: (t) => t.plugin_name ?? undefined, verb: 'Test' },
    compute_stale_embeddings: { mutation: computeStaleEmbeddingsMutation, getArg: () => 'compute_stale_embeddings', verb: 'Recompute', argless: true, icon: <Cpu className="mr-1 h-4 w-4" /> },
    clear_similarity_cache: { mutation: clearSimilarityCacheMutation, getArg: () => 'clear_similarity_cache', verb: 'Clear', argless: true },
  }

  // ── "Fix all": run every row's action once, sequentially (kind to the
  // backend's writer gate), with ONE summary toast + ONE refetch at the end —
  // going through the per-row mutations would fire a toast per entity.
  const [fixAllPending, setFixAllPending] = useState(false)
  const directRun: Record<string, (t: OperationalTarget) => Promise<unknown> | null> = {
    repair_author: (t) => (t.author_id ? repairAuthor(t.author_id) : null),
    backfill_author: (t) => (t.author_id ? queueAuthorHistoryBackfill(t.author_id) : null),
    refresh_monitor: (t) => (t.monitor_id ? refreshFeedMonitor(t.monitor_id) : null),
    enable_source: (t) => (t.source ? enableSource(t.source) : null),
    evaluate_alert: (t) => (t.alert_id ? evaluateAlert(t.alert_id) : null),
    test_plugin: (t) => (t.plugin_name ? testPluginConnection(t.plugin_name) : null),
    compute_stale_embeddings: () => api.post('/ai/compute-embeddings?scope=stale'),
    clear_similarity_cache: () => clearDiscoverySimilarityCache(),
  }
  const runnableTargets = (rows: IssueRow[]): OperationalTarget[] =>
    rows.flatMap((r) => r.targets).filter((t) => !!directRun[t.action] && !!remediation[t.action]?.getArg(t))
  async function fixAll(rows: IssueRow[]) {
    const targets = rows.flatMap((r) => r.targets)
    setFixAllPending(true)
    let ok = 0
    let failed = 0
    for (const t of targets) {
      const fn = directRun[t.action]
      if (!fn) continue
      try {
        const p = fn(t)
        if (!p) continue
        await p
        ok++
      } catch {
        failed++ // a 404 phantom or transient failure — the refetch below re-syncs the list
      }
    }
    setFixAllPending(false)
    invalidateOperational(['authors'], ['feed-monitors'], ['feed-inbox'], ['alerts'], ['discovery-settings'])
    if (failed === 0) {
      toast({ title: `Queued ${ok} fix${ok === 1 ? '' : 'es'}`, description: 'Track progress in Activity.' })
    } else {
      errorToast(`Queued ${ok}, ${failed} failed`, 'The list refreshes with what remains.')
    }
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

    // Monitors: the `monitors` list is a top-20 UI slice — the summary count is
    // the uncapped truth, so the chip metric and the "+N more" math use it.
    const monitorRows = ((sections.feed.data?.monitors ?? []) as unknown as MonitorRow[]).filter(
      (m) => m.health && m.health !== 'ready' && m.health !== 'disabled',
    )
    const monitorsDegraded = Math.max(
      sections.feed.data?.summary?.degraded_monitors ?? 0,
      monitorRows.length,
    )
    // Authors: same pattern — `degraded` / `corpus_health` are capped lists,
    // the summary counters are uncapped.
    const authorsSummary = sections.authors.data?.summary
    const degradedAuthors = (sections.authors.data?.degraded ?? []) as unknown as AuthorRow[]
    const degradedAuthorsTotal = Math.max(
      authorsSummary?.degraded_tracked ?? 0,
      degradedAuthors.length,
    )
    // Identity-resolution attention — same canonical rows the Authors page
    // shows, so this popup and that page can never name different authors.
    const identityRows = (sections.authors.data?.identity_attention ?? []) as unknown as IdentityRow[]
    const identityTotal = Math.max(
      (authorsSummary as { identity_attention_count?: number } | undefined)?.identity_attention_count ?? 0,
      identityRows.length,
    )
    const IDENTITY_STATUS_LABEL: Record<string, string> = {
      error: 'last identity refresh failed',
      no_match: 'OpenAlex found no match for this name',
      needs_manual_review: 'identity candidates need a manual pick',
    }
    const corpusRows = (sections.authors.data?.corpus_health ?? []) as unknown as CorpusRow[]
    const corpusAttentionRows = corpusRows.filter((c) =>
      ['stale', 'thin', 'failed', 'unverified'].includes(c.state ?? ''),
    )
    const corpusBacklogTotal = Math.max(
      (authorsSummary?.stale_backfills ?? 0) +
        (authorsSummary?.thin_backfills ?? 0) +
        (authorsSummary?.failed_backfills ?? 0) +
        (authorsSummary?.unverified_backfills ?? 0),
      corpusAttentionRows.length,
    )
    const pendingBackfills = authorsSummary?.pending_backfills ?? 0
    // AI: the chip is the provider's operational state — saying "operational"
    // with NO provider configured was untrue (AI is opt-in, so absence is a
    // neutral fact, not an error — but it must be said).
    const aiProvider = sections.ai.data?.summary?.embedding_provider ?? 'none'
    const erroredSources = ((sections.discovery.data?.source_diagnostics ?? []) as unknown as SourceRow[]).filter(
      (s) => (s.http_errors ?? 0) > 0 || (s.transport_errors ?? 0) > 0,
    )
    // Only sources with a RECENT error color the card; older blips stay
    // listed in the popup (dated) until newer refreshes displace them.
    const badSources = erroredSources.filter(isFreshError)
    const staleSources = erroredSources.filter((s) => !isFreshError(s))
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
          note: 'Health reflects the last refresh attempt — a fixed monitor stays flagged until its next successful refresh.',
          states: at('monitors'),
          // Name EVERY degraded monitor with cause + age — the actionable state
          // above only carries refresh buttons for the first few.
          reviewItems: monitorRows.slice(0, 12).map((m, i) => ({
            id: String(i),
            primary: m.label || m.author_name || 'Monitor',
            secondary: describeDegraded(m),
          })),
          reviewOverflow: Math.max(0, monitorsDegraded - Math.min(monitorRows.length, 12)),
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
          // Names the sources this card actually observes — OpenAlex goes
          // through its own client and is tracked by the API budget card, so
          // claiming it here would be untrue.
          description: 'Semantic Scholar, Crossref, arXiv & the other enrichment APIs called during feed and discovery refreshes.',
          note: `HTTP behaviour aggregated over recent feed + discovery refreshes — a dated window, not a live probe of right now. Errors older than ${STALE_ERROR_DAYS} days are listed but don't affect status. OpenAlex is tracked separately in the API budget card.`,
          states: at('sources'),
          reviewItems: [
            ...badSources.map((s, i) => ({
              id: `fresh-${i}`,
              primary: s.source || 'Source',
              secondary: humanizeSourceError(s),
            })),
            ...staleSources.map((s, i) => ({
              id: `stale-${i}`,
              primary: `${s.source || 'Source'} — resolved`,
              secondary: `${humanizeSourceError(s)} No errors since ${formatPaperDate(s.last_error_at ?? undefined) || 'then'}.`,
            })),
          ],
          ownerPage: 'settings',
          ownerParams: { anchor: 'external-apis' },
        },
        {
          count: badSources.length,
          // "recent" is load-bearing: errors older than STALE_ERROR_DAYS are
          // listed in the popup (dated) but don't count here.
          countLabel: 'with recent errors',
          healthyLabel: staleSources.length > 0 ? 'no recent errors' : 'all reachable',
          // Disabled sources raise an info state, so severity is never 'ok'
          // and the healthyLabel can't render — name the actual situation
          // instead of a vague "needs attention".
          attentionLabel: disabledSources > 0 ? `${disabledSources} disabled` : 'needs attention',
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
          healthyNote:
            aiProvider === 'none'
              ? 'No embedding provider is configured — Discovery similarity and the paper map are off. AI is opt-in: enable a provider under Settings → AI.'
              : undefined,
          states: at('ai'),
          reviewItems: [],
          ownerPage: 'settings',
          ownerParams: { anchor: 'ai-config' },
        },
        {
          count: 0,
          countLabel: 'issues',
          // "operational" with no provider configured was untrue — absence is a
          // neutral opt-in fact, stated plainly.
          healthyLabel: aiProvider === 'none' ? 'no provider configured' : 'operational',
          attentionLabel: 'needs attention',
        },
      ),
      mk(
        {
          id: 'authors',
          name: 'Tracked authors',
          icon: Users,
          description: 'Followed authors whose identity bridge or historical corpus needs maintenance.',
          note: 'Flags persist until the next successful refresh or backfill — fixing the cause does not clear them instantly.',
          states: at('authors'),
          // Per-author WHY: identity-resolution failures first (same rows the
          // Authors page shows), then degraded monitors (cause + age), then
          // each corpus whose backfill is stale/thin/failed.
          reviewItems: [
            ...identityRows.map((a, i) => ({
              id: `ident-${i}`,
              primary: a.author_name || 'Author',
              secondary:
                (a.reason && a.reason.trim()) ||
                IDENTITY_STATUS_LABEL[a.status ?? ''] ||
                'identity needs manual attention',
            })),
            ...degradedAuthors.slice(0, 12).map((a, i) => ({
              id: `deg-${i}`,
              primary: a.author_name || 'Author',
              secondary: describeDegraded(a),
            })),
            ...corpusAttentionRows.map((c, i) => ({
              id: `corpus-${i}`,
              primary: c.author_name || 'Author',
              secondary: `${CORPUS_STATE_LABEL[c.state ?? ''] ?? c.state}${
                c.detail ? ` — ${c.detail}` : ''
              } · last successful backfill ${formatPaperDate(c.last_success_at ?? undefined) || 'never'}`,
            })),
          ],
          reviewOverflow:
            Math.max(0, identityTotal - identityRows.length) +
            Math.max(0, degradedAuthorsTotal - Math.min(degradedAuthors.length, 12)) +
            Math.max(0, corpusBacklogTotal - corpusAttentionRows.length),
          ownerPage: 'authors',
          ownerParams: { focus: 'needs-attention' },
        },
        {
          count: degradedAuthorsTotal + identityTotal,
          countLabel: 'need attention',
          healthyLabel: 'all healthy',
          // "maintenance due" said nothing about WHY — name the backlog.
          attentionLabel:
            corpusBacklogTotal > 0
              ? `${corpusBacklogTotal} corpora need backfill`
              : pendingBackfills > 0
                ? `${pendingBackfills} backfills queued`
                : 'maintenance due',
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
            // focus=failed → the Activity tab scrolls to + rings the
            // Background-operations card naming each failure.
            ownerParams: { tab: 'activity', focus: 'failed' },
          },
          { count: failedJobs, countLabel: 'failed (24h)', healthyLabel: 'all healthy' },
        ),
      )
    }

    // Worst-first so a critical/degraded card leads the grid.
    return list.sort((a, b) => severityRank(a.severity) - severityRank(b.severity))
  }, [
    sections.operational.data,
    sections.feed.data,
    sections.authors.data,
    sections.discovery.data,
    sections.ai.data,
  ])

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
        <DialogContent className="max-w-xl bg-surface-1">
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

              {hasIssues(openComp) ? (
                (() => {
                  const { rows, broadcast, review } = buildIssueRows(openComp)
                  const sectioned = rows.length > 0 && review.length > 0
                  return (
                    /* Ledger, not stacked cards: hairline-divided rows inside one
                       quiet panel, capped height with internal scroll so a long
                       author list can never push the dialog off screen. */
                    <div className="max-h-[55vh] space-y-3 overflow-y-auto pr-1">
                      {/* Component-wide conditions with no per-entity fix. */}
                      {broadcast.map((state) => (
                        <div
                          key={state.id}
                          className="rounded-sm border border-[var(--color-border)] bg-surface-2 px-3 py-2.5"
                        >
                          <p className="flex items-center gap-2 text-sm font-medium text-alma-800">
                            <span className={cn('h-2 w-2 shrink-0 rounded-full', DOT[(state.severity as Severity) ?? 'warning'])} />
                            {state.label}
                          </p>
                          {state.detail ? <p className="mt-1 pl-4 text-xs leading-relaxed text-slate-500">{state.detail}</p> : null}
                        </div>
                      ))}

                      {/* One row per entity: name + status dot, its own action
                          verbs on the right, the WHY directly underneath. */}
                      {rows.length > 0 ? (
                        <div>
                          <div className="mb-1.5 flex items-center justify-between gap-3">
                            <EyebrowLabel tone="muted">Fix now</EyebrowLabel>
                            {runnableTargets(rows).length > 1 ? (
                              <AsyncButton
                                size="xs"
                                variant="outline"
                                pending={fixAllPending}
                                onClick={() => fixAll(rows)}
                              >
                                Fix all · {runnableTargets(rows).length}
                              </AsyncButton>
                            ) : null}
                          </div>
                          <div className="divide-y divide-[var(--color-border)] rounded-sm border border-[var(--color-border)] bg-surface-2">
                            {rows.map((row) => (
                              <div key={row.key} className="px-3 py-2.5">
                                <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1.5">
                                  <span className="flex min-w-0 flex-1 items-center gap-2">
                                    <span className={cn('h-2 w-2 shrink-0 rounded-full', DOT[row.severity])} />
                                    <span className="truncate text-sm font-medium text-alma-800">{row.name}</span>
                                  </span>
                                  <span className="flex shrink-0 flex-wrap gap-1.5">
                                    {row.targets.map((target) => {
                                      const handler = remediation[target.action]
                                      if (!handler) return null
                                      const arg = handler.getArg(target)
                                      if (!arg) return null
                                      const pending =
                                        handler.mutation.isPending && (handler.argless || handler.mutation.variables === arg)
                                      return (
                                        <AsyncButton
                                          key={`${row.key}-${target.id ?? arg}-${target.action}`}
                                          size="sm"
                                          variant="outline"
                                          icon={handler.icon}
                                          pending={pending}
                                          disabled={fixAllPending}
                                          onClick={() => (handler.argless ? handler.mutation.mutate(undefined) : handler.mutation.mutate(arg))}
                                        >
                                          {handler.verb}
                                        </AsyncButton>
                                      )
                                    })}
                                  </span>
                                </div>
                                {row.causes.map((cause, i) => (
                                  <p key={i} title={cause} className="mt-1 line-clamp-2 pl-4 text-xs leading-relaxed text-slate-500">
                                    {cause}
                                  </p>
                                ))}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}

                      {/* Flagged but with no one-click fix (rate-limited sources,
                          corpus backlog beyond the actionable slice…). */}
                      {review.length > 0 ? (
                        <div>
                          {sectioned ? <EyebrowLabel tone="muted" className="mb-1.5">Also flagged</EyebrowLabel> : null}
                          <div className="divide-y divide-[var(--color-border)] rounded-sm border border-[var(--color-border)] bg-surface-2">
                            {review.map((item) => (
                              <div key={`review-${item.id}`} className="px-3 py-2.5">
                                <p className="flex items-center gap-2 text-sm font-medium text-alma-800">
                                  <span className="h-2 w-2 shrink-0 rounded-full border border-warning-500" />
                                  <span className="truncate">{item.primary}</span>
                                </p>
                                {item.secondary ? (
                                  <p title={item.secondary} className="mt-1 line-clamp-2 pl-4 text-xs leading-relaxed text-slate-500">
                                    {item.secondary}
                                  </p>
                                ) : null}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      {openComp.reviewOverflow ? (
                        <p className="px-1 text-xs text-slate-500">
                          +{openComp.reviewOverflow} more — open {OWNER_LABEL[openComp.ownerPage]?.replace('Open in ', '') ?? 'the owner'} to see all.
                        </p>
                      ) : null}
                    </div>
                  )
                })()
              ) : (
                /* Healthy — explain in plain English what "healthy" means here. */
                <Alert variant="success">
                  <CheckCircle2 className="h-4 w-4" />
                  <AlertDescription>
                    {openComp.healthyNote ?? HEALTHY_NOTE[openComp.id] ?? `${openComp.name} is healthy.`}
                  </AlertDescription>
                </Alert>
              )}

              {/* Footer: the measurement caveat sits with the exit action, out
                  of the reading path. */}
              <div className="flex items-center justify-between gap-4 border-t border-[var(--color-border)] pt-3">
                {openComp.note ? (
                  <p className="min-w-0 flex-1 text-[11px] italic leading-snug text-slate-500">{openComp.note}</p>
                ) : <span />}
                <AsyncButton
                  size="sm"
                  variant="outline"
                  icon={<Activity className="h-4 w-4" />}
                  className="shrink-0"
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
