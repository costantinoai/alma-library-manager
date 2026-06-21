import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  CalendarClock,
  ExternalLink,
  LayoutGrid,
  LayoutList,
  Loader2,
  RefreshCw,
  Rows3,
  Search,
  Settings2,
  UserRound,
} from 'lucide-react'

import {
  feedAdd,
  feedBulkAction,
  feedDislike,
  getApiErrorMessage,
  feedDismiss,
  feedUndoDismiss,
  feedLike,
  feedLove,
  getFeedStatus,
  getFeedSettings,
  updateFeedSettings,
  listFeedMonitors,
  listFeedInbox,
  refreshFeedInbox,
  removeFromLibrary,
  updateReadingStatus,
  type FeedAction,
  type FeedInboxItem,
  type FeedItemStatus,
  type Publication,
} from '@/api/client'
import { PaperDetailPanel } from '@/components/discovery'
import { PageTour, FEED_TOUR } from '@/components/onboarding'
import type { PaperReaction } from '@/components/discovery/PaperActionBar'
import { JargonHint, ListControlBar, PaperCard, RefreshRunningBanner } from '@/components/shared'
import { Switch } from '@/components/ui/switch'
import { RevealList, RevealItem } from '@/components/ui/reveal'
import { DataTable } from '@/components/ui/data-table'
import type { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { SkeletonList } from '@/components/shared'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { useToast, errorToast} from '@/hooks/useToast'
import { usePaperAuthorFollow } from '@/hooks/usePaperAuthorFollow'
import { usePaperUndo } from '@/hooks/usePaperUndo'
import { buildHashRoute, navigateTo, useHashRoute } from '@/lib/hashRoute'
import { invalidateAfterFeedRefresh, invalidateQueries } from '@/lib/queryHelpers'
import { formatDate, formatMonitorTypeLabel, formatPublicationDate, formatRelativeShort, formatTimestamp } from '@/lib/utils'
import { StatusBadge } from '@/components/ui/status-badge'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

type FeedFilter = 'all' | 'new'
type FeedSort = 'chronological' | 'relevance'
type FeedViewMode = 'normal' | 'extended' | 'compact'

/**
 * Feed is chronological and truthful: `feed_items.status` holds whichever
 * reaction the user last applied (apply_feed_action always overwrites), so we
 * can treat it as the single source of truth. `add` is "saved without a
 * preference signal" — it toggles the Save button into "Saved" state but is
 * not itself a like/love/dislike reaction.
 */
function deriveFeedReaction(status?: string | null): PaperReaction {
  if (status === 'like' || status === 'love' || status === 'dislike') return status
  return null
}

function deriveFeedIsSaved(itemStatus?: string | null, paperStatus?: string | null): boolean {
  // Resolved library membership is authoritative: a paper just removed from
  // the Library (status 'removed') reads as not-saved even though the feed
  // item still carries its old 'add'/'like'/'love' action — that's what lets
  // the Save button toggle off. Fall back to the feed action only while the
  // paper row is still unresolved.
  if (paperStatus === 'library') return true
  if (paperStatus === 'removed') return false
  return itemStatus === 'add' || itemStatus === 'like' || itemStatus === 'love'
}

const FEED_STATUS_LABELS: Record<FeedItemStatus, string> = {
  new: 'New',
  add: 'Saved',
  like: 'Liked',
  love: 'Loved',
  dislike: 'Disliked',
  // Dismissed items never appear in the inbox (the list query excludes them),
  // but the label keeps the status map exhaustive over FeedItemStatus.
  dismissed: 'Dismissed',
}

const FEED_FILTERS: readonly FeedFilter[] = ['all', 'new'] as const
const FEED_FILTER_LABELS: Record<FeedFilter, string> = {
  all: 'All',
  new: FEED_STATUS_LABELS.new,
}

function toPublication(item: FeedInboxItem): Publication | null {
  const paper = item.paper
  if (!paper) return null
  return {
    id: paper.id,
    title: paper.title,
    authors: paper.authors ?? '',
    year: paper.year ?? null,
    journal: paper.journal ?? undefined,
    abstract: paper.abstract ?? undefined,
    url: paper.url ?? undefined,
    doi: paper.doi ?? undefined,
    publication_date: paper.publication_date ?? undefined,
    cited_by_count: paper.cited_by_count ?? 0,
    rating: paper.rating ?? 0,
    notes: paper.notes ?? undefined,
    status: paper.status ?? 'tracked',
    added_at: paper.added_at ?? undefined,
    added_from: paper.added_from ?? undefined,
    reading_status: paper.reading_status ?? null,
    openalex_id: paper.openalex_id ?? undefined,
  }
}

function parseBreakdown(raw: unknown): Record<string, any> | null {
  if (!raw) return null
  if (typeof raw === 'object') return raw as Record<string, any>
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw)
    } catch {
      return null
    }
  }
  return null
}

function actionLabel(action: FeedAction): string {
  switch (action) {
    case 'add': return 'Saved to Library with a baseline positive signal'
    case 'like': return 'Saved to Library with a +1 preference signal'
    case 'love': return 'Saved to Library with a +2 preference signal'
    case 'dislike': return 'Recorded a -1 signal and kept the paper out of Library'
    case 'dismiss': return 'Hidden from Feed and recorded a small negative signal'
  }
}

function formatWhyMonitorLabel(monitor: { monitor_label?: string | null; monitor_type?: string | null }): string {
  const label = monitor.monitor_label?.trim() || 'Unnamed monitor'
  const type = formatMonitorTypeLabel(monitor.monitor_type)
  return `${label} (${type})`
}

function joinWhyParts(parts: string[]): string {
  if (parts.length <= 1) return parts[0] || ''
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`
  return `${parts.slice(0, -1).join(', ')}, and ${parts[parts.length - 1]}`
}

function buildFeedExplanation(item: FeedInboxItem): string | null {
  const reasons: string[] = []
  const matchedAuthors = (item.matched_authors ?? []).filter((name) => name.trim().length > 0)
  const matchedMonitors = item.matched_monitors ?? []

  if (matchedAuthors.length > 0) {
    reasons.push(`matched followed author${matchedAuthors.length === 1 ? '' : 's'} ${joinWhyParts(matchedAuthors)}`)
  }
  if (matchedMonitors.length > 0) {
    reasons.push(`matched ${joinWhyParts(matchedMonitors.map((monitor) => formatWhyMonitorLabel(monitor)))}`)
  }
  if (reasons.length === 0 && item.author_name) {
    reasons.push(`came from followed author ${item.author_name}`)
  }
  if (reasons.length === 0) return null
  return `Included in Feed because it ${joinWhyParts(reasons)}.`
}

export function FeedPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const { followedAuthorNames, pendingAuthorName, followAuthor } = usePaperAuthorFollow()
  const route = useHashRoute()
  const authorFilter = route.params.get('author')?.trim() ?? ''

  const [filter, setFilter] = useState<FeedFilter>('all')
  const [sort, setSort] = useState<FeedSort>('chronological')
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [viewMode, setViewMode] = useState<FeedViewMode>('normal')
  // U-12: grows by a page on "Load more" so the inbox isn't hard-capped at 60.
  const [feedLimit, setFeedLimit] = useState(60)

  const feedQuery = useQuery({
    queryKey: ['feed-inbox', filter, sort, feedLimit],
    queryFn: () =>
      listFeedInbox({
        status: filter === 'all' ? undefined : filter,
        sort,
        // First fold is 60; "Load more" grows feedLimit by a page (U-12).
        limit: feedLimit,
        offset: 0,
        since_days: 60,
      }),
    retry: 1,
    placeholderData: (previous) => previous,
    // The inbox only gains rows on an explicit refresh (which invalidates this
    // key); a short staleTime stops a full refetch on every filter toggle /
    // window refocus while keeping it fresh enough.
    staleTime: 30_000,
  })

  const monitorQueryState = useQuery({
    queryKey: ['feed-monitors'],
    queryFn: listFeedMonitors,
    retry: 1,
  })

  const feedStatusQuery = useQuery({
    queryKey: ['feed-status'],
    queryFn: getFeedStatus,
    retry: 1,
    refetchInterval: 60_000,
  })

  // Auto-refresh opt-in. The page toggle and Settings drive the same KV-backed
  // flag; flipping it here just persists the setting (the backend scheduler is
  // the executor), so the inbox never blocks on a refresh.
  const feedSettingsQuery = useQuery({
    queryKey: ['feed-settings'],
    queryFn: getFeedSettings,
    retry: 1,
    staleTime: 30_000,
  })
  const autoRefreshMutation = useMutation({
    mutationFn: (next: boolean) => {
      const settings = feedSettingsQuery.data
      if (!settings) throw new Error('settings not loaded')
      // Enabling with an unset/zero interval would register no job — coerce to a
      // sane default (6h) so the page toggle always produces a working schedule.
      const interval =
        next && settings.refresh_interval_hours <= 0 ? 6 : settings.refresh_interval_hours
      return updateFeedSettings({ auto_refresh_enabled: next, refresh_interval_hours: interval })
    },
    onSuccess: async (saved) => {
      await invalidateQueries(queryClient, ['feed-settings'])
      toast({
        title: saved.auto_refresh_enabled ? 'Auto-refresh on' : 'Auto-refresh off',
        description: saved.auto_refresh_enabled
          ? `The feed inbox will refresh in the background every ${saved.refresh_interval_hours}h.`
          : 'The inbox will only refresh when you click Refresh Inbox.',
      })
    },
    onError: () => errorToast('Could not update auto-refresh'),
  })

  // Scope invalidation narrowly to avoid cascading refetches on unrelated pages.
  // Feed *refresh* only touches feed state; triage *actions* also mutate library state.
  // Background-job completion handlers (useOperationToasts) own insights-diagnostics etc.
  const invalidateFeedRefresh = () => invalidateAfterFeedRefresh(queryClient)

  const invalidateFeedAction = async () => {
    // ['bootstrap'] is intentionally NOT invalidated here — per-action
    // invalidation fired a sidebar-badge refetch on every save / like /
    // love / dislike. The feed-unread badge now refreshes via the
    // Sidebar's 5-min interval and after explicit feed-refresh
    // (invalidateAfterFeedRefresh), which is plenty for a personal
    // tool. Tradeoff: badge can lag up to 5 min after a feed action.
    await invalidateQueries(queryClient,
      ['feed-inbox'],
      ['feed-status'],
      ['feed-monitors'],
      ['papers'],
      ['library-saved'],
      ['library-workflow-summary'],
      ['reading-queue'],
    )
  }

  // Reverses a single dismiss (restores the card + drops the negative
  // signal). Wired to the transient "Undo" button on the dismiss toast.
  const undoDismissMutation = useMutation({
    mutationFn: ({ id }: { id: string }) => feedUndoDismiss(id),
    onSuccess: async () => {
      await invalidateFeedAction()
      toast({ title: 'Dismissal undone', description: 'The paper is back in your Feed.' })
    },
    onError: (err) => errorToast('Undo failed', getApiErrorMessage(err)),
  })

  const undoMutation = usePaperUndo()

  const actionMutation = useMutation({
    mutationFn: async ({ id, action }: { id: string; action: FeedAction }) => {
      if (action === 'add') return feedAdd(id)
      if (action === 'like') return feedLike(id)
      if (action === 'love') return feedLove(id)
      if (action === 'dismiss') return feedDismiss(id)
      return feedDislike(id)
    },
    onSuccess: async (_data, vars) => {
      await invalidateFeedAction()
      // Dismiss is the one "forever" action, so it carries a transient Undo
      // affordance; everything else gets the plain confirmation toast.
      if (vars.action === 'dismiss') {
        toast({
          title: 'Dismissed from Feed',
          description: 'Hidden from your Feed with a small negative signal.',
          action: { label: 'Undo', onClick: () => undoDismissMutation.mutate({ id: vars.id }) },
        })
        return
      }
      toast({ title: 'Feed updated', description: actionLabel(vars.action) })
    },
    onError: (err) => {
      errorToast('Action failed', getApiErrorMessage(err))
    },
  })

  // Reading list is orthogonal to library membership (D2 v3). Toggle
  // adds the paper to the reading list (reading_status='reading'); a
  // second click removes it. Writes no feedback signal — purely workflow.
  const queueMutation = useMutation({
    mutationFn: ({ paperId, nextQueued }: { paperId: string; nextQueued: boolean }) =>
      updateReadingStatus(paperId, nextQueued ? 'reading' : null),
    onSuccess: async (_data, vars) => {
      await invalidateFeedAction()
      toast({
        title: vars.nextQueued ? 'Added to reading list' : 'Removed from reading list',
        description: vars.nextQueued
          ? 'Parked for later — save, like, or love it once you’ve read it.'
          : 'The paper is no longer on your reading list.',
      })
    },
    onError: (err) => errorToast('Reading list update failed', getApiErrorMessage(err)),
  })

  // Library membership toggle (D2/D3): the Feed Save button adds to Library;
  // clicking it again removes the paper (soft transition to 'removed', which
  // also writes the small negative signal that Remove-from-Library always
  // carries — same as removing elsewhere).
  const removeFromLibraryMutation = useMutation({
    mutationFn: ({ paperId }: { paperId: string }) => removeFromLibrary(paperId),
    onSuccess: async () => {
      await invalidateFeedAction()
      toast({ title: 'Removed from library', description: 'The paper is no longer in your Library.' })
    },
    onError: (err) => errorToast('Remove failed', getApiErrorMessage(err)),
  })

  const bulkMutation = useMutation({
    mutationFn: ({ action }: { action: FeedAction }) => feedBulkAction(Array.from(selectedIds), action),
    onSuccess: async () => {
      await invalidateFeedAction()
      const appliedCount = selectedIds.size
      setSelectedIds(new Set())
      toast({ title: 'Bulk action applied', description: `${appliedCount} feed items updated.` })
    },
    onError: (err) => {
      errorToast('Bulk action failed', getApiErrorMessage(err))
    },
  })

  const refreshMutation = useMutation({
    mutationFn: refreshFeedInbox,
    onSuccess: async (data) => {
      const operation = (data.operation as Record<string, unknown> | undefined) ?? {}
      const status = String(data.status ?? operation.status ?? '')
      if (status === 'queued' || status === 'running' || status === 'already_running') {
        await invalidateQueries(queryClient, ['activity-operations'])
        toast({
          title: status === 'already_running' ? 'Refresh already running' : 'Feed refresh queued',
          description: data.message || 'Track progress in Activity. Feed will refresh automatically when the job completes.',
        })
        return
      }

      await invalidateFeedRefresh()
      const result = (data.result as Record<string, number> | undefined) ?? {}
      const created = result.items_created ?? 0
      const monitorsTotal = result.monitors_total ?? 0
      const degraded = result.monitors_degraded ?? 0
      toast({
        title: created > 0 ? 'Feed refreshed' : 'No new papers',
        description: created > 0
          ? `Added ${created} new papers across ${monitorsTotal} monitors${degraded > 0 ? ` (${degraded} degraded)` : ''}.`
          : `No new papers found across ${monitorsTotal} monitors${degraded > 0 ? ` (${degraded} degraded)` : ''}.`,
      })
    },
    onError: (err) => errorToast('Refresh failed', getApiErrorMessage(err)),
  })

  const items = useMemo(() => {
    const baseItems = feedQuery.data?.items ?? []
    if (!authorFilter) return baseItems
    return baseItems.filter((item) => {
      const matchedAuthorIds = item.matched_author_ids ?? []
      return item.author_id === authorFilter || matchedAuthorIds.includes(authorFilter)
    })
  }, [authorFilter, feedQuery.data])

  const total = authorFilter ? items.length : (feedQuery.data?.total ?? 0)
  const filteredAuthorLabel = items[0]?.author_name || authorFilter
  const monitors = monitorQueryState.data ?? []
  const readyMonitors = monitors.filter((monitor) => monitor.health === 'ready').length
  const degradedMonitorList = monitors.filter((monitor) => monitor.health === 'degraded')
  const degradedMonitors = degradedMonitorList.length
  const allVisibleSelected = items.length > 0 && items.every((item) => selectedIds.has(item.id))

  const toggleSelection = (feedItemId: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(feedItemId)) next.delete(feedItemId)
      else next.add(feedItemId)
      return next
    })
  }

  const toggleSelectAllVisible = () => {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (allVisibleSelected) {
        for (const item of items) next.delete(item.id)
      } else {
        for (const item of items) next.add(item.id)
      }
      return next
    })
  }

  // Monitor pulse semantics: amber means at least one monitor is degraded and
  // needs user attention; emerald means the whole surface is healthy.
  const pulseTone = degradedMonitors > 0 ? 'amber' : 'emerald'

  return (
    <div className="space-y-4">
      {/* ── Hero strip ─────────────────────────────────────────────────────
          Quiet context header. The TopBar already shows the "Feed" page
          title in font-brand, so this surface doesn't repeat it. Instead it
          carries the description, a live monitor pulse, a one-tap link to
          Settings, and the primary Refresh action.
      ──────────────────────────────────────────────────────────────────── */}
      <section
        data-tour="feed-hero"
        className="relative overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet"
      >
        {/* Flat chrome paper. The gradient was a v2 holdover that read as
            SaaS-y on the bookish bg — paper is honest, no decoration. */}
        <div className="relative flex flex-col gap-4 p-5 md:flex-row md:items-center md:justify-between md:gap-8">
          <div className="min-w-0 flex-1 space-y-2">
            <p className="max-w-xl text-sm leading-relaxed text-slate-600">
              Deterministic monitoring inbox for followed authors and saved topics or queries.
            </p>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-500">
              <span className="inline-flex items-center gap-2">
                <span className="relative flex h-2 w-2" aria-hidden>
                  <span
                    className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 ${
                      pulseTone === 'amber' ? 'bg-warning-500' : 'bg-success-500'
                    }`}
                  />
                  <span
                    className={`relative inline-flex h-2 w-2 rounded-full ${
                      pulseTone === 'amber' ? 'bg-warning-500' : 'bg-success-500'
                    }`}
                  />
                </span>
                <span>
                  <span className="font-semibold tabular-nums text-slate-800">{monitors.length}</span>
                  <span className="ml-1 text-slate-500">monitors</span>
                </span>
              </span>
              <span className="text-slate-300" aria-hidden>·</span>
              <span className="tabular-nums text-success-700">{readyMonitors} ready</span>
              {degradedMonitors > 0 && (
                <>
                  <span className="text-slate-300" aria-hidden>·</span>
                  {/* U-4: surface WHICH monitors are degraded + why, not just a count. */}
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="cursor-help tabular-nums text-warning-700 underline decoration-dotted underline-offset-2">
                        {degradedMonitors} degraded
                      </span>
                    </TooltipTrigger>
                    <TooltipContent side="bottom" align="start" className="max-w-xs">
                      <p className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-slate-400">
                        Degraded monitors
                      </p>
                      <ul className="space-y-1 text-xs">
                        {degradedMonitorList.slice(0, 8).map((monitor) => (
                          <li key={monitor.id} className="leading-snug">
                            <span className="font-medium text-slate-700">{monitor.label}</span>
                            {(monitor.health_reason || monitor.last_error) && (
                              <span className="text-slate-500"> — {monitor.health_reason || monitor.last_error}</span>
                            )}
                          </li>
                        ))}
                        {degradedMonitorList.length > 8 && (
                          <li className="text-slate-400">+{degradedMonitorList.length - 8} more — see Settings</li>
                        )}
                      </ul>
                    </TooltipContent>
                  </Tooltip>
                </>
              )}
              <button
                type="button"
                data-tour="feed-monitors"
                onClick={() => {
                  window.location.hash = buildHashRoute('settings')
                }}
                className="group inline-flex items-center gap-1 rounded-full px-1.5 py-0.5 text-alma-700 transition-colors hover:bg-alma-50 hover:text-alma-800"
              >
                <Settings2 className="h-3.5 w-3.5" />
                <span className="underline-offset-2 group-hover:underline">Manage in Settings</span>
              </button>
            </div>
            {authorFilter && (
              <p className="text-xs text-alma-700">Filtered to {filteredAuthorLabel}.</p>
            )}
          </div>
          <div className="flex shrink-0 flex-col items-end gap-1">
            <div className="flex items-center gap-1 self-end">
              <PageTour pageKey="feed" steps={FEED_TOUR} />
            </div>
            <Button
              type="button"
              variant="default"
              onClick={() => refreshMutation.mutate()}
              disabled={refreshMutation.isPending}
              className="h-10 px-5"
            >
              {refreshMutation.isPending ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="mr-2 h-4 w-4" />
              )}
              Refresh Inbox
            </Button>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="cursor-default text-xs text-slate-500">
                  {feedStatusQuery.data?.last_refresh_at
                    ? `Last refresh ${formatRelativeShort(feedStatusQuery.data.last_refresh_at)}`
                    : 'No refresh on record yet'}
                </span>
              </TooltipTrigger>
              <TooltipContent side="bottom">
                {feedStatusQuery.data?.last_refresh_at
                  ? formatTimestamp(feedStatusQuery.data.last_refresh_at)
                  : 'Run Refresh Inbox to pull the latest papers.'}
              </TooltipContent>
            </Tooltip>
            <label className="flex cursor-pointer items-center gap-2 self-end text-xs text-slate-500">
              <Switch
                checked={!!feedSettingsQuery.data?.auto_refresh_enabled}
                disabled={!feedSettingsQuery.data || autoRefreshMutation.isPending}
                onCheckedChange={(next) => autoRefreshMutation.mutate(next)}
                aria-label="Toggle feed auto-refresh"
              />
              <span>
                {feedSettingsQuery.data?.auto_refresh_enabled
                  ? `Auto-refresh every ${feedSettingsQuery.data.refresh_interval_hours}h`
                  : 'Auto-refresh off'}
              </span>
              <JargonHint
                title="Auto-refresh"
                description="Opt-in background refresh of the feed inbox on a schedule (set the interval in Settings). It runs without blocking the page — new papers appear automatically. Off by default."
              />
            </label>
          </div>
        </div>
      </section>

      {/* U-9: the four feed actions read very differently to Discovery's, and
          the dislike-vs-dismiss split (D6) is the easy one to get wrong. One
          quiet, collapsed explainer near the top — not per-button tooltips. */}
      <ConceptCallout
        eyebrow="How do the Feed actions work?"
        summary="Save keeps a paper; Dislike keeps it visible but down-weights Discovery; Dismiss hides it for good."
      >
        <p className="mb-2">
          The Feed is a chronological inbox of new papers from your monitors (last 60 days).
          Each action sends a different signal:
        </p>
        <ul className="ml-4 list-disc space-y-1">
          <li><span className="font-medium text-alma-900">Add / Like / Love</span> — saves the paper to your Library (Love rates it 5★).</li>
          <li><span className="font-medium text-alma-900">Dislike</span> — a negative signal to Discovery, but the paper <span className="font-medium">stays in the Feed</span> so the inbox keeps its chronological record.</li>
          <li><span className="font-medium text-alma-900">Dismiss</span> — <span className="font-medium">hides the paper from the Feed for good</span> and sends a small negative signal. You can undo a dismiss right after.</li>
        </ul>
      </ConceptCallout>

      {/* U-1: visible while a feed refresh runs in the background. */}
      <RefreshRunningBanner domain="feed" label="Refreshing feed inbox…" />

      {/* ── Control bar ────────────────────────────────────────────────────
          Single horizontal strip with three zones separated by dividers:
          [filter] · [sort]  …  [counter + select-all] · [view mode]
          Segmented controls for the binary / ternary toggles, a pill
          button for sort. Nothing here mutates data — all controls are
          local view state.
      ──────────────────────────────────────────────────────────────────── */}
      <ListControlBar
        leading={
          <>
            {/* Filter segmented control — only "All" / "New". */}
            <ToggleGroup
              type="single"
              value={filter}
              onValueChange={(value) => {
                // Radix allows deselecting the active item; we require one always active.
                if (value) setFilter(value as FeedFilter)
              }}
              aria-label="Feed filter"
              className="gap-0 rounded-sm bg-surface-2/80 p-0.5"
            >
              {FEED_FILTERS.map((value) => (
                <ToggleGroupItem
                  key={value}
                  value={value}
                  className="h-7 min-w-0 rounded-sm px-3 text-xs font-medium text-slate-600 hover:bg-transparent hover:text-alma-800 data-[state=on]:bg-surface-1 data-[state=on]:text-alma-800 data-[state=on]:shadow-paper-sm data-[state=on]:ring-1 data-[state=on]:ring-[var(--color-border)]"
                >
                  {FEED_FILTER_LABELS[value]}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
            <div className="h-5 w-px bg-slate-200" aria-hidden />
          </>
        }
        sort={{
          label: sort === 'relevance' ? 'Relevance' : 'Recent',
          title:
            sort === 'chronological'
              ? 'Currently sorted chronologically — switch to relevance'
              : 'Currently sorted by relevance — switch to chronological',
          ariaLabel: `Sort by ${sort === 'chronological' ? 'relevance' : 'recent'}`,
          onToggle: () => setSort(sort === 'chronological' ? 'relevance' : 'chronological'),
        }}
        count={total}
        selectAll={{
          allSelected: allVisibleSelected,
          onToggle: toggleSelectAllVisible,
          show: items.length > 0,
        }}
        view={{
          value: viewMode,
          ariaLabel: 'Feed view mode',
          onChange: (value) => setViewMode(value as FeedViewMode),
          options: [
            { value: 'compact', label: 'Compact', icon: Rows3, title: 'Compact table view' },
            { value: 'normal', label: 'Normal', icon: LayoutGrid, title: 'Normal card view' },
            { value: 'extended', label: 'Extended', icon: LayoutList, title: 'Extended view — includes abstracts' },
          ],
        }}
      />

      {/* ── Bulk workflow bar ──────────────────────────────────────────────
          Appears only when at least one card is selected. Visually
          distinct alma tint so the "temporary selection mode" reads
          differently from the permanent control bar above.
      ──────────────────────────────────────────────────────────────────── */}
      {selectedIds.size > 0 && (
        <section
          role="region"
          aria-label="Bulk actions"
          className="flex flex-wrap items-center gap-3 rounded-sm border border-alma-200 bg-alma-50/60 px-4 py-2.5 shadow-sm"
        >
          <div className="flex items-center gap-2.5 text-sm">
            <span className="inline-flex h-6 min-w-[1.5rem] items-center justify-center rounded-full bg-alma-600 px-1.5 text-[11px] font-semibold tabular-nums text-white shadow-sm">
              {selectedIds.size}
            </span>
            <span className="text-slate-700">
              selected
              <span className="mx-1.5 text-slate-300" aria-hidden>·</span>
              <button
                type="button"
                onClick={toggleSelectAllVisible}
                className="text-xs text-alma-700 underline-offset-2 hover:underline"
              >
                {allVisibleSelected ? 'Clear visible' : 'Select all visible'}
              </button>
            </span>
          </div>
          <div className="ml-auto flex flex-wrap items-center gap-1.5">
            <Button size="sm" variant="outline" onClick={() => bulkMutation.mutate({ action: 'add' })} disabled={bulkMutation.isPending}>Save</Button>
            <Button size="sm" variant="outline" onClick={() => bulkMutation.mutate({ action: 'like' })} disabled={bulkMutation.isPending}>Like</Button>
            <Button size="sm" variant="outline" onClick={() => bulkMutation.mutate({ action: 'love' })} disabled={bulkMutation.isPending}>Love</Button>
            <Button size="sm" variant="outline" onClick={() => bulkMutation.mutate({ action: 'dislike' })} disabled={bulkMutation.isPending}>Dislike</Button>
            <Button size="sm" variant="outline" onClick={() => bulkMutation.mutate({ action: 'dismiss' })} disabled={bulkMutation.isPending}>Dismiss</Button>
            <span className="mx-1 h-5 w-px bg-alma-200" aria-hidden />
            <Button size="sm" variant="ghost" onClick={() => setSelectedIds(new Set())} disabled={bulkMutation.isPending}>
              Clear
            </Button>
          </div>
        </section>
      )}

      {feedQuery.isLoading ? (
        <SkeletonList count={5} />
      ) : feedQuery.isError ? (
        <ErrorState message="Failed to load feed inbox." />
      ) : items.length === 0 ? (
        <EmptyState
          icon={Search}
          title="No papers published in the last 60 days"
          description={
            filter === 'all'
              ? 'The Feed only shows papers from the last 60 days by publication date. Run Refresh Inbox to pull new papers, or follow more authors / add new monitors in Settings.'
              : `No ${FEED_FILTER_LABELS[filter].toLowerCase()} papers in the last 60 days. Clear the filter or refresh.`
          }
        />
      ) : viewMode === 'compact' ? (
        <FeedCompactTable
          items={items}
          selectedIds={selectedIds}
          onSelectionChange={setSelectedIds}
          onOpenDetails={(p) => {
            setSelectedPaper(p)
            setDetailOpen(true)
          }}
        />
      ) : (
        <RevealList className="space-y-3">
          {items.map((item, i) => {
            const paper = toPublication(item)
            const matchedAuthors = item.matched_authors ?? []
            const matchedMonitors = item.matched_monitors ?? []
            const cardPaper = {
              id: item.paper_id,
              title: paper?.title || item.paper_id,
              authors: paper?.authors || 'Unknown authors',
              year: paper?.year,
              journal: paper?.journal,
              url: paper?.url,
              doi: paper?.doi,
              publication_date: paper?.publication_date,
              cited_by_count: paper?.cited_by_count,
              rating: paper?.rating,
              status: paper?.status,
              abstract: paper?.abstract,
              // T5 + T15 — surface S2 TLDR + influential count + the
              // paper_signal ranking on Feed cards. Falsy values hide
              // their chips (sparse-field policy).
              tldr: paper?.tldr ?? null,
              influential_citation_count: paper?.influential_citation_count ?? 0,
              global_signal_score: paper?.global_signal_score ?? 0,
            }
            const breakdown = parseBreakdown(item.score_breakdown)
            const explanation = buildFeedExplanation(item)
            const isSelected = selectedIds.has(item.id)
            const reaction = deriveFeedReaction(item.status)
            const isSaved = deriveFeedIsSaved(item.status, paper?.status)
            const isQueued = paper?.reading_status === 'reading'
            const isNew = Boolean(item.is_new)
            return (
              <RevealItem key={item.id} index={i}>
              <div
                className="relative rounded-sm"
                data-tour={i === 0 ? 'feed-card' : undefined}
              >
                <PaperCard
                  selection={{
                    checked: isSelected,
                    onCheckedChange: () => toggleSelection(item.id),
                    ariaLabel: 'Select feed item',
                  }}
                  paper={cardPaper}
                  score={item.signal_value}
                  scoreBreakdown={breakdown}
                  explanation={explanation}
                  followedAuthorNames={followedAuthorNames}
                  followAuthorPendingName={pendingAuthorName}
                  onFollowAuthor={followAuthor}
                  onDetails={() => {
                    setSelectedPaper(paper)
                    setDetailOpen(true)
                  }}
                  onQueue={() =>
                    item.paper_id && queueMutation.mutate({ paperId: item.paper_id, nextQueued: !isQueued })
                  }
                  onAdd={() =>
                    isSaved && item.paper_id
                      ? removeFromLibraryMutation.mutate({ paperId: item.paper_id })
                      : actionMutation.mutate({ id: item.id, action: 'add' })
                  }
                  onLike={() => actionMutation.mutate({ id: item.id, action: 'like' })}
                  onLove={() => actionMutation.mutate({ id: item.id, action: 'love' })}
                  onDislike={() => actionMutation.mutate({ id: item.id, action: 'dislike' })}
                  onDismiss={() => actionMutation.mutate({ id: item.id, action: 'dismiss' })}
                  onUndo={(aspect) => item.paper_id && undoMutation.mutate({ paperId: item.paper_id, aspect })}
                  dismissLabel="Dismiss"
                  dismissTitle="Dismiss — hide from Feed forever and send a small negative signal"
                  dislikeTitle="Negative signal — keeps the paper visible in Feed"
                  actionDisabled={
                    /* U-6: disable only THIS card while its own action is
                       in-flight — not every card in the inbox. */
                    (actionMutation.isPending && actionMutation.variables?.id === item.id) ||
                    (queueMutation.isPending && queueMutation.variables?.paperId === item.paper_id) ||
                    (removeFromLibraryMutation.isPending && removeFromLibraryMutation.variables?.paperId === item.paper_id)
                  }
                  reaction={reaction}
                  isSaved={isSaved}
                  savedClickRemoves
                  isQueued={isQueued}
                  trailingHeader={isNew ? <StatusBadge tone="positive" size="sm">New</StatusBadge> : undefined}
                  forceShowAbstract={viewMode === 'extended'}
                  showActionLabels={viewMode === 'extended'}
                  // Discover-similar pivot — sends the user to Discovery
                  // with this paper as the seed. Hidden when the row has
                  // no resolved paper_id (Feed entries occasionally arrive
                  // before paper resolution finishes).
                  onPivot={item.paper_id ? () => navigateTo('discovery', {
                    seed: item.paper_id!,
                    seedTitle: cardPaper.title,
                  }) : undefined}
                >
                  <div className="mt-2 space-y-1 text-xs text-slate-500">
                    {/* Line 1: the "why" — what monitors or followed authors
                        surfaced this paper. This is the most Feed-specific
                        piece of context so it leads. */}
                    {(matchedAuthors.length > 0 || matchedMonitors.length > 0 || item.author_name) && (
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        {matchedAuthors.length > 0 ? (
                          <div className="flex flex-wrap items-center gap-1.5">
                            <span className="inline-flex items-center gap-1 text-slate-500">
                              <UserRound className="h-3.5 w-3.5" />
                              Matches
                            </span>
                            {matchedAuthors.map((authorName) => (
                              <StatusBadge key={`${item.id}-${authorName}`} tone="info" size="sm">
                                {authorName}
                              </StatusBadge>
                            ))}
                          </div>
                        ) : item.author_name ? (
                          <span className="inline-flex items-center gap-1">
                            <UserRound className="h-3.5 w-3.5" />
                            {item.author_name}
                          </span>
                        ) : null}
                        {matchedMonitors.length > 0 && (
                          <div className="flex flex-wrap items-center gap-1.5">
                            <span className="inline-flex items-center gap-1 text-slate-500">
                              <Search className="h-3.5 w-3.5" />
                              Monitors
                            </span>
                            {matchedMonitors.map((monitor) => {
                              const label = monitor.monitor_label?.trim() || formatMonitorTypeLabel(monitor.monitor_type)
                              const suffix = monitor.monitor_type ? ` (${formatMonitorTypeLabel(monitor.monitor_type)})` : ''
                              return (
                                <Badge
                                  key={`${item.id}-${monitor.monitor_id ?? label}-${monitor.monitor_type ?? 'monitor'}`}
                                  variant="outline"
                                  className="border-slate-200 bg-surface-2 text-slate-700"
                                >
                                  {label}{suffix}
                                </Badge>
                              )
                            })}
                          </div>
                        )}
                        {matchedMonitors.length === 0 && item.monitor_type && item.monitor_type !== 'author' && item.monitor_label && (
                          <Badge variant="outline" className="border-slate-200 bg-surface-2 text-slate-700">
                            {item.monitor_label} ({formatMonitorTypeLabel(item.monitor_type)})
                          </Badge>
                        )}
                      </div>
                    )}
                    {/* Line 2: when the paper was published. "Found {time}"
                        used to live here too but became visual static on long
                        scrolls — it lives only in the paper-details popup now. */}
                    {paper?.publication_date && (
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        <span className="inline-flex items-center gap-1">
                          <CalendarClock className="h-3.5 w-3.5" />
                          Published {formatDate(paper.publication_date)}
                        </span>
                      </div>
                    )}
                  </div>
                  {/* U-6: the "applying" spinner belongs to the in-flight card only. */}
                  {((actionMutation.isPending && actionMutation.variables?.id === item.id) ||
                    (queueMutation.isPending && queueMutation.variables?.paperId === item.paper_id)) && (
                    <div className="mt-2 flex items-center gap-1 text-xs text-slate-500">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      Applying action...
                    </div>
                  )}
                </PaperCard>
              </div>
              </RevealItem>
            )
          })}
        </RevealList>
      )}

      {/* U-12: page beyond the first 60 (within the 60-day window). Hidden when
          an author filter is active (that view is already the full filtered set). */}
      {!authorFilter && items.length > 0 && items.length < total && (
        <div className="mt-4 flex justify-center">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setFeedLimit((n) => n + 60)}
            disabled={feedQuery.isFetching}
          >
            {feedQuery.isFetching ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : null}
            Load more · {items.length} of {total}
          </Button>
        </div>
      )}

      <PaperDetailPanel paper={selectedPaper} open={detailOpen} onOpenChange={setDetailOpen} />
    </div>
  )
}

interface FeedCompactTableProps {
  items: FeedInboxItem[]
  selectedIds: Set<string>
  onSelectionChange: (next: Set<string>) => void
  onOpenDetails: (paper: Publication | null) => void
}

interface FeedCompactRow {
  id: string
  item: FeedInboxItem
  paper: Publication | null
  title: string
  authors: string
  publishedSortKey: string
  publishedLabel: string
  journal: string
  source: string
  isNew: boolean
}

/**
 * Compact table view wired to the shared `<DataTable>` primitive. All column
 * visibility / reorder / resize / sort state persists per-user via
 * ``storageKey="feed.compact"``. Selection + row highlight are provided by
 * DataTable itself via the `selectedIds` / `onSelectionChange` props.
 */
function FeedCompactTable({
  items,
  selectedIds,
  onSelectionChange,
  onOpenDetails,
}: FeedCompactTableProps) {
  const rows: FeedCompactRow[] = useMemo(
    () =>
      items.map((item) => {
        const paper = toPublication(item)
        const matchedAuthors = item.matched_authors ?? []
        const matchedMonitors = item.matched_monitors ?? []
        const source =
          matchedAuthors.length > 0
            ? matchedAuthors.slice(0, 2).join(', ') + (matchedAuthors.length > 2 ? ` +${matchedAuthors.length - 2}` : '')
            : matchedMonitors.length > 0
              ? (matchedMonitors[0].monitor_label?.trim() || formatMonitorTypeLabel(matchedMonitors[0].monitor_type))
              : item.author_name || ''
        return {
          id: item.id,
          item,
          paper,
          title: paper?.title || item.paper_id,
          authors: paper?.authors ?? '',
          publishedSortKey: paper?.publication_date ?? (paper?.year != null ? `${paper.year}-01-01` : ''),
          publishedLabel: formatPublicationDate(paper),
          journal: paper?.journal ?? '',
          source,
          isNew: Boolean(item.is_new),
        }
      }),
    [items],
  )

  const columns: ColumnDef<FeedCompactRow>[] = useMemo(
    () => [
      {
        id: 'title',
        accessorKey: 'title',
        header: 'Title',
        size: 420,
        // Custom flex layout (optional New badge + title + trailing external
        // link) — manage truncation here via `min-w-0` on the name span.
        meta: { cellOverflow: 'none' },
        cell: ({ row }) => (
          <div className="flex min-w-0 items-center gap-1.5">
            {row.original.isNew && <StatusBadge tone="positive">New</StatusBadge>}
            <span className="min-w-0 flex-1 truncate font-medium text-alma-800" title={row.original.title}>
              {row.original.title}
            </span>
            {row.original.paper?.url && (
              <a
                href={row.original.paper.url}
                target="_blank"
                rel="noopener noreferrer"
                className="shrink-0 text-slate-400 hover:text-alma-600"
                title="Open source"
                onClick={(e) => e.stopPropagation()}
              >
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
        ),
      },
      {
        id: 'authors',
        accessorKey: 'authors',
        header: 'Authors',
        size: 200,
        cell: ({ row }) => (
          <span className="text-slate-600" title={row.original.authors}>
            {row.original.authors}
          </span>
        ),
      },
      {
        id: 'published',
        accessorKey: 'publishedSortKey',
        header: 'Published',
        size: 120,
        sortingFn: 'alphanumeric',
        cell: ({ row }) => <span className="whitespace-nowrap text-slate-600">{row.original.publishedLabel}</span>,
      },
      {
        id: 'journal',
        accessorKey: 'journal',
        header: 'Journal',
        size: 180,
        cell: ({ row }) => (
          <span className="text-slate-500" title={row.original.journal}>
            {row.original.journal}
          </span>
        ),
      },
      {
        id: 'source',
        accessorKey: 'source',
        header: 'Source',
        size: 200,
        cell: ({ row }) => (
          <span className="text-slate-500" title={row.original.source}>
            {row.original.source}
          </span>
        ),
      },
    ],
    [],
  )

  return (
    <DataTable
      data={rows}
      columns={columns}
      storageKey="feed.compact"
      getRowId={(row) => row.id}
      onRowClick={(row) => onOpenDetails(row.paper)}
      selectedIds={selectedIds}
      onSelectionChange={onSelectionChange}
    />
  )
}
