import { useState, type ReactNode } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  api,
  queueAuthorHistoryBackfill,
  refreshFeedMonitor,
  type Author,
  type AuthorNeedsAttentionRow,
} from '@/api/client'
import {
  AddIdentifierDialog,
  AffiliationPickerDialog,
  ResolveConflictDialog,
  ReviewProfilesDialog,
} from '@/components/authors/AuthorsNeedsAttentionSection'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

export interface AuthorAttentionRouter {
  openForRow: (row: AuthorNeedsAttentionRow) => void
  isRefreshingFor: (authorId: string) => boolean
  /** Queue the automatic fix for every row that has one (see AUTO_FIX_CODES). */
  fixAll: (rows: AuthorNeedsAttentionRow[]) => void
  isFixingAll: boolean
  /** How many of these rows "Fix all" can actually act on — 0 hides the button. */
  countAutoFixable: (rows: AuthorNeedsAttentionRow[]) => number
  dialogs: ReactNode
}

/**
 * Suggested-action codes whose fix needs no human judgement, so "Fix all" may
 * fire them unattended:
 *   - `refresh` / `retry_refresh` / `resolve_now` → re-run the identity+profile
 *     resolver for that author;
 *   - `refresh_monitor` → re-run the author's feed monitor;
 *   - `backfill_author` → queue the historical works backfill.
 *
 * Everything else is deliberately excluded: `manual_search` means the automatic
 * search already returned zero hits (re-running it just fails again), and
 * `review_candidates` / `review_profiles` / `resolve_conflict` /
 * `pick_affiliation` are decisions only the user can make.
 */
const AUTO_FIX_CODES = new Set([
  'refresh',
  'retry_refresh',
  'resolve_now',
  'refresh_monitor',
  'backfill_author',
])

interface UseAuthorAttentionRouterOptions {
  authorsById?: Map<string, Author>
  onOpenDetail?: (author: Author) => void
}

export function useAuthorAttentionRouter(
  options: UseAuthorAttentionRouterOptions = {},
): AuthorAttentionRouter {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [reviewRow, setReviewRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [identifierRow, setIdentifierRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [conflictRow, setConflictRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [affiliationRow, setAffiliationRow] = useState<AuthorNeedsAttentionRow | null>(null)
  const [fixingAll, setFixingAll] = useState(false)

  const refreshMutation = useMutation({
    mutationFn: (authorId: string) =>
      api.post<{ status?: string; job_id?: string }>(
        `/authors/${encodeURIComponent(authorId)}/identity-profile-refresh`,
      ),
    onSuccess: (data, authorId) => {
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
        ['author-detail', authorId],
      )
      toast({
        title:
          data?.status === 'already_running' ? 'Refresh already running' : 'Refresh queued',
        description: data?.job_id ? `Job ${data.job_id} will update this author.` : undefined,
      })
    },
    onError: () => errorToast('Error', 'Could not queue refresh.'),
  })

  // Operational fixes surfaced by the unified needs-attention feed (the same
  // canonical rows the Health popup shows) — queue the fix directly, no dialog.
  const monitorRefreshMutation = useMutation({
    mutationFn: (monitorId: string) => refreshFeedMonitor(monitorId),
    onSuccess: () => {
      void invalidateQueries(
        queryClient,
        ['authors-needs-attention'],
        ['feed-monitors'],
        ['feed-inbox'],
        ['insights-diag'],
      )
      toast({ title: 'Monitor refresh queued', description: 'Running in Activity.' })
    },
    onError: () => errorToast('Error', 'Monitor refresh failed.'),
  })
  const backfillMutation = useMutation({
    mutationFn: (authorId: string) => queueAuthorHistoryBackfill(authorId),
    onSuccess: () => {
      void invalidateQueries(
        queryClient,
        ['authors-needs-attention'],
        ['authors'],
        ['insights-diag'],
        ['activity-operations'],
      )
      toast({ title: 'Historical backfill queued', description: 'Track progress in Activity.' })
    },
    onError: () => errorToast('Error', 'Historical backfill failed.'),
  })

  const openForRow = (row: AuthorNeedsAttentionRow) => {
    const code = row.suggested_action.code
    if (code === 'refresh_monitor') {
      if (row.monitor_id) monitorRefreshMutation.mutate(row.monitor_id)
      return
    }
    if (code === 'backfill_author') {
      backfillMutation.mutate(row.author_id)
      return
    }
    if (code === 'review_profiles') {
      setReviewRow(row)
      return
    }
    if (code === 'resolve_conflict') {
      setConflictRow(row)
      return
    }
    if (code === 'pick_affiliation') {
      setAffiliationRow(row)
      return
    }
    if (code === 'review_candidates') {
      const author = options.authorsById?.get(row.author_id)
      if (author && options.onOpenDetail) options.onOpenDetail(author)
      return
    }
    if (code === 'manual_search' || code === 'resolve_now' || code === 'retry_refresh') {
      setIdentifierRow(row)
      return
    }
    refreshMutation.mutate(row.author_id)
  }

  /**
   * "Fix all": queue the automatic fix for every auto-fixable row, in sequence
   * so N authors don't hammer the resolver at once. Rows needing a human
   * decision are counted and reported rather than silently skipped, and a
   * failing row doesn't abort the rest — the toast states queued / failed /
   * manual so the result is never overstated.
   */
  const fixAll = async (rows: AuthorNeedsAttentionRow[]) => {
    if (fixingAll) return
    const autoRows = rows.filter((r) => AUTO_FIX_CODES.has(r.suggested_action.code))
    const manualCount = rows.length - autoRows.length
    if (autoRows.length === 0) {
      toast({
        title: 'Nothing to auto-fix',
        description: `All ${rows.length} row${rows.length === 1 ? '' : 's'} need a manual decision.`,
      })
      return
    }
    // One job per target, not per row: an author can hold several attention rows.
    const identityIds = new Set<string>()
    const monitorIds = new Set<string>()
    const backfillIds = new Set<string>()
    for (const row of autoRows) {
      const code = row.suggested_action.code
      if (code === 'refresh_monitor') {
        if (row.monitor_id) monitorIds.add(row.monitor_id)
      } else if (code === 'backfill_author') {
        backfillIds.add(row.author_id)
      } else {
        identityIds.add(row.author_id)
      }
    }

    setFixingAll(true)
    let queued = 0
    let failed = 0
    const run = async (task: Promise<unknown>) => {
      try {
        await task
        queued += 1
      } catch {
        failed += 1
      }
    }
    try {
      for (const id of identityIds) {
        await run(
          api.post(`/authors/${encodeURIComponent(id)}/identity-profile-refresh`),
        )
      }
      for (const id of monitorIds) await run(refreshFeedMonitor(id))
      for (const id of backfillIds) await run(queueAuthorHistoryBackfill(id))
    } finally {
      setFixingAll(false)
      void invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['activity-operations'],
        ['feed-monitors'],
        ['insights-diag'],
      )
    }

    const parts = [`${queued} fix${queued === 1 ? '' : 'es'} queued`]
    if (failed) parts.push(`${failed} failed to queue`)
    if (manualCount) parts.push(`${manualCount} need a manual decision`)
    if (failed) errorToast('Fix all finished with errors', parts.join(' · '))
    else toast({ title: 'Fix all queued', description: `${parts.join(' · ')} — track them in Activity.` })
  }

  const dialogs = (
    <>
      <ReviewProfilesDialog row={reviewRow} onClose={() => setReviewRow(null)} />
      <AddIdentifierDialog row={identifierRow} onClose={() => setIdentifierRow(null)} />
      <ResolveConflictDialog row={conflictRow} onClose={() => setConflictRow(null)} />
      <AffiliationPickerDialog row={affiliationRow} onClose={() => setAffiliationRow(null)} />
    </>
  )

  return {
    openForRow,
    isRefreshingFor: (authorId) =>
      (refreshMutation.isPending && refreshMutation.variables === authorId) ||
      (backfillMutation.isPending && backfillMutation.variables === authorId),
    fixAll: (rows) => void fixAll(rows),
    isFixingAll: fixingAll,
    countAutoFixable: (rows) =>
      rows.filter((r) => AUTO_FIX_CODES.has(r.suggested_action.code)).length,
    dialogs,
  }
}
