import { useRef, useState, type ReactNode } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  api,
  getApiErrorMessage,
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
import { describeJobLaunch } from '@/lib/activity'
import { invalidateAfterAuthorChange, invalidateQueries } from '@/lib/queryHelpers'

export interface AuthorAttentionRouter {
  openForRow: (row: AuthorNeedsAttentionRow) => void
  /** Open the author's panel. Runs no action: that is the row's button. */
  openAuthor: (row: AuthorNeedsAttentionRow) => void
  isRefreshingFor: (authorId: string) => boolean
  /** Queue the automatic fix for every row that has one (see AUTO_FIX_CODES). */
  fixAll: (rows: AuthorNeedsAttentionRow[]) => void
  isFixingAll: boolean
  /** How many of these rows "Fix all" can actually act on — 0 hides the button. */
  countAutoFixable: (rows: AuthorNeedsAttentionRow[]) => number
  dialogs: ReactNode
}

interface UseAuthorAttentionRouterOptions {
  authorsById?: Map<string, Author>
  /** `tab` opens the panel on that tab: a review lands on its candidates. */
  onOpenDetail?: (author: Author, tab?: 'identifiers') => void
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
      void invalidateAfterAuthorChange(queryClient, authorId)
      toast(describeJobLaunch(data, 'Author refresh'))
    },
    onError: (err) => errorToast('Could not queue the refresh', getApiErrorMessage(err)),
  })

  // Operational fixes surfaced by the unified needs-attention feed (the same
  // canonical rows the Health popup shows) — queue the fix directly, no dialog.
  const monitorRefreshMutation = useMutation({
    mutationFn: (row: AuthorNeedsAttentionRow) => refreshFeedMonitor(row.monitor_id ?? ''),
    onSuccess: (data, row) => {
      void invalidateAfterAuthorChange(queryClient, row.author_id)
      void invalidateQueries(queryClient, ['feed-inbox'])
      toast(describeJobLaunch(data, `Monitor refresh of ${row.author_name}`))
    },
    onError: (err) => errorToast('Monitor refresh failed', getApiErrorMessage(err)),
  })
  const backfillMutation = useMutation({
    mutationFn: (authorId: string) => queueAuthorHistoryBackfill(authorId),
    onSuccess: (data, authorId) => {
      void invalidateAfterAuthorChange(queryClient, authorId)
      toast(describeJobLaunch(data, 'Historical backfill'))
    },
    onError: (err) => errorToast('Historical backfill failed', getApiErrorMessage(err)),
  })

  // A row can name an author the loaded list does not hold; the panel loads the
  // rest of the record from the id.
  const authorOf = (row: AuthorNeedsAttentionRow): Author =>
    options.authorsById?.get(row.author_id) ??
    ({ id: row.author_id, name: row.author_name, author_type: 'background' } as Author)
  const openAuthor = (row: AuthorNeedsAttentionRow) => options.onOpenDetail?.(authorOf(row))
  const openIdentifiers = (row: AuthorNeedsAttentionRow) =>
    options.onOpenDetail?.(authorOf(row), 'identifiers')

  // A double click lands before the button can disable: one job per click-burst.
  const startedJobs = useRef(new Set<string>())
  const startOnce = (key: string, start: (onSettled: () => void) => void) => {
    if (startedJobs.current.has(key)) return
    startedJobs.current.add(key)
    start(() => startedJobs.current.delete(key))
  }

  const openForRow = (row: AuthorNeedsAttentionRow) => {
    const code = row.suggested_action.code
    if (code === 'refresh_monitor') {
      startOnce(`monitor:${row.author_id}`, (onSettled) =>
        monitorRefreshMutation.mutate(row, { onSettled }),
      )
      return
    }
    if (code === 'backfill_author') {
      startOnce(`backfill:${row.author_id}`, (onSettled) =>
        backfillMutation.mutate(row.author_id, { onSettled }),
      )
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
      openIdentifiers(row)
      return
    }
    if (code === 'manual_search' || code === 'resolve_now' || code === 'retry_refresh') {
      setIdentifierRow(row)
      return
    }
    startOnce(`refresh:${row.author_id}`, (onSettled) =>
      refreshMutation.mutate(row.author_id, { onSettled }),
    )
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
    // `auto_fixable` comes from `author_attention.AUTO_FIX_CODES`.
    const autoRows = rows.filter((r) => r.auto_fixable)
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
      void invalidateAfterAuthorChange(queryClient)
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
      <AddIdentifierDialog
        row={identifierRow}
        onClose={() => setIdentifierRow(null)}
        onSearchCandidates={(row) => {
          setIdentifierRow(null)
          openIdentifiers(row)
        }}
      />
      <ResolveConflictDialog row={conflictRow} onClose={() => setConflictRow(null)} />
      <AffiliationPickerDialog row={affiliationRow} onClose={() => setAffiliationRow(null)} />
    </>
  )

  return {
    openForRow,
    openAuthor,
    isRefreshingFor: (authorId) =>
      (refreshMutation.isPending && refreshMutation.variables === authorId) ||
      (backfillMutation.isPending && backfillMutation.variables === authorId) ||
      (monitorRefreshMutation.isPending && monitorRefreshMutation.variables?.author_id === authorId),
    fixAll: (rows) => void fixAll(rows),
    isFixingAll: fixingAll,
    countAutoFixable: (rows) =>
      rows.filter((r) => r.auto_fixable).length,
    dialogs,
  }
}
