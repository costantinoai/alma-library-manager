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
  dialogs: ReactNode
}

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
    dialogs,
  }
}
