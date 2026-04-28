import { useEffect, useRef } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { invalidateQueryRoots } from '@/lib/queryHelpers'
import { toast } from './useToast'
import { errorToast } from '@/hooks/useToast'

interface JobStatus {
  job_id: string
  status: string // 'running' | 'completed' | 'failed' | 'cancelled' | 'cancelling' | 'queued'
  operation_key?: string
  message?: string
  finished_at?: string
  parent_job_id?: string
}

const STORAGE_KEY = 'alma-last-seen-ops'
const POLL_INTERVAL = 12000 // 12 seconds

function rootsForOperation(operationKey?: string): string[] {
  const key = (operationKey ?? '').trim()
  if (!key) return []

  if (key === 'feed.refresh_inbox' || key.startsWith('feed.monitor.refresh:')) {
    return [
      'feed-inbox',
      'feed-monitors',
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('authors.')) {
    return [
      'authors',
      'library-followed-authors',
      'author-suggestions',
      'author-dossier',
      'author-publications',
      'feed-monitors',
      'feed-inbox',
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('library-mgmt.') || key.startsWith('library.')) {
    return [
      'library-info',
      'library-workflow-summary',
      'papers',
      'library-saved',
      'library-collections',
      'library-tags',
      'library-topics',
      'library-topics-hierarchy',
      'tag-suggestions',
      'tag-merge-suggestions',
    ]
  }

  if (key.startsWith('imports.')) {
    return [
      'library-import-unresolved',
      'unresolved-imported-publications',
      'papers',
      'library-saved',
      'library-workflow-summary',
      'library-info',
      'library-collections',
      'library-tags',
    ]
  }

  if (key.startsWith('discovery.')) {
    return [
      'lenses',
      'lens-recommendations',
      'lens-signals',
      'lens-branches',
      'discovery-status',
      'discovery-seeded-similar',
      'discovery-explain',
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('graphs.')) {
    return [
      'graph',
      'authors',
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('tags.')) {
    return [
      'library-tags',
      'library-saved',
      'tag-suggestions',
      'tag-merge-suggestions',
      'papers',
    ]
  }

  if (key.startsWith('alerts.')) {
    return [
      'alerts',
      'alert-rules',
      'alert-history',
      'alert-templates',
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('ai.')) {
    return [
      'ai-status',
      'insights-diagnostics',
      'papers',
      'graph',
    ]
  }

  return []
}

/**
 * Hook that monitors completed operations and shows toast notifications
 * for newly completed or failed operations.
 *
 * Tracks seen operations in localStorage to avoid showing duplicate toasts
 * across page reloads.
 */
export function useOperationToasts() {
  const queryClient = useQueryClient()
  const lastSeenRef = useRef<Set<string>>(new Set())
  const hasInitializedRef = useRef(false)

  // Initialize from localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      if (stored) {
        const parsed = JSON.parse(stored)
        if (Array.isArray(parsed)) {
          lastSeenRef.current = new Set(parsed)
        }
      }
    } catch {
      // Ignore parse errors
    }
  }, [])

  // Poll operations
  const { data: operations } = useQuery({
    queryKey: ['activity-operations-toasts'],
    queryFn: () => api.get<JobStatus[]>('/activity'),
    refetchInterval: POLL_INTERVAL,
  })

  useEffect(() => {
    if (!operations || operations.length === 0) {
      return
    }

    // On first load, mark all current completed/failed ops as seen
    // to avoid showing toasts for operations that completed before this session
    if (!hasInitializedRef.current) {
      hasInitializedRef.current = true
      const completedJobIds = operations
        .filter((op) => op.status === 'completed' || op.status === 'failed')
        .map((op) => op.job_id)

      for (const jobId of completedJobIds) {
        lastSeenRef.current.add(jobId)
      }

      // Persist to localStorage
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify([...lastSeenRef.current]))
      } catch {
        // Ignore storage errors
      }
      return
    }

    // Filter to top-level operations only (no parent_job_id)
    const topLevelOps = operations.filter((op) => !op.parent_job_id)

    // Find newly completed or failed operations
    const newCompletedOps = topLevelOps.filter(
      (op) =>
        (op.status === 'completed' || op.status === 'failed') &&
        !lastSeenRef.current.has(op.job_id)
    )

    // Limit to 3 toasts to avoid spam (matches TOAST_LIMIT)
    const toShow = newCompletedOps.slice(0, 3)

    for (const op of toShow) {
      // Mark as seen immediately
      lastSeenRef.current.add(op.job_id)

      const roots = rootsForOperation(op.operation_key)
      if (roots.length > 0) {
        void invalidateQueryRoots(queryClient, ...roots)
      }

      // Show toast
      if (op.status === 'completed') {
        toast({
          title: 'Operation complete',
          description: op.message || op.job_id,
        })
      } else if (op.status === 'failed') {
        errorToast('Operation failed')
      }
    }

    // Persist updated seen set to localStorage
    if (toShow.length > 0) {
      try {
        // Keep only the most recent 100 job IDs to avoid unbounded growth
        const recentJobIds = [...lastSeenRef.current].slice(-100)
        lastSeenRef.current = new Set(recentJobIds)
        localStorage.setItem(STORAGE_KEY, JSON.stringify(recentJobIds))
      } catch {
        // Ignore storage errors
      }
    }
  }, [operations, queryClient])
}
