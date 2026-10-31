import { useEffect, useRef } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/api/client'
import { ADDONS } from '@/addons/registry'
import { invalidateAfterPaperMutation, invalidateQueryRoots } from '@/lib/queryHelpers'
import { isBackgroundTriggerSource, runDisplayStatus } from '@/lib/activity'
import { toast } from './useToast'
import { errorToast } from '@/hooks/useToast'

interface JobStatus {
  result?: { batch?: unknown }
  job_id: string
  status: string // 'running' | 'completed' | 'failed' | 'cancelled' | 'cancelling' | 'queued'
  operation_key?: string
  trigger_source?: string
  message?: string
  finished_at?: string
  parent_job_id?: string
}

const POLL_INTERVAL = 12000 // 12 seconds

/**
 * Friendly toast title for a completed/failed operation, derived from its
 * operation_key domain. The job's own `message` carries the detail (e.g.
 * "Lens 'Library': 50 recommendations generated"), shown as the description.
 */
function operationToastTitle(
  operationKey: string | undefined,
  outcome: 'complete' | 'partial' | 'failed',
): string {
  const key = (operationKey ?? '').trim()
  const domain =
    addonForOperation(key)?.operations?.label ??
    (key === 'papers.reconcile_groups' ? 'Paper groups' :
    key.startsWith('discovery.') ? 'Discovery refresh' :
    key.startsWith('feed.') ? 'Feed refresh' :
    key.startsWith('authors.') ? 'Authors' :
    (key.startsWith('library') ? 'Library' :
    key.startsWith('imports.') ? 'Import' :
    key.startsWith('graphs.') ? 'Graph' :
    key.startsWith('ai.') ? 'AI' :
    key.startsWith('pdf.') ? 'PDF' :
    key.startsWith('addons.') ? 'Add-on' :
    'Operation'))
  if (outcome === 'failed') return `${domain} failed`
  return outcome === 'partial' ? `${domain} partly done` : `${domain} complete`
}

function addonForOperation(key: string) {
  return ADDONS.find((addon) => addon.operations && key.startsWith(addon.operations.prefix))
}

function rootsForOperation(operationKey?: string): string[] {
  const key = (operationKey ?? '').trim()
  if (!key) return []

  const addon = addonForOperation(key)
  if (addon?.operations) return addon.operations.queryRoots

  // Remove changes what the shell, Settings and Home show.
  if (key.startsWith('addons.')) {
    return ['addons', 'bootstrap', 'plugins', 'home', 'home-brief', 'health']
  }

  // Paper PDFs (task 81): a fetch/attach refreshes that paper's PDF state; a
  // PDF-first import can add a paper to the Library.
  if (key.startsWith('pdf.import:')) {
    return ['paper-pdf', 'papers', 'library-saved', 'library-info', 'library-workflow-summary']
  }
  if (key.startsWith('pdf.')) {
    return ['paper-pdf']
  }

  if (key === 'papers.reconcile_groups') {
    return [
      'health', 'library-info', 'library-collections', 'library-tags', 'authors', 'insights',
      // A merge moves add-on rows too.
      ...ADDONS.flatMap((entry) => entry.operations?.queryRoots ?? []),
    ]
  }

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
      'tag-merge-suggestions',
      // A reset clears add-on rows too.
      ...ADDONS.flatMap((entry) => entry.operations?.queryRoots ?? []),
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
      // Recommendation reactions change the space-owned preference field
      // independently of the durable layout payload.
      'insights-diagnostics',
    ]
  }

  if (key.startsWith('ai.')) {
    return [
      'ai-status',
      'insights-diagnostics',
      'papers',
    ]
  }

  // Materialised views (alma.application.materialized_views): when a
  // background rebuild completes, the matching React Query root must
  // refetch so the page swaps the stale payload for the new one. Keys
  // mirror the view_key suffix (`materialize.insights.overview` →
  // `['insights']`).
  if (key.startsWith('materialize.insights.diag.')) {
    return ['insights-diag', 'insights-diagnostics']
  }
  if (key.startsWith('materialize.insights.')) {
    return ['insights']
  }
  if (key.startsWith('materialize.variant:discovery:branches:')) {
    return ['lens-branches']
  }

  return []
}

/**
 * Watches the operation feed and, for each operation that finishes, (a) always
 * refetches the pages it affects and (b) raises at most one outcome toast.
 *
 * Two deliberate rules keep this quiet (it used to flood ~76 toasts per
 * Discovery refresh):
 *
 *  - **Background plumbing never toasts** (cache materialization, hydration,
 *    scheduled sweeps, lane subtasks — see `isBackgroundTriggerSource`). Its
 *    query invalidation still runs so pages swap stale payloads.
 *  - **Dedup is a per-session high-water-mark, in memory only.** On the first
 *    poll we mark everything already-terminal as seen, so a fresh tab starts
 *    from "now" and can never replay a backlog. (The old localStorage set was
 *    capped at 100 IDs; the cap dropped still-relevant IDs and they
 *    re-toasted — that was the bug.)
 *
 * One toast per user-meaningful operation, with the job's own `message` as the
 * feedback line.
 */
export function useOperationToasts() {
  const queryClient = useQueryClient()
  const seenRef = useRef<Set<string>>(new Set())
  const hasInitializedRef = useRef(false)

  // Poll operations. Shares the ['activity-operations'] cache with
  // ActivityPanel and AIConfigCard so all three subscribers ride a
  // single network request — React Query picks the smallest active
  // refetchInterval among observers, so each can still declare its
  // own desired freshness without multiplying server load.
  const { data: operations } = useQuery({
    queryKey: ['activity-operations'],
    queryFn: () => api.get<JobStatus[]>('/activity'),
    refetchInterval: POLL_INTERVAL,
  })

  useEffect(() => {
    if (!operations || operations.length === 0) {
      return
    }

    const isTerminal = (op: JobStatus) =>
      op.status === 'completed' || op.status === 'failed' || op.status === 'cancelled'

    // First poll of this tab: everything already terminal is "old news" —
    // mark it seen so we never replay a backlog. In memory only; a reload
    // simply starts fresh from now.
    if (!hasInitializedRef.current) {
      hasInitializedRef.current = true
      for (const op of operations) {
        if (isTerminal(op)) seenRef.current.add(op.job_id)
      }
      return
    }

    // Operations that became terminal since the last poll.
    const newlyTerminal = operations.filter(
      (op) => isTerminal(op) && !seenRef.current.has(op.job_id),
    )
    if (newlyTerminal.length === 0) {
      return
    }

    for (const op of newlyTerminal) {
      seenRef.current.add(op.job_id)
      // Always refetch affected pages — background plumbing (cache
      // materialization, hydration) is precisely what pages need to pick up,
      // even though it never toasts.
      if (op.operation_key === 'papers.reconcile_groups') {
        void invalidateAfterPaperMutation(queryClient)
      }
      const roots = rootsForOperation(op.operation_key)
      if (roots.length > 0) {
        void invalidateQueryRoots(queryClient, ...roots)
      }
    }

    // Toast only user-meaningful, top-level outcomes — never background
    // plumbing, never a subtask (it rides under its parent), never a plain
    // cancellation. Capped per cycle as a final spam guard.
    const toToast = newlyTerminal.filter(
      (op) =>
        (op.status === 'completed' || op.status === 'failed') &&
        !op.parent_job_id &&
        !isBackgroundTriggerSource(op.trigger_source),
    )

    for (const op of toToast.slice(0, 3)) {
      if (op.status === 'completed') {
        toast({
          title: operationToastTitle(
            op.operation_key,
            runDisplayStatus(op.status, op.result?.batch) === 'partial' ? 'partial' : 'complete',
          ),
          description: op.message || op.job_id,
        })
      } else {
        errorToast(operationToastTitle(op.operation_key, 'failed'), op.message || undefined)
      }
    }
  }, [operations, queryClient])
}
