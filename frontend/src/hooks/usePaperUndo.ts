import { useMutation, useQueryClient } from '@tanstack/react-query'
import { undoPaperFeedback, type UndoAspect } from '@/api/client'
import { invalidateAfterPaperMutation, invalidateQueryRoots } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'

/**
 * Per-aspect "toggle off" for a paper, shared by every PaperCard surface.
 *
 * Re-clicking an applied action routes here via `PaperActionBar.onUndo(aspect)`
 * — Save→membership, Queue→reading, an active reaction→rating. Each undoes only
 * that button's effect (and deletes the matching signal events), then refreshes
 * every surface that observes the paper so the card reconciles.
 *
 * Pass the current `lensId` on Discovery so the lens recompute keys invalidate.
 */
export function usePaperUndo(lensId?: string | null) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ paperId, aspect }: { paperId: string; aspect: UndoAspect }) =>
      undoPaperFeedback(paperId, aspect),
    onSuccess: () => {
      void invalidateAfterPaperMutation(qc, lensId ?? undefined)
      void invalidateQueryRoots(
        qc,
        'lens-recommendations',
        'library-papers',
        'feed-inbox',
        'bootstrap',
        'author-publications',
        'author-detail',
      )
    },
    onError: () => errorToast('Could not undo', 'The database was busy — try again in a moment.'),
  })
}
