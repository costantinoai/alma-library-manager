import {
  useMutation,
  useQueryClient,
  type QueryKey,
} from '@tanstack/react-query'
import {
  getApiErrorMessage,
  undoPaperFeedback,
  type LensRecommendation,
  type Publication,
  type UndoAspect,
} from '@/api/client'
import { invalidateAfterPaperMutation, invalidateQueryRoots } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'

interface UndoCacheSnapshot {
  recommendations: Array<[QueryKey, LensRecommendation[] | undefined]>
  popupPapers: Array<[QueryKey, Publication | undefined]>
}

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
    onMutate: ({ paperId, aspect }): UndoCacheSnapshot => {
      const recommendations =
        qc.getQueriesData<LensRecommendation[]>({
          queryKey: ['lens-recommendations'],
        })
      const popupPapers =
        qc.getQueriesData<Publication>({
          queryKey: ['map-paper-popup', paperId],
        })

      qc.setQueriesData<LensRecommendation[]>(
        { queryKey: ['lens-recommendations'] },
        (current) =>
          current?.map((rec) => {
            if (rec.paper_id !== paperId || !rec.paper) return rec
            const paper = { ...rec.paper }
            let userAction = rec.user_action
            let inLibrary = rec.in_library
            if (aspect === 'rating' || aspect === 'all') paper.rating = 0
            if (aspect === 'reading' || aspect === 'all') {
              paper.reading_status = ''
              if (userAction === 'read') userAction = null
            }
            if (aspect === 'membership' || aspect === 'all') {
              paper.status = 'tracked'
              inLibrary = false
              if (userAction === 'save') userAction = null
            }
            return {
              ...rec,
              in_library: inLibrary,
              user_action: userAction,
              paper,
            }
          }),
      )
      qc.setQueriesData<Publication>(
        { queryKey: ['map-paper-popup', paperId] },
        (paper) => {
          if (!paper) return paper
          const next = { ...paper }
          if (aspect === 'rating' || aspect === 'all') next.rating = 0
          if (aspect === 'reading' || aspect === 'all') next.reading_status = ''
          if (aspect === 'membership' || aspect === 'all') next.status = 'tracked'
          return next
        },
      )

      return { recommendations, popupPapers }
    },
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
    onError: (error, _variables, snapshots) => {
      for (const [key, value] of snapshots?.recommendations ?? []) {
        qc.setQueryData(key, value)
      }
      for (const [key, value] of snapshots?.popupPapers ?? []) {
        qc.setQueryData(key, value)
      }
      errorToast('Could not undo', getApiErrorMessage(error))
    },
  })
}
