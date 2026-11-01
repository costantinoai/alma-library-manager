import { useMemo } from 'react'
import {
  useMutation,
  useQueryClient,
  type QueryKey,
} from '@tanstack/react-query'
import {
  applyPaperAction,
  getApiErrorMessage,
  updateReadingStatus,
  type LensRecommendation,
  type Publication,
  type UndoAspect,
} from '@/api/client'
import { invalidateAfterPaperMutation, invalidateQueryRoots } from '@/lib/queryHelpers'
import { errorToast, toast } from '@/hooks/useToast'

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
 * Goes through the one action route. `announce` confirms an undo the user asked
 * for from a toast; a toggle-off shows its result on the button itself.
 */
export function usePaperUndo(lensId?: string | null) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ paperId, aspect }: { paperId: string; aspect: UndoAspect; announce?: boolean }) =>
      applyPaperAction(paperId, 'undo', { surface: 'papers', undoAspect: aspect }),
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
            if (aspect === 'rating' || aspect === 'all') {
              paper.rating = 0
              // `deriveDiscoveryReaction` reads this first; it must not stay lit.
              if (userAction === 'like' || userAction === 'love' || userAction === 'dislike') {
                userAction = inLibrary ? 'save' : null
              }
            }
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
    onSuccess: (_result, { announce }) => {
      if (announce) toast({ title: 'Undone' })
      void invalidateAfterPaperMutation(qc, lensId ?? undefined)
      void invalidateQueryRoots(
        qc,
        'lens-recommendations',
        'library-papers',
        'feed-inbox',
        'bootstrap',
        'author-publications',
        'author-detail',
        'home-brief',
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

/**
 * The Undo button for an action's toast. `offerUndo(paperId)` takes back the
 * paper's newest action, `offerUndo(paperId, 'inbox')` undoes an Inbox Clear,
 * and `offerReadingUndo(paperId, previous)` restores a reading state, which
 * writes no event and so has no action to take back.
 */
export function useOfferUndo(lensId?: string | null) {
  const qc = useQueryClient()
  const undo = usePaperUndo(lensId)
  const restoreReading = useMutation({
    mutationFn: ({ paperId, previous }: { paperId: string; previous: ReadingState }) =>
      updateReadingStatus(paperId, (previous || null) as Parameters<typeof updateReadingStatus>[1]),
    onSuccess: () => {
      toast({ title: 'Undone' })
      void invalidateAfterPaperMutation(qc, lensId ?? undefined)
      void invalidateQueryRoots(qc, 'lens-recommendations', 'feed-inbox', 'author-publications')
    },
    onError: (error) => errorToast('Could not undo', getApiErrorMessage(error)),
  })
  const undoPaper = undo.mutate
  const restore = restoreReading.mutate
  return useMemo(
    () => ({
      offerUndo: (paperId: string, aspect: 'last' | 'inbox' = 'last') => ({
        label: 'Undo',
        onClick: () => undoPaper({ paperId, aspect, announce: true }),
      }),
      offerReadingUndo: (paperId: string, previous: ReadingState) => ({
        label: 'Undo',
        onClick: () => restore({ paperId, previous }),
      }),
    }),
    [undoPaper, restore],
  )
}

/** A reading state as cards hold it; empty means not on the reading list. */
type ReadingState = string | null | undefined
