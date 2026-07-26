import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  addPaperToCollections,
  applyPaperAction,
  getPaperById,
  updateReadingStatus,
  type OnboardingPaperAction,
} from '@/api/client'
import { usePaperUndo } from '@/hooks/usePaperUndo'
import { errorToast, useToast } from '@/hooks/useToast'
import {
  invalidateAfterPaperMutation,
  invalidateQueries,
} from '@/lib/queryHelpers'
import {
  MapPaperPopup,
  type MapPaperSummary,
} from './MapPaperPopup'

interface CorpusMapPaperPopupProps {
  paperId: string
  fallback: MapPaperSummary
  onClose: () => void
  onOpenDetails?: () => void
}

/** Mutation adapter for corpus/library dots.
 *
 * The visual card stays presentation-only; this adapter reuses the same
 * corpus paper-feedback, reading, collection, and undo APIs as the rest of
 * ALMa and reconciles every map/list observer after a change.
 */
export function CorpusMapPaperPopup({
  paperId,
  fallback,
  onClose,
  onOpenDetails,
}: CorpusMapPaperPopupProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const paperQuery = useQuery({
    queryKey: ['map-paper-popup', paperId],
    queryFn: () => getPaperById(paperId),
    staleTime: 30_000,
  })
  const undoMutation = usePaperUndo()

  const reconcile = async () => {
    await invalidateAfterPaperMutation(queryClient)
    await invalidateQueries(
      queryClient,
      ['map-paper-popup', paperId],
    )
  }

  const feedbackMutation = useMutation({
    mutationFn: (action: Exclude<OnboardingPaperAction, 'dismiss' | 'defer' | 'undo'>) =>
      applyPaperAction(paperId, action, { surface: 'map' }),
    onSuccess: async (_result, action) => {
      const messages: Record<typeof action, string> = {
        add: 'Saved to Library.',
        like: 'Paper rated 4 stars.',
        love: 'Paper rated 5 stars.',
        dislike: 'Paper rated 1 star.',
      }
      toast({ title: 'Paper updated', description: messages[action] })
      await reconcile()
    },
    onError: () => errorToast('Could not update paper', 'Try again in a moment.'),
  })

  const queueMutation = useMutation({
    mutationFn: () => updateReadingStatus(paperId, 'reading'),
    onSuccess: async () => {
      toast({ title: 'Added to reading list', description: 'Marked as Reading.' })
      await reconcile()
    },
    onError: () => errorToast('Queue failed', 'Could not add to reading list.'),
  })

  const collectionsMutation = useMutation({
    mutationFn: (collectionIds: string[]) => addPaperToCollections(paperId, collectionIds),
    onSuccess: reconcile,
  })

  const live = paperQuery.data
  const rating = Number(live?.rating ?? 0)
  const reaction =
    rating >= 5 ? 'love' : rating >= 4 ? 'like' : rating > 0 && rating <= 2 ? 'dislike' : null
  const summary: MapPaperSummary = {
    ...fallback,
    title: live?.title || fallback.title,
    authors: live?.authors || fallback.authors,
    tldr: live?.tldr || fallback.tldr,
    year: live?.year ?? fallback.year,
    journal: live?.journal || fallback.journal,
    citedByCount: live?.cited_by_count ?? fallback.citedByCount,
    statusLabel:
      live?.status === 'library'
        ? 'In your library'
        : fallback.statusLabel,
  }
  const pending =
    feedbackMutation.isPending ||
    queueMutation.isPending ||
    collectionsMutation.isPending ||
    undoMutation.isPending

  return (
    <MapPaperPopup
      paper={summary}
      onClose={onClose}
      onOpenDetails={onOpenDetails}
      onQueue={() => queueMutation.mutate()}
      onAdd={() => feedbackMutation.mutate('add')}
      onLike={() => feedbackMutation.mutate('like')}
      onLove={() => feedbackMutation.mutate('love')}
      onDislike={() => feedbackMutation.mutate('dislike')}
      onUndo={async (aspect) => {
        await undoMutation.mutateAsync({ paperId, aspect })
        await invalidateQueries(queryClient, ['map-paper-popup', paperId])
      }}
      onAddToCollections={async (ids) => {
        await collectionsMutation.mutateAsync(ids)
      }}
      isSaved={live?.status === 'library' || fallback.statusLabel === 'In your library'}
      isQueued={live?.reading_status === 'reading'}
      reaction={reaction}
      pending={pending}
    />
  )
}
