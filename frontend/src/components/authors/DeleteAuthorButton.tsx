/**
 * DeleteAuthorButton — the "this person shouldn't be in my corpus at all" exit
 * from any author surface.
 *
 * It is the LAST resort in the needs-attention ladder, after the resolver
 * (retry), the manual identifier paste, and "can't identify" (which keeps the
 * author but stops the nagging). Deleting is a hard delete, so it is always
 * gated behind a confirm that spells out the three consequences: the author row
 * goes, papers left with no other tracked author go with them, and the person is
 * suppressed from the suggestion rails so they don't reappear on the next
 * OpenAlex co-author expansion.
 */
import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Trash2 } from 'lucide-react'

import { deleteAuthor } from '@/api/client'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/library'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

interface DeleteAuthorButtonProps {
  authorId: string
  authorName: string
  /** Icon-only (row affordance) or a labelled button (dialog footer). */
  variant?: 'icon' | 'labelled'
  disabled?: boolean
  /** Fired after a successful delete — e.g. to close the dialog it lives in. */
  onDeleted?: () => void
}

export function DeleteAuthorButton({
  authorId,
  authorName,
  variant = 'icon',
  disabled,
  onDeleted,
}: DeleteAuthorButtonProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [confirming, setConfirming] = useState(false)

  const mutation = useMutation({
    mutationFn: () => deleteAuthor(authorId),
    onSuccess: async () => {
      setConfirming(false)
      await invalidateQueries(
        queryClient,
        ['authors'],
        ['authors-needs-attention'],
        ['author-detail', authorId],
        ['author-suggestions'],
        ['health'],
        ['papers'],
      )
      toast({ title: 'Author deleted', description: authorName })
      onDeleted?.()
    },
    onError: (err) => errorToast('Could not delete author', String(err)),
  })

  return (
    <>
      {variant === 'icon' ? (
        <Button
          size="sm"
          variant="ghost"
          className="text-slate-400 hover:text-critical-700"
          disabled={disabled || mutation.isPending}
          title={`Delete ${authorName} from the corpus`}
          aria-label={`Delete ${authorName}`}
          onClick={() => setConfirming(true)}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      ) : (
        <Button
          variant="ghost"
          className="text-slate-500 hover:text-critical-700"
          disabled={disabled || mutation.isPending}
          onClick={() => setConfirming(true)}
        >
          <Trash2 className="mr-1 h-3.5 w-3.5" />
          Delete author
        </Button>
      )}

      <ConfirmDialog
        open={confirming}
        onOpenChange={setConfirming}
        title={`Delete ${authorName}?`}
        description={
          `This is permanent, and unlike "can't identify" it does not keep the row: ` +
          `the author is removed, every paper that no author you track still covers is ` +
          `deleted with them, and the person is suppressed from the author suggestion ` +
          `rails so co-author expansion doesn't surface them again.`
        }
        isPending={mutation.isPending}
        onConfirm={() => mutation.mutate()}
      />
    </>
  )
}
