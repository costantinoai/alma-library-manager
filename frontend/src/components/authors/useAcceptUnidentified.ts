import { useMutation, useQueryClient } from '@tanstack/react-query'

import { acceptAuthorUnidentified, reopenAuthorIdentity } from '@/api/client'
import { useToast } from '@/hooks/useToast'
import { authorActionFailed } from '@/lib/authorActions'
import { invalidateAfterAuthorChange } from '@/lib/queryHelpers'

/**
 * "Can't identify": keep the author, stop flagging them. Terminal for every
 * automatic retry, so the toast offers the one way back.
 */
export function useAcceptUnidentified(onDone?: () => void) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  return useMutation({
    mutationFn: ({ id }: { id: string; name: string }) => acceptAuthorUnidentified(id),
    onSuccess: (_data, { id, name }) => {
      void invalidateAfterAuthorChange(queryClient, id)
      toast({
        title: 'Marked as unidentifiable',
        description: name,
        action: {
          label: 'Undo',
          onClick: () => {
            reopenAuthorIdentity(id)
              .then(() => invalidateAfterAuthorChange(queryClient, id))
              .catch((err) => authorActionFailed(queryClient, 'Could not undo', err))
          },
        },
      })
      onDone?.()
    },
    onError: (err) => authorActionFailed(queryClient, 'Could not update the author', err),
  })
}
