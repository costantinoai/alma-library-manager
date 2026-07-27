import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Telescope } from 'lucide-react'

import { createLensFromMapSelection } from '@/api/client'
import { Button } from '@/components/ui/button'
import { errorToast, useToast } from '@/hooks/useToast'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'

interface CreateSelectionLensButtonProps {
  ids: string[]
  scope: 'library' | 'corpus'
  selectionKind: 'papers' | 'authors'
  name: string
  onCreated?: () => void
}

export function CreateSelectionLensButton({
  ids,
  scope,
  selectionKind,
  name,
  onCreated,
}: CreateSelectionLensButtonProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const mutation = useMutation({
    mutationFn: () =>
      createLensFromMapSelection({
        ids,
        scope,
        selection_kind: selectionKind,
        name,
      }),
    onSuccess: async (result) => {
      await invalidateQueries(
        queryClient,
        ['lenses'],
        ['library-collections'],
        ['library-saved'],
        ['library-workflow-summary'],
        ['home-brief'],
        ['graph'],
        ['frontier'],
      )
      onCreated?.()
      toast({
        title: 'Lens created',
        description: `${result.paper_count} papers saved to “${result.name}”.`,
      })
      navigateTo('discovery', { lens: result.lens_id })
    },
    onError: () =>
      errorToast(
        'Could not create lens',
        'Selection changed or contains items outside this map scope.',
      ),
  })

  return (
    <Button
      type="button"
      size="sm"
      className="w-full"
      loading={mutation.isPending}
      disabled={ids.length === 0}
      onClick={() => mutation.mutate()}
      title="Save every selected paper to a new collection, then create a Discovery lens from it"
    >
      <Telescope className="h-3.5 w-3.5" />
      Create lens from selection
    </Button>
  )
}
