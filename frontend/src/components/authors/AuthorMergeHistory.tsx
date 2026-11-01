/**
 * Profiles merged into this author, each with Undo. A merge made before the
 * ledger existed has no row here and cannot be taken back.
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { listAuthorMerges, undoAuthorMerge } from '@/api/client'
import { Button } from '@/components/ui/button'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateAfterAuthorChange } from '@/lib/queryHelpers'

export function AuthorMergeHistory({ authorId }: { authorId: string }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const merges = useQuery({
    queryKey: ['author-merges', authorId],
    queryFn: () => listAuthorMerges(authorId),
  })
  const undo = useMutation({
    mutationFn: (mergeId: string) => undoAuthorMerge(mergeId),
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ['author-merges', authorId] })
      void invalidateAfterAuthorChange(queryClient)
      toast({
        title: 'Merge undone',
        description: `${result.papers_returned} papers returned. These two now count as different people.`,
      })
    },
    onError: (err) => errorToast('Could not undo', err instanceof Error ? err.message : undefined),
  })

  const rows = merges.data?.merges ?? []
  if (rows.length === 0) return null
  return (
    <section className="mt-4">
      <EyebrowLabel tone="muted" className="mb-1">Merged profiles</EyebrowLabel>
      <ul className="space-y-1 text-xs">
        {rows.map((merge) => (
          <li key={merge.id} className="flex items-center justify-between gap-2">
            <span className="min-w-0 truncate text-slate-600">
              {merge.alt_name || merge.alt_openalex_id} · {merge.papers_moved} papers
            </span>
            {merge.undone_at ? (
              <span className="shrink-0 text-slate-500">Undone</span>
            ) : (
              <Button
                variant="muted"
                size="sm"
                className="h-6 shrink-0 px-1.5 text-caption"
                disabled={undo.isPending}
                onClick={() => undo.mutate(merge.id)}
              >
                Undo
              </Button>
            )}
          </li>
        ))}
      </ul>
    </section>
  )
}
