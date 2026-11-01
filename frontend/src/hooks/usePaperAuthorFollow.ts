import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { followAuthorFromPaper, listFollowedAuthors, unfollowAuthor } from '@/api/client'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { normalizeAuthorName } from '@/lib/utils'

// Re-export so existing callers that imported the helper from this hook
// continue to work. Canonical implementation lives in `lib/utils.ts`.
export { normalizeAuthorName }

export function usePaperAuthorFollow() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [pendingAuthorName, setPendingAuthorName] = useState<string | null>(null)

  const followedAuthorsQuery = useQuery({
    queryKey: ['library-followed-authors'],
    queryFn: listFollowedAuthors,
    retry: 1,
  })

  const followedAuthorNames = useMemo(
    () =>
      new Set(
        (followedAuthorsQuery.data ?? [])
          .map((item) => normalizeAuthorName(item.name))
          .filter((item) => item.length > 0),
      ),
    [followedAuthorsQuery.data],
  )

  /** Followed author ids by normalised name, so a name on a paper can be
   *  unfollowed without a lookup. */
  const followedAuthorIdByName = useMemo(
    () =>
      new Map(
        (followedAuthorsQuery.data ?? [])
          .map((item) => [normalizeAuthorName(item.name), item.author_id] as const)
          .filter(([name]) => name.length > 0),
      ),
    [followedAuthorsQuery.data],
  )
  const followedAuthorIds = useMemo(
    () => new Set((followedAuthorsQuery.data ?? []).map((item) => item.author_id)),
    [followedAuthorsQuery.data],
  )

  const refreshFollowQueries = () =>
    invalidateQueries(
      queryClient,
      ['authors'],
      ['library-followed-authors'],
      ['feed-monitors'],
      ['author-suggestions'],
    )

  const followMutation = useMutation({
    mutationFn: ({ paperId, authorName }: { paperId: string; authorName: string }) =>
      followAuthorFromPaper({ paper_id: paperId, author_name: authorName }),
    onMutate: ({ authorName }) => {
      setPendingAuthorName(normalizeAuthorName(authorName))
    },
    onSuccess: async (data) => {
      await refreshFollowQueries()
      toast({
        title: data.already_followed ? 'Author already followed' : 'Author followed',
        description: `${data.author.name} will contribute to Feed on the next refresh.`,
      })
    },
    onError: () => {
      errorToast('Could not follow author')
    },
    onSettled: () => {
      setPendingAuthorName(null)
    },
  })

  const unfollowMutation = useMutation({
    mutationFn: ({ authorId }: { authorId: string; authorName: string }) => unfollowAuthor(authorId),
    onMutate: ({ authorName }) => {
      setPendingAuthorName(normalizeAuthorName(authorName))
    },
    onSuccess: async (_data, { authorName }) => {
      await refreshFollowQueries()
      toast({ title: 'Author unfollowed', description: `${authorName} no longer feeds your Feed.` })
    },
    onError: () => {
      errorToast('Could not unfollow author')
    },
    onSettled: () => {
      setPendingAuthorName(null)
    },
  })

  return {
    followedAuthorNames,
    followedAuthorIds,
    followedAuthorIdByName,
    pendingAuthorName,
    unfollowAuthor: (authorId: string, authorName: string) =>
      unfollowMutation.mutate({ authorId, authorName }),
    followAuthor: (authorName: string, paperId: string) =>
      followMutation.mutate({ authorName, paperId }),
  }
}
