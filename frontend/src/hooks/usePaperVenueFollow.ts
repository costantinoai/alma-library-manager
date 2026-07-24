import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { createFeedMonitor, getApiErrorMessage, listFeedMonitors } from '@/api/client'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

/** Follow a journal (venue) straight from a paper card, mirroring
 * `usePaperAuthorFollow`. Resolution to an OpenAlex source id happens in the
 * VenueHoverCard (via `venueSearch`); this hook owns follow-state truth + the
 * create mutation. */
export function usePaperVenueFollow() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [pendingVenueName, setPendingVenueName] = useState<string | null>(null)

  const monitorsQuery = useQuery({
    queryKey: ['feed-monitors'],
    queryFn: listFeedMonitors,
    retry: 1,
  })

  // Followed venues keyed by BOTH lowercased display name AND lowercased source
  // id, so a card can resolve follow-state whether it knows the name or the id.
  const followedVenueKeys = useMemo(() => {
    const keys = new Set<string>()
    for (const monitor of monitorsQuery.data ?? []) {
      if (monitor.monitor_type !== 'venue') continue
      const name = String((monitor.config?.query as string | undefined) ?? monitor.label ?? '')
        .trim()
        .toLowerCase()
      if (name) keys.add(name)
      const sourceId = String((monitor.config?.source_id as string | undefined) ?? monitor.monitor_key ?? '')
        .trim()
        .toLowerCase()
      if (sourceId) keys.add(sourceId)
    }
    return keys
  }, [monitorsQuery.data])

  const followMutation = useMutation({
    mutationFn: ({
      sourceId,
      displayName,
      keywords,
    }: {
      sourceId: string
      displayName: string
      keywords?: string[]
    }) =>
      createFeedMonitor({
        monitor_type: 'venue',
        query: displayName,
        source_id: sourceId,
        filter_keywords: keywords ?? [],
      }),
    onMutate: ({ displayName }) => {
      setPendingVenueName(displayName.trim().toLowerCase())
    },
    onSuccess: async (monitor) => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({
        title: 'Journal followed',
        description: `${monitor.label} will collect new papers in Feed → Journals.`,
      })
    },
    onError: (err: unknown) => {
      const message = getApiErrorMessage(err)
      errorToast(message?.includes('already') ? 'You already follow this journal' : 'Could not follow journal')
    },
    onSettled: () => {
      setPendingVenueName(null)
    },
  })

  return {
    followedVenueKeys,
    pendingVenueName,
    isVenueFollowed: (key?: string | null) =>
      !!key && followedVenueKeys.has(key.trim().toLowerCase()),
    followVenue: (args: { sourceId: string; displayName: string; keywords?: string[] }) =>
      followMutation.mutate(args),
  }
}
