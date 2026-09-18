import { useQuery } from '@tanstack/react-query'

import { previewLensBranches, refreshLensBranches, type Lens } from '@/api/client'
import { useAsyncRefresh } from './useAsyncRefresh'
import { useDebounce } from './useDebounce'

/** Stored data renders immediately; an explicit POST owns background work.
 * Closing the panel stops reads/polling without cancelling its durable job. */
export function useBranchPreview(lens: Lens | null, resolution: number, enabled: boolean) {
  const debouncedResolution = useDebounce(resolution, 500)
  const lensId = lens?.id
  const identity = JSON.stringify([lensId, lens?.branch_controls, lens?.last_suggestion_set_id, debouncedResolution])
  const query = useQuery({
    queryKey: ['lens-branches', lensId, identity],
    queryFn: () => previewLensBranches(lensId!, { max_branches: 8, resolution: debouncedResolution }),
    enabled: enabled && Boolean(lensId),
    staleTime: 60_000,
  })

  const refreshState = useAsyncRefresh({
    queryKey: ['lens-branches', lensId, identity],
    enabled: enabled && Boolean(lensId),
    request: force => refreshLensBranches(lensId!, { max_branches: 8, resolution: debouncedResolution }, force),
  })

  return { ...query, ...refreshState }
}
