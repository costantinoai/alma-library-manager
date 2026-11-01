/** Read the backend's connection answers, and run a check. */
import { useCallback } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  checkConnection,
  getApiErrorMessage,
  getConnections,
  type ConnectionRecord,
  type ConnectionsResponse,
} from '@/api/client'
import { errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

const QUERY_KEY = ['connections']

export function useConnections() {
  return useQuery({
    queryKey: QUERY_KEY,
    queryFn: getConnections,
    // A check started elsewhere (another tab, a save) is followed until it ends.
    refetchInterval: (query) =>
      query.state.data?.connections.some((c) => c.state === 'checking') ? 2_000 : false,
  })
}

export function useConnection(id: string): ConnectionRecord | undefined {
  return useConnections().data?.connections.find((c) => c.id === id)
}

/** Run a check and refresh every surface that reads its verdict. */
export function useCheckMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: checkConnection,
    onError: (error) => errorToast('Could not run the check', getApiErrorMessage(error)),
    onSettled: () => invalidateQueries(queryClient, QUERY_KEY, ['home-brief'], ['plugins']),
  })
}

/**
 * After a save: check whichever of `ids` now holds a value no check has seen.
 * Those are the ones the save changed, so an unchanged value is not probed.
 */
export function useCheckAfterSave() {
  const queryClient = useQueryClient()
  const { mutate } = useCheckMutation()
  return useCallback(
    async (ids: string[]) => {
      await queryClient.invalidateQueries({ queryKey: QUERY_KEY })
      const data = await queryClient.fetchQuery<ConnectionsResponse>({
        queryKey: QUERY_KEY,
        queryFn: getConnections,
      })
      data.connections
        .filter((c) => ids.includes(c.id) && c.state === 'unverified' && c.checkable)
        .forEach((c) => mutate(c.id))
    },
    [queryClient, mutate],
  )
}
