/**
 * useRegionSelection — the ONE region-select primitive every map host
 * mounts (user call 2026-07-25). Owns the lassoed id set, the drop
 * anchor, and (for paper regions) the shared `/graphs/region/describe`
 * characterisation — label, top terms, sample, honest counts. Hosts add
 * their own meaning on top (adopt as a Direction, inspector digest,
 * list filter); the selection lifecycle is identical everywhere.
 */
import { useCallback, useState } from 'react'
import { useMutation } from '@tanstack/react-query'

import { describeRegion, type RegionDescription } from '@/api/client'

export interface RegionSelectionState {
  /** Node ids under the lasso — null when no region is active. */
  ids: string[] | null
  /** Screen anchor of the lasso corner (for hosts that drop a popover). */
  anchor: { x: number; y: number } | null
  /** Shared vocabulary characterisation (paper regions only). */
  description: RegionDescription | null
  describing: boolean
  /** Register a new selection; describes it when `describe` is on. */
  select: (ids: string[], anchor?: { x: number; y: number }) => void
  clear: () => void
}

export function useRegionSelection({ describe = true }: { describe?: boolean } = {}): RegionSelectionState {
  const [ids, setIds] = useState<string[] | null>(null)
  const [anchor, setAnchor] = useState<{ x: number; y: number } | null>(null)
  const describeMutation = useMutation({ mutationFn: (ids: string[]) => describeRegion(ids) })
  const { mutate, reset } = describeMutation

  const select = useCallback(
    (nextIds: string[], nextAnchor?: { x: number; y: number }) => {
      setIds(nextIds)
      setAnchor(nextAnchor ?? null)
      // The describe endpoint caps at 300 ids — same clip as Discovery.
      if (describe) mutate(nextIds.slice(0, 300))
    },
    [describe, mutate],
  )
  const clear = useCallback(() => {
    setIds(null)
    setAnchor(null)
    reset()
  }, [reset])

  return {
    ids,
    anchor,
    description: describeMutation.data ?? null,
    describing: describeMutation.isPending,
    select,
    clear,
  }
}
