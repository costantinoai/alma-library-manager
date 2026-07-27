/**
 * useRegionSelection — the ONE region-select primitive every map host
 * mounts (user call 2026-07-25). Owns the lassoed id set, the drop
 * anchor, and (for paper regions) the shared `/graphs/region/describe`
 * characterisation — label, top terms, sample, honest counts. Hosts add
 * their own meaning on top (adopt as a Direction, inspector digest,
 * list filter); the selection lifecycle is identical everywhere.
 */
import { useCallback, useEffect, useState } from 'react'
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

/** Dedupe lasso ids and intersect them with dots in the current payload.
 * Exported so non-paper hosts use the exact same subset rule. */
export function selectionWithinVisible(
  ids: string[],
  visibleIds?: ReadonlySet<string>,
): string[] {
  const unique = listUnique(ids)
  return visibleIds ? unique.filter((id) => visibleIds.has(id)) : unique
}

function listUnique(ids: string[]): string[] {
  return [...new Set(ids.map((id) => String(id || '').trim()).filter(Boolean))]
}

export function useRegionSelection({
  describe = true,
  visibleIds,
}: {
  describe?: boolean
  visibleIds?: ReadonlySet<string>
} = {}): RegionSelectionState {
  const [ids, setIds] = useState<string[] | null>(null)
  const [anchor, setAnchor] = useState<{ x: number; y: number } | null>(null)
  const describeMutation = useMutation({ mutationFn: (ids: string[]) => describeRegion(ids) })
  const { mutate, reset } = describeMutation

  const select = useCallback(
    (nextIds: string[], nextAnchor?: { x: number; y: number }) => {
      const scoped = selectionWithinVisible(nextIds, visibleIds)
      if (scoped.length === 0) {
        setIds(null)
        setAnchor(null)
        reset()
        return
      }
      setIds(scoped)
      setAnchor(nextAnchor ?? null)
      // The describe endpoint caps at 300 ids — same clip as Discovery.
      if (describe) mutate(scoped.slice(0, 300))
    },
    [describe, mutate, reset, visibleIds],
  )
  const clear = useCallback(() => {
    setIds(null)
    setAnchor(null)
    reset()
  }, [reset])

  // A scope/layer switch can replace the payload between pointer-up and
  // mutation completion. Never retain ids no longer rendered.
  useEffect(() => {
    if (!ids || !visibleIds || ids.every((id) => visibleIds.has(id))) return
    clear()
  }, [clear, ids, visibleIds])

  return {
    ids,
    anchor,
    description: describeMutation.data ?? null,
    describing: describeMutation.isPending,
    select,
    clear,
  }
}
