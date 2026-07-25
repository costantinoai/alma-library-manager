import { useMutation, useQueryClient } from '@tanstack/react-query'

import { updateLens, type CustomDirection, type Lens } from '@/api/client'
import { errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

export type BranchState = 'normal' | 'pinned' | 'boosted' | 'muted'

/** The lens's saved branch controls, with every field defaulted. */
export interface ResolvedBranchControls {
  temperature: number
  resolution: number
  pinned: string[]
  muted: string[]
  boosted: string[]
  custom_directions: CustomDirection[]
}

export const BRANCH_TEMPERATURE_DEFAULT = 0.28
export const BRANCH_RESOLUTION_DEFAULT = 1.0

export function resolveBranchControls(lens: Lens | null | undefined): ResolvedBranchControls {
  const c = lens?.branch_controls ?? null
  return {
    temperature: c?.temperature ?? BRANCH_TEMPERATURE_DEFAULT,
    resolution: c?.resolution ?? BRANCH_RESOLUTION_DEFAULT,
    pinned: c?.pinned ?? [],
    muted: c?.muted ?? [],
    boosted: c?.boosted ?? [],
    custom_directions: c?.custom_directions ?? [],
  }
}

/**
 * THE single writer for `lens.branch_controls`.
 *
 * Branch state is editable from two places — Branch Studio and the frontier
 * map's legend chips — and the backend accepts the WHOLE control object on
 * every write. So a partial write from either surface would silently wipe
 * whatever the other one owns (this is exactly how a temperature save used to
 * drop adopted directions). Routing both through `saveControls` means the
 * merge happens once, here, and neither surface can forget a field.
 */
export function useBranchControls(lens: Lens | null | undefined) {
  const queryClient = useQueryClient()
  const controls = resolveBranchControls(lens)

  const saveControls = useMutation({
    mutationFn: (patch: Partial<ResolvedBranchControls>) => {
      if (!lens?.id) throw new Error('No lens')
      // Merge over the CURRENT saved controls, never over defaults.
      return updateLens(lens.id, { branch_controls: { ...controls, ...patch } })
    },
    onSuccess: (updated) => {
      queryClient.setQueryData<Lens[]>(['lenses'], (prev) =>
        (prev ?? []).map((item) => (item.id === updated.id ? updated : item)),
      )
      void invalidateQueries(queryClient, ['lenses'], ['lens-branches', updated.id])
    },
    onError: () => errorToast('Branch controls failed', 'Could not save branch controls.'),
  })

  /** Which bucket a branch currently sits in. */
  const stateOf = (branchId: string): BranchState => {
    if (controls.muted.includes(branchId)) return 'muted'
    if (controls.pinned.includes(branchId)) return 'pinned'
    if (controls.boosted.includes(branchId)) return 'boosted'
    return 'normal'
  }

  /** Move a branch to exactly one bucket (or none) and persist immediately. */
  const setBranchState = (branchId: string, state: BranchState) => {
    const pinned = controls.pinned.filter((v) => v !== branchId)
    const boosted = controls.boosted.filter((v) => v !== branchId)
    const muted = controls.muted.filter((v) => v !== branchId)
    if (state === 'pinned') pinned.push(branchId)
    if (state === 'boosted') boosted.push(branchId)
    if (state === 'muted') muted.push(branchId)
    saveControls.mutate({ pinned, boosted, muted })
  }

  /** Chip click cycles the two states a map legend needs: boost ⇄ mute ⇄ off. */
  const cycleBranchState = (branchId: string, target: 'boosted' | 'muted') => {
    setBranchState(branchId, stateOf(branchId) === target ? 'normal' : target)
  }

  return {
    controls,
    stateOf,
    setBranchState,
    cycleBranchState,
    saveControls,
    isPending: saveControls.isPending,
  }
}
