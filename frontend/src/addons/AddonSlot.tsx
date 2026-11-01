import { Suspense } from 'react'

import { LoadingState } from '@/components/ui/LoadingState'
import { useBootstrap } from '@/hooks/useBootstrap'

import { slotEntries, type AddonSlotName } from './registry'

/**
 * A place in a core page that add-ons may fill. It renders nothing while no
 * add-on fills it, and each part suspends here, never in the page around it.
 */
export function AddonSlot({ name }: { name: AddonSlotName }) {
  const { data: bootstrap } = useBootstrap()
  return (
    <>
      {slotEntries(name)
        .filter(({ addonId }) =>
          bootstrap?.addons.some((state) => state.id === addonId && state.enabled),
        )
        .map(({ addonId, Component }) => (
          <Suspense key={addonId} fallback={<LoadingState />}>
            <Component />
          </Suspense>
        ))}
    </>
  )
}
