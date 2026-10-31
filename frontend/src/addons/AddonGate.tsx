import { Suspense } from 'react'
import { PlugZap } from 'lucide-react'

import { useBootstrap } from '@/hooks/useBootstrap'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
import { LoadingState } from '@/components/ui/LoadingState'
import { navigateTo } from '@/lib/hashRoute'

import type { AddonUi } from './registry'

/**
 * The one door to an add-on page. While the add-on is off its chunk is never
 * fetched and the address explains itself instead of showing a broken page.
 */
export function AddonGate({ addon }: { addon: AddonUi }) {
  const { data: bootstrap, isLoading } = useBootstrap()
  if (isLoading) return null

  const state = bootstrap?.addons.find((entry) => entry.id === addon.id)
  if (state?.enabled) {
    const Page = addon.page
    // The page chunk loads after the bootstrap read, so the shell's page motion
    // has already started. Suspending to the shell's own boundary would hide that
    // motion mid-flight, and it then stays at opacity 0: a blank page.
    return (
      <Suspense fallback={<LoadingState />}>
        <Page />
      </Suspense>
    )
  }
  return (
    <EmptyState
      icon={PlugZap}
      title={`${addon.label} is switched off`}
      description="Turn it on in Settings under Add-ons. Switching off keeps its data; only Remove deletes it."
      action={
        <Button variant="outline" onClick={() => navigateTo('settings', { anchor: 'addons' })}>
          Open Settings
        </Button>
      }
    />
  )
}
