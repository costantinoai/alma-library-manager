import { PlugZap } from 'lucide-react'

import { useBootstrap } from '@/hooks/useBootstrap'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'
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
    return <Page />
  }
  return (
    <EmptyState
      icon={PlugZap}
      title={`${addon.label} is switched off`}
      description="Turn it on in Settings under Add-ons. Anything it stored is still there."
      action={
        <Button variant="outline" onClick={() => navigateTo('settings', { anchor: 'addons' })}>
          Open Settings
        </Button>
      }
    />
  )
}
