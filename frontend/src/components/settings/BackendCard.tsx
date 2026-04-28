import { Server } from 'lucide-react'

import type { Settings } from '@/api/client'
import { OptionCard, SettingsCard } from '@/components/settings/primitives'
import { RadioGroup } from '@/components/ui/radio-group'

interface BackendCardProps {
  backend: Settings['backend']
  onBackendChange: (backend: Settings['backend']) => void
}

export function BackendCard({ backend, onBackendChange }: BackendCardProps) {
  return (
    <SettingsCard
      icon={Server}
      title="Data Backend"
      description="Choose which service to use for fetching publication data."
    >
      <RadioGroup
        value={backend}
        onValueChange={(value) => onBackendChange(value as Settings['backend'])}
        className="grid grid-cols-1 gap-3 sm:grid-cols-2"
      >
        <OptionCard
          value="openalex"
          selected={backend === 'openalex'}
          title="OpenAlex"
          description="Primary — open API with credits tracking."
        />
        <OptionCard
          value="scholar"
          selected={backend === 'scholar'}
          title="Google Scholar"
          description="Fallback — direct scraping."
        />
      </RadioGroup>
    </SettingsCard>
  )
}
