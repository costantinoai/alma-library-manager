import { Globe } from 'lucide-react'

import { type Settings } from '@/api/client'
import { SettingsCard, SettingsSections } from '@/components/settings/primitives'
import { OpenAlexSection } from '@/components/settings/OpenAlexSection'
import { SemanticScholarSection } from '@/components/settings/SemanticScholarSection'

interface ExternalApisCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
  onSave: () => void
  isSaving: boolean
  saveSuccess: boolean
}

/**
 * External APIs panel — the single home for the upstream metadata sources
 * ALMa queries. Replaces the previously separate OpenAlex and Semantic
 * Scholar cards: each is now a collapsible `SettingsSection` so the two
 * connections read as one group with one save scope (the Connections
 * section's "Save connection settings" footer persists both).
 *
 * The OpenAlex section hides itself when the active backend isn't OpenAlex
 * (see `OpenAlexSection`), so this panel collapses to just Semantic Scholar
 * in that case.
 */
export function ExternalApisCard({
  formData,
  onFormDataChange,
  onSave,
  isSaving,
  saveSuccess,
}: ExternalApisCardProps) {
  return (
    <SettingsCard
      icon={Globe}
      title="External APIs"
      description="Upstream metadata sources ALMa queries. OpenAlex is the active backend's primary source; Semantic Scholar adds SPECTER2 vectors and paper/author recommendations."
    >
      <SettingsSections>
        <OpenAlexSection
          formData={formData}
          onFormDataChange={onFormDataChange}
          onSave={onSave}
          isSaving={isSaving}
          saveSuccess={saveSuccess}
        />
        <SemanticScholarSection formData={formData} onFormDataChange={onFormDataChange} />
      </SettingsSections>
    </SettingsCard>
  )
}
