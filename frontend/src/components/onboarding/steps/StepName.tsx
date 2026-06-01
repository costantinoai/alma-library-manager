import { useState } from 'react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { setOnboardingProfile } from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

export function StepName({ state, patch, next, back }: StepContext) {
  const [name, setName] = useState(state.name)
  const [saving, setSaving] = useState(false)

  const commit = async () => {
    const trimmed = name.trim()
    patch({ name: trimmed })
    if (trimmed) {
      setSaving(true)
      try {
        await setOnboardingProfile(trimmed)
      } catch {
        /* non-fatal — the greeting just won't persist */
      } finally {
        setSaving(false)
      }
    }
    next()
  }

  return (
    <StepShell
      eyebrow="Hello"
      title="What should I call you?"
      lead="Just a first name is plenty — it's only used to make ALMa feel a little less like software."
      footer={
        <StepNav
          onBack={back}
          onSkip={next}
          onContinue={commit}
          continueLabel="Continue"
          continueLoading={saving}
        />
      }
    >
      <div className="max-w-sm space-y-2">
        <Label htmlFor="onboarding-name" className="text-slate-600">
          Your name
        </Label>
        <Input
          id="onboarding-name"
          value={name}
          autoFocus
          placeholder="e.g. Andrea"
          maxLength={120}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') commit()
          }}
        />
      </div>
    </StepShell>
  )
}
