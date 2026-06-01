import { useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { completeOnboarding } from '@/api/client'
import { OnboardingShell } from './OnboardingShell'
import { useOnboardingState } from './useOnboardingState'
import type { StepComponent, StepContext } from './types'
import { StepWelcome } from './steps/StepWelcome'
import { StepVision } from './steps/StepVision'
import { StepName } from './steps/StepName'
import { StepConnect } from './steps/StepConnect'
import { StepIdentity } from './steps/StepIdentity'
import { StepFollow } from './steps/StepFollow'
import { StepReact } from './steps/StepReact'
import { StepKeywords } from './steps/StepKeywords'
import { StepLens } from './steps/StepLens'
import { StepBranches } from './steps/StepBranches'
import { StepDiscovery } from './steps/StepDiscovery'
import { StepTriage } from './steps/StepTriage'
import { StepDone } from './steps/StepDone'

const STEPS: StepComponent[] = [
  StepWelcome,
  StepVision,
  StepName,
  StepConnect,
  StepIdentity,
  StepFollow,
  StepReact,
  StepKeywords,
  StepLens,
  StepBranches,
  StepDiscovery,
  StepTriage,
  StepDone,
]

/**
 * OnboardingFlow — the step machine. Owns persisted state + navigation, renders
 * the active step inside the OnboardingShell. `finish` marks onboarding done on
 * the server (so the gate stops showing), clears local state, and drops the user
 * into Discovery.
 */
export function OnboardingFlow() {
  const qc = useQueryClient()
  const { state, patch, reset } = useOnboardingState()

  const step = Math.min(Math.max(state.step, 0), STEPS.length - 1)

  const next = useCallback(
    () => patch({ step: Math.min(step + 1, STEPS.length - 1) }),
    [patch, step],
  )
  const back = useCallback(() => patch({ step: Math.max(step - 1, 0) }), [patch, step])

  const finish = useCallback(async () => {
    try {
      await completeOnboarding()
    } catch {
      /* non-fatal — the gate will still re-check on next boot */
    }
    reset()
    invalidateQueries(qc, ['bootstrap'])
    navigateTo('discovery')
  }, [qc, reset])

  const ctx: StepContext = { state, patch, next, back, finish, total: STEPS.length }
  const Active = STEPS[step]

  return (
    <OnboardingShell step={step} total={STEPS.length}>
      <Active {...ctx} />
    </OnboardingShell>
  )
}
