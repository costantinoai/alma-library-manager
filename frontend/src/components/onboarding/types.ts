import type * as React from 'react'

/**
 * Onboarding shared types.
 *
 * The flow is a 12-step client-side machine. State that must survive a reload
 * (so a half-finished onboarding resumes) lives in `OnboardingState`, persisted
 * to localStorage by `useOnboardingState`. Everything else (server counts,
 * suggestions, recommendations) is read live via React Query in each step.
 */

export interface OwnerInfo {
  /** Canonical local authors.id once ingested. */
  author_id: string
  openalex_id: string
  name: string
}

export interface OnboardingState {
  /** Active step index into the STEPS array. */
  step: number
  /** The user's display name (also persisted server-side via /onboarding/profile). */
  name: string
  /** The user's own author profile, once ingested in the Identity step. */
  owner: OwnerInfo | null
  /** Activity job id of the owner backfill, for an optional progress link. */
  ownerJobId: string | null
  /** Keyword monitors created in the Keywords step (for the summary). */
  keywords: string[]
  /** The library lens created in the Lens step. */
  lensId: string | null
  /** Whether the first discovery run has been kicked off. */
  discoveryRun: boolean
}

export const INITIAL_STATE: OnboardingState = {
  step: 0,
  name: '',
  owner: null,
  ownerJobId: null,
  keywords: [],
  lensId: null,
  discoveryRun: false,
}

/** Everything a step needs to render and advance the flow. */
export interface StepContext {
  state: OnboardingState
  patch: (partial: Partial<OnboardingState>) => void
  /** Advance to the next step. */
  next: () => void
  /** Go back one step. */
  back: () => void
  /** Mark onboarding complete on the server and close the flow. */
  finish: () => void
  /** True while the completion / skip write is in flight. */
  finishing: boolean
  /** Total number of steps (for "Step n of N"). */
  total: number
}

export type StepComponent = (ctx: StepContext) => React.ReactElement

export interface StepDef {
  id: string
  /** Eyebrow shown in the persistent header (e.g. "YOUR IDENTITY"). */
  eyebrow: string
  Component: StepComponent
}
