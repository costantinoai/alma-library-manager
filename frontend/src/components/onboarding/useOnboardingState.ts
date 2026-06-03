import { useCallback, useEffect, useRef, useState } from 'react'
import { INITIAL_STATE, type OnboardingState } from './types'

export const ONBOARDING_STORAGE_KEY = 'alma.onboarding.state'

export function clearPersistedOnboardingState() {
  try {
    localStorage.removeItem(ONBOARDING_STORAGE_KEY)
  } catch {
    /* ignore blocked storage */
  }
}

/**
 * Onboarding state + localStorage persistence so a half-finished first run
 * resumes after a reload (mirrors the guarded localStorage pattern in
 * `AppShell.tsx`). Returns the state, a shallow `patch`, and a `reset`.
 */
export function useOnboardingState() {
  const [state, setState] = useState<OnboardingState>(() => {
    try {
      const raw = localStorage.getItem(ONBOARDING_STORAGE_KEY)
      if (raw) {
        const parsed = JSON.parse(raw) as Partial<OnboardingState>
        return { ...INITIAL_STATE, ...parsed }
      }
    } catch {
      /* ignore corrupt/blocked storage — start fresh */
    }
    return INITIAL_STATE
  })

  // Debounced-ish write: persist whenever state changes.
  const firstRun = useRef(true)
  const skipNextPersist = useRef(false)
  useEffect(() => {
    if (firstRun.current) {
      firstRun.current = false
    }
    if (skipNextPersist.current) {
      skipNextPersist.current = false
      return
    }
    try {
      localStorage.setItem(ONBOARDING_STORAGE_KEY, JSON.stringify(state))
    } catch {
      /* storage blocked — non-fatal */
    }
  }, [state])

  const patch = useCallback((partial: Partial<OnboardingState>) => {
    setState((prev) => ({ ...prev, ...partial }))
  }, [])

  const reset = useCallback(() => {
    skipNextPersist.current = true
    clearPersistedOnboardingState()
    setState(INITIAL_STATE)
  }, [])

  return { state, patch, reset }
}
