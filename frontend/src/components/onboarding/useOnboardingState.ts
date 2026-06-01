import { useCallback, useEffect, useRef, useState } from 'react'
import { INITIAL_STATE, type OnboardingState } from './types'

const STORAGE_KEY = 'alma.onboarding.state'

/**
 * Onboarding state + localStorage persistence so a half-finished first run
 * resumes after a reload (mirrors the guarded localStorage pattern in
 * `AppShell.tsx`). Returns the state, a shallow `patch`, and a `reset`.
 */
export function useOnboardingState() {
  const [state, setState] = useState<OnboardingState>(() => {
    try {
      const raw = localStorage.getItem(STORAGE_KEY)
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
  useEffect(() => {
    if (firstRun.current) {
      firstRun.current = false
    }
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state))
    } catch {
      /* storage blocked — non-fatal */
    }
  }, [state])

  const patch = useCallback((partial: Partial<OnboardingState>) => {
    setState((prev) => ({ ...prev, ...partial }))
  }, [])

  const reset = useCallback(() => {
    try {
      localStorage.removeItem(STORAGE_KEY)
    } catch {
      /* ignore */
    }
    setState(INITIAL_STATE)
  }, [])

  return { state, patch, reset }
}
