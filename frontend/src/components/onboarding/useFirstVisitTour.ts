import { useCallback, useEffect, useState } from 'react'

const TOUR_KEY_PREFIX = 'alma.tour.'

/**
 * Forget every page's "I've seen the tour" flag, so the first-visit tours run
 * again on the next visit to each page.
 *
 * Settings → Restart onboarding calls this alongside
 * `clearPersistedOnboardingState`: replaying the welcome but NOT the page tours
 * replayed only half the onboarding, and the half it skipped is the half that
 * explains the pages you actually work in.
 */
export function clearPageTourState() {
  try {
    Object.keys(localStorage)
      .filter((key) => key.startsWith(TOUR_KEY_PREFIX))
      .forEach((key) => localStorage.removeItem(key))
  } catch {
    /* ignore blocked storage */
  }
}

/** Once-per-page tour state backed by localStorage. */
export function useFirstVisitTour(pageKey: string, enabled = true) {
  const storageKey = `${TOUR_KEY_PREFIX}${pageKey}.completed`
  const [open, setOpen] = useState(false)

  useEffect(() => {
    if (!enabled) return
    try {
      if (localStorage.getItem(storageKey) !== 'done') {
        const timeout = window.setTimeout(() => setOpen(true), 600)
        return () => window.clearTimeout(timeout)
      }
    } catch {
      // Storage blocked: never auto-show.
    }
  }, [storageKey, enabled])

  const complete = useCallback(() => {
    try {
      localStorage.setItem(storageKey, 'done')
    } catch {
      // Storage blocked: closing the tour still works for this session.
    }
    setOpen(false)
  }, [storageKey])

  const relaunch = useCallback(() => setOpen(true), [])

  return { open, complete, relaunch }
}
