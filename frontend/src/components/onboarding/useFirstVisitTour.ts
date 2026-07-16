import { useCallback, useEffect, useState } from 'react'

/** Once-per-page tour state backed by localStorage. */
export function useFirstVisitTour(pageKey: string, enabled = true) {
  const storageKey = `alma.tour.${pageKey}.completed`
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
