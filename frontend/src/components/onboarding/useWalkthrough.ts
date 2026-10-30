import { useCallback, useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query'
import {
  getApiErrorMessage,
  getWalkthroughs,
  importWalkthroughFlags,
  recordWalkthroughOutcome,
  replayWalkthrough,
  type WalkthroughCloseOutcome,
  type WalkthroughState,
} from '@/api/client'
import { errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

export const WALKTHROUGHS_QUERY_KEY: string[] = ['walkthroughs']
export const WELCOME_WALKTHROUGH = 'welcome'

const TOUR_OPEN_DELAY_MS = 600

/** The flags this browser kept before state moved to the database. */
const LEGACY_TOUR_FLAG = /^alma\.tour\.(.+)\.completed$/
const LEGACY_WIZARD_PROGRESS_KEY = 'alma.onboarding.state'

type WalkthroughMap = Record<string, WalkthroughState>

function readLegacyTourFlags(): { keys: string[]; ids: string[] } {
  const keys: string[] = []
  const ids: string[] = []
  try {
    for (const key of Object.keys(localStorage)) {
      const match = LEGACY_TOUR_FLAG.exec(key)
      if (!match) continue
      keys.push(key)
      if (localStorage.getItem(key) === 'done') ids.push(match[1])
    }
  } catch {
    /* blocked storage holds nothing to import */
  }
  return { keys, ids }
}

/**
 * Hand this browser's old flags to the server, one way, then delete them.
 *
 * The flags stay until the server has them, so a failed request retries on the
 * next read. An error propagates to the query, which then has no data and shows
 * no overlay.
 */
async function importLegacyBrowserState() {
  const { keys, ids } = readLegacyTourFlags()
  if (ids.length > 0) await importWalkthroughFlags(ids)
  try {
    keys.forEach((key) => localStorage.removeItem(key))
    localStorage.removeItem(LEGACY_WIZARD_PROGRESS_KEY)
  } catch {
    /* blocked storage: nothing was stored */
  }
}

async function fetchWalkthroughs(): Promise<WalkthroughMap> {
  await importLegacyBrowserState()
  const { walkthroughs } = await getWalkthroughs()
  return Object.fromEntries(walkthroughs.map((state) => [state.id, state]))
}

/**
 * Server walkthrough state, keyed by id. This is the only input to any overlay:
 * `data` stays undefined while loading and after a failure, and every caller
 * treats that as "show nothing".
 */
export function useWalkthroughs() {
  return useQuery({
    queryKey: WALKTHROUGHS_QUERY_KEY,
    queryFn: fetchWalkthroughs,
    staleTime: 60_000,
  })
}

function storeWalkthrough(qc: QueryClient, state: WalkthroughState) {
  qc.setQueryData<WalkthroughMap>(WALKTHROUGHS_QUERY_KEY, (prev) =>
    prev ? { ...prev, [state.id]: state } : prev,
  )
}

/** Reflect a close in the cache at once, so nothing re-arms while it saves. */
export function markWalkthroughClosed(
  qc: QueryClient,
  id: string,
  outcome: WalkthroughCloseOutcome,
) {
  const state = qc.getQueryData<WalkthroughMap>(WALKTHROUGHS_QUERY_KEY)?.[id]
  if (state) storeWalkthrough(qc, { ...state, pending: false, outcome })
}

/**
 * One walkthrough's overlay state. It opens when the server reports it pending,
 * and never over the first-run wizard: the wizard (z-60) sits above a tour
 * (z-55), and they would fight on boot.
 */
export function useWalkthrough(id: string) {
  const qc = useQueryClient()
  const { data } = useWalkthroughs()
  const [open, setOpen] = useState(false)
  // Closed in this mount: a failed save must not reopen the overlay on a loop.
  const closedRef = useRef(false)

  const due = data?.[id]?.pending === true && data[WELCOME_WALKTHROUGH]?.pending === false

  useEffect(() => {
    if (!due || open || closedRef.current) return
    const timeout = window.setTimeout(() => setOpen(true), TOUR_OPEN_DELAY_MS)
    return () => window.clearTimeout(timeout)
  }, [due, open])

  const close = useCallback(
    (outcome: WalkthroughCloseOutcome) => {
      closedRef.current = true
      setOpen(false)
      markWalkthroughClosed(qc, id, outcome)
      recordWalkthroughOutcome(id, outcome).then(
        (state) => storeWalkthrough(qc, state),
        (err) => {
          errorToast('Could not save the walkthrough', getApiErrorMessage(err))
          void invalidateQueries(qc, WALKTHROUGHS_QUERY_KEY)
        },
      )
    },
    [id, qc],
  )

  const relaunch = useCallback(async () => {
    try {
      storeWalkthrough(qc, await replayWalkthrough(id))
      closedRef.current = false
      setOpen(true)
    } catch (err) {
      errorToast('Could not open the walkthrough', getApiErrorMessage(err))
    }
  }, [id, qc])

  return { open, close, relaunch }
}
