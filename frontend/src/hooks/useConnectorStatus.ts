import { useEffect, useRef } from 'react'
import { api } from '@/api/client'
import {
  readConnectorMarker,
  decideConnectorNotice,
  type ConnectorPing,
} from '@/lib/connector'
import { showConnectorToast } from '@/components/connector/ConnectorToast'

const DISMISSED_KEY = 'alma-connector-notice-dismissed'

/**
 * Startup check for the ALMa browser connector. Reads the marker the extension
 * stamps on <html>, and — only when the installed connector's save-contract no
 * longer matches this ALMa build — surfaces a single update/problem toast.
 *
 *   - connector not installed  -> nothing (we don't nag to install it here)
 *   - connector compatible     -> silent (the healthy case stays quiet)
 *   - contract mismatch        -> one toast, then quiet until versions change
 *
 * Best-effort throughout: a missing marker, an unreachable backend, or an
 * unknown contract all resolve to "say nothing" rather than a false alarm.
 */
export function useConnectorStatus() {
  const firedRef = useRef(false)

  useEffect(() => {
    // document_start guarantees the marker is on <html> before React mounts,
    // so a single read at mount is sufficient.
    const marker = readConnectorMarker()
    if (!marker) return // connector not installed -> nothing to say

    let cancelled = false

    void (async () => {
      let ping: ConnectorPing
      try {
        ping = await api.get<ConnectorPing>('/extension/ping')
      } catch {
        return // backend unreachable -> don't fabricate a problem
      }
      if (cancelled || firedRef.current) return

      const notice = decideConnectorNotice(marker, ping)
      if (!notice) return // compatible -> silent

      try {
        if (localStorage.getItem(DISMISSED_KEY) === notice.signature) return
      } catch {
        // storage unavailable (private mode, quota) — fall through and show it
      }

      firedRef.current = true
      showConnectorToast(notice, () => {
        try {
          localStorage.setItem(DISMISSED_KEY, notice.signature)
        } catch {
          // ignore storage errors
        }
      })
    })()

    return () => {
      cancelled = true
    }
  }, [])
}
