import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient, type QueryKey } from '@tanstack/react-query'

import { getApiErrorMessage, waitForJob } from '@/api/client'

type Options = {
  /** Every input affecting the snapshot must be represented in this key. */
  queryKey: QueryKey
  enabled?: boolean
  request: (force: boolean) => Promise<{ job_id: string | null }>
}

/** One lifecycle for stored snapshots: POST, await durable job, invalidate reads.
 * Closing/switching stops local polling, never the durable worker. Backend owns
 * cross-client job deduplication and short gated publication. */
export function useAsyncRefresh({ queryKey, enabled = true, request }: Options) {
  const client = useQueryClient()
  const identity = JSON.stringify(queryKey)
  const callbacks = useRef({ request, queryKey })
  useEffect(() => { callbacks.current = { request, queryKey } })
  const [version, setVersion] = useState(0)
  const consumedVersion = useRef(0)
  const busy = useRef(false)
  const [building, setBuilding] = useState(false)
  const [buildError, setBuildError] = useState<string | null>(null)

  useEffect(() => {
    if (!enabled) {
      setBuilding(false)
      setBuildError(null)
      return
    }
    const controller = new AbortController()
    busy.current = true
    setBuilding(true)
    setBuildError(null)
    void (async () => {
      try {
        // StrictMode cleanup can cancel before sending a duplicate POST.
        await Promise.resolve()
        if (controller.signal.aborted) return
        const force = version !== consumedVersion.current
        consumedVersion.current = version
        const { request: launch, queryKey: key } = callbacks.current
        const job = await launch(force)
        if (controller.signal.aborted) return
        if (job.job_id) await waitForJob(job.job_id, {
          signal: controller.signal, intervalMs: 1500, timeoutMs: 300_000,
        })
        if (!controller.signal.aborted) await client.invalidateQueries({ queryKey: key })
      } catch (error) {
        if (!controller.signal.aborted) setBuildError(getApiErrorMessage(error))
      } finally {
        if (!controller.signal.aborted) {
          busy.current = false
          setBuilding(false)
        }
      }
    })()
    return () => {
      controller.abort()
      busy.current = false
    }
  }, [client, enabled, identity, version])

  const refresh = useCallback(() => {
    if (!enabled || busy.current) return
    busy.current = true
    setVersion(value => value + 1)
  }, [enabled])
  return { building: enabled && building, buildError, refresh }
}
