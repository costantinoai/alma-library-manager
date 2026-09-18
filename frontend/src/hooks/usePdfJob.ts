import { useCallback, useState } from 'react'

import { waitForJob, type JobEnvelope } from '@/api/client'

/**
 * Run one PDF Activity job and follow it to the end, exposing its live step
 * ("Asking Unpaywall…") while it runs.
 *
 * Every PDF operation (find, attach, import) is a queued job that answers
 * with an envelope; this is the one place the UI waits on one. `run` resolves
 * with the job's result and rejects with its failure message.
 */
export function usePdfJob() {
  const [running, setRunning] = useState(false)
  const [message, setMessage] = useState<string | null>(null)

  const run = useCallback(async <T,>(start: () => Promise<JobEnvelope>): Promise<T> => {
    setRunning(true)
    setMessage(null)
    try {
      const envelope = await start()
      setMessage(envelope.message ?? null)
      return await waitForJob<T>(envelope.job_id, {
        intervalMs: 1_000,
        timeoutMs: 5 * 60_000,
        onProgress: (status) => setMessage(status.message ?? null),
      })
    } finally {
      setRunning(false)
    }
  }, [])

  return { run, running, message }
}
