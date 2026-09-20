/**
 * Activity / operation classification shared by the Activity pane and the
 * operation-toast hook.
 *
 * ALMa runs two kinds of jobs:
 *  - **User-meaningful operations** — something the user explicitly started
 *    (a lens refresh, a feed refresh, following an author, an import). These
 *    show normally in the Activity pane and raise exactly one outcome toast.
 *  - **Background plumbing** — work ALMa schedules for itself: cache
 *    materialization (`materialize_*`, trigger `auto`), per-paper/author
 *    hydration (`auto:paper_insert`, `auto:author_follow`), scheduled sweeps
 *    (`scheduler`), and retrieval lane subtasks (`subtask`). These are muted
 *    in the pane and never toast — surfacing them as first-class activity was
 *    the source of the "76 entries/toasts per refresh" noise.
 *
 * Classification keys off `trigger_source`, the single field every job stamps
 * (see `set_job_status` in src/alma/api/scheduler.py).
 */
export function isBackgroundTriggerSource(src?: string | null): boolean {
  if (!src) return false // null/unknown → treat as foreground (user-meaningful)
  return (
    src === 'auto' ||
    src.startsWith('auto:') ||
    src === 'scheduler' ||
    src === 'subtask'
  )
}

export interface LaunchMessage {
  title: string
  description: string
}

/**
 * Toast for a request that ENQUEUED a background job — the ONE wording for
 * "started / already running / nothing to do", whatever surface launched it.
 *
 * Eight components used to spell this themselves ("Refresh queued", "Resolve
 * started", "Smart tagging started", "Already running" …), each with its own
 * idea of what to say about the job. `label` names the operation; the
 * backend's `message`, when it sends one, is the most specific thing to show
 * and wins; `startedDetail` replaces the generic Activity pointer for a
 * surface with something better to say. Takes any envelope-shaped response
 * (`JobEnvelope`, `RunMaintenanceResponse`, …).
 */
export function describeJobLaunch(
  envelope: { status?: string | null; job_id?: string | null; message?: string | null } | null | undefined,
  label: string,
  { startedDetail }: { startedDetail?: string } = {},
): LaunchMessage {
  const status = String(envelope?.status ?? '')
  const jobId = envelope?.job_id
  const message = envelope?.message || undefined
  if (status === 'already_running') {
    return {
      title: `${label} already running`,
      description: message ?? (jobId ? `Job ${jobId} is still going. Track it in Activity.` : 'Track it in Activity.'),
    }
  }
  if (status === 'noop') {
    return { title: 'Nothing to run', description: message ?? 'No eligible items.' }
  }
  return {
    title: `${label} started`,
    description: message ?? startedDetail ?? (jobId ? `Job ${jobId} — track it in Activity.` : 'Track it in Activity.'),
  }
}
