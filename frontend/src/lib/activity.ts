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
