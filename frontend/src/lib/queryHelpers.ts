/**
 * Shared query invalidation helpers for TanStack Query.
 *
 * Replaces the repeated pattern of 5-10 individual invalidateQueries() calls
 * per mutation with a single call.
 */
import type { QueryClient } from '@tanstack/react-query'

/**
 * Invalidate multiple query keys in parallel.
 *
 * @example
 *   await invalidateQueries(queryClient,
 *     ['papers'], ['library-saved'], ['feed-inbox'],
 *   )
 *
 * @example With dynamic keys
 *   await invalidateQueries(queryClient,
 *     ['lens-recommendations', lensId],
 *     ['lens-signals', lensId],
 *   )
 */
export function invalidateQueries(
  qc: QueryClient,
  ...keys: readonly unknown[][]
): Promise<void[]> {
  return Promise.all(keys.map((k) => qc.invalidateQueries({ queryKey: k })))
}

/**
 * Invalidate every query whose root key matches one of the provided strings.
 *
 * This is useful for Activity-driven refreshes where we only know the domain
 * that changed (`feed`, `authors`, `library`, ...) rather than the full query
 * key tuple currently mounted in the UI.
 */
export function invalidateQueryRoots(
  qc: QueryClient,
  ...roots: string[]
): Promise<void[]> {
  const uniqueRoots = [...new Set(roots.map((root) => root.trim()).filter(Boolean))]
  return Promise.all(
    uniqueRoots.map((root) =>
      qc.invalidateQueries({
        predicate: (query) => query.queryKey[0] === root,
      }),
    ),
  )
}

// ── Named invalidation groups ───────────────────────────────────────────────
//
// These exist because the same cluster of query keys gets invalidated after
// the same logical domain event. Naming the cluster makes the call site read
// as intent ("after a paper mutation, refresh everything that observes
// libraries") instead of a key soup, and it centralises the set so adding a
// new consumer (e.g. ['reading-queue']) happens once.

/**
 * After ANY change to one paper's own state — membership, rating, reading
 * status — from any surface. Every list that embeds that state must re-read
 * it. Optionally scoped to a specific lens for Discovery-side recomputes.
 *
 * `lens-recommendations` is here because each recommendation carries the live
 * paper (`rec.paper.status`, `.reading_status`, `.rating`) and the cache is
 * kept 60 s. Without it, taking a paper off the reading list in Library left
 * Discovery showing it "Queued" — and clicking that card ran UNDO, so the
 * paper could not be put back from Discovery (2026-09-19).
 */
export function invalidateAfterPaperMutation(
  qc: QueryClient,
  lensId?: string | null,
): Promise<void[]> {
  const keys: readonly unknown[][] = [
    ['library-saved'],
    ['papers'],
    ['feed-inbox'],
    ['library-workflow-summary'],
    ['reading-queue'],
    ['lens-recommendations'],
  ]
  if (lensId) {
    return invalidateQueries(qc, ...keys, ['lens-signals', lensId])
  }
  return invalidateQueries(qc, ...keys)
}

/** After archiving or retrying a failed capture record. */
export function invalidateAfterCaptureMutation(qc: QueryClient): Promise<void[]> {
  return invalidateQueries(
    qc,
    ['capture-attention'],
    ['inbox-status'],
    ['home-brief'],
    ['papers'],
  )
}

/**
 * After the Feed inbox has been (successfully) refreshed. Invalidates the
 * inbox list, the monitor list, the last-refresh status, and the sidebar
 * bootstrap badge.
 */
export function invalidateAfterFeedRefresh(qc: QueryClient): Promise<void[]> {
  return invalidateQueries(
    qc,
    ['feed-inbox'],
    ['feed-monitors'],
    ['feed-status'],
    ['bootstrap'],
  )
}
