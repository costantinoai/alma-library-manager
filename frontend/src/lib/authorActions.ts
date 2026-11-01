import type { QueryClient } from '@tanstack/react-query'

import { ApiError, getApiErrorMessage } from '@/api/client'
import { errorToast } from '@/hooks/useToast'
import { invalidateAfterAuthorChange } from '@/lib/queryHelpers'

/**
 * A failed author action says why, and when the server refused it the list is
 * refetched: another tab or a finished job may have changed the row, and what
 * the user sees should catch up rather than offer the same dead button again.
 */
export function authorActionFailed(qc: QueryClient, title: string, err: unknown): void {
  errorToast(title, getApiErrorMessage(err))
  if (err instanceof ApiError && err.status >= 400 && err.status < 500) {
    void invalidateAfterAuthorChange(qc)
  }
}
