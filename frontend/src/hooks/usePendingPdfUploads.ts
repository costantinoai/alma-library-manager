import { useQuery } from '@tanstack/react-query'

import { listPendingPdfUploads } from '@/api/client'

/** The one query key for the kept-but-unidentified PDF uploads. */
export const PENDING_PDF_UPLOADS_KEY = ['pdf-pending-uploads'] as const

/**
 * The PDFs ALMa could not identify, still waiting for a DOI or title.
 *
 * The staged file outlives the dialog (a day), so this is what brings an
 * unresolved import back after a reload. Invalidate `PENDING_PDF_UPLOADS_KEY`
 * whenever a retry resolves one or the user gives one up.
 */
export function usePendingPdfUploads(enabled = true) {
  return useQuery({
    queryKey: PENDING_PDF_UPLOADS_KEY,
    queryFn: listPendingPdfUploads,
    enabled,
    staleTime: 15_000,
  })
}
