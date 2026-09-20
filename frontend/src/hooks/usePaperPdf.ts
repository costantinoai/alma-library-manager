import { useQuery } from '@tanstack/react-query'
import { ApiError, getPaperPdfDetails, type PaperPdfState } from '@/api/client'

/**
 * The one owner of a paper's PDF state: the details panel, the reader page and
 * the Activity toasts' `['paper-pdf', id]` invalidation all read this query.
 *
 * A missing paper (404) or a rejected read is an ANSWER, not a hiccup: the
 * reader turns it into a message straight away instead of sitting on the
 * global two-retry backoff for six seconds.
 */
export function usePaperPdf(paperId: string, initialPdf?: PaperPdfState | null) {
  return useQuery({
    queryKey: ['paper-pdf', paperId],
    queryFn: () => getPaperPdfDetails(paperId),
    initialData: initialPdf ? { title: '', pdf: initialPdf } : undefined,
    staleTime: 30_000,
    retry: (failureCount, error) =>
      !(error instanceof ApiError && error.status >= 400 && error.status < 500) && failureCount < 1,
  })
}
