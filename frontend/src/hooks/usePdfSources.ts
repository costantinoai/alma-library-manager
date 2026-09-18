import { useQuery } from '@tanstack/react-query'

import { listPlugins } from '@/api/client'

/**
 * Whether any PDF-source plugin is switched on (Settings → Plugins).
 *
 * Reads the SAME `['plugins']` query the Plugins section writes, so toggling
 * a source there updates every "find PDF" affordance without a reload. The
 * list is tiny and changes only on a Settings action, hence the long
 * staleness.
 */
export function usePdfFetchEnabled(): boolean {
  const plugins = useQuery({
    queryKey: ['plugins'],
    queryFn: listPlugins,
    staleTime: 5 * 60_000,
  })
  return (plugins.data ?? []).some(
    (plugin) => plugin.enabled && plugin.capabilities.includes('pdf_source'),
  )
}
