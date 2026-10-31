import { useQuery } from '@tanstack/react-query'

import { getBootstrap } from '@/api/client'

/** The one bootstrap read: sidebar counts and which add-ons are on. */
export function useBootstrap() {
  return useQuery({
    queryKey: ['bootstrap'],
    // Timer-driven poll → background so the always-mounted Sidebar doesn't pin
    // the app "active" between real interactions (41.1).
    queryFn: () => getBootstrap({ background: true }),
    staleTime: 60_000,
    refetchInterval: 300_000,
  })
}
