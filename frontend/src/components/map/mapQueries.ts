/**
 * One query/cache contract for every semantic-map host.
 *
 * Layouts are durable artifacts, not ordinary rapidly-changing API rows.
 * Their client cache therefore outlives page mounts and is invalidated by
 * graph jobs/mutations. Discovery's frontier contains live recommendations,
 * so it keeps a shorter (but still navigation-friendly) freshness window.
 */
import {
  queryOptions,
  type QueryClient,
  type QueryKey,
} from '@tanstack/react-query'

import {
  api,
  getFrontier,
  type FrontierResponse,
  type GraphData,
} from '@/api/client'
import {
  AUTHOR_MAP_DEFAULTS,
  PAPER_MAP_DEFAULTS,
  readMapSessionValue,
} from './mapSessionState'

export const MAP_LAYOUT_GC_TIME = 30 * 60_000
export const FRONTIER_STALE_TIME = 5 * 60_000
// Layout jobs are durable background work, not interactive RPCs. Eight-second
// polling keeps the loading state responsive without hammering SQLite while a
// CPU-bound clustering pass is running.
export const MAP_BUILD_POLL_TIME = 8_000

export interface MapBuildStatus {
  status: 'building'
  message?: string
  job_id?: string
}

export interface MapQueryResult<T> {
  /** Last valid payload for this exact query key, if one has arrived. */
  payload?: T
  /** A real server-side layout build, distinct from an ordinary refetch. */
  build?: MapBuildStatus
}

function isBuilding(value: unknown): value is MapBuildStatus {
  return (
    !!value &&
    typeof value === 'object' &&
    (value as { status?: unknown }).status === 'building'
  )
}

/**
 * Preserve a valid same-key canvas if the server temporarily reports that its
 * artifact is rebuilding. This never borrows data from another scope/variant:
 * callers pass only the cache entry for the exact query key.
 */
export function retainReadyMapPayload<T>(
  previous: MapQueryResult<T> | undefined,
  incoming: T | MapBuildStatus,
): MapQueryResult<T> {
  if (!isBuilding(incoming)) return { payload: incoming }
  return {
    payload: previous?.payload,
    build: incoming,
  }
}

function canonicalParams(params: Record<string, string>): string {
  const entries = Object.entries(params).sort(([a], [b]) => a.localeCompare(b))
  return new URLSearchParams(entries).toString()
}

export function graphQueryKey(
  endpoint: 'paper-map' | 'author-network',
  params: Record<string, string>,
) {
  return ['graph', endpoint, canonicalParams(params)] as const
}

export function graphQueryOptions(
  queryClient: QueryClient,
  endpoint: 'paper-map' | 'author-network',
  params: Record<string, string>,
  /**
   * Speculative warm-up (sidebar hover). Sends `prefetch=true`, which the route
   * honours by reporting `building` WITHOUT enqueuing a layout build: brushing a
   * nav item must never start minutes of background clustering the user never
   * asked for. It is deliberately NOT part of the query key — a prefetch and the
   * page's own read address the same cache entry, which is the entire point.
   */
  options?: { prefetch?: boolean },
) {
  const qs = canonicalParams(params)
  const requestQs = options?.prefetch ? `${qs}&prefetch=true` : qs
  const queryKey = graphQueryKey(endpoint, params)
  return queryOptions({
    queryKey,
    queryFn: async () => {
      const incoming = await api.get<GraphData | MapBuildStatus>(
        `/graphs/${endpoint}?${requestQs}`,
      )
      return retainReadyMapPayload(
        queryClient.getQueryData<MapQueryResult<GraphData>>(queryKey),
        incoming,
      )
    },
    // A layout does not become stale with time. Graph jobs and mutations
    // explicitly invalidate this root when the durable artifact changes.
    staleTime: Infinity,
    // Keep the common maps warm across navigation, while eventually evicting
    // abandoned tuning variants in a long-running tab.
    gcTime: MAP_LAYOUT_GC_TIME,
    refetchInterval: (query) => {
      const delivery = query.state.data?.payload?.metadata?.delivery as
        | { rebuilding?: boolean }
        | undefined
      return query.state.data?.build || delivery?.rebuilding
        ? MAP_BUILD_POLL_TIME
        : false
    },
  })
}

export function frontierQueryKey(
  lensId: string,
  showSeen: boolean,
  showEdges: boolean,
) {
  return ['frontier', lensId, showSeen, showEdges] as const
}

export function frontierQueryOptions(
  queryClient: QueryClient,
  lensId: string,
  showSeen: boolean,
  showEdges: boolean,
) {
  const queryKey = frontierQueryKey(lensId, showSeen, showEdges)
  return queryOptions({
    queryKey,
    queryFn: async () => {
      const incoming = await getFrontier(
        lensId,
        showSeen ? 300 : 0,
        showEdges,
      )
      return retainReadyMapPayload(
        queryClient.getQueryData<MapQueryResult<FrontierResponse>>(queryKey),
        incoming,
      )
    },
    enabled: !!lensId,
    staleTime: FRONTIER_STALE_TIME,
    gcTime: MAP_LAYOUT_GC_TIME,
    refetchInterval: (query) =>
      query.state.data?.build ? MAP_BUILD_POLL_TIME : false,
  })
}

export function paperMapParams({
  scope,
  resolution,
  blend,
}: {
  scope: 'library' | 'corpus'
  resolution: number
  blend: { sem: number; coauth: number; refs: number; cocite: number }
}): Record<string, string> {
  const params: Record<string, string> = {
    scope,
    cluster_resolution: resolution.toFixed(1),
  }
  if (scope === 'library') {
    params.w_semantic = String(blend.sem)
    params.w_coauthorship = String(blend.coauth)
    params.w_bibliographic = String(blend.refs)
    params.w_cocitation = String(blend.cocite)
  }
  return params
}

/**
 * Warm the map the user is indicating in the sidebar. The route chunk is
 * handled separately; this function owns only map data and reads the same
 * session preferences the destination page will use.
 */
export function prefetchMapPage(
  queryClient: QueryClient,
  page: 'map' | 'authors',
): Promise<void> {
  if (page === 'map') {
    const scope = readMapSessionValue<'library' | 'corpus'>(
      'paper-map',
      'scope',
      PAPER_MAP_DEFAULTS.scope,
    )
    const resolution = readMapSessionValue(
      'paper-map',
      'resolution',
      PAPER_MAP_DEFAULTS.resolution,
    )
    const blend = readMapSessionValue(
      'paper-map',
      'blend',
      PAPER_MAP_DEFAULTS.blend,
    )
    return queryClient.prefetchQuery(
      graphQueryOptions(
        queryClient,
        'paper-map',
        paperMapParams({ scope, resolution, blend }),
        { prefetch: true },
      ),
    )
  }

  const scope = readMapSessionValue<'library' | 'corpus'>(
    'author-map',
    'scope',
    AUTHOR_MAP_DEFAULTS.scope,
  )
  const resolution = readMapSessionValue(
    'author-map',
    'resolution',
    AUTHOR_MAP_DEFAULTS.resolution,
  )
  return queryClient.prefetchQuery(
    graphQueryOptions(
      queryClient,
      'author-network',
      { scope, cluster_resolution: resolution.toFixed(1) },
      { prefetch: true },
    ),
  )
}

/** Useful to consumers that need to invalidate without importing internals. */
export function isMapQueryKey(queryKey: QueryKey): boolean {
  return queryKey[0] === 'graph' || queryKey[0] === 'frontier'
}
