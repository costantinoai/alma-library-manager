import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Network, Map, AlertCircle, Loader2, CircleOff, Layers3 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { EmptyState } from '@/components/ui/empty-state'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { api, refreshClusterLabels, type GraphData, type GraphNode } from '@/api/client'
import {
  ForceGraph,
  type GraphPhysicsConfig,
  LAYER_COLORS,
  LAYER_LABELS,
  LARGE_GRAPH_THRESHOLD,
  LARGE_GRAPH_EDGE_THRESHOLD,
} from './ForceGraph'
import { GraphControls } from './GraphControls'
import { InsightsPaperDrilldown, type DrilldownTarget } from '@/components/insights/InsightsPaperDrilldown'
import { formatPercent } from '@/lib/format'
import { useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

type GraphView = 'paper-map' | 'author-network'

const VIEW_CONFIG: Record<GraphView, { label: string; icon: typeof Map; description: string }> = {
  'paper-map': {
    label: 'Paper Map',
    icon: Map,
    description: 'Publications positioned by similarity, colored by cluster',
  },
  'author-network': {
    label: 'Author Network',
    icon: Network,
    description: 'Co-authorship connections between tracked researchers',
  },
}

interface ClusterSummary {
  id: number
  label: string
  topic_text?: string
  description?: string
  label_model?: string
  cluster_signature?: string
  size: number
  x?: number
  y?: number
  top_topics?: string[]
  word_cloud?: Array<{ term: string; weight: number }>
  avg_citations?: number
  avg_rating?: number
  year_range?: { min?: number | null; max?: number | null }
  publication_date_range?: { min?: string | null; max?: string | null }
  sample_papers?: Array<{
    paper_id: string
    title: string
    year?: number | null
    publication_date?: string | null
    cited_by_count?: number
    journal?: string | null
  }>
}

export type LabelMode = 'cluster' | 'topic'
export type ColorBy = 'cluster' | 'year' | 'rating' | 'citations'
export type SizeBy = 'citations' | 'uniform' | 'rating'
// Author-network encodings (parity with the paper map, but over AUTHOR
// attributes — h-index / productivity / citations — which papers don't have).
// Applied client-side from node metadata, so changing them is instant.
export type AuthorColorBy = 'cluster' | 'citations' | 'h_index' | 'publications'
export type AuthorSizeBy = 'publications' | 'citations' | 'h_index' | 'uniform'

const DEFAULT_GRAPH_PHYSICS: GraphPhysicsConfig = {
  repulsion: -30,
  linkDistance: 60,
  linkStrength: 0.3,
  velocityDecay: 0.4,
  cooldownTicks: 80,
  nodeScale: 1,
  baseSize: 3,
}

function formatDate(value: unknown): string {
  const text = String(value || '').trim()
  if (!text) return 'Unknown'
  return text
}

function metadataText(metadata: Record<string, unknown>, key: string): string {
  return String(metadata[key] || '').trim()
}

function metadataNumber(metadata: Record<string, unknown>, key: string): number | null {
  const value = metadata[key]
  if (value === null || value === undefined || value === '') {
    return null
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function metadataList(metadata: Record<string, unknown>, key: string): string[] {
  const value = metadata[key]
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0) : []
}

export function GraphPanel() {
  const [activeView, setActiveView] = useState<GraphView>('paper-map')
  const [searchQuery, setSearchQuery] = useState('')
  const [showLabels, setShowLabels] = useState(false)
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const [selectedClusterId, setSelectedClusterId] = useState<number | null>(null)
  const [labelMode, setLabelMode] = useState<LabelMode>('cluster')
  const [colorBy, setColorBy] = useState<ColorBy>('cluster')
  const [sizeBy, setSizeBy] = useState<SizeBy>('citations')
  // Edges OFF by default — rendering thousands on every frame is the corpus
  // perf hit; the toggle turns them on (and a focused cluster always shows its
  // own edges). This is a pure RENDER flag now; edges are always in the payload
  // so toggling is instant.
  const [showEdges, setShowEdges] = useState(false)
  const [showTopics, setShowTopics] = useState(false)
  const [showWordCloud, setShowWordCloud] = useState(false)
  // Cluster name labels at each centroid — a real toggle now (default off),
  // instead of being force-on for the author view (which read as "words always on").
  const [showClusterLabels, setShowClusterLabels] = useState(false)
  // Word-cloud density (how many words) + size (how big) sliders.
  const [wordCloudDensity, setWordCloudDensity] = useState(1)
  const [wordCloudSize, setWordCloudSize] = useState(1)
  const [includeCorpus, setIncludeCorpus] = useState(false)
  // Typed edge layers toggled OFF (Phase 3 / I-11). Empty ⇒ all layers shown.
  const [hiddenLayers, setHiddenLayers] = useState<Set<string>>(new Set())
  const [physics, setPhysics] = useState<GraphPhysicsConfig>(DEFAULT_GRAPH_PHYSICS)
  // Cluster detail knob (Phase 3): 1.0 = default; >1 finer (more clusters),
  // <1 coarser. A non-default value bypasses the MV cache (live re-cluster).
  const [clusterResolution, setClusterResolution] = useState(1.0)
  // Author-network encodings — applied client-side from node metadata (below),
  // so changing them never refetches.
  const [authorColorBy, setAuthorColorBy] = useState<AuthorColorBy>('cluster')
  const [authorSizeBy, setAuthorSizeBy] = useState<AuthorSizeBy>('publications')
  // I-19: the paper-list drilldown opened from a cluster (null = closed).
  const [drilldown, setDrilldown] = useState<DrilldownTarget | null>(null)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const scope = includeCorpus ? 'corpus' : 'library'
  // show_edges is hardcoded true so the edges are always in the (cached) payload
  // — the Edges toggle is a client-side RENDER flag, so flipping it never
  // refetches. cluster_resolution is the only fetch-affecting graph option here.
  const queryParams = activeView === 'paper-map'
    ? `?label_mode=${labelMode}&color_by=${colorBy}&size_by=${sizeBy}&show_edges=true&show_topics=${showTopics}&scope=${scope}&cluster_resolution=${clusterResolution}`
    : `?scope=${scope}&cluster_resolution=${clusterResolution}`

  // Only the params that actually change the FETCH belong in the key. The author
  // view's color/size/edges encodings are applied client-side, so they must NOT
  // be in its key (else toggling them would refetch + flash a spinner).
  const queryKey =
    activeView === 'paper-map'
      ? ['graph', 'paper-map', labelMode, colorBy, sizeBy, showTopics, scope, clusterResolution]
      : ['graph', 'author-network', scope, clusterResolution]
  const { data, isLoading, error } = useQuery<GraphData>({
    queryKey,
    queryFn: () => api.get<GraphData>(`/graphs/${activeView}${queryParams}`),
    staleTime: 60_000,
  })

  const rebuildMutation = useMutation({
    // I-3: pass the displayed scope so Rebuild refreshes the graph the user is
    // actually viewing (Corpus rebuild was silently rebuilding Library before).
    mutationFn: () => api.post<{ status?: string; job_id?: string }>(`/graphs/rebuild?scope=${scope}`),
    onSuccess: (result) => {
      void invalidateQueries(
        queryClient,
        ['graph'],
        ...(result?.status === 'queued' && result.job_id ? [['activity-operations']] : []),
      )
      if (result?.status === 'queued' && result.job_id) {
        toast({ title: `Graph rebuild queued (${scope})`, description: `Job ${result.job_id} is running.` })
      } else {
        toast({ title: `Graph rebuild complete (${scope})` })
      }
    },
  })

  const labelRefreshMutation = useMutation({
    mutationFn: () =>
      refreshClusterLabels({
        graph_type: activeView === 'paper-map' ? 'paper_map' : 'author_network',
        scope,
      }),
    onSuccess: (result) => {
      void invalidateQueries(
        queryClient,
        ['graph'],
        ...(result?.status === 'queued' && result.job_id ? [['activity-operations']] : []),
      )
      if (result?.status === 'queued' && result.job_id) {
        toast({
          title: 'Cluster relabelling queued',
          description: 'Watch Activity for per-cluster progress.',
        })
      } else if (result?.status === 'already_running') {
        toast({
          title: 'Already running',
          description: 'A cluster-label refresh is already in progress.',
        })
      } else {
        toast({ title: 'Cluster labels refreshed' })
      }
    },
  })

  useEffect(() => {
    setSelectedNode(null)
    setSelectedClusterId(null)
    setSearchQuery('')
  }, [activeView])

  const config = VIEW_CONFIG[activeView]
  const clusters = useMemo(
    () => ((data?.metadata?.clusters || []) as ClusterSummary[]).sort((a, b) => b.size - a.size),
    [data],
  )
  const selectedCluster = useMemo(
    () => clusters.find((cluster) => cluster.id === selectedClusterId) || null,
    [clusters, selectedClusterId],
  )
  // Author-view color/size encodings, computed client-side from node metadata
  // (pub_count / h_index / citation_count — already on each node), so toggling
  // them is instant with no refetch. Default (cluster colour + publications
  // size) passes the backend payload through untouched.
  const displayData = useMemo<GraphData | undefined>(() => {
    if (
      !data ||
      activeView !== 'author-network' ||
      (authorColorBy === 'cluster' && authorSizeBy === 'publications')
    ) {
      return data
    }
    const num = (m: Record<string, unknown> | undefined, k: string) =>
      Number((m?.[k] as number) ?? 0) || 0
    const maxOf = (k: string) => Math.max(1, ...data.nodes.map((n) => num(n.metadata, k)))
    const maxCit = maxOf('citation_count')
    const maxH = maxOf('h_index')
    const maxPub = maxOf('pub_count')
    // Slate → folio gradient (matches the paper map's citation ramp).
    const grad = (t: number) => {
      const c = Math.max(0, Math.min(1, t))
      const r = Math.round(148 * (1 - c) + 37 * c)
      const g = Math.round(163 * (1 - c) + 99 * c)
      const b = Math.round(184 * (1 - c) + 235 * c)
      const hex = (v: number) => v.toString(16).padStart(2, '0')
      return `#${hex(r)}${hex(g)}${hex(b)}`
    }
    return {
      ...data,
      nodes: data.nodes.map((n) => {
        const m = n.metadata as Record<string, unknown> | undefined
        let color = n.color
        if (authorColorBy === 'citations') color = grad(num(m, 'citation_count') / maxCit)
        else if (authorColorBy === 'h_index') color = grad(num(m, 'h_index') / maxH)
        else if (authorColorBy === 'publications') color = grad(num(m, 'pub_count') / maxPub)
        let size = n.size
        if (authorSizeBy === 'uniform') size = 1
        else if (authorSizeBy === 'citations') size = 0.6 + 2.4 * (num(m, 'citation_count') / maxCit)
        else if (authorSizeBy === 'h_index') size = 0.6 + 2.4 * (num(m, 'h_index') / maxH)
        else size = 0.6 + 2.4 * (num(m, 'pub_count') / maxPub)
        return { ...n, color, size }
      }),
    }
  }, [data, activeView, authorColorBy, authorSizeBy])
  const selectedNodePublicationDate = selectedNode ? metadataText(selectedNode.metadata, 'publication_date') : ''
  const selectedNodeYear = selectedNode ? metadataNumber(selectedNode.metadata, 'year') : null
  const selectedNodeCitations = selectedNode ? metadataNumber(selectedNode.metadata, 'cited_by_count') : null
  const selectedNodeRating = selectedNode ? metadataNumber(selectedNode.metadata, 'rating') : null
  const selectedNodeClusterLabel = selectedNode ? metadataText(selectedNode.metadata, 'cluster_label') : ''
  const selectedNodeJournal = selectedNode ? metadataText(selectedNode.metadata, 'journal') : ''
  const selectedNodeAuthors = selectedNode ? metadataText(selectedNode.metadata, 'authors') : ''
  const selectedNodeTopics = selectedNode ? metadataList(selectedNode.metadata, 'topics') : []
  const graphErrorMessage = error instanceof Error ? error.message : 'Could not load graph data.'
  const method = String(data?.metadata?.method || '')
  const note = String(data?.metadata?.note || '')
  // Typed edge layers (Phase 3 / I-11): per-layer counts from metadata drive
  // the filter chips; `visibleLayers` is the set NOT toggled off.
  const edgeLayers = (data?.metadata?.edge_layers || {}) as Record<string, number>
  // I-14: the clustering method/diagnostics the backend already emits — surfaced
  // in a panel so the projection's honesty (coverage, retained outliers,
  // stability) is visible, not just implied by the dots.
  const clustering = (data?.metadata?.clustering || {}) as {
    method?: string
    n_clusters?: number
    outlier_count?: number
    coverage?: number
    stability?: number | null
    params?: Record<string, unknown>
  }
  const hasClustering = clustering.n_clusters !== undefined
  const layerKeys = useMemo(
    () => Object.keys(edgeLayers).filter((key) => (edgeLayers[key] || 0) > 0),
    [edgeLayers],
  )
  const visibleLayers = useMemo(
    () => layerKeys.filter((key) => !hiddenLayers.has(key)),
    [layerKeys, hiddenLayers],
  )
  const toggleLayer = (key: string) =>
    setHiddenLayers((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  const nodeDetailTitle = activeView === 'paper-map' ? 'Selected Paper' : 'Selected Author'
  const nodeDetailSubtitle = activeView === 'paper-map'
    ? 'Paper-level metadata and how it sits inside the active cluster.'
    : 'Author-level metadata and how this node sits inside the active cluster.'

  useEffect(() => {
    if (!selectedClusterId) return
    if (!clusters.some((cluster) => cluster.id === selectedClusterId)) {
      setSelectedClusterId(null)
    }
  }, [clusters, selectedClusterId])

  const handleNodeClick = (node: GraphNode) => {
    setSelectedNode(node)
    if (typeof node.cluster_id === 'number') {
      setSelectedClusterId(node.cluster_id)
    }
  }

  const updatePhysics = (patch: Partial<GraphPhysicsConfig>) => {
    setPhysics((current) => ({ ...current, ...patch }))
  }

  return (
    <div className="space-y-4">
      {/* I-14: in-page explainer for the studio's vocabulary so the geometry is
          read correctly (positions = similarity, not force physics; outliers are
          honest; edges are typed). One ConceptCallout per surface, near the top. */}
      <ConceptCallout
        eyebrow="How to read this"
        summary="Dots are positioned by content similarity; color marks a discovered cluster; edges are typed relationships."
      >
        <p>
          Each node is a paper (or author) placed by a non-linear projection of its
          768-dimensional embedding, so <strong>near = similar in content</strong> —
          the layout is geometry, not a force simulation you should read into.
        </p>
        <p className="mt-2">
          <strong>Clusters</strong> are discovered by density (HDBSCAN), not imposed:
          a paper too sparse to assign confidently stays an{' '}
          <strong>outlier</strong> (slate, unclustered) rather than being forced into
          the nearest group. <strong>Coverage</strong> is the share of nodes that did
          get a confident cluster; <strong>stability</strong> is how repeatable the
          clustering is across re-projections. <strong>Edges</strong> are typed —
          semantic (embedding neighbours), bibliographic coupling (shared
          references), co-authorship — and each layer can be toggled.
        </p>
      </ConceptCallout>

      <div className="flex gap-2">
        {(Object.keys(VIEW_CONFIG) as GraphView[]).map((view) => {
          const viewConfig = VIEW_CONFIG[view]
          const Icon = viewConfig.icon
          return (
            <Button
              key={view}
              variant={activeView === view ? 'default' : 'outline'}
              size="sm"
              onClick={() => setActiveView(view)}
            >
              <Icon className="mr-1.5 h-4 w-4" />
              {viewConfig.label}
            </Button>
          )
        })}
      </div>

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between gap-3">
            <div>
              <CardTitle className="text-base">{config.label}</CardTitle>
              <p className="mt-0.5 text-sm text-slate-500">{config.description}</p>
            </div>
            {data && (
              <span className="text-xs text-slate-400">
                {data.nodes.length} nodes · {data.edges.length} edges
              </span>
            )}
          </div>
        </CardHeader>
        <CardContent>
          <GraphControls
            searchQuery={searchQuery}
            onSearchChange={setSearchQuery}
            onRebuild={() => rebuildMutation.mutate()}
            isRebuilding={rebuildMutation.isPending}
            showLabels={showLabels}
            onToggleLabels={() => setShowLabels((value) => !value)}
            method={method}
            note={note}
            clusters={clusters}
            selectedClusterId={selectedClusterId}
            onClusterSelect={setSelectedClusterId}
            onExtraAction={undefined}
            extraActionLabel={undefined}
            isExtraActionPending={undefined}
            isPaperMap={activeView === 'paper-map'}
            labelMode={labelMode}
            onLabelModeChange={setLabelMode}
            colorBy={colorBy}
            onColorByChange={setColorBy}
            sizeBy={sizeBy}
            onSizeByChange={setSizeBy}
            showEdges={showEdges}
            onShowEdgesChange={setShowEdges}
            showTopics={showTopics}
            onShowTopicsChange={setShowTopics}
            showWordCloud={showWordCloud}
            onShowWordCloudChange={setShowWordCloud}
            showClusterLabels={showClusterLabels}
            onShowClusterLabelsChange={setShowClusterLabels}
            wordCloudDensity={wordCloudDensity}
            onWordCloudDensityChange={setWordCloudDensity}
            wordCloudSize={wordCloudSize}
            onWordCloudSizeChange={setWordCloudSize}
            includeCorpus={includeCorpus}
            onIncludeCorpusChange={setIncludeCorpus}
            clusterResolution={clusterResolution}
            onClusterResolutionChange={setClusterResolution}
            authorColorBy={authorColorBy}
            onAuthorColorByChange={setAuthorColorBy}
            authorSizeBy={authorSizeBy}
            onAuthorSizeByChange={setAuthorSizeBy}
            physics={physics}
            onPhysicsChange={updatePhysics}
            onResetPhysics={() => setPhysics(DEFAULT_GRAPH_PHYSICS)}
            onRefreshLabels={() => labelRefreshMutation.mutate()}
            isRefreshingLabels={labelRefreshMutation.isPending}
          />

          <div className="mt-4 overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-2">
            {data && data.nodes.length > 0 && (
              <span className="sr-only">
                Interactive {config.label.toLowerCase()} visualization showing {data.nodes.length} nodes and {data.edges.length} connections. Use the controls panel to filter and configure the graph.
              </span>
            )}
            {isLoading ? (
              <div className="flex h-[72vh] min-h-[640px] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-slate-400" />
              </div>
            ) : error ? (
              <div className="flex h-[72vh] min-h-[640px] flex-col items-center justify-center text-slate-500">
                <AlertCircle className="mb-2 h-8 w-8" />
                <p className="text-sm">Failed to load graph data</p>
                <p className="mt-1 text-xs text-slate-400">{graphErrorMessage}</p>
              </div>
            ) : data && data.nodes.length > 0 ? (
              <div className="flex flex-col gap-2">
                {/* On a large graph edges are hidden until you zoom in (perf +
                    they're a hairball when zoomed out) — say so, so an empty
                    edge view at fit doesn't read as "no edges". */}
                {showEdges &&
                  selectedClusterId === null &&
                  (data.nodes.length > LARGE_GRAPH_THRESHOLD ||
                    data.edges.length > LARGE_GRAPH_EDGE_THRESHOLD) && (
                    <p className="text-xs text-slate-400">
                      Zoom in to reveal edges, or select a cluster to see its connections.
                    </p>
                  )}
                {showEdges && layerKeys.length > 0 && (
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-xs font-medium text-slate-500">Edge layers</span>
                    {layerKeys.map((key) => {
                      const active = !hiddenLayers.has(key)
                      return (
                        <button
                          key={key}
                          type="button"
                          onClick={() => toggleLayer(key)}
                          aria-pressed={active}
                          className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs transition-colors ${
                            active
                              ? 'border-edge-2 bg-surface-2 text-slate-700'
                              : 'border-edge-1 bg-surface-1 text-slate-400'
                          }`}
                          title={`${active ? 'Hide' : 'Show'} ${LAYER_LABELS[key] || key} edges`}
                        >
                          <span
                            className="inline-block h-2 w-2 rounded-full"
                            style={{ backgroundColor: active ? LAYER_COLORS[key] : 'transparent', border: active ? 'none' : '1px solid currentColor' }}
                          />
                          {LAYER_LABELS[key] || key}
                          <span className="opacity-60">{edgeLayers[key]}</span>
                        </button>
                      )
                    })}
                  </div>
                )}
                <ForceGraph
                  data={displayData ?? data}
                  height={Math.max(640, Math.round(window.innerHeight * 0.72))}
                  onNodeClick={handleNodeClick}
                  showLabels={showLabels}
                  highlightSearch={searchQuery}
                  selectedNodeId={selectedNode?.id || null}
                  selectedClusterId={selectedClusterId}
                  showClusterLabels={showClusterLabels}
                  showWordCloud={showWordCloud}
                  showEdges={showEdges}
                  wordCloudDensity={wordCloudDensity}
                  wordCloudSize={wordCloudSize}
                  clusters={clusters}
                  physics={physics}
                  visibleLayers={layerKeys.length ? visibleLayers : undefined}
                />
              </div>
            ) : (
              <div className="flex h-[72vh] min-h-[640px] flex-col items-center justify-center text-slate-500">
                <Map className="mb-2 h-8 w-8" />
                <p className="text-sm">No data available</p>
                <p className="mt-1 text-xs text-slate-400">Add authors and refresh to generate graph data</p>
              </div>
            )}
          </div>

          {/* I-14: method & diagnostics panel — the projection/clustering facts the
              backend already emits, made visible so the map's honesty is legible. */}
          {data && data.nodes.length > 0 && hasClustering && (
            <div className="mt-4 rounded-sm border border-[var(--color-border)] bg-surface-1 p-4">
              <div className="mb-3 flex items-baseline justify-between gap-2">
                <p className="text-sm font-semibold text-alma-800">Method &amp; diagnostics</p>
                <span className="text-xs text-slate-400">
                  {scope === 'corpus' ? 'Corpus' : 'Library'} scope
                  {clusterResolution !== 1.0 ? ` · resolution ${clusterResolution.toFixed(1)}×` : ''}
                </span>
              </div>
              <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
                <Diagnostic label="Layout" value={method || '—'} />
                <Diagnostic label="Clusters" value={String(clustering.n_clusters ?? '—')} />
                <Diagnostic
                  label="Coverage"
                  value={clustering.coverage != null ? formatPercent(clustering.coverage, 0) : '—'}
                  hint="share of nodes confidently clustered"
                />
                <Diagnostic
                  label="Outliers"
                  value={String(clustering.outlier_count ?? 0)}
                  hint="nodes too sparse to assign"
                />
                <Diagnostic
                  label="Stability"
                  value={clustering.stability != null ? clustering.stability.toFixed(2) : 'n/a'}
                  hint="repeatability across re-projections (mean ARI)"
                />
                <Diagnostic
                  label="Method"
                  value={String(clustering.method || '—')}
                  hint={
                    clustering.params
                      ? Object.entries(clustering.params)
                          .map(([k, v]) => `${k}=${v}`)
                          .join(', ')
                      : undefined
                  }
                />
              </div>

              {clusters.length > 0 && (
                <details className="mt-3 rounded-sm border border-[var(--color-border)] bg-surface-2">
                  <summary className="cursor-pointer px-3 py-2 text-xs font-medium text-slate-600">
                    Clusters ({clusters.length}) — accessible list
                  </summary>
                  {/* Keyboard/SR alternative to the canvas: a real list of clusters
                      that selects the active cluster on click (I-14 a11y). */}
                  <ul className="max-h-64 divide-y divide-[var(--color-border)] overflow-y-auto">
                    {clusters.map((cluster) => {
                      const active = cluster.id === selectedClusterId
                      return (
                        <li key={cluster.id}>
                          <button
                            type="button"
                            aria-pressed={active}
                            onClick={() => setSelectedClusterId(cluster.id)}
                            className={`flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-sm transition-colors ${
                              active ? 'bg-accent-soft text-alma-folio' : 'text-slate-700 hover:bg-surface-1'
                            }`}
                          >
                            <span className="min-w-0 flex-1 truncate">{cluster.label}</span>
                            <span className="shrink-0 text-xs text-slate-400">{cluster.size} papers</span>
                          </button>
                        </li>
                      )
                    })}
                  </ul>
                </details>
              )}
            </div>
          )}

          <div className="mt-4 grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
            <div className="rounded-sm border border-[var(--color-border)] bg-surface-1 p-4 shadow-paper-sm shadow-sm">
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Layers3 className="h-4 w-4 text-slate-500" />
                  <div>
                    <p className="text-sm font-semibold text-alma-800">Active Cluster</p>
                    <p className="text-xs text-slate-500">Cluster-level context, topics, and representative papers.</p>
                  </div>
                </div>
                {selectedClusterId !== null && (
                  <Button variant="ghost" size="sm" onClick={() => setSelectedClusterId(null)}>
                    Clear
                  </Button>
                )}
              </div>
              {selectedCluster ? (
                <div className="space-y-3">
                  <div className="rounded-lg bg-surface-2 p-3">
                    <p className="text-lg font-semibold text-alma-800">{showTopics && selectedCluster.topic_text ? selectedCluster.topic_text : selectedCluster.label}</p>
                    {selectedCluster.description && (
                      <p className="mt-1 text-xs italic text-slate-600">{selectedCluster.description}</p>
                    )}
                    <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-600">
                      <Badge variant="secondary">{selectedCluster.size} papers</Badge>
                      {selectedCluster.avg_citations !== undefined && <Badge variant="outline">Avg citations {selectedCluster.avg_citations}</Badge>}
                      {selectedCluster.avg_rating !== undefined && selectedCluster.avg_rating > 0 && <Badge variant="outline">Avg rating {selectedCluster.avg_rating.toFixed(2)}</Badge>}
                      {selectedCluster.year_range?.min && selectedCluster.year_range?.max && (
                        <Badge variant="outline">
                          {selectedCluster.year_range.min}–{selectedCluster.year_range.max}
                        </Badge>
                      )}
                      {selectedCluster.label_model && (
                        <Badge variant="outline" className="text-[10px]">via {selectedCluster.label_model}</Badge>
                      )}
                    </div>
                    {/* I-19: drill from this cluster to ALL its papers (not just
                        the 4 representatives below), via the shared route. */}
                    <Button
                      variant="outline"
                      size="sm"
                      className="mt-3"
                      onClick={() =>
                        setDrilldown({
                          filterType: 'cluster',
                          filterValue: String(selectedCluster.id),
                          scope,
                          title: `Papers in cluster: ${selectedCluster.label}`,
                        })
                      }
                    >
                      View all {selectedCluster.size} papers
                    </Button>
                  </div>
                  {selectedCluster.top_topics && selectedCluster.top_topics.length > 0 && (
                    <div>
                      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Top Topics</p>
                      <div className="flex flex-wrap gap-1.5">
                        {selectedCluster.top_topics.map((topic) => (
                          <Badge key={topic} variant="secondary">{topic}</Badge>
                        ))}
                      </div>
                    </div>
                  )}
                  {selectedCluster.sample_papers && selectedCluster.sample_papers.length > 0 && (
                    <div>
                      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Representative Papers</p>
                      <div className="space-y-2">
                        {selectedCluster.sample_papers.map((paper) => (
                          <button
                            key={paper.paper_id}
                            type="button"
                            onClick={() => {
                              const match = data?.nodes.find((node) => node.id === paper.paper_id)
                              if (match) {
                                setSelectedNode(match)
                                if (typeof match.cluster_id === 'number') {
                                  setSelectedClusterId(match.cluster_id)
                                }
                              }
                            }}
                            className="w-full rounded-sm border border-[var(--color-border)] bg-surface-2 px-3 py-2 text-left hover:border-[var(--color-border)] hover:bg-surface-1"
                          >
                            <p className="text-sm font-medium text-alma-800">{paper.title}</p>
                            <p className="mt-1 text-xs text-slate-500">
                              {paper.journal || 'Unknown venue'} · {paper.publication_date || paper.year || 'Unknown date'} · {paper.cited_by_count || 0} citations
                            </p>
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <EmptyState
                  icon={CircleOff}
                  title="Select a cluster or click a paper to focus the map."
                  className="min-h-[220px]"
                />
              )}
            </div>

            <div className="rounded-sm border border-[var(--color-border)] bg-surface-1 p-4 shadow-paper-sm shadow-sm">
              <div className="mb-3">
                <p className="text-sm font-semibold text-alma-800">{nodeDetailTitle}</p>
                <p className="text-xs text-slate-500">{nodeDetailSubtitle}</p>
              </div>
              {selectedNode ? (
                <div className="space-y-3">
                  <div className="rounded-lg bg-surface-2 p-3">
                    <p className="text-base font-semibold text-alma-800">{selectedNode.name}</p>
                    <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-600">
                      {selectedNodePublicationDate && <Badge variant="outline">{formatDate(selectedNodePublicationDate)}</Badge>}
                      {selectedNodeYear !== null && <Badge variant="outline">Year {String(selectedNodeYear)}</Badge>}
                      {selectedNodeCitations !== null && (
                        <Badge variant="outline">{String(selectedNodeCitations)} citations</Badge>
                      )}
                      {selectedNodeRating !== null && selectedNodeRating > 0 && (
                        <Badge variant="outline">Rating {'★'.repeat(selectedNodeRating)}</Badge>
                      )}
                      {selectedNodeClusterLabel && <Badge variant="secondary">{selectedNodeClusterLabel}</Badge>}
                    </div>
                  </div>
                  {selectedNodeJournal && (
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Venue</p>
                      <p className="mt-1 text-sm text-slate-700">{selectedNodeJournal}</p>
                    </div>
                  )}
                  {selectedNodeAuthors && (
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Authors</p>
                      <p className="mt-1 text-sm text-slate-700">{selectedNodeAuthors}</p>
                    </div>
                  )}
                  {selectedNodeTopics.length > 0 && (
                    <div>
                      <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Topics</p>
                      <div className="flex flex-wrap gap-1.5">
                        {selectedNodeTopics.slice(0, 8).map((topic) => (
                          <Badge key={topic} variant="secondary">{topic}</Badge>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <EmptyState
                  icon={CircleOff}
                  title={
                    activeView === 'paper-map'
                      ? 'Click a paper node to inspect it and highlight its cluster.'
                      : 'Click an author node to inspect it and highlight its cluster.'
                  }
                  className="min-h-[220px]"
                />
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      <InsightsPaperDrilldown target={drilldown} onClose={() => setDrilldown(null)} />
    </div>
  )
}

/** Small labelled stat for the Method & diagnostics panel (I-14). */
function Diagnostic({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-sm bg-surface-2 px-3 py-2" title={hint}>
      <p className="text-[11px] uppercase tracking-wide text-slate-400">{label}</p>
      <p className="mt-0.5 truncate text-sm font-medium text-alma-800" title={value}>
        {value}
      </p>
    </div>
  )
}
