import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Network, Map, AlertCircle, Loader2, CircleOff, Layers3 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { EmptyState } from '@/components/ui/empty-state'
import { api, refreshClusterLabels, type GraphData, type GraphNode } from '@/api/client'
import { ForceGraph, type GraphPhysicsConfig } from './ForceGraph'
import { GraphControls } from './GraphControls'
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
  const [showEdges, setShowEdges] = useState(true)
  const [showTopics, setShowTopics] = useState(false)
  const [showWordCloud, setShowWordCloud] = useState(false)
  const [includeCorpus, setIncludeCorpus] = useState(false)
  const [physics, setPhysics] = useState<GraphPhysicsConfig>(DEFAULT_GRAPH_PHYSICS)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const scope = includeCorpus ? 'corpus' : 'library'
  const queryParams = activeView === 'paper-map'
    ? `?label_mode=${labelMode}&color_by=${colorBy}&size_by=${sizeBy}&show_edges=${showEdges}&show_topics=${showTopics}&scope=${scope}`
    : `?scope=${scope}`

  const { data, isLoading, error } = useQuery<GraphData>({
    queryKey: ['graph', activeView, labelMode, colorBy, sizeBy, showEdges, showTopics, scope],
    queryFn: () => api.get<GraphData>(`/graphs/${activeView}${queryParams}`),
    staleTime: 60_000,
  })

  const rebuildMutation = useMutation({
    mutationFn: () => api.post<{ status?: string; job_id?: string }>('/graphs/rebuild'),
    onSuccess: (result) => {
      void invalidateQueries(
        queryClient,
        ['graph'],
        ...(result?.status === 'queued' && result.job_id ? [['activity-operations']] : []),
      )
      if (result?.status === 'queued' && result.job_id) {
        toast({ title: 'Graph rebuild queued', description: `Job ${result.job_id} is running.` })
      } else {
        toast({ title: 'Graph rebuild complete' })
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
            includeCorpus={includeCorpus}
            onIncludeCorpusChange={setIncludeCorpus}
            physics={physics}
            onPhysicsChange={updatePhysics}
            onResetPhysics={() => setPhysics(DEFAULT_GRAPH_PHYSICS)}
            onRefreshLabels={() => labelRefreshMutation.mutate()}
            isRefreshingLabels={labelRefreshMutation.isPending}
          />

          <div className="mt-4 overflow-hidden rounded-sm border border-[var(--color-border)] bg-parchment-50">
            {data && data.nodes.length > 0 && (
              <span className="sr-only">
                Interactive {config.label.toLowerCase()} visualization showing {data.nodes.length} nodes and {data.edges.length} connections. Use the controls panel to filter and configure the graph.
              </span>
            )}
            {isLoading ? (
              <div className="flex h-[560px] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-slate-400" />
              </div>
            ) : error ? (
              <div className="flex h-[560px] flex-col items-center justify-center text-slate-500">
                <AlertCircle className="mb-2 h-8 w-8" />
                <p className="text-sm">Failed to load graph data</p>
                <p className="mt-1 text-xs text-slate-400">{graphErrorMessage}</p>
              </div>
            ) : data && data.nodes.length > 0 ? (
              <ForceGraph
                data={data}
                height={560}
                onNodeClick={handleNodeClick}
                showLabels={showLabels}
                highlightSearch={searchQuery}
                selectedNodeId={selectedNode?.id || null}
                selectedClusterId={selectedClusterId}
                showClusterLabels={activeView === 'paper-map' && (showTopics || labelMode !== 'cluster')}
                showWordCloud={activeView === 'paper-map' && showWordCloud}
                clusters={clusters}
                physics={physics}
              />
            ) : (
              <div className="flex h-[560px] flex-col items-center justify-center text-slate-500">
                <Map className="mb-2 h-8 w-8" />
                <p className="text-sm">No data available</p>
                <p className="mt-1 text-xs text-slate-400">Add authors and refresh to generate graph data</p>
              </div>
            )}
          </div>

          <div className="mt-4 grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
            <div className="rounded-sm border border-[var(--color-border)] bg-alma-paper p-4 shadow-paper-sm shadow-sm">
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
                  <div className="rounded-lg bg-parchment-50 p-3">
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
                            className="w-full rounded-sm border border-[var(--color-border)] bg-parchment-50 px-3 py-2 text-left hover:border-[var(--color-border)] hover:bg-alma-chrome"
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

            <div className="rounded-sm border border-[var(--color-border)] bg-alma-paper p-4 shadow-paper-sm shadow-sm">
              <div className="mb-3">
                <p className="text-sm font-semibold text-alma-800">{nodeDetailTitle}</p>
                <p className="text-xs text-slate-500">{nodeDetailSubtitle}</p>
              </div>
              {selectedNode ? (
                <div className="space-y-3">
                  <div className="rounded-lg bg-parchment-50 p-3">
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
    </div>
  )
}
