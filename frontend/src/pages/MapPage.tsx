/**
 * MapPage — the top-level corpus map AND the deep drill-down host
 * (task 50 M3 / 50-A / 50-M).
 *
 * Same primitives + idioms as every other map (50-K: GraphMapView →
 * SemanticMap, MapToolbar/MapLegend vocabulary) — but THIS host adds the
 * depth the others don't need:
 *
 *   - click a paper → its cluster HIGHLIGHTS (everything else dims) and the
 *     inspector column fills: paper info, cluster stats, the paper's
 *     strongest typed links (its neighbourhood);
 *   - nothing selected → the overview strip: nodes/links/clusters, coverage,
 *     stability, method — the map's vital signs;
 *   - fine tuning in ONE Advanced popover (50-I): cluster detail as a
 *     continuous slider (0.5–3.0, background variant builds), dot size,
 *     and the layout-blend sliders (semantic / co-authors / shared refs /
 *     cited together — library scope, where the fused layout runs).
 *
 * Clicking selects; the paper panel opens from the inspector's own button.
 */
import { useMemo, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { BookOpen, Map as MapIcon, X } from 'lucide-react'

import {
  api,
  getPaperById,
  refreshClusterLabels,
  type GraphData,
  type GraphNode,
  type Publication,
} from '@/api/client'
import { PaperDetailPanel } from '@/components/discovery'
import { GraphMapView } from '@/components/map/GraphMapView'
import {
  MapDisplayTuningRows,
  MapModeSwitch,
  MapTuningPopover,
  SliderRow,
} from '@/components/map/MapChrome'
import { EDGE_LAYER_LABELS } from '@/components/map/mapNodeStyle'
import { MetricTile } from '@/components/shared/MetricTile'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { StatusBadge } from '@/components/ui/status-badge'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useToast } from '@/hooks/useToast'

interface ClusterMeta {
  id: number
  label: string
  size: number
  avg_citations?: number
  avg_rating?: number
  year_range?: { min?: number; max?: number }
  top_topics?: string[]
  sample_papers?: Array<{ title: string; year?: number; cited_by_count?: number }>
}

export function MapPage() {
  const [scope, setScope] = useState<'library' | 'corpus'>('corpus')
  const [resolution, setResolution] = useState(1.5)
  const [sizeScale, setSizeScale] = useState(1)
  const [wordScale, setWordScale] = useState(1)
  const [wordCount, setWordCount] = useState(3)
  // Layout blend (the old "physics") — library scope only; the corpus stays
  // on the cached pure-semantic substrate path.
  const [blend, setBlend] = useState({ sem: 1, coauth: 0, refs: 0, cocite: 0 })
  const [payload, setPayload] = useState<GraphData | null>(null)
  const [selected, setSelected] = useState<GraphNode | null>(null)
  const [panelPaper, setPanelPaper] = useState<Publication | null>(null)
  const [panelOpen, setPanelOpen] = useState(false)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const rebuildMutation = useMutation({
    mutationFn: () => api.post<{ status?: string }>(`/graphs/rebuild?scope=${scope}`),
    onSuccess: (r) => {
      void invalidateQueries(queryClient, ['graph'])
      toast({ title: r?.status === 'queued' ? `Layout rebuild queued (${scope})` : `Layout rebuilt (${scope})` })
    },
  })
  const relabelMutation = useMutation({
    mutationFn: () => refreshClusterLabels({ graph_type: 'paper_map', scope }),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['graph'])
      toast({ title: 'Cluster relabelling queued', description: 'Watch Activity for progress.' })
    },
  })

  const params = useMemo(() => {
    const p: Record<string, string> = { scope, cluster_resolution: resolution.toFixed(1) }
    if (scope === 'library') {
      p.w_semantic = String(blend.sem)
      p.w_coauthorship = String(blend.coauth)
      p.w_bibliographic = String(blend.refs)
      p.w_cocitation = String(blend.cocite)
    }
    return p
  }, [scope, resolution, blend])

  const clustering = useMemo(
    () =>
      ((((payload?.metadata ?? {}) as Record<string, unknown>).clustering ?? {}) as Record<
        string,
        unknown
      >),
    [payload],
  )
  const clusters = useMemo(
    () =>
      ((((payload?.metadata ?? {}) as Record<string, unknown>).clusters ?? []) as ClusterMeta[]),
    [payload],
  )
  const selectedCluster = useMemo(
    () =>
      selected && typeof selected.cluster_id === 'number'
        ? clusters.find((c) => c.id === selected.cluster_id) ?? null
        : null,
    [selected, clusters],
  )
  // The paper's neighbourhood: its strongest typed links, named.
  const neighbours = useMemo(() => {
    if (!selected || !payload) return []
    const names = new Map(payload.nodes.map((n) => [n.id, n.name]))
    return payload.edges
      .filter((e) => e.source === selected.id || e.target === selected.id)
      .sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0))
      .slice(0, 6)
      .map((e) => ({
        name: names.get(e.source === selected.id ? e.target : e.source) ?? '?',
        type: EDGE_LAYER_LABELS[String(e.edge_type ?? '')] ?? String(e.edge_type ?? 'link'),
        weight: e.weight,
      }))
  }, [selected, payload])

  return (
    <div className="space-y-4 p-6">
      <header>
        <h1 className="flex items-center gap-2 text-2xl font-semibold text-alma-800">
          <MapIcon className="h-6 w-6 text-alma-600" />
          Map
        </h1>
        <p className="text-sm text-slate-500">
          Your corpus as territory — click a paper to inspect it, its cluster, and its neighbourhood.
        </p>
      </header>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_21rem]">
        <GraphMapView
          endpoint="paper-map"
          params={params}
          nodeKind={(n) => (n.in_library === false ? 'corpus' : 'library')}
          onPayload={setPayload}
          selectedNodeId={selected?.id ?? null}
          focusClusterId={
            selected && typeof selected.cluster_id === 'number' && selected.cluster_id >= 0
              ? selected.cluster_id
              : null
          }
          sizeScale={sizeScale}
          toponymScale={wordScale}
          toponymWordCount={wordCount}
          // 50-M: click SELECTS (accent ring + cluster focus + inspector);
          // the paper panel opens from the inspector, deliberately. A click
          // on the background deselects — cluster focus clears, inspector
          // returns to the overview.
          onOpenNode={(n) => setSelected((cur) => (cur?.id === n.id ? null : n))}
          onBackgroundClick={() => setSelected(null)}
          hoverCard={(n) => (
            <>
              <p className="line-clamp-2 font-medium text-alma-800">{n.name}</p>
              {typeof n.metadata?.authors === 'string' && n.metadata.authors && (
                <p className="mt-0.5 line-clamp-1 text-slate-500">{String(n.metadata.authors)}</p>
              )}
              <p className="mt-0.5 text-slate-500">
                {n.in_library === false ? 'Tracked' : 'In your library'}
                {n.metadata?.year ? ` · ${n.metadata.year}` : ''}
                {n.metadata?.journal ? ` · ${String(n.metadata.journal)}` : ''}
                {typeof n.metadata?.cited_by_count === 'number'
                  ? ` · ${n.metadata.cited_by_count} citations`
                  : ''}
              </p>
              {typeof n.metadata?.score === 'number' && (
                <p className="mt-0.5 font-medium text-alma-800">Score {Math.round(Number(n.metadata.score))}/100</p>
              )}
              {typeof n.metadata?.rating === 'number' && (n.metadata.rating as number) > 0 && (
                <p className="mt-0.5 text-slate-500">your rating: {'★'.repeat(Number(n.metadata.rating))}</p>
              )}
              {typeof n.metadata?.cluster_label === 'string' &&
                n.metadata.cluster_label !== 'Unclustered' && (
                  <p className="mt-0.5 text-slate-400">cluster: {String(n.metadata.cluster_label)}</p>
                )}
            </>
          )}
          height={620}
          toolbarExtras={
            <>
              <MapModeSwitch
                value={scope}
                onChange={(v) => {
                  setScope(v)
                  setSelected(null)
                }}
                options={[
                  { value: 'corpus', label: 'Corpus', title: 'Every tracked paper' },
                  { value: 'library', label: 'Library', title: 'Only papers you saved' },
                ]}
              />
              <MapTuningPopover title="Fine tuning — cluster detail, dot size, words, layout blend, rebuilds">
                <SliderRow
                  label="Cluster detail"
                  value={resolution}
                  min={0.5}
                  max={3}
                  step={0.1}
                  format={(v) => `${v.toFixed(1)}×`}
                  onCommit={(v) => setResolution(Number(v.toFixed(1)))}
                />
                <MapDisplayTuningRows
                  sizeScale={sizeScale}
                  onSizeScale={setSizeScale}
                  wordScale={wordScale}
                  onWordScale={setWordScale}
                  wordCount={wordCount}
                  onWordCount={setWordCount}
                />
                  {scope === 'library' ? (
                    <div className="space-y-3 border-t border-[var(--color-border)] pt-3">
                      <p className="font-medium text-alma-800">
                        Layout blend
                        <span className="ml-1 font-normal text-slate-400">
                          — pull related papers together
                        </span>
                      </p>
                      <SliderRow label="Semantic" value={blend.sem} min={0} max={1} step={0.05} format={(v) => v.toFixed(2)} onCommit={(v) => setBlend((b) => ({ ...b, sem: v }))} />
                      <SliderRow label="Shared authors" value={blend.coauth} min={0} max={1} step={0.05} format={(v) => v.toFixed(2)} onCommit={(v) => setBlend((b) => ({ ...b, coauth: v }))} />
                      <SliderRow label="Shared references" value={blend.refs} min={0} max={1} step={0.05} format={(v) => v.toFixed(2)} onCommit={(v) => setBlend((b) => ({ ...b, refs: v }))} />
                      <SliderRow label="Cited together" value={blend.cocite} min={0} max={1} step={0.05} format={(v) => v.toFixed(2)} onCommit={(v) => setBlend((b) => ({ ...b, cocite: v }))} />
                    </div>
                  ) : (
                    <p className="border-t border-[var(--color-border)] pt-2 text-[11px] text-slate-400">
                      Layout blend is available on the Library scope — the corpus keeps the fast cached layout.
                    </p>
                  )}
                  <div className="space-y-1.5 border-t border-[var(--color-border)] pt-2">
                    <Button size="sm" variant="outline" className="w-full" disabled={rebuildMutation.isPending} onClick={() => rebuildMutation.mutate()}>
                      Rebuild layout ({scope})
                    </Button>
                    <Button size="sm" variant="outline" className="w-full" disabled={relabelMutation.isPending} onClick={() => relabelMutation.mutate()}>
                      Refresh cluster labels
                    </Button>
                  </div>
              </MapTuningPopover>
            </>
          }
        />

        {/* ── Inspector column (50-M) ─────────────────────────────────── */}
        <Card className="self-start">
          <CardContent className="space-y-4 p-4 text-xs">
            {!selected ? (
              <>
                <p className="text-sm font-semibold text-alma-800">Map overview</p>
                <div className="grid grid-cols-2 gap-2">
                  <MetricTile label="Papers" value={payload?.nodes.length ?? '—'} align="center" />
                  <MetricTile label="Links" value={payload?.edges.length ?? '—'} align="center" />
                  <MetricTile label="Clusters" value={String(clustering.n_clusters ?? clusters.length ?? '—')} align="center" />
                  <MetricTile
                    label="Clustered"
                    value={
                      typeof clustering.coverage === 'number'
                        ? `${Math.round((clustering.coverage as number) * 100)}%`
                        : '—'
                    }
                    align="center"
                  />
                </div>
                <p className="text-[11px] text-slate-400">
                  {String(clustering.method ?? '')}
                  {clustering.stability != null ? ` · stability ${clustering.stability}` : ''}
                  {typeof clustering.outlier_count === 'number'
                    ? ` · ${clustering.outlier_count} unclustered`
                    : ''}
                </p>
                <div className="border-t border-[var(--color-border)] pt-3">
                  <p className="mb-1.5 font-medium text-alma-800">Largest clusters</p>
                  <ul className="space-y-1">
                    {clusters
                      .slice()
                      .sort((a, b) => b.size - a.size)
                      .slice(0, 6)
                      .map((c) => (
                        <li key={c.id} className="flex items-baseline justify-between gap-2">
                          <span className="truncate text-slate-600">{c.label}</span>
                          <span className="shrink-0 tabular-nums text-slate-400">{c.size}</span>
                        </li>
                      ))}
                  </ul>
                </div>
                <p className="text-[11px] text-slate-400">
                  Click a paper on the map to inspect it, its cluster, and its neighbourhood.
                </p>
              </>
            ) : (
              <>
                <div className="flex items-start justify-between gap-2">
                  <p className="text-sm font-semibold leading-snug text-alma-800">{selected.name}</p>
                  <button
                    type="button"
                    onClick={() => setSelected(null)}
                    className="rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                    aria-label="Clear selection"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                {typeof selected.metadata?.authors === 'string' && (
                  <p className="line-clamp-2 text-slate-500">{String(selected.metadata.authors)}</p>
                )}
                <p className="text-slate-500">
                  {selected.metadata?.year ? `${selected.metadata.year}` : ''}
                  {selected.metadata?.journal ? ` · ${String(selected.metadata.journal)}` : ''}
                  {typeof selected.metadata?.cited_by_count === 'number'
                    ? ` · ${selected.metadata.cited_by_count} citations`
                    : ''}
                </p>
                <Button
                  size="sm"
                  className="w-full"
                  onClick={async () => {
                    try {
                      setPanelPaper(await getPaperById(String(selected.metadata?.paper_id ?? selected.id)))
                      setPanelOpen(true)
                    } catch {
                      /* stale id — next rebuild trues the payload */
                    }
                  }}
                >
                  <BookOpen className="mr-1.5 h-3.5 w-3.5" />
                  Open paper
                </Button>

                {selectedCluster && (
                  <div className="border-t border-[var(--color-border)] pt-3">
                    <p className="mb-1 font-medium text-alma-800">Cluster — {selectedCluster.label}</p>
                    <p className="text-slate-500">
                      {selectedCluster.size} papers
                      {typeof selectedCluster.avg_citations === 'number'
                        ? ` · avg ${Math.round(selectedCluster.avg_citations)} citations`
                        : ''}
                      {selectedCluster.year_range?.min
                        ? ` · ${selectedCluster.year_range.min}–${selectedCluster.year_range.max ?? ''}`
                        : ''}
                    </p>
                    {(selectedCluster.top_topics ?? []).length > 0 && (
                      <div className="mt-1.5 flex flex-wrap gap-1">
                        {(selectedCluster.top_topics ?? []).slice(0, 6).map((t) => (
                          <StatusBadge key={t} tone="neutral" size="sm">
                            {t}
                          </StatusBadge>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {neighbours.length > 0 && (
                  <div className="border-t border-[var(--color-border)] pt-3">
                    <p className="mb-1 font-medium text-alma-800">Neighbourhood</p>
                    <ul className="space-y-1">
                      {neighbours.map((nb, i) => (
                        <li key={i} className="text-slate-600">
                          <span className="line-clamp-1">{nb.name}</span>
                          <span className="text-[11px] text-slate-400">{nb.type}</span>
                        </li>
                      ))}
                    </ul>
                    <p className="mt-1 text-[11px] text-slate-400">
                      Turn on Links in the toolbar to see these drawn.
                    </p>
                  </div>
                )}
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <PaperDetailPanel paper={panelPaper} open={panelOpen} onOpenChange={setPanelOpen} />
    </div>
  )
}
