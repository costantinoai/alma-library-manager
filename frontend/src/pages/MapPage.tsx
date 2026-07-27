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
 * Clicking opens the shared paper mini-popup and selects as a side effect;
 * full paper detail remains one explicit action away.
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BookOpen, LassoSelect, Map as MapIcon, X } from 'lucide-react'

import { MapRegionCard } from '@/components/map/MapRegionCard'
import { CreateSelectionLensButton } from '@/components/map/CreateSelectionLensButton'

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
import { CorpusMapPaperPopup } from '@/components/map/CorpusMapPaperPopup'
import type { MapPaperNeighbour } from '@/components/map/MapPaperPopup'
import { paperMapParams } from '@/components/map/mapQueries'
import {
  PAPER_MAP_DEFAULTS,
  useMapSessionState,
} from '@/components/map/mapSessionState'
import { useRegionSelection } from '@/components/map/useRegionSelection'
import { AuthorMapPanel } from '@/components/map/AuthorMapPanel'
import { useSignalField } from '@/components/map/useSignalField'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'

/** The two substrates this page can draw. */
type MapKind = 'papers' | 'authors'
import {
  MapDisplayTuningRows,
  MapModeSwitch,
  MapTuningPopover,
  SliderRow,
} from '@/components/map/MapChrome'
import { EDGE_LAYER_LABELS } from '@/components/map/mapNodeStyle'
import { MetricTile } from '@/components/shared/MetricTile'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { MetaLine, PageIntro } from '@/components/ui/page-intro'
import { PageTour, MAP_TOUR } from '@/components/onboarding'
import { StatusBadge } from '@/components/ui/status-badge'
import { invalidateQueries } from '@/lib/queryHelpers'
import { useHashRoute } from '@/lib/hashRoute'
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

function popupNeighbours(
  payload: GraphData | null,
  nodeId: string,
): MapPaperNeighbour[] {
  if (!payload) return []
  const nodesById = new Map(payload.nodes.map((node) => [node.id, node]))
  return payload.edges
    .filter((edge) => edge.source === nodeId || edge.target === nodeId)
    .sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0))
    .slice(0, 4)
    .flatMap((edge) => {
      const id = edge.source === nodeId ? edge.target : edge.source
      const neighbour = nodesById.get(id)
      if (!neighbour) return []
      return [{
        id,
        title: neighbour.name,
        relation:
          EDGE_LAYER_LABELS[String(edge.edge_type ?? '')] ??
          String(edge.edge_type ?? 'Map link'),
      }]
    })
}

export function MapPage() {
  const [scope, setScope] = useMapSessionState<'library' | 'corpus'>(
    'paper-map',
    'scope',
    PAPER_MAP_DEFAULTS.scope,
  )
  const [resolution, setResolution] = useMapSessionState(
    'paper-map',
    'resolution',
    PAPER_MAP_DEFAULTS.resolution,
  )
  const [sizeScale, setSizeScale] = useMapSessionState(
    'paper-map',
    'sizeScale',
    PAPER_MAP_DEFAULTS.sizeScale,
  )
  const [dotOpacity, setDotOpacity] = useMapSessionState(
    'paper-map',
    'dotOpacity',
    PAPER_MAP_DEFAULTS.dotOpacity,
  )
  const [terrainOpacity, setTerrainOpacity] = useMapSessionState(
    'paper-map',
    'terrainOpacity',
    PAPER_MAP_DEFAULTS.terrainOpacity,
  )
  const [wordScale, setWordScale] = useMapSessionState(
    'paper-map',
    'wordScale',
    PAPER_MAP_DEFAULTS.wordScale,
  )
  const [wordCount, setWordCount] = useMapSessionState(
    'paper-map',
    'wordCount',
    PAPER_MAP_DEFAULTS.wordCount,
  )
  // Layout blend (the old "physics") — library scope only; the corpus stays
  // on the cached pure-semantic substrate path.
  const [blend, setBlend] = useMapSessionState(
    'paper-map',
    'blend',
    PAPER_MAP_DEFAULTS.blend,
  )
  // Which substrate is on the plate. Session-scoped like every other map
  // control, so switching tabs and coming back keeps the view you were in.
  const [mapKind, setMapKind] = useMapSessionState<MapKind>('paper-map', 'kind', 'papers')
  const [payload, setPayload] = useState<GraphData | null>(null)
  const [selected, setSelected] = useState<GraphNode | null>(null)
  // Region selection (lasso): the inspector characterises the selected
  // patch — vocabulary, area score, strongest/weakest papers, top authors.
  const [selectMode, setSelectMode] = useState(false)
  const [panelPaper, setPanelPaper] = useState<Publication | null>(null)
  const [panelOpen, setPanelOpen] = useState(false)
  const queryClient = useQueryClient()
  const { toast } = useToast()

  // "Show on map" deep link (#/map?paper=<id>): once the payload is in,
  // select that paper — accent ring, cluster focus, inspector. Fired once
  // per id so a manual deselect afterwards sticks.
  const route = useHashRoute()
  const deepLinkPaperId = route.params.get('paper')
  const didDeepLinkRef = useRef<string | null>(null)

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
  const visibleMapIds = useMemo(
    () => new Set((payload?.nodes ?? []).map((node) => node.id)),
    [payload],
  )
  // Shared region primitive — the SAME select→describe lifecycle as
  // Discovery's "Select a direction" (useRegionSelection).
  const regionSel = useRegionSelection({ visibleIds: visibleMapIds })
  const regionIds = regionSel.ids
  const regionDesc = regionSel.description

  // Live internal scores (same space-owned endpoint the terrain uses):
  // hover Score, region area score, and cluster "area scores" all read
  // from HERE, never from the cached layout payload.
  const signalField = useSignalField(true)
  const scoresById = signalField.scoresById
  // Area score per cluster: mean live score of its scored papers.
  const clusterAreaScores = useMemo(() => {
    const acc = new Map<number, { sum: number; n: number }>()
    for (const n of payload?.nodes ?? []) {
      const cid = typeof n.cluster_id === 'number' ? n.cluster_id : -1
      if (cid < 0) continue
      const s = scoresById.get(n.id)
      if (s == null) continue
      const row = acc.get(cid)
      if (row) {
        row.sum += s
        row.n += 1
      } else acc.set(cid, { sum: s, n: 1 })
    }
    return new Map([...acc.entries()].map(([cid, { sum, n }]) => [cid, sum / n]))
  }, [payload, scoresById])

  // Region digest: everything the inspector says about a lassoed patch.
  const region = useMemo(() => {
    if (!regionIds || !payload) return null
    const inRegion = payload.nodes.filter((n) => regionIds.includes(n.id))
    const scored = inRegion
      .map((n) => ({ node: n, score: scoresById.get(n.id) }))
      .filter((r): r is { node: GraphNode; score: number } => r.score != null)
      .sort((a, b) => b.score - a.score)
    const areaScore = scored.length
      ? scored.reduce((a, r) => a + r.score, 0) / scored.length
      : null
    const authorTally = new Map<string, number>()
    for (const n of inRegion) {
      const raw = typeof n.metadata?.authors === 'string' ? n.metadata.authors : ''
      for (const a of raw.split(/[;,]/)) {
        const name = a.trim()
        if (name) authorTally.set(name, (authorTally.get(name) ?? 0) + 1)
      }
    }
    const topAuthors = [...authorTally.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
    return {
      papers: inRegion,
      inLibrary: inRegion.filter((n) => n.in_library !== false).length,
      scored,
      areaScore,
      highest: scored.slice(0, 3),
      lowest: scored.length > 3 ? scored.slice(-3).reverse() : [],
      topAuthors,
    }
  }, [regionIds, payload, scoresById])


  const params = useMemo(
    () => paperMapParams({ scope, resolution, blend }),
    [scope, resolution, blend],
  )

  useEffect(() => {
    if (!deepLinkPaperId || !payload || didDeepLinkRef.current === deepLinkPaperId) return
    didDeepLinkRef.current = deepLinkPaperId
    const node = payload.nodes.find(
      (n) => n.id === deepLinkPaperId || String(n.metadata?.paper_id ?? '') === deepLinkPaperId,
    )
    if (node) setSelected(node)
    else
      toast({
        title: 'Not on the map yet',
        description: 'This paper has no placement — it appears once it has a vector.',
      })
  }, [deepLinkPaperId, payload, toast])

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
  const selectedPaperId = selected
    ? String(selected.metadata?.paper_id ?? selected.id)
    : null
  const selectedPaperQuery = useQuery({
    queryKey: ['map-selected-paper', selectedPaperId],
    queryFn: () => getPaperById(selectedPaperId as string),
    enabled: !!selectedPaperId,
    staleTime: 30_000,
  })
  const selectedPaper = selectedPaperQuery.data
  const selectedYear =
    selectedPaper?.year ??
    (typeof selected?.metadata?.year === 'number' ? selected.metadata.year : null)
  const selectedJournal =
    selectedPaper?.journal ??
    (typeof selected?.metadata?.journal === 'string' ? selected.metadata.journal : null)
  const selectedPaperAuthorNames = useMemo(() => {
    const raw =
      selectedPaper?.authors ??
      (typeof selected?.metadata?.authors === 'string' ? selected.metadata.authors : '')
    return raw
      .split(/[;,]/)
      .map((name) => name.trim())
      .filter(Boolean)
  }, [selected, selectedPaper?.authors])

  // People are part of the selected paper AND part of its surrounding area.
  // Count their appearances inside this cluster so the author drilldown says
  // more than merely repeating the byline.
  const selectedClusterAuthors = useMemo(() => {
    if (!selected || !payload || typeof selected.cluster_id !== 'number') return []
    const counts = new Map<string, number>()
    for (const node of payload.nodes) {
      if (node.cluster_id !== selected.cluster_id) continue
      const raw = typeof node.metadata?.authors === 'string' ? node.metadata.authors : ''
      for (const author of raw.split(/[;,]/)) {
        const name = author.trim()
        if (name) counts.set(name, (counts.get(name) ?? 0) + 1)
      }
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
  }, [payload, selected])
  // The paper's neighbourhood: its strongest typed links, named.
  const neighbours = useMemo(() => {
    if (!selected || !payload) return []
    const names = new Map(payload.nodes.map((n) => [n.id, n.name]))
    return payload.edges
      .filter((e) => e.source === selected.id || e.target === selected.id)
      .sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0))
      .slice(0, 6)
      .map((e) => ({
        id: e.source === selected.id ? e.target : e.source,
        name: names.get(e.source === selected.id ? e.target : e.source) ?? '?',
        type: EDGE_LAYER_LABELS[String(e.edge_type ?? '')] ?? String(e.edge_type ?? 'link'),
        weight: e.weight,
      }))
  }, [selected, payload])

  const openPaperDetails = async (paperId: string) => {
    try {
      setPanelPaper(await getPaperById(paperId))
      setPanelOpen(true)
    } catch {
      /* A stale graph id will reconcile on the next layout rebuild. */
    }
  }

  return (
    <div className="space-y-6">
      <PageIntro
        icon={MapIcon}
        lede="Your corpus as territory."
        detail={
          mapKind === 'authors'
            ? 'Every author placed by what they write about — click one to inspect them, their community, and who sits nearby.'
            : 'Every paper placed by what it is about — click one to inspect it, its cluster, and its neighbourhood.'
        }
        tour={<PageTour pageKey="map" steps={MAP_TOUR} />}
        meta={<MetaLine items={[<span>Layout rebuilt under Fine tuning, never per visit</span>]} />}
        guide={{
          /* The map has more vocabulary than any other surface — territory,
             regions, clusters, scope, layout blend. One explainer at the top so
             the words are learnable in place. */
          summary:
            'Papers are placed by what they are about: near neighbours are semantically close, and the words name the region under them.',
          children: (
            <>
              <p className="mb-2">
                The layout is a <span className="font-medium text-alma-900">durable artifact</span>, not
                something recomputed per visit — so the same paper sits in the same place every time you
                come back, and a new paper is placed next to its nearest neighbours as soon as it has a
                vector. Rebuilding it is a deliberate act, under Fine tuning.
              </p>
              <p className="mb-2">
                <span className="font-medium text-alma-900">Corpus</span> and{' '}
                <span className="font-medium text-alma-900">Library</span> are the same territory at two
                scopes — every tracked paper, or only what you saved. A{' '}
                <span className="font-medium text-alma-900">cluster</span> is a region the layout found on
                its own; its label is drawn from the words its papers share.
              </p>
              <p>
                <span className="font-medium text-alma-900">Select region</span> is the question-asking
                tool: drag a box around any patch and ALMa characterises what lives there — vocabulary,
                strongest papers, top authors — which you can then turn into a lens for Discovery.
              </p>
            </>
          ),
        }}
      />

      <ToggleGroup
        type="single"
        variant="segment"
        value={mapKind}
        onValueChange={(next) => next && setMapKind(next as MapKind)}
        aria-label="Which map to show"
        className="w-fit"
        data-tour="map-kind"
      >
        <ToggleGroupItem value="papers">Papers</ToggleGroupItem>
        <ToggleGroupItem value="authors">Authors</ToggleGroupItem>
      </ToggleGroup>

      {/* Papers and authors are two views of ONE territory, so they share
          this page: same masthead, same guide, same plate — only the
          substrate changes (user call 2026-07-27). The author map used to be
          a banded section on the Authors page, which left that page carrying
          a map AND people-management, and split the map vocabulary across
          two surfaces. */}
      {mapKind === 'authors' ? (
        <AuthorMapPanel />
      ) : (
        <div className="space-y-4">
          <div data-tour="map-plate">
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
            dotOpacity={dotOpacity}
            terrainOpacity={terrainOpacity}
            toponymScale={wordScale}
            toponymWordCount={wordCount}
            // Popup-primary click semantics: the shared mini-card opens at the
            // dot; selection + cluster focus + inspector remain side effects.
            // Background clears both the card (inside SemanticMap) and focus.
            onOpenNode={(n) => {
              regionSel.clear()
              setSelected(n)
            }}
            onBackgroundClick={() => {
              setSelected(null)
              regionSel.clear()
            }}
            lassoMode={selectMode}
            onLasso={(ids, anchor) => {
              setSelected(null)
              setSelectMode(false)
              regionSel.select(ids, anchor)
            }}
            // BOTH surfaces, deliberately (user call 2026-07-26): the on-plate
            // card answers the gesture where it happened, the dense drilldown
            // below keeps the full breakdown. A spatial act needs immediate
            // feedback; a region worth reading needs room.
            plateOverlay={
              region && (
                <MapRegionCard
                  kind="Area"
                  icon={<LassoSelect className="h-3.5 w-3.5 text-alma-folio" />}
                  count={region.papers.length}
                  pending={regionSel.describing}
                  insufficient={region.papers.length < 5}
                  insufficientMessage="Too few papers to characterize — drag a larger patch (5+)."
                  onClose={() => regionSel.clear()}
                  actions={
                    <CreateSelectionLensButton
                      ids={regionIds ?? []}
                      scope={scope}
                      selectionKind="papers"
                      name={`${regionSel.description?.label ?? 'Map selection'} · map selection`}
                      onCreated={() => regionSel.clear()}
                    />
                  }
                >
                  <p className="text-sm font-semibold capitalize text-alma-800">
                    {regionSel.description?.label ?? `${region.papers.length} papers`}
                  </p>
                  <p className="mt-1.5 text-[11px] text-slate-500">
                    {region.inLibrary} in your library ·{' '}
                    {region.papers.length - region.inLibrary} tracked
                    {region.areaScore != null
                      ? ` · area score ${Math.round(region.areaScore)}/100`
                      : ''}
                  </p>
                  {region.highest.length > 0 && (
                    <ul className="mt-1.5 space-y-0.5">
                      {region.highest.map(({ node }) => (
                        <li key={node.id} className="line-clamp-1 text-[11px] text-slate-400">
                          · {node.name}
                        </li>
                      ))}
                    </ul>
                  )}
                  <p className="mt-2 text-[11px] text-slate-400">
                    Full breakdown — vocabulary, strongest and weakest papers, top
                    authors — is in the Region panel below.
                  </p>
                </MapRegionCard>
              )
            }
            hoverCard={(n) => {
              const score = scoresById.get(n.id)
              const areaScore =
                typeof n.cluster_id === 'number' && n.cluster_id >= 0
                  ? clusterAreaScores.get(n.cluster_id)
                  : undefined
              return (
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
                  {(score != null || areaScore != null) && (
                    <p className="mt-0.5 font-medium text-alma-800">
                      {score != null ? `Score ${Math.round(score)}/100` : 'Never scored'}
                      {areaScore != null ? ` · area ${Math.round(areaScore)}/100` : ''}
                    </p>
                  )}
                  {typeof n.metadata?.rating === 'number' && (n.metadata.rating as number) > 0 && (
                    <p className="mt-0.5 text-slate-500">your rating: {'★'.repeat(Number(n.metadata.rating))}</p>
                  )}
                  {typeof n.metadata?.cluster_label === 'string' &&
                    n.metadata.cluster_label !== 'Unclustered' && (
                      <p className="mt-0.5 text-slate-400">cluster: {String(n.metadata.cluster_label)}</p>
                    )}
                </>
              )
            }}
            renderClickCard={(n, close) => {
              const paperId = String(n.metadata?.paper_id ?? n.id)
              const score = scoresById.get(n.id)
              return (
                <CorpusMapPaperPopup
                  paperId={paperId}
                  onClose={close}
                  onOpenDetails={() => {
                    close()
                    void openPaperDetails(paperId)
                  }}
                  fallback={{
                    id: paperId,
                    title: n.name,
                    authors:
                      typeof n.metadata?.authors === 'string'
                        ? n.metadata.authors
                        : undefined,
                    year:
                      typeof n.metadata?.year === 'number'
                        ? n.metadata.year
                        : undefined,
                    journal:
                      typeof n.metadata?.journal === 'string'
                        ? n.metadata.journal
                        : undefined,
                    citedByCount:
                      typeof n.metadata?.cited_by_count === 'number'
                        ? n.metadata.cited_by_count
                        : undefined,
                    score,
                    statusLabel: n.in_library === false ? 'Tracked' : 'In your library',
                    clusterLabel:
                      typeof n.metadata?.cluster_label === 'string'
                        ? n.metadata.cluster_label
                        : undefined,
                    neighbours: popupNeighbours(payload, n.id),
                  }}
                />
              )
            }}
            height={620}
            toolbarExtras={
              <>
                {/* Real box, not `display:contents` — a spotlight needs a
                    measurable rect, and the child is inline-flex either way. */}
                <span data-tour="map-scope" className="inline-flex">
                  <MapModeSwitch
                    value={scope}
                    onChange={(v) => {
                      setScope(v)
                      setSelected(null)
                      regionSel.clear()
                    }}
                    options={[
                      { value: 'corpus', label: 'Corpus', title: 'Every tracked paper' },
                      { value: 'library', label: 'Library', title: 'Only papers you saved' },
                    ]}
                  />
                </span>
                <button
                  type="button"
                  data-tour="map-select"
                  onClick={() => {
                    setSelectMode((s) => !s)
                    regionSel.clear()
                  }}
                  className={
                    selectMode
                      ? 'inline-flex items-center gap-1.5 rounded-sm border border-accent-edge bg-accent-soft px-2.5 py-1 text-xs font-medium text-alma-folio'
                      : 'inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet'
                  }
                  title="Drag a box around a patch of papers — the inspector characterises the region (vocabulary, area score, strongest papers, top authors)"
                >
                  <LassoSelect className="h-3.5 w-3.5" />
                  Select region
                </button>
                <MapTuningPopover title="Fine tuning — terrain opacity, cluster detail, dot size, dot opacity, words, layout blend, rebuilds">
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
                    dotOpacity={dotOpacity}
                    onDotOpacity={setDotOpacity}
                    terrainOpacity={terrainOpacity}
                    onTerrainOpacity={setTerrainOpacity}
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
          </div>

          {/* Drilldowns live BELOW the full-width plate. Selecting a paper
              reveals two balanced columns: paper/relationships and
              cluster/authors (user call 2026-07-26). */}
          {region ? (
            <Card>
              <CardContent className="space-y-4 p-4 text-xs">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-sm font-semibold text-alma-800">
                    Region — {region.papers.length} papers
                  </p>
                  <button
                    type="button"
                    onClick={() => regionSel.clear()}
                    className="rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                    aria-label="Clear region"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                {regionSel.describing && <p className="text-slate-400">Characterising…</p>}
                {regionDesc?.sufficient && regionDesc.label && (
                  <p className="font-medium text-alma-800">“{regionDesc.label}”</p>
                )}
                {regionDesc && !regionDesc.sufficient && (
                  <p className="text-slate-400">Too few papers to characterise the vocabulary.</p>
                )}
                {(regionDesc?.top_terms ?? []).length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {(regionDesc?.top_terms ?? []).slice(0, 8).map((term) => (
                      <StatusBadge key={term} tone="neutral" size="sm">{term}</StatusBadge>
                    ))}
                  </div>
                )}
                <p className="text-slate-500">
                  {region.inLibrary} in your library · {region.papers.length - region.inLibrary} tracked
                  {region.areaScore != null
                    ? ` · area score ${Math.round(region.areaScore)}/100 (${region.scored.length} scored)`
                    : ' · no scored papers here yet'}
                </p>
                <div className="grid gap-4 border-t border-[var(--color-border)] pt-3 md:grid-cols-3">
                  <div>
                    <p className="mb-1 font-medium text-alma-800">Strongest here</p>
                    <ul className="space-y-1">
                      {region.highest.map(({ node, score }) => (
                        <li key={node.id} className="flex items-baseline justify-between gap-2">
                          <button
                            type="button"
                            className="line-clamp-1 text-left text-slate-600 hover:text-alma-800 hover:underline"
                            onClick={() => {
                              regionSel.clear()
                              setSelected(node)
                            }}
                          >
                            {node.name}
                          </button>
                          <span className="shrink-0 tabular-nums text-slate-400">{Math.round(score)}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div>
                    <p className="mb-1 font-medium text-alma-800">Weakest here</p>
                    <ul className="space-y-1">
                      {region.lowest.map(({ node, score }) => (
                        <li key={node.id} className="flex items-baseline justify-between gap-2">
                          <button
                            type="button"
                            className="line-clamp-1 text-left text-slate-600 hover:text-alma-800 hover:underline"
                            onClick={() => {
                              regionSel.clear()
                              setSelected(node)
                            }}
                          >
                            {node.name}
                          </button>
                          <span className="shrink-0 tabular-nums text-slate-400">{Math.round(score)}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div>
                    <p className="mb-1 font-medium text-alma-800">Most present authors</p>
                    <ul className="space-y-1">
                      {region.topAuthors.map(([name, count]) => (
                        <li key={name} className="flex items-baseline justify-between gap-2">
                          <span className="truncate text-slate-600">{name}</span>
                          <span className="shrink-0 tabular-nums text-slate-400">{count}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </CardContent>
            </Card>
          ) : selected ? (
            <div className="grid items-start gap-4 lg:grid-cols-2">
              <Card>
                <CardContent className="space-y-4 p-4 text-xs">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                        Paper drilldown
                      </p>
                      <h2 className="mt-1 text-base font-semibold leading-snug text-alma-800">
                        {selectedPaper?.title || selected.name}
                      </h2>
                    </div>
                    <button
                      type="button"
                      onClick={() => setSelected(null)}
                      className="shrink-0 rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                      aria-label="Clear selection"
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>

                  {selectedPaperAuthorNames.length > 0 && (
                    <p className="text-slate-500">{selectedPaperAuthorNames.join(' · ')}</p>
                  )}
                  <div className="flex flex-wrap gap-1">
                    <StatusBadge tone={selected.in_library === false ? 'neutral' : 'accent'} size="sm">
                      {selected.in_library === false ? 'Tracked corpus' : 'In your library'}
                    </StatusBadge>
                    {selectedYear != null && (
                      <StatusBadge tone="neutral" size="sm">
                        {selectedYear}
                      </StatusBadge>
                    )}
                    {selectedJournal && (
                      <StatusBadge tone="neutral" size="sm">
                        {selectedJournal}
                      </StatusBadge>
                    )}
                    {(selectedPaper?.cited_by_count ??
                      (typeof selected.metadata?.cited_by_count === 'number'
                        ? selected.metadata.cited_by_count
                        : 0)) > 0 && (
                      <StatusBadge tone="neutral" size="sm">
                        {(selectedPaper?.cited_by_count ??
                          Number(selected.metadata?.cited_by_count ?? 0)).toLocaleString()} citations
                      </StatusBadge>
                    )}
                  </div>

                  {scoresById.get(selected.id) != null && (
                    <div className="flex items-center justify-between rounded-sm border border-edge-2 px-3 py-2">
                      <div>
                        <p className="font-medium text-alma-800">Internal score</p>
                        <p className="text-[10px] text-slate-400">Latest Discovery relevance</p>
                      </div>
                      <ScoreMeter score={scoresById.get(selected.id) as number} />
                    </div>
                  )}

                  {selectedPaper?.tldr && (
                    <div className="rounded-sm border border-edge-2 bg-surface-2 px-3 py-2">
                      <p className="mb-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                        TLDR
                      </p>
                      <p className="text-xs italic leading-relaxed text-slate-600">
                        {selectedPaper.tldr}
                      </p>
                    </div>
                  )}
                  {selectedPaper?.abstract && (
                    <div>
                      <p className="mb-1 font-medium text-alma-800">Abstract</p>
                      <p className="line-clamp-6 leading-relaxed text-slate-600">
                        {selectedPaper.abstract}
                      </p>
                    </div>
                  )}

                  <Button
                    size="sm"
                    onClick={() => void openPaperDetails(selectedPaperId as string)}
                  >
                    <BookOpen className="mr-1.5 h-3.5 w-3.5" />
                    Open full paper
                  </Button>

                  {neighbours.length > 0 && (
                    <div className="border-t border-[var(--color-border)] pt-3">
                      <p className="mb-2 font-medium text-alma-800">Strongest relationships</p>
                      <ul className="space-y-2">
                        {neighbours.map((neighbour) => (
                          <li key={neighbour.id}>
                            <button
                              type="button"
                              className="line-clamp-1 text-left font-medium text-slate-700 hover:text-alma-800 hover:underline"
                              onClick={() => {
                                const node = payload?.nodes.find((item) => item.id === neighbour.id)
                                if (node) setSelected(node)
                              }}
                            >
                              {neighbour.name}
                            </button>
                            <p className="text-[10px] text-slate-400">{neighbour.type}</p>
                          </li>
                        ))}
                      </ul>
                      <p className="mt-2 text-[10px] text-slate-400">
                        Turn on Links to see these relationships on the plate.
                      </p>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardContent className="space-y-4 p-4 text-xs">
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                      Cluster &amp; authors
                    </p>
                    <h2 className="mt-1 text-base font-semibold text-alma-800">
                      {selectedCluster?.label ?? 'Unclustered'}
                    </h2>
                  </div>

                  {selectedCluster ? (
                    <>
                      <div className="grid grid-cols-3 gap-2">
                        <MetricTile label="Papers" value={selectedCluster.size} align="center" />
                        <MetricTile
                          label="Area score"
                          value={
                            clusterAreaScores.get(selectedCluster.id) != null
                              ? Math.round(clusterAreaScores.get(selectedCluster.id) as number)
                              : '—'
                          }
                          align="center"
                        />
                        <MetricTile
                          label="Avg citations"
                          value={
                            selectedCluster.avg_citations != null
                              ? Math.round(selectedCluster.avg_citations)
                              : '—'
                          }
                          align="center"
                        />
                      </div>
                      {selectedCluster.year_range?.min && (
                        <p className="text-slate-500">
                          Publication span {selectedCluster.year_range.min}–
                          {selectedCluster.year_range.max ?? selectedCluster.year_range.min}
                        </p>
                      )}
                      {(selectedCluster.top_topics ?? []).length > 0 && (
                        <div>
                          <p className="mb-1.5 font-medium text-alma-800">Cluster vocabulary</p>
                          <div className="flex flex-wrap gap-1">
                            {(selectedCluster.top_topics ?? []).slice(0, 8).map((topic) => (
                              <StatusBadge key={topic} tone="neutral" size="sm">{topic}</StatusBadge>
                            ))}
                          </div>
                        </div>
                      )}
                      {(selectedCluster.sample_papers ?? []).length > 0 && (
                        <div>
                          <p className="mb-1.5 font-medium text-alma-800">Representative papers</p>
                          <ul className="space-y-1">
                            {(selectedCluster.sample_papers ?? []).slice(0, 4).map((paper, index) => (
                              <li key={`${paper.title}-${index}`} className="text-slate-600">
                                <span className="line-clamp-1">{paper.title}</span>
                                <span className="text-[10px] text-slate-400">
                                  {paper.year ? `${paper.year}` : ''}
                                  {paper.cited_by_count != null
                                    ? `${paper.year ? ' · ' : ''}${paper.cited_by_count} citations`
                                    : ''}
                                </span>
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </>
                  ) : (
                    <p className="text-slate-500">
                      This paper is outside the current cluster assignment.
                    </p>
                  )}

                  <div className="border-t border-[var(--color-border)] pt-3">
                    <p className="mb-2 font-medium text-alma-800">Authors in this area</p>
                    {selectedClusterAuthors.length > 0 ? (
                      <ul className="grid gap-1.5 sm:grid-cols-2">
                        {selectedClusterAuthors.map(([name, count]) => {
                          const onPaper = selectedPaperAuthorNames.includes(name)
                          return (
                            <li key={name} className="flex min-w-0 items-center gap-2">
                              <a
                                href={`#/authors?q=${encodeURIComponent(name)}`}
                                className="min-w-0 flex-1 truncate font-medium text-slate-700 hover:text-alma-800 hover:underline"
                              >
                                {name}
                              </a>
                              {onPaper && <StatusBadge tone="accent" size="sm">this paper</StatusBadge>}
                              <span className="shrink-0 tabular-nums text-slate-400">{count}</span>
                            </li>
                          )
                        })}
                      </ul>
                    ) : (
                      <p className="text-slate-400">No author metadata is available for this area.</p>
                    )}
                    <p className="mt-2 text-[10px] text-slate-400">
                      Count = papers by that author inside this cluster.
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>
          ) : (
            <Card data-tour="map-inspector">
              <CardContent className="space-y-4 p-4 text-xs">
                <p className="text-sm font-semibold text-alma-800">Map overview</p>
                <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
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
                  <ul className="grid gap-x-6 gap-y-1 md:grid-cols-2">
                    {clusters
                      .slice()
                      .sort((a, b) => b.size - a.size)
                      .slice(0, 8)
                      .map((cluster) => (
                        <li key={cluster.id} className="flex items-baseline justify-between gap-2">
                          <span className="truncate text-slate-600">{cluster.label}</span>
                          <span className="shrink-0 tabular-nums text-slate-400">{cluster.size}</span>
                        </li>
                      ))}
                  </ul>
                </div>
                <p className="text-[11px] text-slate-400">
                  Click a paper for its popup and the paper / cluster / author drilldowns below.
                </p>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      <PaperDetailPanel paper={panelPaper} open={panelOpen} onOpenChange={setPanelOpen} />
    </div>
  )
}
