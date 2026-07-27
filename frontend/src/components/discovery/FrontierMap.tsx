/**
 * FrontierMap — Discovery's host of the shared `MapSurface`.
 *
 * This file owns Discovery-specific MEANING only: the frontier query, the
 * seen-layer toggle, the branch grouping and its steering chips, the
 * recommendation popup, and the lasso → describe → adopt-a-Direction loop.
 * Everything else — session state, colour modes, terrain, colourbars, edge
 * chips, the plate itself — comes from `MapSurface`, the one host every map in
 * the app renders through.
 *
 * It used to be a parallel implementation of that host (945 lines, its own
 * `useMapField` wiring, its own legend, its own `SemanticMap` call), and the
 * cost was exactly what task 64 was opened for: when the terrain ramp moved to
 * a ±0.5 domain the Map page followed and this file did not, so its colourbar
 * claimed `-1 … +1` beside a gradient that no longer used it. Nothing failed,
 * because nothing tied the two together. Now there is nothing to tie.
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronUp,
  Eye,
  EyeOff,
  LassoSelect,
  Sparkles,
} from 'lucide-react'

import { type FrontierNode, type Lens } from '@/api/client'
import { CreateSelectionLensButton } from '@/components/map/CreateSelectionLensButton'
import { MapRegionCard } from '@/components/map/MapRegionCard'
import { useRegionSelection } from '@/components/map/useRegionSelection'
import { useBranchControls } from '@/hooks/useBranchControls'
import { CorpusMapPaperPopup } from '@/components/map/CorpusMapPaperPopup'
import type { MapPaperNeighbour } from '@/components/map/MapPaperPopup'
import { MapToggle } from '@/components/map/MapChrome'
import {
  MapSurface,
  MapSurfaceLoading,
  type MapGrouping,
  type MapSurfaceNode,
} from '@/components/map/MapSurface'
import { MAP_INK } from '@/components/map/mapNodeStyle'
import { frontierQueryOptions } from '@/components/map/mapQueries'
import { useMapSessionState } from '@/components/map/mapSessionState'
import { StatusBadge } from '@/components/ui/status-badge'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'
import { ErrorState } from '@/components/ui/ErrorState'

interface FrontierMapProps {
  lensId: string | null
  /** The lens itself — needed to read/write branch controls from the legend
   *  chips. Optional so the map still renders read-only without it. */
  lens?: Lens | null
  /** Open the shared PaperDetailPanel for a paper (same as the list views). */
  onSelectPaper: (paperId: string) => void
  /** Adopt a selected region as a custom direction on the current lens
   *  (writes branch_controls.custom_directions + refreshes). */
  onAdoptDirection?: (direction: {
    label: string
    terms: string[]
    member_paper_ids: string[]
  }) => void
  /** 50-B map→list sync: filter the rec list below to a lassoed region. */
  onFilterList?: (paperIds: string[]) => void
  /** Recommendation card supplied by DiscoveryPage so it can reuse the
   *  already-mounted recommendation mutations rather than duplicating them. */
  renderRecommendationPopup: (
    node: FrontierNode,
    close: () => void,
    neighbours: MapPaperNeighbour[],
  ) => React.ReactNode
}

/** Map distinct branch ids → {color, label, index} in first-seen order. */
function useBranchColors(nodes: FrontierNode[]) {
  return useMemo(() => {
    const map = new Map<string, { color: string; label: string; index: number; count: number }>()
    for (const n of nodes) {
      if (n.layer !== 'rec' || !n.branch_id) continue
      const existing = map.get(n.branch_id)
      if (existing) {
        existing.count += 1
      } else {
        const index = map.size
        map.set(n.branch_id, {
          color: branchMapColor(index),
          label: (n.branch_label || 'branch').trim(),
          index,
          count: 1,
        })
      }
    }
    return map
  }, [nodes])
}

function frontierNeighbours(
  node: FrontierNode,
  nodes: FrontierNode[],
  edges: ReadonlyArray<{ source: string; target: string; edge_type: string; weight: number }>,
): MapPaperNeighbour[] {
  const byId = new Map(nodes.map((item) => [item.paper_id, item]))
  const connected = edges
    .filter((edge) => edge.source === node.paper_id || edge.target === node.paper_id)
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 4)
    .flatMap((edge) => {
      const id = edge.source === node.paper_id ? edge.target : edge.source
      const other = byId.get(id)
      if (!other) return []
      return [{
        id,
        title: other.title || id,
        relation:
          edge.edge_type === 'bibliographic_coupling'
            ? 'Shares references'
            : 'Cited together',
      }]
    })
  if (connected.length > 0) return connected

  // The edge payload is opt-in. When links are off, nearest substrate points
  // are still honest semantic neighbours: position is the map's meaning.
  const nearest: Array<{ node: FrontierNode; distance: number }> = []
  for (const candidate of nodes) {
    if (candidate.paper_id === node.paper_id) continue
    const distance = (candidate.x - node.x) ** 2 + (candidate.y - node.y) ** 2
    const insertAt = nearest.findIndex((item) => distance < item.distance)
    if (insertAt < 0) nearest.push({ node: candidate, distance })
    else nearest.splice(insertAt, 0, { node: candidate, distance })
    if (nearest.length > 4) nearest.pop()
  }
  return nearest.map(({ node: candidate }) => ({
    id: candidate.paper_id,
    title: candidate.title || candidate.paper_id,
    relation:
      candidate.cluster_id === node.cluster_id && node.cluster_id != null
        ? 'Nearby · same cluster'
        : 'Nearby in semantic space',
  }))
}

export function FrontierMap({
  lensId,
  lens,
  onSelectPaper,
  onAdoptDirection,
  onFilterList,
  renderRecommendationPopup,
}: FrontierMapProps) {
  // Both are FETCH parameters, not view filters — the server only ships the
  // seen layer and the citation edges when asked — so they live here and the
  // surface renders them controlled. Session-scoped like every other map knob.
  const [showSeen, setShowSeen] = useMapSessionState('frontier', 'showSeen', false)
  const [showEdges, setShowEdges] = useMapSessionState('frontier', 'showEdges', false)
  const [highlightBranch, setHighlightBranch] = useState<string | null>(null)
  const [selectMode, setSelectMode] = useState(false)
  // Clicking a paper HIGHLIGHTS its cluster (everything else dims);
  // clicking the background clears it (user call 2026-07-25, all maps).
  const [focusClusterId, setFocusClusterId] = useState<number | null>(null)

  const queryClient = useQueryClient()
  const query = useQuery(
    frontierQueryOptions(queryClient, lensId ?? '', showSeen, showEdges),
  )
  const data = query.data?.payload
  const building = query.data?.build
  const nodes = useMemo(() => data?.nodes ?? [], [data])
  const edges = useMemo(() => data?.edges ?? [], [data])
  const counts = data?.counts
  const branchColors = useBranchColors(nodes)
  const branchControls = useBranchControls(lens)
  const nodesById = useMemo(() => new Map(nodes.map((n) => [n.paper_id, n])), [nodes])
  const visibleMapIds = useMemo(() => new Set(nodes.map((node) => node.paper_id)), [nodes])
  // Selection can never outlive the exact layer/filter payload that drew it.
  const region = useRegionSelection({ visibleIds: visibleMapIds })

  // Corpus cluster hues come from the SPACE (`cluster_hues`, ranked over the
  // whole substrate), never from this deck: Discovery renders a different
  // subset of the same corpus layout than the Map page does, and ranking
  // locally gave one cluster a different colour on each surface.
  const clusterHues = useMemo(() => {
    const spaceHue = new Map<number, number>()
    for (const [id, index] of Object.entries(data?.cluster_hues ?? {})) {
      spaceHue.set(Number(id), Number(index))
    }
    const counted = new Map<number, number>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      counted.set(n.cluster_id, (counted.get(n.cluster_id) ?? 0) + 1)
    }
    const ordered = [...counted.entries()].sort((a, b) => b[1] - a[1])
    return new Map(ordered.map(([id], i) => [id, branchMapColor(spaceHue.get(id) ?? i)]))
  }, [nodes, data])

  // Recs that were NOT in the previous payload — the visible end of the
  // adopt-a-direction loop. Tracked across refetches for this session only;
  // an empty previous set (first load) marks nothing, so a cold open is calm.
  const prevRecIds = useRef<Set<string> | null>(null)
  const [newRecIds, setNewRecIds] = useState<Set<string>>(new Set())
  useEffect(() => {
    if (data?.status !== 'ready') return
    const current = new Set(nodes.filter((n) => n.layer === 'rec').map((n) => n.paper_id))
    const previous = prevRecIds.current
    if (previous && previous.size > 0) {
      const fresh = new Set([...current].filter((id) => !previous.has(id)))
      if (fresh.size > 0) setNewRecIds(fresh)
    }
    prevRecIds.current = current
  }, [nodes, data?.status])

  // FrontierNode → the surface's vocabulary-free shape. The ONLY translation.
  const surfaceNodes = useMemo<MapSurfaceNode[]>(
    () =>
      nodes.map((n) => ({
        id: n.paper_id,
        x: n.x,
        y: n.y,
        kind: n.layer === 'library' ? 'library' : n.layer === 'rec' ? 'suggestion' : 'seen',
        groupId: typeof n.cluster_id === 'number' ? n.cluster_id : null,
        groupColor:
          typeof n.cluster_id === 'number' ? clusterHues.get(n.cluster_id) : undefined,
        groupLabel: n.cluster_label ?? undefined,
        // Score is the SIZE channel here, which is why this surface does not
        // also offer it as a colour mode.
        sizeValue: n.layer === 'rec' ? (n.score ?? null) : null,
        year: typeof n.year === 'number' ? n.year : null,
        score: typeof n.score === 'number' ? n.score : null,
        name: n.title ?? undefined,
        halo: newRecIds.has(n.paper_id),
      })),
    [nodes, clusterHues, newRecIds],
  )

  const surfaceEdges = useMemo(
    () =>
      edges.map((e) => ({
        source: e.source,
        target: e.target,
        weight: e.weight,
        type: e.edge_type,
      })),
    [edges],
  )

  // Area score per cluster — mean suggestion score, for the hover card.
  const clusterAreaScores = useMemo(() => {
    const acc = new Map<number, { sum: number; n: number }>()
    for (const n of nodes) {
      if (n.layer !== 'rec' || typeof n.score !== 'number') continue
      const cid = typeof n.cluster_id === 'number' ? n.cluster_id : -1
      if (cid < 0) continue
      const row = acc.get(cid)
      if (row) {
        row.sum += n.score
        row.n += 1
      } else acc.set(cid, { sum: n.score, n: 1 })
    }
    return new Map([...acc.entries()].map(([cid, { sum, n }]) => [cid, sum / n]))
  }, [nodes])

  // 47-H: ONE grouping at a time. Branch colouring is the frontier's default
  // (the recs are its hero layer); corpus clusters are the alternative lens on
  // the same points. Never both — two colourings on one scatter is a lie about
  // which structure you're looking at. Branches is genuinely host meaning: a
  // lens branch is not a corpus cluster, so the surface cannot derive it.
  const groupings = useMemo<MapGrouping[]>(() => {
    const byId = new Map(nodes.map((n) => [n.paper_id, n]))
    return [
      {
        id: 'branches',
        label: 'Branches',
        title: 'Colour suggestions by the lens branch that found them',
        // Toponyms name CLUSTERS, so they would be lying under this grouping.
        hideToponyms: true,
        colorFor: (node) => {
          const n = byId.get(node.id)
          if (!n || n.layer !== 'rec') return undefined
          return n.branch_id ? branchColors.get(n.branch_id)?.color : branchMapColor(0)
        },
        dimmed: (node) => {
          if (highlightBranch == null) return false
          const n = byId.get(node.id)
          return n?.layer === 'rec' && n.branch_id !== highlightBranch
        },
        chips:
          branchColors.size > 0 ? (
            // Branch chips — highlight on click, and (when the lens is
            // available) steer the branch inline. Boost/mute here write the
            // SAME branch_controls Branch Studio writes, through the shared
            // hook: one state, two views.
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {[...branchColors.entries()].map(([id, b]) => {
                const state = lens ? branchControls.stateOf(id) : 'normal'
                return (
                  <span
                    key={id}
                    className={cn(
                      'inline-flex items-center gap-1 rounded-full border transition-colors',
                      highlightBranch === id
                        ? 'border-transparent text-white'
                        : 'border-control-edge bg-control-quiet text-slate-600',
                      state === 'muted' && 'opacity-50',
                    )}
                    style={highlightBranch === id ? { background: b.color } : undefined}
                  >
                    <button
                      type="button"
                      onClick={() => setHighlightBranch((h) => (h === id ? null : id))}
                      className="inline-flex items-center gap-1 rounded-full py-0.5 pl-1.5 hover:opacity-80"
                      title={`Highlight the "${b.label}" branch`}
                    >
                      <span
                        className="inline-block h-2 w-2 rounded-full"
                        style={{ background: b.color }}
                      />
                      {b.label} · {b.count}
                    </button>
                    {lens && (
                      <span className="flex items-center pr-1">
                        <button
                          type="button"
                          onClick={() => branchControls.cycleBranchState(id, 'boosted')}
                          disabled={branchControls.isPending}
                          className={cn(
                            'rounded-full p-0.5 transition-colors hover:text-alma-folio',
                            state === 'boosted'
                              ? 'text-alma-folio'
                              : 'opacity-50 hover:opacity-100',
                          )}
                          title={state === 'boosted' ? 'Remove boost' : 'Boost this branch'}
                        >
                          <ChevronUp className="h-3 w-3" />
                        </button>
                        <button
                          type="button"
                          onClick={() => branchControls.cycleBranchState(id, 'muted')}
                          disabled={branchControls.isPending}
                          className={cn(
                            'rounded-full p-0.5 transition-colors hover:text-warning-700',
                            state === 'muted'
                              ? 'text-warning-700'
                              : 'opacity-50 hover:opacity-100',
                          )}
                          title={state === 'muted' ? 'Unmute this branch' : 'Mute this branch'}
                        >
                          <ChevronDown className="h-3 w-3" />
                        </button>
                      </span>
                    )}
                  </span>
                )
              })}
            </div>
          ) : undefined,
      },
      {
        id: 'clusters',
        label: 'Clusters',
        title: 'Colour every paper by its corpus cluster',
        colorFor: (node) => node.groupColor,
      },
    ]
  }, [nodes, branchColors, highlightBranch, lens, branchControls])

  const cancelRegion = () => region.clear()
  const adoptRegion = () => {
    if (!region.ids || !region.description?.sufficient) return
    onAdoptDirection?.({
      label: region.description.label,
      terms: region.description.top_terms,
      member_paper_ids: region.ids,
    })
    region.clear()
    setSelectMode(false)
  }

  if (!lensId) {
    return (
      <div className="flex h-[420px] items-center justify-center rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        Select a lens to plot its frontier.
      </div>
    )
  }
  if (query.isError && !data) {
    return (
      <ErrorState
        title="The Suggestions Map could not be loaded"
        message="Its saved layout is still intact. Retry the map request."
        actionLabel="Try again"
        onAction={() => void query.refetch()}
        actionPending={query.isFetching}
        className="min-h-[420px]"
      />
    )
  }
  if (!data) {
    return (
      <MapSurfaceLoading
        building={Boolean(building)}
        message="Building the semantic layout — this runs once, then it’s cached…"
      />
    )
  }

  return (
    <MapSurface
      stateKey="frontier"
      fieldKind="paper"
      nodes={surfaceNodes}
      edges={surfaceEdges}
      // The frontier is a pure read of the durable substrate — no knob here
      // re-fits it — so this host is permanently in the substrate frame and
      // gets the whole space-owned field, off-view papers included. Toggling
      // "show seen" therefore never changes the terrain.
      frame="substrate"
      fallbackIsSubstrate
      groupings={groupings}
      defaultColourMode="branches"
      // Score is already the dot-size channel here; colouring by it too would
      // say one thing twice.
      offerScoreMode={false}
      linksLabel="Citation links"
      showEdges={showEdges}
      onShowEdgesChange={setShowEdges}
      // The frontier's find-by-title lives in the list below the map.
      searchable={false}
      tuningInToolbar
      height={520}
      building={Boolean(building)}
      refreshing={query.isFetching}
      focusGroupId={focusClusterId}
      lassoMode={selectMode}
      onLasso={(ids, anchor) => region.select(ids, anchor)}
      onBackgroundClick={() => setFocusClusterId(null)}
      onOpenNode={(id) => {
        const n = nodesById.get(id)
        if (!n) return
        // The popup is owned by SemanticMap. The ONLY click side effect is
        // cluster focus. Navigating to a recommendation's list row requires the
        // explicit "Go to paper" action inside its popup.
        setFocusClusterId(
          typeof n.cluster_id === 'number' && n.cluster_id >= 0 ? n.cluster_id : null,
        )
      }}
      toolbarExtras={
        <>
          <MapToggle
            active={showSeen}
            onClick={() => setShowSeen((s) => !s)}
            title="Show the top papers you've seen but not acted on — the frontier"
          >
            {showSeen ? <Eye className="h-3.5 w-3.5" /> : <EyeOff className="h-3.5 w-3.5" />}
            Show everything I’ve seen
          </MapToggle>
          {onAdoptDirection && (
            <MapToggle
              active={selectMode}
              onClick={() => {
                setSelectMode((s) => !s)
                cancelRegion()
              }}
              title="Drag a box around a cluster of papers to name it and explore that direction"
            >
              <LassoSelect className="h-3.5 w-3.5" />
              Select a direction
            </MapToggle>
          )}
        </>
      }
      legendCounts={
        <>
          <span className="inline-flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ background: MAP_INK.library }}
            />
            Library {counts ? `(${counts.library})` : ''} — filled
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full border-2 bg-transparent"
              style={{ borderColor: branchMapColor(0) }}
            />
            Suggestions {counts ? `(${counts.recs})` : ''} — hollow
          </span>
        </>
      }
      legendExtras={
        <>
          {showSeen && (
            <span className="inline-flex items-center gap-1.5 text-slate-400">
              <span
                className="inline-block h-1.5 w-1.5 rounded-full"
                style={{ background: MAP_INK.ambientSoft }}
              />
              {counts
                ? `showing ${counts.seen_shown} nearest of ${counts.seen_total} seen` +
                  (data?.seen_ranked_by === 'lens' ? ' (nearest to this lens)' : '')
                : 'seen'}
            </span>
          )}
          {counts && counts.recs_unplaced > 0 && (
            <span className="text-slate-400">
              · {counts.recs_unplaced} suggestion
              {counts.recs_unplaced === 1 ? '' : 's'} not on the map (no abstract yet)
            </span>
          )}
        </>
      }
      hoverCard={(id) => {
        const n = nodesById.get(id)
        if (!n) return null
        const area =
          typeof n.cluster_id === 'number' && n.cluster_id >= 0
            ? clusterAreaScores.get(n.cluster_id)
            : undefined
        const hasScore = typeof n.score === 'number' && n.layer === 'rec'
        return (
          <>
            <p className="line-clamp-2 font-medium text-alma-800">{n.title || n.paper_id}</p>
            <p className="mt-0.5 text-slate-500">
              {n.layer === 'library' ? 'In your library' : n.layer === 'rec' ? 'Suggestion' : 'Seen'}
              {n.year ? ` · ${n.year}` : ''}
            </p>
            {(hasScore || area != null) && (
              <p className="mt-0.5 font-medium text-alma-800">
                {hasScore ? `Score ${Math.round(n.score as number)}/100` : ''}
                {hasScore && area != null ? ' · ' : ''}
                {area != null ? `area ${Math.round(area)}/100` : ''}
              </p>
            )}
            {n.branch_label && <p className="mt-0.5 text-slate-500">branch: {n.branch_label}</p>}
            {n.cluster_label && n.cluster_label !== 'Unclustered' && (
              <p className="mt-0.5 text-slate-400">cluster: {n.cluster_label}</p>
            )}
          </>
        )
      }}
      renderClickCard={(id, close) => {
        const n = nodesById.get(id)
        if (!n) return null
        const neighbours = frontierNeighbours(n, nodes, edges)
        if (n.layer === 'rec') return renderRecommendationPopup(n, close, neighbours)
        return (
          <CorpusMapPaperPopup
            paperId={n.paper_id}
            onClose={close}
            onOpenDetails={() => {
              close()
              onSelectPaper(n.paper_id)
            }}
            fallback={{
              id: n.paper_id,
              title: n.title || n.paper_id,
              year: n.year,
              score: n.score,
              statusLabel: n.layer === 'library' ? 'In your library' : 'Seen',
              branchLabel: n.branch_label,
              clusterLabel: n.cluster_label,
              neighbours,
            }}
          />
        )
      }}
      plateOverlay={
        // Region popover — the describe payload + adopt action. Meaning
        // (label + terms + counts) is shown before the action, per 47 §8.
        region.ids ? (
          <MapRegionCard
            kind="Direction"
            icon={<Sparkles className="h-3.5 w-3.5 text-alma-folio" />}
            count={region.ids.length}
            pending={region.describing || !region.description}
            insufficient={region.description?.sufficient === false}
            insufficientMessage="Too few papers to characterize — select a larger cluster (5+)."
            onClose={cancelRegion}
            actions={
              <CreateSelectionLensButton
                ids={region.ids}
                scope="corpus"
                selectionKind="papers"
                name={`${region.description?.label ?? 'Discovery direction'} · map selection`}
                onCreated={() => {
                  cancelRegion()
                  setSelectMode(false)
                }}
              />
            }
          >
            {region.description && (
              <>
                <p className="text-sm font-semibold capitalize text-alma-800">
                  {region.description.label}
                </p>
                {region.description.top_terms.length > 0 && (
                  <div className="mt-1.5 flex flex-wrap gap-1">
                    {region.description.top_terms.slice(0, 6).map((t) => (
                      <StatusBadge key={t} tone="neutral" size="sm">
                        {t}
                      </StatusBadge>
                    ))}
                  </div>
                )}
                <p className="mt-2 text-[11px] text-slate-500">
                  {region.description.counts.library} in library ·{' '}
                  {region.description.counts.recs} suggestions ·{' '}
                  {region.description.counts.seen} seen here
                </p>
                {region.description.sample.length > 0 && (
                  <ul className="mt-1.5 space-y-0.5">
                    {region.description.sample.map((s, i) => (
                      <li key={i} className="line-clamp-1 text-[11px] text-slate-400">
                        · {s}
                      </li>
                    ))}
                  </ul>
                )}
                <div className="mt-3 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={adoptRegion}
                    className="inline-flex flex-1 items-center justify-center gap-1.5 rounded-sm bg-alma-800 px-2.5 py-1.5 text-xs font-medium text-alma-cream hover:bg-alma-900"
                  >
                    <Sparkles className="h-3.5 w-3.5" />
                    Explore this direction
                  </button>
                  {onFilterList && (
                    <button
                      type="button"
                      onClick={() => {
                        if (region.ids) onFilterList(region.ids)
                        cancelRegion()
                        setSelectMode(false)
                      }}
                      className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1.5 text-xs text-slate-600 hover:bg-control-quiet"
                      title="Show only these papers in the list below"
                    >
                      Filter list
                    </button>
                  )}
                  <button
                    type="button"
                    onClick={cancelRegion}
                    className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1.5 text-xs text-slate-600 hover:bg-control-quiet"
                  >
                    Cancel
                  </button>
                </div>
              </>
            )}
          </MapRegionCard>
        ) : undefined
      }
    />
  )
}
