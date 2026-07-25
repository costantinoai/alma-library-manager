/**
 * FrontierMap — the Discovery host of the shared `<SemanticMap>` primitive
 * (task 50 M2; layers/decisions from task 47 P3/P8).
 *
 * This file owns Discovery-specific meaning ONLY: the frontier query, layer
 * toggles, the branches/clusters grouping switch (47-H: one grouping at a
 * time), branch legend chips that steer `branch_controls` through the shared
 * hook, and the lasso → describe → adopt-a-Direction loop. All rendering,
 * hit-testing, zoom/pan, toponym placement, and node semantics live in
 * `components/map/SemanticMap` — the same instrument every other map host
 * uses, so visuals and knobs cannot fork per surface (50-E/50-F).
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronUp,
  Eye,
  EyeOff,
  LassoSelect,
  Loader2,
  Share2,
  Sparkles,
  Type,
  X,
} from 'lucide-react'

import { describeRegion, getFrontier, type FrontierNode, type Lens, type RegionDescription } from '@/api/client'
import { useBranchControls } from '@/hooks/useBranchControls'
import { SemanticMap, type SemanticMapNode } from '@/components/map/SemanticMap'
import { EDGE_LAYER_COLORS, EDGE_LAYER_FALLBACK_COLOR, MAP_INK, yearRampColor, yearRampLimits } from '@/components/map/mapNodeStyle'
import { StatusBadge } from '@/components/ui/status-badge'
import { branchMapColor } from '@/lib/palette'
import { cn } from '@/lib/utils'

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
  /** Clicking a SUGGESTION dot jumps to its row in the list below (select +
   *  transient pulse) instead of opening the popup — the map navigates the
   *  deck. Library/seen dots (no list row) still open the paper panel. */
  onSelectRec?: (paperId: string) => void
}

/** A pending region selection: the ids under the lasso + its describe payload. */
interface RegionSelection {
  ids: string[]
  anchor: { x: number; y: number }
  description?: RegionDescription
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

export function FrontierMap({ lensId, lens, onSelectPaper, onAdoptDirection, onFilterList, onSelectRec }: FrontierMapProps) {
  const [showSeen, setShowSeen] = useState(false)
  const [showEdges, setShowEdges] = useState(false)
  // Words on/off is the user's call, not the grouping's side effect.
  const [showNames, setShowNames] = useState(true)
  const [highlightBranch, setHighlightBranch] = useState<string | null>(null)
  // 47-H: ONE grouping at a time. Branch colouring is the frontier's default
  // (the recs are its hero layer); corpus clusters are the alternative lens on
  // the same points. Never both — two colourings on one scatter is a lie about
  // which structure you're looking at.
  const [groupBy, setGroupBy] = useState<'branches' | 'clusters' | 'year' | 'heat'>('branches')
  // Legend chips as toggles: a dimmed cluster recedes (never disappears —
  // the territory stays honest), so you can mute the mega-cluster and read
  // the rest. Reset on grouping switch.
  const [dimmedClusters, setDimmedClusters] = useState<Set<number>>(new Set())
  const [hiddenEdgeTypes, setHiddenEdgeTypes] = useState<Set<string>>(new Set())
  const [selectMode, setSelectMode] = useState(false)
  const [region, setRegion] = useState<RegionSelection | null>(null)

  const query = useQuery({
    queryKey: ['frontier', lensId, showSeen, showEdges],
    queryFn: () => getFrontier(lensId as string, showSeen ? 300 : 0, showEdges),
    enabled: !!lensId,
    // Poll while the corpus layout is still building.
    refetchInterval: (q) => (q.state.data?.status === 'building' ? 2500 : false),
    staleTime: 30_000,
  })

  const nodes = useMemo(() => query.data?.nodes ?? [], [query.data])
  const edges = useMemo(() => query.data?.edges ?? [], [query.data])
  const counts = query.data?.counts
  const branchColors = useBranchColors(nodes)
  const branchControls = useBranchControls(lens)
  const nodesById = useMemo(() => new Map(nodes.map((n) => [n.paper_id, n])), [nodes])

  // Corpus clusters present on the map, largest first, each with a stable hue.
  // Unclustered (-1) is deliberately excluded from the legend: it is the
  // absence of a group, not a group.
  const clusterColors = useMemo(() => {
    const tally = new Map<number, { label: string; count: number }>()
    for (const n of nodes) {
      if (typeof n.cluster_id !== 'number' || n.cluster_id < 0) continue
      const row = tally.get(n.cluster_id)
      if (row) row.count += 1
      else tally.set(n.cluster_id, { label: n.cluster_label || `Cluster ${n.cluster_id}`, count: 1 })
    }
    const ordered = [...tally.entries()].sort((a, b) => b[1].count - a[1].count)
    return new Map(
      ordered.map(([id, v], i) => [id, { ...v, color: branchMapColor(i), index: i }]),
    )
  }, [nodes])

  // Recs that were NOT in the previous payload — the visible end of the
  // adopt-a-direction loop. Tracked across refetches for this session only;
  // an empty previous set (first load) marks nothing, so a cold open is calm.
  const prevRecIds = useRef<Set<string> | null>(null)
  const [newRecIds, setNewRecIds] = useState<Set<string>>(new Set())
  useEffect(() => {
    if (query.data?.status !== 'ready') return
    const current = new Set(nodes.filter((n) => n.layer === 'rec').map((n) => n.paper_id))
    const previous = prevRecIds.current
    if (previous && previous.size > 0) {
      const fresh = new Set([...current].filter((id) => !previous.has(id)))
      if (fresh.size > 0) setNewRecIds(fresh)
    }
    prevRecIds.current = current
  }, [nodes, query.data?.status])

  const yearRange = useMemo(
    () => yearRampLimits(nodes.map((n) => Number(n.year))),
    [nodes],
  )

  // Heat valence (50-J): what carries signal HERE — a strong suggestion and
  // your library are positive mass, weak suggestions negative, seen papers
  // neutral. View-only wash under the dots; never a discovery input.
  const heatValues = useMemo(() => {
    if (groupBy !== 'heat') return undefined
    const m = new Map<string, number>()
    for (const n of nodes) {
      if (n.layer === 'rec')
        m.set(n.paper_id, Math.max(-1, Math.min(1, ((n.score ?? 50) - 50) / 50)))
      else if (n.layer === 'library') m.set(n.paper_id, 0.35)
      else m.set(n.paper_id, 0)
    }
    return m
  }, [groupBy, nodes])

  // ── FrontierNode → SemanticMapNode: meaning mapping only (50-E) ──────────
  const mapNodes = useMemo<SemanticMapNode[]>(() => {
    return nodes.map((n): SemanticMapNode => {
      const kind = n.layer === 'library' ? 'library' : n.layer === 'rec' ? 'suggestion' : 'seen'
      // Colour follows whichever grouping is active — never both (47-H).
      let color: string | undefined
      if (groupBy === 'clusters') {
        color =
          typeof n.cluster_id === 'number' ? clusterColors.get(n.cluster_id)?.color : undefined
      } else if (groupBy === 'year') {
        color =
          yearRange && typeof n.year === 'number' && n.year > 1800
            ? yearRampColor(n.year, yearRange.lo, yearRange.hi)
            : MAP_INK.ambientSoft
      } else if (groupBy === 'branches' && n.layer === 'rec') {
        color = n.branch_id ? branchColors.get(n.branch_id)?.color : branchMapColor(0)
      }
      return {
        id: n.paper_id,
        x: n.x,
        // Flip Y so higher-y sits at the top, like a chart (legacy convention).
        y: 1 - n.y,
        kind,
        color,
        sizeValue: n.layer === 'rec' ? (n.score ?? null) : null,
        clusterId: n.cluster_id ?? undefined,
        clusterLabel: n.cluster_label ?? undefined,
        dimmed:
          (groupBy === 'branches' &&
            highlightBranch != null &&
            n.layer === 'rec' &&
            n.branch_id !== highlightBranch) ||
          (groupBy === 'clusters' &&
            typeof n.cluster_id === 'number' &&
            dimmedClusters.has(n.cluster_id)),
        halo: newRecIds.has(n.paper_id),
      }
    })
  }, [nodes, groupBy, clusterColors, branchColors, highlightBranch, newRecIds, dimmedClusters, yearRange])

  const mapEdges = useMemo(
    () =>
      edges
        .filter((e) => !hiddenEdgeTypes.has(e.edge_type))
        .map((e) => ({
        source: e.source,
        target: e.target,
        color: EDGE_LAYER_COLORS[e.edge_type] ?? EDGE_LAYER_FALLBACK_COLOR,
      })),
    [edges, hiddenEdgeTypes],
  )

  const describeMutation = useMutation({
    mutationFn: (ids: string[]) => describeRegion(ids),
    onSuccess: (desc) => setRegion((r) => (r ? { ...r, description: desc } : r)),
  })

  const cancelRegion = () => setRegion(null)
  const adoptRegion = () => {
    if (!region?.description || !region.description.sufficient) return
    onAdoptDirection?.({
      label: region.description.label,
      terms: region.description.top_terms,
      member_paper_ids: region.ids,
    })
    setRegion(null)
    setSelectMode(false)
  }

  if (!lensId) {
    return (
      <div className="flex h-[420px] items-center justify-center rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        Select a lens to plot its frontier.
      </div>
    )
  }
  if (query.isLoading || query.data?.status === 'building') {
    return (
      <div className="flex h-[420px] flex-col items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-1 text-sm text-slate-500">
        <Loader2 className="h-5 w-5 animate-spin text-alma-folio" />
        {query.data?.status === 'building'
          ? 'Building the semantic layout — this runs once, then it’s cached…'
          : 'Loading the map…'}
      </div>
    )
  }

  return (
    <div className="overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1">
      {/* Toolbar — a real bar above the plate, never buttons floating on it
          (user call 2026-07-25; also the 50-I controls contract). */}
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-border)] bg-surface-2 px-3 py-2">
        <button
          type="button"
          onClick={() => setShowSeen((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showSeen
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title="Show the top papers you've seen but not acted on — the frontier"
        >
          {showSeen ? <Eye className="h-3.5 w-3.5" /> : <EyeOff className="h-3.5 w-3.5" />}
          Show everything I’ve seen
        </button>
        <button
          type="button"
          onClick={() => setShowEdges((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showEdges
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title="Draw citation links (shared references + cited-together) between the papers on the map"
        >
          <Share2 className="h-3.5 w-3.5" />
          Citation links{showEdges && counts?.edges ? ` · ${counts.edges}` : ''}
        </button>
        <button
          type="button"
          onClick={() => setShowNames((s) => !s)}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
            showNames && groupBy === 'clusters'
              ? 'border-accent-edge bg-accent-soft text-alma-folio'
              : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
          title={groupBy === 'clusters' ? 'Cluster names on the map' : 'Names show in the Clusters grouping'}
        >
          <Type className="h-3.5 w-3.5" />
          Names
        </button>
        {/* 47-H: one grouping at a time — this is a switch, not two toggles. */}
        {clusterColors.size > 0 && (
          <div className="inline-flex overflow-hidden rounded-sm border border-[var(--color-border)]">
            {(['branches', 'clusters', 'year', 'heat'] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => {
                  setGroupBy(mode)
                  setHighlightBranch(null)
                  setDimmedClusters(new Set())
                }}
                className={cn(
                  'px-2 py-1 text-xs font-medium transition-colors',
                  groupBy === mode
                    ? 'bg-accent-soft text-alma-folio'
                    : 'bg-control-well text-slate-600 hover:bg-control-quiet',
                )}
                title={
                  mode === 'branches'
                    ? 'Colour suggestions by the lens branch that found them'
                    : mode === 'clusters'
                      ? 'Colour every paper by its corpus cluster'
                      : mode === 'year'
                        ? 'Recency ramp — older fades, newer leads'
                        : 'Local signal wash — green where your saves and strong suggestions sit, red where weak ones do (view only)'
                }
              >
                {mode === 'branches' ? 'Branches' : mode === 'clusters' ? 'Clusters' : mode === 'year' ? 'Year' : 'Heat'}
              </button>
            ))}
          </div>
        )}
        {onAdoptDirection && (
          <button
            type="button"
            onClick={() => {
              setSelectMode((s) => !s)
              cancelRegion()
            }}
            className={cn(
              'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
              selectMode
                ? 'border-accent-edge bg-accent-soft text-alma-folio'
                : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
            )}
            title="Drag a box around a cluster of papers to name it and explore that direction"
          >
            <LassoSelect className="h-3.5 w-3.5" />
            Select a direction
          </button>
        )}
      </div>

      <SemanticMap
        nodes={mapNodes}
        edges={mapEdges}
        showEdges={showEdges}
        showToponyms={showNames && groupBy === 'clusters'}
        heatValues={heatValues}
        height={520}
        lassoMode={selectMode}
        onLasso={(ids, anchor) => {
          setRegion({ ids, anchor })
          describeMutation.mutate(ids.slice(0, 300))
        }}
        onClickNode={(id) => {
          const n = nodesById.get(id)
          if (n?.layer === 'rec' && onSelectRec) onSelectRec(id)
          else onSelectPaper(id)
        }}
        renderHover={(id) => {
          const n = nodesById.get(id)
          if (!n) return null
          return (
            <>
              <p className="line-clamp-2 font-medium text-alma-800">{n.title || n.paper_id}</p>
              <p className="mt-0.5 text-slate-500">
                {n.layer === 'library' ? 'In your library' : n.layer === 'rec' ? 'Suggestion' : 'Seen'}
                {n.year ? ` · ${n.year}` : ''}
              </p>
              {typeof n.score === 'number' && n.layer === 'rec' && (
                <p className="mt-0.5 font-medium text-alma-800">Score {Math.round(n.score)}/100</p>
              )}
              {n.branch_label && <p className="mt-0.5 text-slate-500">branch: {n.branch_label}</p>}
              {n.cluster_label && n.cluster_label !== 'Unclustered' && (
                <p className="mt-0.5 text-slate-400">cluster: {n.cluster_label}</p>
              )}
            </>
          )
        }}
        className="rounded-none border-0"
      >
        {/* Region popover — the describe payload + adopt action. Meaning
            (label + terms + counts) is shown before the action, per 47 §8. */}
        {region && (
          <div className="absolute right-3 top-3 z-20 w-72 rounded-sm border border-[var(--color-border)] bg-surface-2 p-3 shadow-paper-lg">
            <div className="mb-2 flex items-start justify-between gap-2">
              <div className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
                <Sparkles className="h-3.5 w-3.5 text-alma-folio" />
                Direction
              </div>
              <button
                type="button"
                onClick={cancelRegion}
                className="rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                aria-label="Cancel selection"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
            {describeMutation.isPending || !region.description ? (
              <div className="flex items-center gap-2 py-3 text-xs text-slate-500">
                <Loader2 className="h-4 w-4 animate-spin text-alma-folio" />
                Characterizing {region.ids.length} papers…
              </div>
            ) : !region.description.sufficient ? (
              <p className="py-2 text-xs text-slate-500">
                Too few papers to characterize — select a larger cluster (5+).
              </p>
            ) : (
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
                        onFilterList(region.ids)
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
          </div>
        )}

      </SemanticMap>

      {/* Legend — its own section BELOW the plate (user call 2026-07-25):
          ownership semantics from the registry (filled vs hollow), grouping
          chips per the active mode. Never an overlay hiding dots. */}
      <div className="border-t border-[var(--color-border)] bg-surface-2 px-3 py-2.5 text-xs">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-slate-600">
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
            {groupBy === 'year' && yearRange && (
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: yearRampColor(yearRange.lo, yearRange.lo, yearRange.hi) }} />
                {yearRange.lo}
                <span aria-hidden>→</span>
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: yearRampColor(yearRange.hi, yearRange.lo, yearRange.hi) }} />
                {yearRange.hi} (10th–90th pct)
              </span>
            )}
            {showSeen && (
              <span className="inline-flex items-center gap-1.5 text-slate-400">
                <span
                  className="inline-block h-1.5 w-1.5 rounded-full"
                  style={{ background: MAP_INK.ambientSoft }}
                />
                {counts
                  ? `showing ${counts.seen_shown} nearest of ${counts.seen_total} seen` +
                    (query.data?.seen_ranked_by === 'lens' ? ' (nearest to this lens)' : '')
                  : 'seen'}
              </span>
            )}
          </div>
          {showSeen && (
            <p className="mt-1 text-[11px] text-slate-400">
              Seen = surfaced in an EARLIER refresh and never acted on (not saved, not dismissed).
              They are not in the current deck — each new refresh builds a fresh one — but they are
              your unworked frontier: lasso a patch of them to explore it as a Direction.
            </p>
          )}
          {showEdges && edges.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {Object.entries(
                edges.reduce<Record<string, number>>((acc, e) => {
                  acc[e.edge_type] = (acc[e.edge_type] ?? 0) + 1
                  return acc
                }, {}),
              ).map(([type, count]) => {
                const off = hiddenEdgeTypes.has(type)
                return (
                  <button
                    key={type}
                    type="button"
                    aria-pressed={!off}
                    onClick={() =>
                      setHiddenEdgeTypes((prev) => {
                        const next = new Set(prev)
                        if (next.has(type)) next.delete(type)
                        else next.add(type)
                        return next
                      })
                    }
                    className={cn(
                      'inline-flex items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
                      off && 'opacity-40 line-through',
                    )}
                    title={off ? `Show ${type} links` : `Hide ${type} links`}
                  >
                    <span
                      className="inline-block h-1.5 w-3 rounded-full"
                      style={{ background: EDGE_LAYER_COLORS[type] ?? EDGE_LAYER_FALLBACK_COLOR }}
                    />
                    {type === 'bibliographic_coupling' ? 'Shared references' : type === 'co_citation' ? 'Cited together' : type} · {count}
                  </button>
                )
              })}
            </div>
          )}
          {/* Branch chips — highlight on click, and (when the lens is available)
              steer the branch inline. Boost/mute here write the SAME
              branch_controls Branch Studio writes, through the shared hook: one
              state, two views. */}
          {groupBy === 'branches' && branchColors.size > 0 && (
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
                            state === 'boosted' ? 'text-alma-folio' : 'opacity-50 hover:opacity-100',
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
                            state === 'muted' ? 'text-warning-700' : 'opacity-50 hover:opacity-100',
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
          )}

          {/* Cluster chips — TOGGLES: click to dim a cluster's dots (they
              recede, never vanish), click again to restore. Corpus clusters
              aren't steerable, so this is a reading aid, not a signal. */}
          {groupBy === 'clusters' && clusterColors.size > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
              {[...clusterColors.entries()].slice(0, 8).map(([id, c]) => {
                const dimmed = dimmedClusters.has(id)
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() =>
                      setDimmedClusters((prev) => {
                        const next = new Set(prev)
                        if (next.has(id)) next.delete(id)
                        else next.add(id)
                        return next
                      })
                    }
                    aria-pressed={!dimmed}
                    className={cn(
                      'inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
                      dimmed && 'opacity-40',
                    )}
                    title={
                      dimmed
                        ? `Show "${c.label}" (${c.count} papers)`
                        : `Dim "${c.label}" (${c.count} papers)`
                    }
                  >
                    <span
                      className="inline-block h-2 w-2 shrink-0 rounded-full"
                      style={{ background: c.color }}
                    />
                    <span className={cn('truncate', dimmed && 'line-through')}>{c.label}</span>
                    <span className="shrink-0 text-slate-400">· {c.count}</span>
                  </button>
                )
              })}
            </div>
          )}
          {counts && counts.recs_unplaced > 0 && (
            <p className="mt-1.5 text-[11px] text-slate-400">
              {counts.recs_unplaced} suggestion{counts.recs_unplaced === 1 ? '' : 's'} not on the map (no abstract yet)
            </p>
          )}
        </div>
    </div>
  )
}
