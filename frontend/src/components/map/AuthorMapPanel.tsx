/**
 * AuthorMapPanel — the author map, as a plate the Map page can host.
 *
 * It used to be a banded section on the Authors page. Authors and papers are
 * two views of ONE territory, so they belong on one page behind a switcher
 * (user call 2026-07-27); the Authors page is now people-management —
 * followed grid, suggestions rail, identity conflicts — and the map lives with
 * the other map.
 *
 * Self-contained on purpose: it owns its own map session state, its own region
 * selection and its own author-detail dialog, so the host page supplies
 * nothing. The identity lookups it shares with the Authors page come from
 * `useAuthorIdentity`, one hook over the same React Query caches, so the two
 * surfaces cannot disagree about who is followed or suggested.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { LassoSelect, UserPlus, X } from 'lucide-react'

import {
  followAuthor,
  unfollowAuthor,
  type Author,
  type AuthorSuggestion,
  type GraphData,
  type GraphNode,
} from '@/api/client'
import { AuthorDetailPanel } from '@/components/AuthorDetailPanel'
import {
  authorSuggestionReasons,
  authorSuggestionSourceLabel,
} from '@/components/authors/authorSuggestionEvidence'
import { CreateSelectionLensButton } from '@/components/map/CreateSelectionLensButton'
import { GraphMapView } from '@/components/map/GraphMapView'
import { MapAuthorPopup } from '@/components/map/MapAuthorPopup'
import { MapRegionCard } from '@/components/map/MapRegionCard'
import {
  MapDisplayTuningRows,
  MapModeSwitch,
  MapTuningPopover,
  SliderRow,
} from '@/components/map/MapChrome'
import type { MapNodeKind } from '@/components/map/mapNodeStyle'
import { AUTHOR_MAP_DEFAULTS, useMapSessionState } from '@/components/map/mapSessionState'
import { useAuthorField } from '@/components/map/useAuthorField'
import { selectionWithinVisible } from '@/components/map/useRegionSelection'
import { MetricTile } from '@/components/shared/MetricTile'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { SignalChip } from '@/components/shared/SignalChip'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { StatusBadge } from '@/components/ui/status-badge'
import { errorToast, useToast } from '@/hooks/useToast'
import { authorKey, useAuthorIdentity } from '@/hooks/useAuthorIdentity'
import { invalidateQueries } from '@/lib/queryHelpers'

interface AuthorClusterMeta {
  id: number
  label: string
  size: number
  description?: string
  word_cloud?: Array<{ term: string; weight: number }>
}

export function AuthorMapPanel() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const {
    authors,
    authorsByKey,
    followedKeys,
    suggestionForNode,
    suggestions,
    ownerId,
  } = useAuthorIdentity()

  // The author dialog is the panel's own: the Map page hosts no author UI.
  const [selectedAuthor, setSelectedAuthor] = useState<Author | null>(null)
  const [selectedSuggestion, setSelectedSuggestion] = useState<AuthorSuggestion | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)

  const openDetail = (author: Author) => {
    setSelectedSuggestion(null)
    setSelectedAuthor(author)
    setDetailOpen(true)
  }

  const openSuggestionDetail = (s: AuthorSuggestion) => {
    if (s.existing_author_id) {
      const existing = authors.find((a) => a.id === s.existing_author_id)
      if (existing) {
        openDetail(existing)
        return
      }
    }
    const synth: Author = {
      id: s.existing_author_id ?? s.openalex_id ?? s.key,
      name: s.name,
      openalex_id: s.openalex_id ?? undefined,
      author_type: 'background',
    }
    setSelectedSuggestion(s)
    setSelectedAuthor(synth)
    setDetailOpen(true)
  }

  // Network map scope: your library's authors, or the full tracked corpus
  // (which includes the authors of suggested papers).
  const [networkScope, setNetworkScope] = useMapSessionState<
    'library' | 'corpus'
  >('author-map', 'scope', AUTHOR_MAP_DEFAULTS.scope)
  const [networkResolution, setNetworkResolution] = useMapSessionState(
    'author-map',
    'resolution',
    AUTHOR_MAP_DEFAULTS.resolution,
  )
  const [networkSizeScale, setNetworkSizeScale] = useMapSessionState(
    'author-map',
    'sizeScale',
    AUTHOR_MAP_DEFAULTS.sizeScale,
  )
  const [networkDotOpacity, setNetworkDotOpacity] = useMapSessionState(
    'author-map',
    'dotOpacity',
    AUTHOR_MAP_DEFAULTS.dotOpacity,
  )
  const [networkTerrainOpacity, setNetworkTerrainOpacity] = useMapSessionState(
    'author-map',
    'terrainOpacity',
    AUTHOR_MAP_DEFAULTS.terrainOpacity,
  )
  const [networkWordScale, setNetworkWordScale] = useMapSessionState(
    'author-map',
    'wordScale',
    AUTHOR_MAP_DEFAULTS.wordScale,
  )
  const [networkWordCount, setNetworkWordCount] = useMapSessionState(
    'author-map',
    'wordCount',
    AUTHOR_MAP_DEFAULTS.wordCount,
  )
  const [networkSelectMode, setNetworkSelectMode] = useState(false)
  // Region ids only — the shared `/graphs/region/describe` characterisation is a
  // PAPER vocabulary pass, so the author map derives its own meaning from the
  // nodes it already holds instead of asking for a description of author ids.
  const [networkRegionIds, setNetworkRegionIds] = useState<string[] | null>(null)
  const [networkSelected, setNetworkSelected] = useState<GraphNode | null>(null)
  const [networkPayload, setNetworkPayload] = useState<GraphData | null>(null)
  const networkVisibleIds = useMemo(
    () => new Set((networkPayload?.nodes ?? []).map((node) => node.id)),
    [networkPayload],
  )
  useEffect(() => {
    setNetworkRegionIds((current) => {
      if (!current) return current
      const scoped = selectionWithinVisible(current, networkVisibleIds)
      return scoped.length === current.length ? current : scoped.length ? scoped : null
    })
  }, [networkVisibleIds])
  // Do not build the expensive author field in parallel with a missing map.
  // Once a payload exists it stays live for popup/drilldown scores, and the map
  // itself shares this exact React Query cache when Score/Terrain is active.
  const networkAuthorField = useAuthorField(
    networkPayload !== null || networkSelected !== null,
  )

  const networkFollowMutation = useMutation({
    mutationFn: ({
      author,
      isFollowed,
    }: {
      author: { id: string; name: string }
      isFollowed: boolean
    }) => {
      if (isFollowed) return unfollowAuthor(author.id)
      return followAuthor(author.id, true, author.name).then(() => undefined)
    },
    onSuccess: async (_result, { author, isFollowed }) => {
      await invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['author-detail', author.id],
      )
      toast({
        title: isFollowed ? 'Unfollowed' : 'Followed',
        description: isFollowed
          ? `${author.name} is no longer followed.`
          : `${author.name} will contribute to Feed on the next refresh.`,
      })
    },
    onError: (_error, { isFollowed }) =>
      errorToast(
        isFollowed ? 'Could not unfollow author' : 'Could not follow author',
        'Try again in a moment.',
      ),
  })


  /** Map membership tier — the SAME common space the paper maps draw: what is
   *  yours (filled), what is being offered (hollow), and the corpus context
   *  behind both (faint). Followed and library-co-author read as one "yours"
   *  tier because both are people you have already committed to; the dashed
   *  halo separates followed within it. */
  const networkNodeKind = useCallback(
    (n: GraphNode): MapNodeKind => {
      const key = authorKey(n.id)
      if (followedKeys.has(key) || n.in_library) return 'author_library'
      if (suggestionForNode(n)) return 'author_suggested'
      return 'author_corpus'
    },
    [followedKeys, suggestionForNode],
  )
  const networkNodeHalo = useCallback(
    (n: GraphNode) => followedKeys.has(authorKey(n.id)),
    [followedKeys],
  )

  /**
   * The lassoed author region. Meaning is derived from the nodes the plate
   * already holds — who they are, how many you already follow, which of them
   * the engine is offering, and the topics their cluster labels agree on.
   */
  const networkRegion = useMemo(() => {
    if (!networkRegionIds || !networkPayload) return null
    const ids = new Set(networkRegionIds)
    const members = networkPayload.nodes.filter((node) => ids.has(node.id))
    if (members.length === 0) return null
    const followed = members.filter((node) => followedKeys.has(authorKey(node.id)))
    const suggested = members.filter((node) => suggestionForNode(node) != null)
    const notFollowed = members.filter((node) => !followedKeys.has(authorKey(node.id)))
    const byPubs = [...members].sort(
      (a, b) =>
        (typeof b.metadata?.pub_count === 'number' ? b.metadata.pub_count : 0) -
        (typeof a.metadata?.pub_count === 'number' ? a.metadata.pub_count : 0),
    )
    const topicTally = new Map<string, number>()
    for (const node of members) {
      const label =
        typeof node.metadata?.cluster_label === 'string'
          ? node.metadata.cluster_label
          : ''
      if (label) topicTally.set(label, (topicTally.get(label) ?? 0) + 1)
    }
    const topics = [...topicTally.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4)
    return { members, followed, suggested, notFollowed, byPubs, topics }
  }, [networkRegionIds, networkPayload, followedKeys, suggestionForNode])

  /** Follow every member of the region you do not already follow. */
  const followRegionMutation = useMutation({
    mutationFn: async (nodes: GraphNode[]) => {
      let followedCount = 0
      const failures: string[] = []
      for (const node of nodes) {
        const local = authorsByKey.get(authorKey(node.id))
        const targetId = local?.id ?? authorKey(node.id)
        try {
          await followAuthor(targetId, true, node.name)
          followedCount += 1
        } catch {
          failures.push(node.name)
        }
      }
      return { followedCount, failures }
    },
    onSuccess: async ({ followedCount, failures }) => {
      await invalidateQueries(
        queryClient,
        ['authors'],
        ['library-followed-authors'],
        ['author-suggestions'],
      )
      // Report what actually happened, including the part that did not: a bulk
      // action that silently drops members is worse than one that refuses.
      if (failures.length) {
        errorToast(
          `Followed ${followedCount}, ${failures.length} failed`,
          failures.slice(0, 3).join(', ') + (failures.length > 3 ? '…' : ''),
        )
        return
      }
      toast({
        title: `Following ${followedCount} author${followedCount === 1 ? '' : 's'}`,
        description: 'They will contribute to Feed on the next refresh.',
      })
    },
    onError: () => errorToast('Could not follow this region', 'Try again in a moment.'),
  })

  const networkClusters = useMemo(
    () =>
      ((((networkPayload?.metadata ?? {}) as Record<string, unknown>).clusters ??
        []) as AuthorClusterMeta[]),
    [networkPayload],
  )
  const networkSelectedCluster = useMemo(
    () =>
      networkSelected && typeof networkSelected.cluster_id === 'number'
        ? networkClusters.find((cluster) => cluster.id === networkSelected.cluster_id) ?? null
        : null,
    [networkClusters, networkSelected],
  )
  const networkSelectedSuggestion = networkSelected
    ? suggestionForNode(networkSelected)
    : null
  const networkSelectedLocal = networkSelected
    ? authorsByKey.get(authorKey(networkSelected.id)) ?? null
    : null
  const networkSelectedField = networkSelected
    ? networkAuthorField.entriesById.get(authorKey(networkSelected.id))
    : undefined
  const networkSelectedScore =
    networkSelectedField?.score ??
    (typeof networkSelected?.metadata?.score === 'number'
      ? networkSelected.metadata.score
      : null)
  const networkSelectedFollowTargetId = networkSelected
    ? networkSelectedLocal?.id ?? authorKey(networkSelected.id)
    : ''
  const networkSelectedIsFollowed = networkSelected
    ? followedKeys.has(authorKey(networkSelected.id))
    : false
  const networkSelectedInterests = networkSelected
    ? networkSelectedLocal?.interests ??
      (Array.isArray(networkSelected.metadata?.interests)
        ? networkSelected.metadata.interests.filter(
            (value): value is string => typeof value === 'string',
          )
        : [])
    : []

  // Author maps deliberately draw no links: semantic proximity already is the
  // relationship. The drilldown names the nearest people rather than reviving
  // a hairball of co-author lines.
  const networkNearbyAuthors = useMemo(() => {
    if (!networkSelected || !networkPayload) return []
    return networkPayload.nodes
      .filter((node) => node.id !== networkSelected.id)
      .map((node) => ({
        node,
        distance: Math.hypot(node.x - networkSelected.x, node.y - networkSelected.y),
      }))
      .sort((a, b) => a.distance - b.distance)
      .slice(0, 6)
  }, [networkPayload, networkSelected])
  const networkCommunityMembers = useMemo(() => {
    if (!networkSelected || !networkPayload || typeof networkSelected.cluster_id !== 'number') {
      return []
    }
    return networkPayload.nodes
      .filter(
        (node) =>
          node.cluster_id === networkSelected.cluster_id && node.id !== networkSelected.id,
      )
      .sort(
        (a, b) =>
          Number(b.metadata?.pub_count ?? 0) - Number(a.metadata?.pub_count ?? 0),
      )
      .slice(0, 8)
  }, [networkPayload, networkSelected])
  const networkCommunitySuggestionCount = useMemo(
    () =>
      networkSelected && networkPayload && typeof networkSelected.cluster_id === 'number'
        ? networkPayload.nodes.filter(
            (node) =>
              node.cluster_id === networkSelected.cluster_id && suggestionForNode(node) != null,
          ).length
        : 0,
    [networkPayload, networkSelected, suggestionForNode],
  )

  // Suggestions that have no dot on the plate. Reported as ONE honest line, not
  // as a second card grid — the rail below already renders every suggestion, so
  // duplicating them inside the map card said the same thing twice.
  //
  // Only meaningful once a payload has ARRIVED: while the corpus view is still
  // building, `networkPayload` is null, nothing is "placed", and the count
  // reads as "all 30 suggestions are unplaceable" — which is false. Measured
  // 2026-07-26: 23 of 30 had ≥2 tracked papers AND an embedded paper, i.e. they
  // were simply waiting on the layout rebuild.
  const unplacedSuggestions = useMemo(() => {
    if (networkScope !== 'corpus' || !networkPayload) return []
    const placed = new Set<string>()
    for (const node of networkPayload?.nodes ?? []) {
      placed.add(authorKey(node.id))
      if (typeof node.metadata?.openalex_id === 'string') {
        placed.add(authorKey(node.metadata.openalex_id))
      }
      const local = authorsByKey.get(authorKey(node.id))
      if (local) {
        placed.add(authorKey(local.id))
        placed.add(authorKey(local.openalex_id))
      }
    }
    return (suggestions).filter((suggestion) => {
      const identities = [
        suggestion.key,
        suggestion.openalex_id,
        suggestion.existing_author_id,
        ...(suggestion.alt_openalex_ids ?? []),
      ]
        .map(authorKey)
        .filter(Boolean)
      return !identities.some((identity) => placed.has(identity))
    })
  }, [authorsByKey, networkPayload, networkScope, suggestions])
  const placedSuggestionCount =
    networkScope === 'corpus'
      ? Math.max(0, (suggestions.length) - unplacedSuggestions.length)
      : 0

  // WHY each one is off the plate — the two reasons are not the same promise.
  // The map needs an author with ≥2 in-scope papers (`_MIN_AUTHOR_PUBS`) AND a
  // vector. Someone sitting on one paper does NOT arrive "after embedding": they
  // need a SECOND tracked paper first, which may never come. Saying "they join
  // automatically after enrichment" to all of them was a promise we can't keep.
  const unplacedNeedingPapers = useMemo(
    () => unplacedSuggestions.filter((s) => (s.local_paper_count ?? 0) < 2).length,
    [unplacedSuggestions],
  )
  const unplacedAwaitingLayout = unplacedSuggestions.length - unplacedNeedingPapers

  const openNetworkNodeDetail = (node: GraphNode) => {
    const local = authorsByKey.get(authorKey(node.id))
    const suggestion = suggestionForNode(node)
    if (suggestion && !local) {
      openSuggestionDetail(suggestion)
      return
    }
    openDetail(
      local ?? {
        id: authorKey(node.id),
        name: node.name,
        openalex_id:
          typeof node.metadata?.openalex_id === 'string'
            ? node.metadata.openalex_id
            : undefined,
        author_type: followedKeys.has(authorKey(node.id)) ? 'followed' : 'background',
      },
    )
  }

  return (
    <div className="space-y-4">
      <ConceptCallout
        eyebrow="How to read this map"
        summary="Every author in scope on one plate — filled dots are yours, gold outlines are current suggestions, faint dots are context."
      >
        <p>
          Each dot is one author, placed by what they write about, so authors working on similar
          things sit together and the map shows the research communities behind your corpus.
        </p>
        <p className="mt-2">
          <strong>Who is who</strong> reads the same way as the paper maps — one common space,
          three tiers. A <strong>filled dot</strong> is yours: an author you follow, or a
          co-author of a paper you saved (a <strong>dashed ring</strong> marks the followed ones
          specifically). A <strong>gold outline</strong> marks a current author suggestion — the
          same suggestions as the rail below, shown where they actually sit relative to your
          people. Everyone else is <strong>faint</strong>: context, not a claim.
        </p>
        <p className="mt-2">
          <strong>Colour modes:</strong> Clusters shows the communities;{' '}
          <strong>Score and the Terrain overlay use the engine&apos;s internal criteria</strong>.
          Score is the mean of an author&apos;s papers&apos; latest relevance scores (0–100, green
          strong / red weak) — the same scoring Discovery uses. Terrain is the mean{' '}
          <em>signal</em> of the papers of theirs you have an opinion on (saved, rated, dismissed,
          or scored), so a green region is a community your own behaviour keeps endorsing. Authors
          you have no signal on leave the paper bare rather than washing the terrain flat.
          Terrain composes with any colouring.
        </p>
        <p className="mt-2">
          <strong>Scope:</strong> Library shows only authors of papers you saved; Corpus widens
          to every tracked paper, including the authors behind current suggestions — useful for
          spotting who anchors an area you haven&apos;t saved into yet.
        </p>
      </ConceptCallout>
      <GraphMapView
        endpoint="author-network"
        params={{
          scope: networkScope,
          cluster_resolution: networkResolution.toFixed(1),
        }}
        // Year is meaningless for an author; Score/Heat reflect the mean
        // internal score of the author's papers (same criteria as
        // Discovery) — user call 2026-07-25.
        toolbarExtras={
          <>
            <MapModeSwitch
              value={networkScope}
              onChange={(next) => {
                setNetworkScope(next)
                setNetworkSelected(null)
                setNetworkPayload(null)
                setNetworkRegionIds(null)
              }}
              options={[
                { value: 'library', label: 'Library', title: 'Authors of papers you saved' },
                { value: 'corpus', label: 'Corpus', title: 'Authors across every tracked paper — including suggestions' },
              ]}
            />
            <button
              type="button"
              onClick={() => {
                setNetworkSelectMode((on) => !on)
                setNetworkRegionIds(null)
              }}
              className={
                networkSelectMode
                  ? 'inline-flex items-center gap-1.5 rounded-sm border border-accent-edge bg-accent-soft px-2.5 py-1 text-xs font-medium text-alma-folio'
                  : 'inline-flex items-center gap-1.5 rounded-sm border border-control-edge bg-control-well px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-control-quiet'
              }
              title="Drag a box around a patch of authors — see who they are, how many you already follow, and follow the rest in one action"
            >
              <LassoSelect className="h-3.5 w-3.5" />
              Select region
            </button>
            <MapTuningPopover title="Fine tuning — terrain opacity, cluster detail, dot size, dot opacity, words">
              <SliderRow
                label="Cluster detail"
                value={networkResolution}
                min={0.5}
                max={3}
                step={0.1}
                format={(v) => `${v.toFixed(1)}×`}
                onCommit={(v) => setNetworkResolution(Number(v.toFixed(1)))}
              />
              <MapDisplayTuningRows
                sizeScale={networkSizeScale}
                onSizeScale={setNetworkSizeScale}
                dotOpacity={networkDotOpacity}
                onDotOpacity={setNetworkDotOpacity}
                terrainOpacity={networkTerrainOpacity}
                onTerrainOpacity={setNetworkTerrainOpacity}
                wordScale={networkWordScale}
                onWordScale={setNetworkWordScale}
                wordCount={networkWordCount}
                onWordCount={setNetworkWordCount}
              />
            </MapTuningPopover>
          </>
        }
        sizeScale={networkSizeScale}
        dotOpacity={networkDotOpacity}
        terrainOpacity={networkTerrainOpacity}
        toponymScale={networkWordScale}
        toponymWordCount={networkWordCount}
        onPayload={setNetworkPayload}
        selectedNodeId={networkSelected?.id ?? null}
        focusClusterId={
          networkSelected &&
          typeof networkSelected.cluster_id === 'number' &&
          networkSelected.cluster_id >= 0
            ? networkSelected.cluster_id
            : null
        }
        // Membership tiers — the shared common space (see networkNodeKind).
        nodeKind={networkNodeKind}
        nodeSuggestionOutline={(node) =>
          networkScope === 'corpus' && suggestionForNode(node) != null
        }
        // The author map draws no link layer: adjacency here already IS
        // collaboration (co-authorship is a layout input), so lines would
        // re-state position as topology and bury the dots.
        showLinks={false}
        // Popup-primary: selection + cluster focus are the secondary map
        // effect; full detail is an explicit action inside the card.
        onOpenNode={setNetworkSelected}
        onBackgroundClick={() => {
          setNetworkSelected(null)
          setNetworkRegionIds(null)
        }}
        lassoMode={networkSelectMode}
        onLasso={(ids) => {
          setNetworkSelected(null)
          setNetworkSelectMode(false)
          const scoped = selectionWithinVisible(ids, networkVisibleIds)
          setNetworkRegionIds(scoped.length ? scoped : null)
        }}
        plateOverlay={
          networkRegion && (
            <MapRegionCard
              kind="Area"
              icon={<LassoSelect className="h-3.5 w-3.5 text-alma-folio" />}
              count={networkRegion.members.length}
              onClose={() => setNetworkRegionIds(null)}
              actions={
                <CreateSelectionLensButton
                  ids={networkRegion.members.map((node) => node.id)}
                  scope={networkScope}
                  selectionKind="authors"
                  name={`${networkRegion.topics[0]?.[0] ?? 'Author area'} · map selection`}
                  onCreated={() => setNetworkRegionIds(null)}
                />
              }
            >
              <p className="text-sm font-semibold text-alma-800">
                {networkRegion.members.length} author
                {networkRegion.members.length === 1 ? '' : 's'}
              </p>
              <p className="mt-1.5 text-[11px] text-slate-500">
                {networkRegion.followed.length} already followed ·{' '}
                {networkRegion.suggested.length} suggested ·{' '}
                {networkRegion.notFollowed.length} new to you
              </p>
              {networkRegion.topics.length > 0 && (
                <div className="mt-1.5 flex flex-wrap gap-1">
                  {networkRegion.topics.map(([label]) => (
                    <StatusBadge key={label} tone="neutral" size="sm">
                      {label}
                    </StatusBadge>
                  ))}
                </div>
              )}
              {networkRegion.byPubs.length > 0 && (
                <ul className="mt-1.5 space-y-0.5">
                  {networkRegion.byPubs.slice(0, 3).map((node) => (
                    <li key={node.id} className="line-clamp-1 text-[11px] text-slate-400">
                      · {node.name}
                    </li>
                  ))}
                </ul>
              )}
              <div className="mt-3 flex items-center gap-2">
                <button
                  type="button"
                  disabled={
                    networkRegion.notFollowed.length === 0 ||
                    followRegionMutation.isPending
                  }
                  onClick={() => followRegionMutation.mutate(networkRegion.notFollowed)}
                  className="inline-flex flex-1 items-center justify-center gap-1.5 rounded-sm bg-alma-800 px-2.5 py-1.5 text-xs font-medium text-alma-cream hover:bg-alma-900 disabled:cursor-not-allowed disabled:opacity-50"
                  title={
                    networkRegion.notFollowed.length === 0
                      ? 'You already follow everyone in this area'
                      : 'Follow every author here you do not already follow'
                  }
                >
                  <UserPlus className="h-3.5 w-3.5" />
                  {followRegionMutation.isPending
                    ? 'Following…'
                    : `Follow ${networkRegion.notFollowed.length}`}
                </button>
                <button
                  type="button"
                  onClick={() => setNetworkRegionIds(null)}
                  className="rounded-sm border border-control-edge bg-control-well px-2.5 py-1.5 text-xs text-slate-600 hover:bg-control-quiet"
                >
                  Cancel
                </button>
              </div>
              <p className="mt-2 text-[11px] text-slate-400">
                Full member list is in the Area panel below.
              </p>
            </MapRegionCard>
          )
        }
        // Dashed halo = an author you follow (host-documented meaning).
        nodeHalo={networkNodeHalo}
        hoverCard={(n) => {
          const key = authorKey(n.id)
          const suggestion = suggestionForNode(n)
          const score =
            networkAuthorField.scoresById.get(key) ??
            (typeof n.metadata?.score === 'number' ? n.metadata.score : null)
          return (
            <>
              <p className="line-clamp-2 font-medium text-alma-800">{n.name}</p>
              <p className="mt-0.5 text-slate-500">
                {typeof n.metadata?.pub_count === 'number' ? `${n.metadata.pub_count} papers` : ''}
                {typeof n.metadata?.h_index === 'number' ? ` · h-index ${n.metadata.h_index}` : ''}
                {followedKeys.has(key)
                  ? ' · followed'
                  : n.in_library
                    ? ' · in your library'
                    : suggestion
                      ? ' · suggested'
                      : ''}
              </p>
              {score != null && (
                <p className="mt-0.5 font-medium text-alma-800">
                  Score {Math.round(score)}/100 · mean of their papers
                </p>
              )}
              {suggestion && (
                <p className="mt-0.5 text-gold-700">
                  {authorSuggestionSourceLabel(suggestion.suggestion_type)}
                </p>
              )}
            </>
          )
        }}
        renderClickCard={(n, close) => {
          const local = authorsByKey.get(authorKey(n.id))
          const isFollowed = followedKeys.has(authorKey(n.id))
          const suggestion = suggestionForNode(n)
          const liveScore =
            networkAuthorField.scoresById.get(authorKey(n.id)) ??
            (typeof n.metadata?.score === 'number' ? n.metadata.score : undefined)
          // Follow/unfollow must target the LOCAL author row when we have
          // one. Posting the payload's upper-cased OpenAlex id would not
          // match `authors.id` and would mint a duplicate author under the
          // other casing; the folded id matches the stored convention.
          const followTargetId = local?.id ?? authorKey(n.id)
          const openNetworkDetail = () => {
            close()
            openNetworkNodeDetail(n)
          }
          return (
            <MapAuthorPopup
              author={{
                id: followTargetId,
                name: local?.name || n.name,
                affiliation:
                  local?.affiliation ||
                  (typeof n.metadata?.affiliation === 'string'
                    ? n.metadata.affiliation
                    : undefined),
                publicationCount:
                  typeof n.metadata?.pub_count === 'number'
                    ? n.metadata.pub_count
                    : local?.publication_count,
                hIndex:
                  typeof n.metadata?.h_index === 'number'
                    ? n.metadata.h_index
                    : local?.h_index,
                score: liveScore,
                clusterLabel:
                  typeof n.metadata?.cluster_label === 'string'
                    ? n.metadata.cluster_label
                    : undefined,
                interests: local?.interests,
                suggestion: suggestion
                  ? {
                      source: authorSuggestionSourceLabel(suggestion.suggestion_type),
                      reasons: authorSuggestionReasons(suggestion),
                      score: suggestion.score,
                    }
                  : null,
              }}
              isFollowed={isFollowed}
              isOwner={ownerId != null && authorKey(n.id) === authorKey(ownerId)}
              pending={
                networkFollowMutation.isPending &&
                networkFollowMutation.variables?.author.id === followTargetId
              }
              onFollow={() =>
                networkFollowMutation.mutate({
                  author: { id: followTargetId, name: local?.name || n.name },
                  isFollowed: false,
                })
              }
              onUnfollow={() =>
                networkFollowMutation.mutate({
                  author: { id: followTargetId, name: local?.name || n.name },
                  isFollowed: true,
                })
              }
              onOpenDetails={openNetworkDetail}
              onClose={close}
            />
          )
        }}
        legendExtras={
          <>
            <span className="inline-flex items-center gap-1.5 text-gold-700">
              <span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-gold-400" />
              gold outline = current suggestion
            </span>
            <span className="inline-flex items-center gap-1.5 text-slate-400">
              <span className="inline-block h-2.5 w-2.5 rounded-full border border-dashed border-slate-500" />
              dashed ring = followed
            </span>
            {networkScope === 'corpus' && (
              <span className="text-slate-400">
                {placedSuggestionCount}/{suggestions.length} suggestions on plate
              </span>
            )}
          </>
        }
        height={480}
      />

      {/* Dense area drilldown — the on-plate card answers the gesture, this
          answers the question. Same two-section shape the Map page uses. */}
      {networkRegion && (
        <div className="grid items-start gap-4 border-t border-[var(--color-border)] pt-4 lg:grid-cols-2">
          <Card>
            <CardContent className="space-y-3 p-4 text-xs">
              <div className="flex items-start justify-between gap-2">
                <p className="text-sm font-semibold text-alma-800">
                  Area — {networkRegion.members.length} authors
                </p>
                <button
                  type="button"
                  onClick={() => setNetworkRegionIds(null)}
                  className="rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                  aria-label="Clear area selection"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
              <p className="text-slate-500">
                {networkRegion.followed.length} you already follow ·{' '}
                {networkRegion.suggested.length} currently suggested ·{' '}
                {networkRegion.notFollowed.length} new to you
              </p>
              {networkRegion.topics.length > 0 && (
                <div>
                  <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                    Research communities here
                  </p>
                  <div className="mt-1 flex flex-wrap gap-1">
                    {networkRegion.topics.map(([label, count]) => (
                      <StatusBadge key={label} tone="neutral" size="sm">
                        {label} · {count}
                      </StatusBadge>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardContent className="space-y-2 p-4 text-xs">
              <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                Members — most published first
              </p>
              <ul className="max-h-64 space-y-1 overflow-y-auto">
                {networkRegion.byPubs.map((node) => {
                  const key = authorKey(node.id)
                  return (
                    <li
                      key={node.id}
                      className="flex items-center justify-between gap-2 border-b border-[var(--color-border)] pb-1 last:border-0"
                    >
                      <button
                        type="button"
                        onClick={() => setNetworkSelected(node)}
                        className="min-w-0 flex-1 truncate text-left text-alma-800 hover:text-alma-folio"
                        title="Open this author's card"
                      >
                        {node.name}
                      </button>
                      <span className="shrink-0 text-slate-400">
                        {typeof node.metadata?.pub_count === 'number'
                          ? `${node.metadata.pub_count}p`
                          : ''}
                      </span>
                      {followedKeys.has(key) ? (
                        <StatusBadge tone="positive" size="sm">
                          Followed
                        </StatusBadge>
                      ) : suggestionForNode(node) ? (
                        <StatusBadge tone="accent" size="sm">
                          Suggested
                        </StatusBadge>
                      ) : null}
                    </li>
                  )
                })}
              </ul>
            </CardContent>
          </Card>
        </div>
      )}

      {networkSelected && (
        <div className="grid items-start gap-4 border-t border-[var(--color-border)] pt-4 lg:grid-cols-2">
          <Card>
            <CardContent className="space-y-4 p-4 text-xs">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                    Author drilldown
                  </p>
                  <h2 className="mt-1 text-base font-semibold text-alma-800">
                    {networkSelectedLocal?.name || networkSelected.name}
                  </h2>
                  {(networkSelectedLocal?.affiliation ||
                    (typeof networkSelected.metadata?.affiliation === 'string'
                      ? networkSelected.metadata.affiliation
                      : '')) && (
                    <p className="mt-1 text-slate-500">
                      {networkSelectedLocal?.affiliation ||
                        String(networkSelected.metadata?.affiliation)}
                    </p>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => setNetworkSelected(null)}
                  className="shrink-0 rounded-sm p-0.5 text-slate-400 hover:bg-control-quiet hover:text-slate-600"
                  aria-label="Clear author selection"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>

              <div className="flex flex-wrap gap-1">
                {ownerId != null &&
                  authorKey(networkSelected.id) === authorKey(ownerId) && (
                    <StatusBadge tone="accent" size="sm">This is you</StatusBadge>
                  )}
                {networkSelectedIsFollowed && (
                  <StatusBadge tone="positive" size="sm">Following</StatusBadge>
                )}
                {networkSelected.in_library && (
                  <StatusBadge tone="accent" size="sm">In your library</StatusBadge>
                )}
                {networkSelectedSuggestion && (
                  <StatusBadge tone="warning" size="sm">Suggested</StatusBadge>
                )}
              </div>

              <div className="grid grid-cols-3 gap-2">
                <MetricTile
                  label="Papers"
                  value={Number(
                    networkSelected.metadata?.pub_count ??
                      networkSelectedLocal?.publication_count ??
                      0,
                  )}
                  align="center"
                />
                <MetricTile
                  label="h-index"
                  value={Number(
                    networkSelected.metadata?.h_index ??
                      networkSelectedLocal?.h_index ??
                      0,
                  )}
                  align="center"
                />
                <MetricTile
                  label="Citations"
                  value={Number(
                    networkSelected.metadata?.author_citedby ??
                      networkSelectedLocal?.citedby ??
                      networkSelected.metadata?.citation_count ??
                      0,
                  )}
                  align="center"
                />
              </div>

              {networkSelectedScore != null && (
                <div className="flex items-center justify-between rounded-sm border border-edge-2 px-3 py-2">
                  <div>
                    <p className="font-medium text-alma-800">Internal score</p>
                    <p className="text-[10px] text-slate-400">
                      Mean relevance of scored papers
                    </p>
                  </div>
                  <ScoreMeter score={networkSelectedScore} />
                </div>
              )}

              {networkSelectedField && networkSelectedField.signal_papers > 0 && (
                <p className="text-slate-500">
                  Preference terrain here rests on {networkSelectedField.signal_papers} of{' '}
                  {networkSelectedField.papers} in-scope papers carrying a signal.
                </p>
              )}

              {networkSelectedInterests.length > 0 && (
                <div>
                  <p className="mb-1.5 font-medium text-alma-800">Research interests</p>
                  <div className="flex flex-wrap gap-1">
                    {networkSelectedInterests.slice(0, 8).map((interest) => (
                      <SignalChip key={interest} kind="topic">{interest}</SignalChip>
                    ))}
                  </div>
                </div>
              )}

              {networkSelectedSuggestion && (
                <div className="space-y-2 rounded-sm border border-gold-300/70 bg-gold-50/50 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div>
                      <p className="font-medium text-gold-700">Why this author is suggested</p>
                      <p className="text-[11px] text-slate-500">
                        Source: {authorSuggestionSourceLabel(
                          networkSelectedSuggestion.suggestion_type,
                        )}
                      </p>
                    </div>
                    <ScoreMeter score={networkSelectedSuggestion.score} />
                  </div>
                  <ul className="space-y-1 text-slate-600">
                    {authorSuggestionReasons(networkSelectedSuggestion).map((reason) => (
                      <li key={reason}>• {reason}</li>
                    ))}
                  </ul>
                </div>
              )}

              <div className="flex flex-wrap gap-2 border-t border-[var(--color-border)] pt-3">
                {!(ownerId != null &&
                  authorKey(networkSelected.id) === authorKey(ownerId)) && (
                  <Button
                    size="sm"
                    variant={networkSelectedIsFollowed ? 'outline' : 'default'}
                    disabled={networkFollowMutation.isPending}
                    onClick={() =>
                      networkFollowMutation.mutate({
                        author: {
                          id: networkSelectedFollowTargetId,
                          name: networkSelectedLocal?.name || networkSelected.name,
                        },
                        isFollowed: networkSelectedIsFollowed,
                      })
                    }
                  >
                    {networkSelectedIsFollowed ? 'Unfollow' : 'Follow'}
                  </Button>
                )}
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => openNetworkNodeDetail(networkSelected)}
                >
                  Open full profile
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="space-y-4 p-4 text-xs">
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  Community &amp; relationships
                </p>
                <h2 className="mt-1 text-base font-semibold text-alma-800">
                  {networkSelectedCluster?.label ?? 'Unclustered'}
                </h2>
                {networkSelectedCluster?.description && (
                  <p className="mt-1 text-slate-500">
                    {networkSelectedCluster.description}
                  </p>
                )}
              </div>

              <div className="grid grid-cols-3 gap-2">
                <MetricTile
                  label="Authors"
                  value={networkSelectedCluster?.size ?? 1}
                  align="center"
                />
                <MetricTile
                  label="Suggestions"
                  value={networkCommunitySuggestionCount}
                  align="center"
                  tone={networkCommunitySuggestionCount > 0 ? 'warning' : 'neutral'}
                />
                <MetricTile
                  label="Nearby shown"
                  value={networkNearbyAuthors.length}
                  align="center"
                />
              </div>

              {(networkSelectedCluster?.word_cloud?.length ?? 0) > 0 && (
                <div>
                  <p className="mb-1.5 font-medium text-alma-800">Community vocabulary</p>
                  <div className="flex flex-wrap gap-1">
                    {networkSelectedCluster?.word_cloud?.slice(0, 8).map((item) => (
                      <SignalChip key={item.term} kind="topic">{item.term}</SignalChip>
                    ))}
                  </div>
                </div>
              )}

              <div>
                <p className="mb-2 font-medium text-alma-800">Nearest authors on the map</p>
                {networkNearbyAuthors.length > 0 ? (
                  <ul className="grid gap-1.5 sm:grid-cols-2">
                    {networkNearbyAuthors.map(({ node, distance }) => {
                      const suggestion = suggestionForNode(node)
                      return (
                        <li key={node.id}>
                          <button
                            type="button"
                            className="flex w-full items-center gap-2 rounded-sm px-1 py-0.5 text-left hover:bg-control-quiet"
                            onClick={() => setNetworkSelected(node)}
                          >
                            <span className="min-w-0 flex-1 truncate font-medium text-slate-700">
                              {node.name}
                            </span>
                            {suggestion && (
                              <span
                                className="h-2 w-2 shrink-0 rounded-full border-2 border-gold-400"
                                title="Current suggestion"
                              />
                            )}
                            <span className="shrink-0 tabular-nums text-slate-400">
                              {Math.round(distance * 100)}
                            </span>
                          </button>
                        </li>
                      )
                    })}
                  </ul>
                ) : (
                  <p className="text-slate-500">No other placed authors in this scope.</p>
                )}
                <p className="mt-2 text-[10px] text-slate-400">
                  Lower distance means more similar paper embeddings; it is not a quality score.
                </p>
              </div>

              {networkCommunityMembers.length > 0 && (
                <div className="border-t border-[var(--color-border)] pt-3">
                  <p className="mb-2 font-medium text-alma-800">Anchors in this community</p>
                  <ul className="grid gap-1.5 sm:grid-cols-2">
                    {networkCommunityMembers.map((node) => (
                      <li key={node.id}>
                        <button
                          type="button"
                          className="flex w-full items-center justify-between gap-2 text-left text-slate-600 hover:text-alma-800 hover:underline"
                          onClick={() => setNetworkSelected(node)}
                        >
                          <span className="truncate">{node.name}</span>
                          <span className="shrink-0 tabular-nums text-slate-400">
                            {Number(node.metadata?.pub_count ?? 0)} papers
                          </span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* ONE quiet footnote, not a second suggestions list. The rail directly
          below this card already renders every suggestion as a full card, so
          a grid here said the same thing twice — and it gave ONE reason for a
          set that has two, promising a dot to authors who can never get one
          from enrichment alone. */}
      {networkScope === 'corpus' && unplacedSuggestions.length > 0 && (
        <p className="border-t border-[var(--color-border)] pt-3 text-xs text-slate-500">
          {unplacedSuggestions.length} of {suggestions.length} current
          suggestions have no dot yet
          {unplacedAwaitingLayout > 0 && (
            <> — {unplacedAwaitingLayout} join at the next layout rebuild</>
          )}
          {unplacedNeedingPapers > 0 && (
            <>
              {unplacedAwaitingLayout > 0 ? ', ' : ' — '}
              {unplacedNeedingPapers} need a second tracked paper first (the map places an
              author from two or more papers that carry vectors)
            </>
          )}
          . All of them stay in the rail below.
        </p>
      )}

      <AuthorDetailPanel
        author={selectedAuthor}
        suggestion={selectedSuggestion}
        open={detailOpen}
        onOpenChange={setDetailOpen}
      />
    </div>
  )
}
