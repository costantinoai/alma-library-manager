import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Compass,
  Flame,
  GitBranch,
  Loader2,
  Pin,
  RefreshCw,
  Rocket,
  Sparkles,
  Volume2,
  VolumeX,
} from 'lucide-react'

import { previewLensBranches, updateLens, type Lens, type LensBranchItem } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { EmptyState } from '@/components/ui/empty-state'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { StatusBadge } from '@/components/ui/status-badge'
import { JargonHint, MetricTile } from '@/components/shared'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

interface BranchExplorerPanelProps {
  lens: Lens | null
}

// Branch palette — pinned to the v3 brand. Eight distinguishable slots
// drawn from navy / Folio-blue / gold / parchment / pale tones so the
// branch color dots stay consistent with the rest of the product.
const BRANCH_COLORS = [
  '#0F1E36', // alma-800 (navy)
  '#1E5B86', // alma-folio (Folio binding blue)
  '#C49A45', // gold-400
  '#6F98BB', // pale-500
  '#344E7C', // alma-500 (mid navy)
  '#C2A86B', // parchment-500
  '#A77E36', // gold-500
  '#3D5F7C', // pale-700
] as const
const BRANCH_PRESETS = [
  { id: 'steady', label: 'Steady', description: 'Tight continuity around core taste.', temperature: 0.12 },
  { id: 'balanced', label: 'Balanced', description: 'Stable mix of continuity and exploration.', temperature: 0.28 },
  { id: 'broaden', label: 'Broaden', description: 'Widen retrieval without going fully exploratory.', temperature: 0.46 },
  { id: 'serendipity', label: 'Serendipity', description: 'Push lateral discovery and exploratory variants.', temperature: 0.7 },
] as const

function normalizeControls(lens: Lens | null) {
  return {
    temperature: lens?.branch_controls?.temperature ?? 0.28,
    pinned: lens?.branch_controls?.pinned ?? [],
    muted: lens?.branch_controls?.muted ?? [],
    boosted: lens?.branch_controls?.boosted ?? [],
  }
}

// Branch state tinting. Branches are NOT content — they are
// lens/navigation controls. So branch tiles use the chrome ladder:
// chrome-elev fill (lighter than the chrome BranchExplorerPanel host
// card, so a stack reads as "well lit on bound page"). Pinned tints
// Folio-blue, boosted tints gold, muted dims back to canvas.
function branchTone(state: 'normal' | 'pinned' | 'boosted' | 'muted') {
  if (state === 'pinned')
    return 'border-alma-folio/40 bg-[color-mix(in_srgb,_var(--color-alma-folio)_8%,_var(--color-alma-chrome-elev))]'
  if (state === 'boosted')
    return 'border-gold-300 bg-[color-mix(in_srgb,_var(--color-gold-400)_10%,_var(--color-alma-chrome-elev))]'
  if (state === 'muted')
    return 'border-[var(--color-border-cool)] bg-alma-canvas opacity-75'
  return 'border-[var(--color-border-cool)] bg-alma-chrome-elev'
}

function closestPreset(temperature: number) {
  return BRANCH_PRESETS.reduce((best, preset) => {
    const currentDistance = Math.abs((best?.temperature ?? 0.28) - temperature)
    const nextDistance = Math.abs(preset.temperature - temperature)
    return nextDistance < currentDistance ? preset : best
  }, BRANCH_PRESETS[1])
}

export function BranchExplorerPanel({ lens }: BranchExplorerPanelProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [temperature, setTemperature] = useState(0.28)
  const [pinned, setPinned] = useState<string[]>([])
  const [muted, setMuted] = useState<string[]>([])
  const [boosted, setBoosted] = useState<string[]>([])

  useEffect(() => {
    const controls = normalizeControls(lens)
    setTemperature(controls.temperature)
    setPinned(controls.pinned)
    setMuted(controls.muted)
    setBoosted(controls.boosted)
  }, [lens?.id, lens?.branch_controls])

  const branchQuery = useQuery({
    queryKey: ['lens-branches', lens?.id, lens?.branch_controls],
    queryFn: () => previewLensBranches(lens?.id as string, { max_branches: 8 }),
    enabled: Boolean(lens?.id),
    staleTime: 30_000,
  })

  const saveControlsMutation = useMutation({
    mutationFn: () =>
      updateLens(lens?.id as string, {
        branch_controls: {
          temperature,
          pinned,
          muted,
          boosted,
        },
      }),
    onSuccess: (updatedLens) => {
      queryClient.setQueryData<Lens[]>(['lenses'], (prev) => {
        const current = prev ?? []
        return current.map((item) => (item.id === updatedLens.id ? updatedLens : item))
      })
      void invalidateQueries(queryClient, ['lenses'], ['lens-branches', updatedLens.id])
      toast({
        title: 'Branch controls applied',
        description: 'Saved controls will shape the next discovery refresh for this lens.',
      })
    },
    onError: () => {
      errorToast('Branch controls failed', 'Could not save branch controls.')
    },
  })

  const branches = branchQuery.data?.branches ?? []
  const coldStart = (lens?.last_retrieval_summary as Record<string, any> | null)?.cold_start ?? null
  const savedControls = normalizeControls(lens)
  const isDirty = useMemo(() => {
    const normalizeArray = (values: string[]) => [...values].sort()
    const sameArray = (a: string[], b: string[]) => {
      const left = normalizeArray(a)
      const right = normalizeArray(b)
      return left.length === right.length && left.every((value, index) => value === right[index])
    }
    return (
      Math.abs((savedControls.temperature ?? 0.28) - temperature) > 0.001
      || !sameArray(savedControls.pinned, pinned)
      || !sameArray(savedControls.muted, muted)
      || !sameArray(savedControls.boosted, boosted)
    )
  }, [boosted, muted, pinned, savedControls, temperature])

  const setBranchState = (branchId: string, state: 'normal' | 'pinned' | 'boosted' | 'muted') => {
    const nextPinned = pinned.filter((value) => value !== branchId)
    const nextBoosted = boosted.filter((value) => value !== branchId)
    const nextMuted = muted.filter((value) => value !== branchId)
    if (state === 'pinned') nextPinned.push(branchId)
    if (state === 'boosted') nextBoosted.push(branchId)
    if (state === 'muted') nextMuted.push(branchId)
    setPinned(nextPinned)
    setBoosted(nextBoosted)
    setMuted(nextMuted)
  }

  const summary = useMemo(() => {
    const activeCount = branches.filter((branch) => branch.is_active !== false).length
    // Count branches whose auto_weight has moved off the 1.0 neutral baseline.
    // That's the visible signal that the engine is actually reshaping budget
    // from past outcomes — replaces the old manual "suggestions" counter.
    const autoTunedCount = branches.filter((branch) => {
      const weight = Number(branch.auto_weight ?? 1.0)
      return Math.abs(weight - 1.0) >= 0.05
    }).length
    return {
      branchCount: branches.length,
      activeCount,
      pinnedCount: pinned.length,
      boostedCount: boosted.length,
      mutedCount: muted.length,
      autoTunedCount,
    }
  }, [branches, boosted.length, muted.length, pinned.length])
  const activePreset = useMemo(() => closestPreset(temperature), [temperature])

  if (!lens?.id) {
    return (
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 font-brand text-base text-alma-800">
            <GitBranch className="h-5 w-5 text-alma-folio" />
            Branch Studio
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-slate-500">Select a lens to inspect and steer the branch structure.</CardContent>
      </Card>
    )
  }

  return (
    <Card className="overflow-hidden">
      <CardHeader className="border-b border-[var(--color-border)] bg-parchment-50">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <div className="inline-flex items-center gap-1">
                <StatusBadge tone="accent" size="sm">Foreground Control Surface</StatusBadge>
                <JargonHint
                  title="Foreground Control Surface"
                  description="Settings here apply to this lens's next refresh run, not past runs. Click Apply Controls to persist; the next Discovery refresh will use them. Think of it as a live dashboard, not a permanent config."
                />
              </div>
              {coldStart?.state ? (
                <Badge variant="outline" className="capitalize">
                  Topic cold start: {String(coldStart.state).replace(/_/g, ' ')}
                </Badge>
              ) : null}
            </div>
            <div>
              <CardTitle className="flex items-center gap-2 font-brand text-xl text-alma-800">
                <GitBranch className="h-5 w-5 text-alma-folio" />
                Branch Studio
                <JargonHint
                  title="Branch Studio"
                  description={
                    <>
                      Each lens keeps a set of <strong>branches</strong> — clusters of related
                      papers, authors, and topics that Discovery pursues independently. A branch
                      is <strong>pinned</strong> to guarantee continued coverage,
                      <strong> boosted</strong> to expand its surface area in the next refresh,
                      or <strong>muted</strong> to stop spending retrieval budget on it. The
                      Studio is where you steer those choices for this lens.
                    </>
                  }
                  side="right"
                />
              </CardTitle>
              <p className="mt-1 max-w-3xl text-sm text-slate-600">
                Steer where Discovery spends retrieval budget for <span className="font-medium text-alma-800">{lens.name}</span>.
                Pin keeps a branch alive, boost gives it more surface area, mute removes it from refresh.
              </p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button size="sm" variant="outline" onClick={() => branchQuery.refetch()} disabled={branchQuery.isFetching}>
              {branchQuery.isFetching ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="mr-1 h-3.5 w-3.5" />}
              Preview
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => {
                const controls = normalizeControls(lens)
                setTemperature(controls.temperature)
                setPinned(controls.pinned)
                setMuted(controls.muted)
                setBoosted(controls.boosted)
              }}
              disabled={!isDirty}
            >
              Reset Draft
            </Button>
            <Button size="sm" onClick={() => saveControlsMutation.mutate()} disabled={saveControlsMutation.isPending || !isDirty}>
              {saveControlsMutation.isPending ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" /> : null}
              Apply Controls
            </Button>
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-6 p-5">
        {/* In-page explainer — uses the canonical ConceptCallout so
            similar "What is this?" boxes elsewhere in the app (Signal
            Lab, Insights graph clusters, etc.) all look the same. */}
        <ConceptCallout
          summary="Branches are clusters Discovery has carved out of your library — click for the full explanation."
        >
          <p>
            <strong className="text-alma-800">Branches</strong> are clusters of related papers
            the lens has noticed in your saved library. The clustering runs on the SPECTER2
            embedding of every saved paper, then auto-labels each cluster from the most
            distinctive topic terms shared by its members — so the words you see ("models",
            "nlp", "diffusion", etc.) are pulled directly from the papers you saved, not
            chosen by an AI.
          </p>
          <p>
            <strong className="text-alma-800">Core Pull</strong> = topics every paper in
            the cluster shares (the cluster's center of gravity).
            <strong className="text-alma-800"> Explore Push</strong> = neighbouring topics
            just outside the cluster — the lateral expansion the next refresh will reach
            toward. Each refresh budgets a slice of retrieval calls per branch.
          </p>
          <p>
            <strong className="text-alma-800">Pin</strong> a branch to guarantee continued
            coverage; <strong className="text-alma-800">Boost</strong> to give it more
            surface area; <strong className="text-alma-800">Mute</strong> to stop spending
            budget on it. Changes apply on the next Discovery refresh.
          </p>
        </ConceptCallout>

        {/* Number tiles — adaptive grid (auto-fit packs as many
            ~140px-min tiles per row as the available width allows)
            using the canonical MetricTile primitive with `align=
            "center"` so the row reads as a small scoreboard. */}
        <div className="grid gap-3 [grid-template-columns:repeat(auto-fit,minmax(140px,1fr))]">
          {[
            { label: 'Seeds', value: branchQuery.data?.seed_count ?? 0 },
            { label: 'Branches', value: summary.branchCount },
            { label: 'Active', value: summary.activeCount },
            // v3 simplification: just "Pinned" instead of the v2
            // "Pinned / Boosted" combined cluster — boost still has
            // its own per-branch state badge, so the summary doesn't
            // need to double-count it.
            { label: 'Pinned', value: summary.pinnedCount },
            { label: 'Muted', value: summary.mutedCount },
            { label: 'Auto-tuned', value: summary.autoTunedCount },
          ].map((tile) => (
            <MetricTile
              key={tile.label}
              label={tile.label}
              value={tile.value}
              align="center"
            />
          ))}
        </div>

        <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
          <div className="rounded-sm border border-[var(--color-border)] bg-alma-paper p-4 shadow-paper-sm">
            <div className="flex items-center justify-between gap-4">
              <div>
                <div className="flex items-center gap-1.5">
                  <p className="font-brand text-sm font-semibold text-alma-800">Exploration Temperature</p>
                  <JargonHint
                    title="Exploration Temperature"
                    description={
                      <>
                        Controls how far the next refresh wanders from what the lens already knows
                        about. <strong>Low</strong> (≈0.1–0.25) keeps search tight and
                        continuity-focused — same topics, same authors, same venues. <strong>High</strong>
                        (≈0.5–0.8) pushes broader query variants and lateral expansion, at the cost
                        of lower average precision. The presets below are reasonable starting
                        points; tune live and Apply Controls to persist.
                      </>
                    }
                  />
                </div>
                <p className="mt-0.5 text-xs text-slate-500">Low keeps refresh tight around core continuity. High pushes more lateral search and exploratory query variants.</p>
              </div>
              <div className="rounded-sm border border-[var(--color-border)] bg-parchment-100 px-3 py-1 font-brand text-sm font-semibold text-alma-800 tabular-nums">
                {temperature.toFixed(2)}
              </div>
            </div>
            <div className="mt-4 space-y-2">
              <input
                type="range"
                min={0}
                max={1}
                step={0.05}
                value={temperature}
                onChange={(e) => setTemperature(Number(e.target.value))}
                className="w-full accent-alma-folio"
              />
              <div className="flex items-center justify-between text-[11px] font-medium uppercase tracking-wide text-slate-500">
                <span>Focused continuity</span>
                <span>Exploratory expansion</span>
              </div>
              <div className="flex flex-wrap gap-2 pt-2">
                {BRANCH_PRESETS.map((preset) => {
                  const active = activePreset.id === preset.id && Math.abs(temperature - preset.temperature) < 0.08
                  return (
                    <Button
                      key={preset.id}
                      type="button"
                      size="sm"
                      variant={active ? 'default' : 'outline'}
                      onClick={() => setTemperature(preset.temperature)}
                    >
                      {preset.label}
                    </Button>
                  )
                })}
              </div>
              <p className="text-xs text-slate-500">
                Active preset: <span className="font-medium text-alma-800">{activePreset.label}</span>. {activePreset.description}
              </p>
            </div>
          </div>

          {/* Auto-tuning explainer — replaces the old "smart suggestions"
              panel with a one-glance summary of what the engine is doing on
              its own. No buttons: each branch tile carries its own
              auto-weight badge and reason, so the user can drill in there. */}
          <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50 p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-brand text-sm font-semibold text-alma-800">Auto-tuning</p>
                <p className="mt-1 text-xs text-slate-500">
                  Branch budgets adapt automatically from your past saves and
                  dismisses. Manual Pin / Boost / Mute below stay available
                  as overrides.
                </p>
              </div>
              <Sparkles className="h-4 w-4 text-alma-folio" />
            </div>
            <div className="mt-4 flex flex-wrap gap-2 text-xs">
              <Badge variant="outline">{summary.autoTunedCount} auto-tuned</Badge>
              <Badge variant="outline">{summary.branchCount - summary.autoTunedCount} neutral</Badge>
            </div>
            <p className="mt-3 text-xs text-slate-500">
              An auto-tuned branch is one where past outcomes pushed its
              retrieval budget off the neutral 1.0× baseline (up if you've
              been saving from it, down if you've been dismissing). Look at
              the badge on each branch tile to see the live weight and the
              reason behind it.
            </p>
          </div>
        </div>

        {coldStart ? (
          <div className="rounded-sm border border-gold-300 bg-[color-mix(in_srgb,_var(--color-gold-400)_8%,_var(--color-alma-cream))] p-4">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p className="font-brand text-sm font-semibold text-alma-800">Topic Cold-Start Validation</p>
                <p className="mt-1 text-xs text-slate-500">Whether this lens can explore beyond local seeds when topic-driven discovery has thin local support.</p>
              </div>
              <StatusBadge tone="warning" className="capitalize">{String(coldStart.state ?? 'unknown').replace(/_/g, ' ')}</StatusBadge>
            </div>
            <div className="mt-3 grid gap-2 text-sm text-slate-600 md:grid-cols-3">
              <span>Query: <span className="font-medium text-alma-800">{String(coldStart.query ?? 'n/a')}</span></span>
              <span>Seed papers: <span className="font-medium text-alma-800">{Number(coldStart.seed_count ?? 0)}</span></span>
              <span>External results: <span className="font-medium text-alma-800">{Number(coldStart.external_results ?? 0)}</span></span>
            </div>
          </div>
        ) : null}

        {branchQuery.isLoading ? (
          <div className="flex items-center justify-center rounded-sm border border-[var(--color-border)] bg-alma-chrome py-16 text-sm text-slate-500">
            <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Building branch studio...
          </div>
        ) : branches.length === 0 ? (
          <EmptyState
            title="No branch structure yet"
            description="Add more embedded library papers or refresh after more intake."
          />
        ) : (
          <div className="grid gap-3 xl:grid-cols-2">
            {branches.map((branch: LensBranchItem, idx) => {
              const color = BRANCH_COLORS[idx % BRANCH_COLORS.length]
              const localState = muted.includes(branch.id)
                ? 'muted'
                : pinned.includes(branch.id)
                  ? 'pinned'
                  : boosted.includes(branch.id)
                    ? 'boosted'
                    : 'normal'
              const stateClass = branchTone(localState)
              const corePreview = branch.core_topics.slice(0, 3).join(' · ')
              return (
                <details
                  key={branch.id}
                  className={`group overflow-hidden rounded-sm border transition-all ${stateClass}`}
                >
                  {/* Compact summary row — what's always visible: color
                      dot, label, the first 3 core topics in dim text, a
                      "score · seeds" mini-line, the state badge. Click
                      to expand for full stats / topics / samples. */}
                  <summary className="grid cursor-pointer select-none items-center gap-3 px-3 py-2.5 [&::-webkit-details-marker]:hidden grid-cols-[auto_1fr_auto]">
                    <span
                      className="h-2.5 w-2.5 rounded-full ring-1 ring-[var(--color-border)]"
                      style={{ backgroundColor: color }}
                      aria-hidden
                    />
                    <div className="min-w-0">
                      <p className="truncate font-brand text-sm font-semibold text-alma-900">{branch.label}</p>
                      {corePreview && (
                        <p className="truncate text-[11px] text-slate-500">{corePreview}</p>
                      )}
                    </div>
                    <div className="flex items-center gap-1.5 text-[11px] tabular-nums text-slate-500">
                      <span className="hidden sm:inline">{branch.seed_count}s</span>
                      <span className="hidden sm:inline">·</span>
                      <span>{branch.branch_score.toFixed(2)}</span>
                      {localState !== 'normal' && (
                        <Badge size="sm" className="ml-1 capitalize">{localState}</Badge>
                      )}
                      <span className="ml-1 text-[10px] font-bold uppercase tracking-[0.14em] text-slate-400 group-open:hidden">+</span>
                      <span className="ml-1 hidden text-[10px] font-bold uppercase tracking-[0.14em] text-slate-400 group-open:inline">−</span>
                    </div>
                  </summary>

                  <div className="space-y-3 border-t border-[var(--color-border)] p-3">
                    {branch.direction_hint && (
                      <p className="text-xs text-slate-500">{branch.direction_hint}</p>
                    )}
                    {/* Auto-weight callout — replaces the discrete tuning_hint
                        (cool/strong/narrow). Shows the live multiplier the
                        next refresh will apply to this branch's retrieval
                        budget plus the one-line reason from the engine. */}
                    {(() => {
                      const weight = Number(branch.auto_weight ?? 1.0)
                      const tuned = Math.abs(weight - 1.0) >= 0.05
                      if (!tuned && !branch.auto_weight_reason) return null
                      const tone = weight >= 1.05
                        ? 'border-alma-folio/40 bg-[color-mix(in_srgb,_var(--color-alma-folio)_8%,_var(--color-alma-paper))] text-alma-800'
                        : weight <= 0.95
                          ? 'border-gold-300 bg-[color-mix(in_srgb,_var(--color-gold-400)_8%,_var(--color-alma-paper))] text-alma-800'
                          : 'border-[var(--color-border)] bg-parchment-50 text-alma-800'
                      return (
                        <div className={`flex items-center justify-between gap-3 rounded-sm border px-3 py-2 text-xs ${tone}`}>
                          <span className="truncate">{branch.auto_weight_reason ?? `auto-weight ${weight.toFixed(2)}×`}</span>
                          <span className="shrink-0 font-brand text-sm font-semibold tabular-nums">{weight.toFixed(2)}×</span>
                        </div>
                      )
                    })()}

                    <div className="flex flex-wrap gap-1.5 text-[11px]">
                      <Badge variant="secondary">{branch.seed_count} seeds</Badge>
                      <Badge variant="outline">score {branch.branch_score.toFixed(2)}</Badge>
                      {(branch.recommendation_count ?? 0) > 0 && (
                        <Badge variant="outline">{branch.recommendation_count} outcomes</Badge>
                      )}
                    </div>

                    {(branch.recommendation_count ?? 0) > 0 && (
                      <div className="grid gap-2 md:grid-cols-4">
                        {[
                          { label: 'Positive', value: `${Math.round((branch.positive_rate ?? 0) * 100)}%` },
                          { label: 'Dismissed', value: `${Math.round((branch.dismiss_rate ?? 0) * 100)}%` },
                          { label: 'Engaged', value: `${Math.round((branch.engagement_rate ?? 0) * 100)}%` },
                          { label: 'Sources', value: branch.unique_sources ?? 0 },
                        ].map((tile) => (
                          <div key={tile.label} className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-2.5 shadow-paper-inset-cool">
                            <EyebrowLabel tone="muted">{tile.label}</EyebrowLabel>
                            <p className="mt-1 font-brand text-base font-semibold text-alma-800">{tile.value}</p>
                          </div>
                        ))}
                      </div>
                    )}

                    <div className="grid gap-2 md:grid-cols-2">
                      <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-2.5 shadow-paper-inset-cool">
                        <div className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-[0.16em] text-alma-folio">
                          <Volume2 className="h-3 w-3" /> Core Pull
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {branch.core_topics.slice(0, 5).map((topic) => (
                            <Badge key={`${branch.id}-core-${topic}`} className="bg-alma-folio-soft text-alma-800">{topic}</Badge>
                          ))}
                        </div>
                      </div>
                      <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-2.5 shadow-paper-inset-cool">
                        <div className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-[0.16em] text-gold-600">
                          <Compass className="h-3 w-3" /> Explore Push
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {branch.explore_topics.slice(0, 5).map((topic) => (
                            <Badge key={`${branch.id}-explore-${topic}`} className="bg-gold-100 text-gold-700">{topic}</Badge>
                          ))}
                        </div>
                      </div>
                    </div>

                    {branch.sample_papers.length > 0 && (
                      <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-2.5 shadow-paper-inset-cool">
                        <EyebrowLabel tone="muted">Representative seeds</EyebrowLabel>
                        <div className="mt-1.5 space-y-1">
                          {branch.sample_papers.slice(0, 2).map((paper, paperIndex) => (
                            <div key={`${branch.id}-paper-${paper.paper_id ?? paperIndex}`} className="flex items-start justify-between gap-3 text-xs">
                              <span className="line-clamp-2 text-alma-800">{paper.title}</span>
                              <span className="shrink-0 rounded-sm bg-gold-100 px-1.5 py-0.5 text-[10px] font-medium text-gold-700">{paper.rating.toFixed(1)}★</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    <div className="flex flex-wrap items-center gap-1.5">
                      <Button type="button" size="sm" variant={localState === 'pinned' ? 'default' : 'outline'} onClick={() => setBranchState(branch.id, 'pinned')}>
                        <Pin className="mr-1 h-3 w-3" /> Pin
                      </Button>
                      <Button type="button" size="sm" variant={localState === 'boosted' ? 'default' : 'outline'} onClick={() => setBranchState(branch.id, 'boosted')}>
                        <Rocket className="mr-1 h-3 w-3" /> Boost
                      </Button>
                      <Button type="button" size="sm" variant={localState === 'muted' ? 'default' : 'outline'} onClick={() => setBranchState(branch.id, 'muted')}>
                        <VolumeX className="mr-1 h-3 w-3" /> Mute
                      </Button>
                      <Button type="button" size="sm" variant="ghost" onClick={() => setBranchState(branch.id, 'normal')}>
                        Clear
                      </Button>
                      {branch.is_active === false ? (
                        <Badge variant="outline" className="ml-auto text-[10px]">inactive after controls</Badge>
                      ) : (
                        <Badge variant="outline" className="ml-auto text-[10px]">
                          <Flame className="mr-1 h-3 w-3" /> active in refresh
                        </Badge>
                      )}
                    </div>
                  </div>
                </details>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
