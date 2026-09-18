import { useCallback, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, RotateCcw, Save, Sparkles } from 'lucide-react'

import {
  api,
  type DiscoveryBranchSettings,
  type DiscoveryMonitorDefaults,
  type DiscoverySettings,
  type DiscoveryStrategies,
  type DiscoveryWeights,
} from '@/api/client'
import {
  AsyncButton,
  SettingsCard,
  SettingsNumberField,
  SettingsSection,
  SettingsSections,
  OptionCard,
  ToggleRow,
} from '@/components/settings/primitives'
import { RANKER_OUTCOME_KEY, RankerOutcomeLine } from '@/components/settings/RankerOutcomeLine'
import { Checkbox } from '@/components/ui/checkbox'
import { Form } from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { RadioGroup } from '@/components/ui/radio-group'
import { Slider } from '@/components/ui/slider'
import { StatusBadge } from '@/components/ui/status-badge'
import { useToast, errorToast } from '@/hooks/useToast'

// ---------------------------------------------------------------------------
// Label tables — one slider per ranking family, named for the family it drives.
//
// Each slider sets a weight in the ONE ranker (`ranker.FAMILY_SPECS`), whose
// families are what the paper card's score breakdown shows. The names match
// the panel's rows on purpose: a control should be findable from the thing it
// changed. `text_similarity` is the one slider that drives two families —
// stated in its description rather than left for the user to infer.
// ---------------------------------------------------------------------------

const WEIGHT_LABELS: {
  key: keyof DiscoveryWeights
  label: string
  description: string
  /** The ranking families this slider drives (`ranker.FAMILY_SPECS.weight_setting`). */
  families: string[]
}[] = [
  { key: 'text_similarity', label: 'Semantic + Lexical', families: ['semantic', 'lexical'], description: 'Drives two families: embedding similarity to what you keep (87%) and terminology overlap (13%).' },
  { key: 'topic_score', label: 'Topic', families: ['topic'], description: 'Overlap with the topics your rated papers cluster on.' },
  { key: 'source_relevance', label: 'Retrieval', families: ['retrieval'], description: 'How strongly the search channels surfaced it, and how many agreed.' },
  { key: 'author_affinity', label: 'Author', families: ['author'], description: 'Authors you follow or repeatedly save.' },
  { key: 'recency_boost', label: 'Recency', families: ['recency'], description: 'How recently it was published.' },
  { key: 'citation_quality', label: 'Citation', families: ['citation'], description: 'Citation weight, and citation-graph proximity to your library.' },
  { key: 'feedback_adj', label: 'Feedback', families: ['feedback'], description: 'Your explicit verdicts on similar papers.' },
  { key: 'preference_affinity', label: 'Preference', families: ['preference'], description: 'The taste profile accumulated from Signal Lab and your history.' },
  { key: 'journal_affinity', label: 'Venue', families: ['venue'], description: 'Journals and conferences you read.' },
]

const STRATEGY_LABELS: { key: keyof DiscoveryStrategies; label: string; description: string }[] = [
  { key: 'related_works', label: 'Related Works', description: 'Use graph-style related-paper retrieval.' },
  { key: 'topic_search', label: 'Topic Search', description: 'Search explicit topic queries from the lens context.' },
  { key: 'followed_authors', label: 'Followed Authors', description: 'Pull direct continuity candidates from monitored authors.' },
  { key: 'coauthor_network', label: 'Co-author Network', description: 'Expand through collaborator neighborhoods.' },
  { key: 'citation_chain', label: 'Citation Chain', description: 'Walk references, citations, and related works.' },
  { key: 'semantic_scholar', label: 'Semantic Scholar', description: 'Include Semantic Scholar in external search.' },
  { key: 'branch_explorer', label: 'Branch Explorer', description: 'Use branch-aware retrieval budgets and controls.' },
  { key: 'taste_topics', label: 'Favorite Topic Lanes', description: 'Retrieve from preferred topics, not just rank them.' },
  { key: 'taste_authors', label: 'Favorite Author Lanes', description: 'Retrieve from preferred authors.' },
  { key: 'taste_venues', label: 'Favorite Venue Lanes', description: 'Retrieve from preferred venues and journals.' },
  { key: 'recent_wins', label: 'Recent Win Lanes', description: 'Reuse recent strong interactions as query seeds.' },
]

const SOURCE_LABELS: Array<{ key: keyof DiscoverySettings['sources']; label: string; description: string }> = [
  { key: 'openalex', label: 'OpenAlex', description: 'Primary scholarly metadata and graph source.' },
  { key: 'semantic_scholar', label: 'Semantic Scholar', description: 'Alternative discovery index and metadata enrichment.' },
  { key: 'crossref', label: 'Crossref', description: 'Broad DOI-oriented metadata fallback.' },
  { key: 'arxiv', label: 'arXiv', description: 'Preprint lane for arXiv.' },
  { key: 'biorxiv', label: 'bioRxiv', description: 'Preprint lane for bioRxiv and medRxiv-style freshness.' },
  { key: 'europe_pmc', label: 'Europe PMC', description: 'Biomedical articles, preprints, and full abstracts.' },
]

const RECOMMENDATION_MODES: Array<{ value: string; label: string; description: string }> = [
  { value: 'explore', label: 'Explore', description: 'Reweights ranking towards recency and away from author, venue and citation; widens branch spread.' },
  { value: 'balanced', label: 'Balanced', description: 'Uses your weights as set, with moderate branch spread.' },
  { value: 'exploit', label: 'Exploit', description: 'Reweights ranking towards author, venue and learned preference, away from recency; narrows branch spread.' },
]

// ---------------------------------------------------------------------------
// Defaults + shape hydration.
// ---------------------------------------------------------------------------

const DEFAULT_DISCOVERY: DiscoverySettings = {
  // Placeholder until the API answers; the backend's DEFAULT_SIGNAL_WEIGHTS
  // (fitted 2026-09-18) is the owner and the reset route serves it.
  weights: {
    source_relevance: 0.14,
    topic_score: 0,
    text_similarity: 0.51,
    author_affinity: 0,
    journal_affinity: 0.05,
    recency_boost: 0,
    citation_quality: 0.12,
    feedback_adj: 0.09,
    preference_affinity: 0.09,
  },
  strategies: {
    related_works: true,
    topic_search: true,
    followed_authors: true,
    coauthor_network: true,
    citation_chain: true,
    semantic_scholar: true,
    branch_explorer: true,
    taste_topics: true,
    taste_authors: true,
    taste_venues: true,
    recent_wins: true,
  },
  limits: {
    max_results: 50,
    min_score: 0,
    max_candidates_per_strategy: 20,
    recency_window_years: 10,
    feedback_decay_days_full: 90,
    feedback_decay_days_half: 180,
  },
  schedule: {
    refresh_enabled: false,
    refresh_interval_hours: 6,
    graph_maintenance_interval_hours: 24,
  },
  cache: {
    similarity_ttl_hours: 24,
  },
  sources: {
    openalex: { enabled: true },
    semantic_scholar: { enabled: true },
    crossref: { enabled: true },
    arxiv: { enabled: true },
    biorxiv: { enabled: true },
    europe_pmc: { enabled: true },
  },
  branches: {
    temperature: 0.28,
    max_clusters: 6,
    max_active_for_retrieval: 4,
    query_core_variants: 2,
    query_explore_variants: 2,
  },
  monitor_defaults: {
    author_per_refresh: 20,
    search_limit: 15,
    search_temperature: 0.22,
    recency_years: 2,
    include_preprints: true,
    semantic_scholar_bulk: true,
  },
  embedding_model: 'allenai/specter2_base',
  recommendation_mode: 'balanced',
}

function mergeDiscoverySettings(input?: Partial<DiscoverySettings> | null): DiscoverySettings {
  return {
    ...DEFAULT_DISCOVERY,
    ...(input ?? {}),
    weights: { ...DEFAULT_DISCOVERY.weights, ...((input?.weights ?? {}) as Partial<DiscoveryWeights>) },
    strategies: {
      ...DEFAULT_DISCOVERY.strategies,
      ...((input?.strategies ?? {}) as Partial<DiscoveryStrategies>),
    },
    limits: { ...DEFAULT_DISCOVERY.limits, ...((input?.limits ?? {}) as Partial<DiscoverySettings['limits']>) },
    schedule: {
      ...DEFAULT_DISCOVERY.schedule,
      ...((input?.schedule ?? {}) as Partial<DiscoverySettings['schedule']>),
    },
    cache: { ...DEFAULT_DISCOVERY.cache, ...((input?.cache ?? {}) as Partial<DiscoverySettings['cache']>) },
    sources: {
      openalex: { ...DEFAULT_DISCOVERY.sources.openalex, ...(input?.sources?.openalex ?? {}) },
      semantic_scholar: {
        ...DEFAULT_DISCOVERY.sources.semantic_scholar,
        ...(input?.sources?.semantic_scholar ?? {}),
      },
      crossref: { ...DEFAULT_DISCOVERY.sources.crossref, ...(input?.sources?.crossref ?? {}) },
      arxiv: { ...DEFAULT_DISCOVERY.sources.arxiv, ...(input?.sources?.arxiv ?? {}) },
      biorxiv: { ...DEFAULT_DISCOVERY.sources.biorxiv, ...(input?.sources?.biorxiv ?? {}) },
      europe_pmc: {
        ...DEFAULT_DISCOVERY.sources.europe_pmc,
        ...(input?.sources?.europe_pmc ?? {}),
      },
    },
    branches: {
      ...DEFAULT_DISCOVERY.branches,
      ...((input?.branches ?? {}) as Partial<DiscoveryBranchSettings>),
    },
    monitor_defaults: {
      ...DEFAULT_DISCOVERY.monitor_defaults,
      ...((input?.monitor_defaults ?? {}) as Partial<DiscoveryMonitorDefaults>),
    },
    embedding_model: input?.embedding_model ?? DEFAULT_DISCOVERY.embedding_model,
    recommendation_mode: input?.recommendation_mode ?? DEFAULT_DISCOVERY.recommendation_mode,
  }
}

// Zod schema — tight enough to validate numeric bounds, loose enough to
// mirror the backend contract (extra keys are passed through). Used as the
// resolver so the global save button is guarded by the same rules as the
// per-field number inputs.
const weightShape = z.number().min(0).max(1)
const sourcePolicy = z.object({ enabled: z.boolean() })
const discoverySchema = z.object({
  weights: z.object({
    source_relevance: weightShape,
    topic_score: weightShape,
    text_similarity: weightShape,
    author_affinity: weightShape,
    journal_affinity: weightShape,
    recency_boost: weightShape,
    citation_quality: weightShape,
    feedback_adj: weightShape,
    preference_affinity: weightShape,
  }),
  strategies: z.object({
    related_works: z.boolean(),
    topic_search: z.boolean(),
    followed_authors: z.boolean(),
    coauthor_network: z.boolean(),
    citation_chain: z.boolean(),
    semantic_scholar: z.boolean(),
    branch_explorer: z.boolean(),
    taste_topics: z.boolean(),
    taste_authors: z.boolean(),
    taste_venues: z.boolean(),
    recent_wins: z.boolean(),
  }),
  limits: z.object({
    max_results: z.number().int().min(10).max(200),
    min_score: z.number().min(0).max(100),
    max_candidates_per_strategy: z.number().int().min(5).max(50),
    recency_window_years: z.number().int().min(1).max(20),
    feedback_decay_days_full: z.number().int().min(1).max(3650),
    feedback_decay_days_half: z.number().int().min(1).max(3650),
  }),
  schedule: z.object({
    refresh_enabled: z.boolean(),
    refresh_interval_hours: z.number().int().min(0).max(168),
    graph_maintenance_interval_hours: z.number().int().min(0).max(168),
  }),
  cache: z.object({ similarity_ttl_hours: z.number().int().min(1).max(168) }),
  sources: z.object({
    openalex: sourcePolicy,
    semantic_scholar: sourcePolicy,
    crossref: sourcePolicy,
    arxiv: sourcePolicy,
    biorxiv: sourcePolicy,
    europe_pmc: sourcePolicy,
  }),
  branches: z.object({
    temperature: z.number().min(0).max(1),
    max_clusters: z.number().int().min(2).max(12),
    max_active_for_retrieval: z.number().int().min(1).max(12),
    query_core_variants: z.number().int().min(1).max(4),
    query_explore_variants: z.number().int().min(1).max(4),
  }),
  monitor_defaults: z.object({
    author_per_refresh: z.number().int().min(1).max(100),
    search_limit: z.number().int().min(1).max(50),
    search_temperature: z.number().min(0).max(1),
    recency_years: z.number().int().min(0).max(10),
    include_preprints: z.boolean(),
    semantic_scholar_bulk: z.boolean(),
  }),
  embedding_model: z.string(),
  recommendation_mode: z.string(),
})

type DiscoveryForm = z.infer<typeof discoverySchema>

// ---------------------------------------------------------------------------
// Card
// ---------------------------------------------------------------------------

export function DiscoveryWeightsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const discoveryQuery = useQuery({
    queryKey: ['discovery-settings'],
    queryFn: () => api.get<DiscoverySettings>('/discovery/settings'),
    retry: 1,
  })

  const form = useForm<DiscoveryForm>({
    resolver: zodResolver(discoverySchema),
    defaultValues: DEFAULT_DISCOVERY,
  })

  useEffect(() => {
    if (discoveryQuery.data) form.reset(mergeDiscoverySettings(discoveryQuery.data))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [discoveryQuery.data])

  const values = form.watch()
  // Not memoised on purpose: react-hook-form mutates `values.weights` in place,
  // so its reference never changes and a memo keyed on it kept the PRE-edit
  // total. Every slider's live "share" was then wrong until the next save
  // (0.9 over the old 1.1 read 82% instead of 47%). Nine numbers: just add.
  const weightSum = Object.values(values.weights).reduce((s, v) => s + (v ?? 0), 0)

  const discSaveMutation = useMutation({
    mutationFn: (data: DiscoveryForm) => api.put<DiscoverySettings>('/discovery/settings', data),
    onSuccess: (data) => {
      const merged = mergeDiscoverySettings(data)
      queryClient.setQueryData(['discovery-settings'], merged)
      // The backend re-measures after a weights change; show that, not the old number.
      void queryClient.invalidateQueries({ queryKey: RANKER_OUTCOME_KEY })
      form.reset(merged)
      toast({ title: 'Saved', description: 'Discovery settings saved.' })
    },
    onError: () => errorToast('Error', 'Failed to save discovery settings.'),
  })

  const discResetMutation = useMutation({
    mutationFn: () => api.post<DiscoverySettings>('/discovery/settings/reset'),
    onSuccess: (data) => {
      const merged = mergeDiscoverySettings(data)
      queryClient.setQueryData(['discovery-settings'], merged)
      // The backend re-measures after a weights change; show that, not the old number.
      void queryClient.invalidateQueries({ queryKey: RANKER_OUTCOME_KEY })
      form.reset(merged)
      toast({ title: 'Reset', description: 'Discovery settings restored to defaults.' })
    },
    onError: () => errorToast('Error', 'Failed to reset discovery settings.'),
  })

  const setValue = form.setValue
  const setWeight = useCallback(
    (key: keyof DiscoveryWeights, val: number) => {
      setValue(`weights.${key}` as const, Math.round(val * 100) / 100, { shouldDirty: true })
    },
    [setValue],
  )

  // The sliders are RELATIVE: the ranker rescales them to sum to 1 and applies
  // the recommendation mode's multipliers before any score is computed. This
  // badge used to warn whenever the raw numbers did not add to 1 — a rule the
  // backend never had. It now shows the one number a reader needs to interpret
  // any score: what an all-average paper scores under the saved weights.
  const savedReference = discoveryQuery.data?.reference_score
  const effective = discoveryQuery.data?.effective_weights ?? {}
  const headerStat = (
    <StatusBadge tone="neutral" size="sm" title="A paper that is average on every signal scores this under the saved weights. Read every score against it.">
      {savedReference != null ? `Typical paper scores ${Math.round(savedReference)}` : `Sum ${weightSum.toFixed(2)}`}
    </StatusBadge>
  )

  if (discoveryQuery.isLoading) {
    return (
      <SettingsCard icon={Sparkles} title="Discovery Settings">
        <p className="text-sm text-slate-500">Loading discovery settings...</p>
      </SettingsCard>
    )
  }

  if (discoveryQuery.isError) {
    return (
      <SettingsCard icon={Sparkles} title="Discovery Settings">
        <p className="flex items-center gap-2 text-sm text-critical-600">
          <AlertCircle className="h-4 w-4" /> Failed to load discovery settings.
        </p>
      </SettingsCard>
    )
  }

  return (
    <SettingsCard
      icon={Sparkles}
      title="Discovery Settings"
      description="Tune retrieval quality, source usage, branch behavior, and Feed monitor defaults from one control plane."
      roomy
    >
      <Form {...form}>
        <form
          className="space-y-6"
          onSubmit={form.handleSubmit((data) => discSaveMutation.mutate(data))}
        >
          <SettingsSections>
            {/* Recommendation mode */}
            <SettingsSection title="Recommendation Mode" defaultOpen>
              <RadioGroup
                value={values.recommendation_mode}
                onValueChange={(value) => form.setValue('recommendation_mode', value, { shouldDirty: true })}
                className="grid gap-2 lg:grid-cols-3"
              >
                {RECOMMENDATION_MODES.map((option) => (
                  <OptionCard
                    key={option.value}
                    value={option.value}
                    selected={values.recommendation_mode === option.value}
                    title={option.label}
                    description={option.description}
                  />
                ))}
              </RadioGroup>
            </SettingsSection>

            {/* Signal weights — sliders so the 0-1 budget is visual */}
            <SettingsSection title="Signal Weights" trailing={headerStat}>
              <RankerOutcomeLine />
              <div className="grid gap-4 lg:grid-cols-2">
                {WEIGHT_LABELS.map((item) => (
                  <WeightSlider
                    key={item.key}
                    label={item.label}
                    description={item.description}
                    value={values.weights[item.key] ?? 0}
                    share={
                      weightSum > 0
                        ? (values.weights[item.key] ?? 0) / weightSum
                        : undefined
                    }
                    savedShare={item.families.reduce(
                      (sum, family) => sum + (effective[family] ?? 0),
                      0,
                    )}
                    onChange={(v) => setWeight(item.key, v)}
                  />
                ))}
              </div>
              <p className="mt-3 text-xs text-slate-500">
                Weights are relative: the ranker rescales them to sum to 1, and the
                Explore / Exploit modes multiply some of them on top. "Share" is what
                each slider actually gets. Changes apply the next time a lens is
                refreshed — already-ranked decks keep the scores they were given.
              </p>
              <p className="mt-2 text-xs text-slate-500">
                The defaults are fitted to outcomes: they are the weights that best
                ranked papers later kept above papers rejected and above the rest of
                the corpus. Topic, Author and Recency start at 0 because they did not
                help there — most of a monitored corpus is already by authors you
                follow. Raise them if your library behaves differently.
              </p>
            </SettingsSection>

            {/* Retrieval strategies — 12 toggles, all-on is the expected
                default. Collapsed so the weights view stays the first thing
                users see. */}
            <SettingsSection title="Retrieval Strategies" defaultOpen={false}>
              <div className="grid gap-3 lg:grid-cols-2">
                {STRATEGY_LABELS.map((item) => (
                  <ToggleRow
                    key={item.key}
                    title={item.label}
                    description={item.description}
                    checked={!!values.strategies[item.key]}
                    onCheckedChange={(value) =>
                      form.setValue(`strategies.${item.key}` as const, value, { shouldDirty: true })
                    }
                  />
                ))}
              </div>
            </SettingsSection>

            {/* Source control */}
            <SettingsSection title="Source Control">
              <div className="grid gap-3">
                {SOURCE_LABELS.map((source) => (
                  <div key={source.key} className="rounded-sm border border-[var(--color-border)] p-3">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <p className="text-sm font-medium text-alma-800">{source.label}</p>
                        <p className="text-xs text-slate-500">{source.description}</p>
                      </div>
                      <div className="flex items-center gap-4">
                        <label className="flex items-center gap-2 text-sm text-slate-700">
                          <Checkbox
                            checked={values.sources[source.key].enabled}
                            onCheckedChange={(value) =>
                              form.setValue(
                                `sources.${source.key}.enabled` as const,
                                value === true,
                                { shouldDirty: true },
                              )
                            }
                          />
                          Enabled
                        </label>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </SettingsSection>

            {/* Branch tuning — advanced knobs that reshape the branch
                explorer's retrieval budget. Defaults are tuned; collapse
                so only power users see them. */}
            <SettingsSection title="Branch Behavior" defaultOpen={false}>
              <div className="space-y-3">
                <SettingsNumberField
                  label="Temperature"
                  description="Global default for branch exploration intensity."
                  value={values.branches.temperature}
                  min={0}
                  max={1}
                  step="0.01"
                  onChange={(v) => form.setValue('branches.temperature', v, { shouldDirty: true })}
                />
                <SettingsNumberField
                  label="Max Branches"
                  description="How many clusters to build from seed papers."
                  value={values.branches.max_clusters}
                  min={2}
                  max={12}
                  onChange={(v) => form.setValue('branches.max_clusters', v, { shouldDirty: true })}
                />
                <SettingsNumberField
                  label="Active Retrieval Branches"
                  description="How many active branches can consume retrieval budget."
                  value={values.branches.max_active_for_retrieval}
                  min={1}
                  max={12}
                  onChange={(v) =>
                    form.setValue('branches.max_active_for_retrieval', v, { shouldDirty: true })
                  }
                />
                <SettingsNumberField
                  label="Core Query Variants"
                  description="Query planner variants for core branch retrieval."
                  value={values.branches.query_core_variants}
                  min={1}
                  max={4}
                  onChange={(v) =>
                    form.setValue('branches.query_core_variants', v, { shouldDirty: true })
                  }
                />
                <SettingsNumberField
                  label="Explore Query Variants"
                  description="Query planner variants for exploratory branch retrieval."
                  value={values.branches.query_explore_variants}
                  min={1}
                  max={4}
                  onChange={(v) =>
                    form.setValue('branches.query_explore_variants', v, { shouldDirty: true })
                  }
                />
              </div>
            </SettingsSection>

            {/* Feed monitor defaults — apply to new monitors, not existing
                ones. Power-user tuning; collapse. */}
            <SettingsSection title="Feed Monitor Defaults" defaultOpen={false}>
              <div className="space-y-3">
                <SettingsNumberField
                  label="Author Papers Per Refresh"
                  description="How many recent papers to pull per monitored author."
                  value={values.monitor_defaults.author_per_refresh}
                  min={1}
                  max={100}
                  onChange={(v) =>
                    form.setValue('monitor_defaults.author_per_refresh', v, { shouldDirty: true })
                  }
                />
                <SettingsNumberField
                  label="Non-author Search Limit"
                  description="Candidate cap per topic and keyword monitor refresh."
                  value={values.monitor_defaults.search_limit}
                  min={1}
                  max={50}
                  onChange={(v) =>
                    form.setValue('monitor_defaults.search_limit', v, { shouldDirty: true })
                  }
                />
                <SettingsNumberField
                  label="Monitor Search Temperature"
                  description="How exploratory topic and keyword monitor search should be before Feed applies strict matching."
                  value={values.monitor_defaults.search_temperature}
                  min={0}
                  max={1}
                  step="0.01"
                  onChange={(v) =>
                    form.setValue('monitor_defaults.search_temperature', v, { shouldDirty: true })
                  }
                />
                <SettingsNumberField
                  label="Feed Recency Window (Years)"
                  description="Maximum paper age for Feed monitor refreshes. Feed stays recent even if full-history fetching is enabled elsewhere."
                  value={values.monitor_defaults.recency_years}
                  min={0}
                  max={10}
                  onChange={(v) =>
                    form.setValue('monitor_defaults.recency_years', v, { shouldDirty: true })
                  }
                />
                <ToggleRow
                  title="Include Preprints"
                  description="Allow arXiv and bioRxiv in topic and keyword monitor refreshes."
                  checked={!!values.monitor_defaults.include_preprints}
                  onCheckedChange={(value) =>
                    form.setValue('monitor_defaults.include_preprints', value, { shouldDirty: true })
                  }
                />
                <ToggleRow
                  title="Semantic Scholar Bulk Search"
                  description="Use the non-interactive bulk endpoint for monitor refreshes."
                  checked={!!values.monitor_defaults.semantic_scholar_bulk}
                  onCheckedChange={(value) =>
                    form.setValue('monitor_defaults.semantic_scholar_bulk', value, { shouldDirty: true })
                  }
                />
              </div>
            </SettingsSection>

            {/* Limits + schedule */}
            <SettingsSection title="Limits, Refresh & Cache" defaultOpen={false}>
              <div className="grid gap-6 lg:grid-cols-2">
                <div className="space-y-3">
                  <h5 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    Limits
                  </h5>
                  <SettingsNumberField
                    label="Max Results"
                    value={values.limits.max_results}
                    min={10}
                    max={200}
                    onChange={(v) => form.setValue('limits.max_results', v, { shouldDirty: true })}
                  />
                  <SettingsNumberField
                    label="Min Relevance Score"
                    description="Drop recommendations scoring below this (0-100). 0 keeps everything; raise it to hide weak, off-topic matches at the tail of the feed."
                    value={values.limits.min_score}
                    min={0}
                    max={100}
                    step={5}
                    onChange={(v) => form.setValue('limits.min_score', v, { shouldDirty: true })}
                  />
                  <SettingsNumberField
                    label="Max Candidates / Strategy"
                    value={values.limits.max_candidates_per_strategy}
                    min={5}
                    max={50}
                    onChange={(v) =>
                      form.setValue('limits.max_candidates_per_strategy', v, { shouldDirty: true })
                    }
                  />
                  <SettingsNumberField
                    label="Recency Window (Years)"
                    value={values.limits.recency_window_years}
                    min={1}
                    max={20}
                    onChange={(v) =>
                      form.setValue('limits.recency_window_years', v, { shouldDirty: true })
                    }
                  />
                  <SettingsNumberField
                    label="Full Feedback Decay (Days)"
                    value={values.limits.feedback_decay_days_full}
                    min={1}
                    max={3650}
                    onChange={(v) =>
                      form.setValue('limits.feedback_decay_days_full', v, { shouldDirty: true })
                    }
                  />
                  <SettingsNumberField
                    label="Half Feedback Decay (Days)"
                    value={values.limits.feedback_decay_days_half}
                    min={1}
                    max={3650}
                    onChange={(v) =>
                      form.setValue('limits.feedback_decay_days_half', v, { shouldDirty: true })
                    }
                  />
                </div>
                <div className="space-y-3">
                  <h5 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    Refresh &amp; Cache
                  </h5>
                  <ToggleRow
                    title="Auto-refresh recommendations"
                    description="Opt in to refresh Discovery in the background on the interval below. Off by default; runs without blocking the page."
                    checked={!!values.schedule.refresh_enabled}
                    onCheckedChange={(value) =>
                      form.setValue('schedule.refresh_enabled', value, { shouldDirty: true })
                    }
                  />
                  <SettingsNumberField
                    label="Auto-refresh Interval (Hours)"
                    description="How often to refresh Discovery when auto-refresh is enabled."
                    value={values.schedule.refresh_interval_hours}
                    min={0}
                    max={168}
                    onChange={(v) =>
                      form.setValue('schedule.refresh_interval_hours', v, { shouldDirty: true })
                    }
                  />
                  <SettingsNumberField
                    label="Graph Maintenance Interval (Hours)"
                    description="0 disables scheduled reference-edge backfill."
                    value={values.schedule.graph_maintenance_interval_hours}
                    min={0}
                    max={168}
                    onChange={(v) =>
                      form.setValue('schedule.graph_maintenance_interval_hours', v, {
                        shouldDirty: true,
                      })
                    }
                  />
                  <SettingsNumberField
                    label="Similarity Cache TTL (Hours)"
                    description="How long seeded similarity results stay cached."
                    value={values.cache.similarity_ttl_hours}
                    min={1}
                    max={168}
                    onChange={(v) =>
                      form.setValue('cache.similarity_ttl_hours', v, { shouldDirty: true })
                    }
                  />
                </div>
              </div>
            </SettingsSection>
          </SettingsSections>

          {/* Footer */}
          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[var(--color-border)] pt-4">
            <p className="text-sm text-slate-500">
              Discovery settings affect Discovery refresh and Feed monitor intake.
            </p>
            <div className="flex items-center gap-2">
              <AsyncButton
                type="button"
                variant="outline"
                icon={<RotateCcw className="h-4 w-4" />}
                pending={discResetMutation.isPending}
                disabled={discSaveMutation.isPending}
                onClick={() => discResetMutation.mutate()}
              >
                Reset Defaults
              </AsyncButton>
              <AsyncButton
                type="submit"
                icon={<Save className="h-4 w-4" />}
                pending={discSaveMutation.isPending}
                disabled={discResetMutation.isPending}
              >
                Save Discovery Settings
              </AsyncButton>
            </div>
          </div>
        </form>
      </Form>
    </SettingsCard>
  )
}

/**
 * Signal-weight row — slider for visual budgeting plus a right-aligned number
 * readout so power users can still type exact values. The slider/number pair
 * stays in lockstep because both drive the same `onChange` callback.
 */
function WeightSlider({
  label,
  description,
  value,
  share,
  savedShare,
  onChange,
}: {
  label: string
  description?: string
  value: number
  /** This slider's fraction of the current (unsaved) slider total. */
  share?: number
  /** The fraction the ranker gives it under the SAVED settings, mode applied. */
  savedShare?: number
  onChange: (value: number) => void
}) {
  const safeValue = Number.isFinite(value) ? value : 0
  const pct = (v: number | undefined) => (v == null ? null : `${Math.round(v * 100)}%`)
  return (
    <div className="space-y-1.5 rounded-sm border border-[var(--color-border)] p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="text-sm font-medium text-slate-800">
            {label}
            {share != null && (
              <span className="ml-2 font-mono text-[11px] font-normal text-slate-500" title="Share of the ranking this slider gets once all sliders are rescaled to sum to 1.">
                share {pct(share)}
                {savedShare != null && Math.abs(savedShare - share) > 0.005 && (
                  <span className="text-slate-400"> · saved {pct(savedShare)}</span>
                )}
              </span>
            )}
          </p>
          {description ? <p className="text-xs text-slate-500">{description}</p> : null}
        </div>
        <Input
          type="number"
          className="h-8 w-20 text-right"
          min={0}
          max={1}
          step="0.01"
          value={safeValue}
          onChange={(event) => onChange(Number(event.target.value))}
        />
      </div>
      <Slider
        value={[safeValue]}
        min={0}
        max={1}
        step={0.01}
        onValueChange={(next) => onChange(next[0] ?? 0)}
      />
    </div>
  )
}
