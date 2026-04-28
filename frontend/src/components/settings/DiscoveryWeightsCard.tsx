import { useCallback, useEffect, useMemo } from 'react'
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
import { Checkbox } from '@/components/ui/checkbox'
import { Form } from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { RadioGroup } from '@/components/ui/radio-group'
import { Slider } from '@/components/ui/slider'
import { StatusBadge } from '@/components/ui/status-badge'
import { useToast, errorToast } from '@/hooks/useToast'

// ---------------------------------------------------------------------------
// Label tables — same taxonomy as before, but consumed by the primitives.
// ---------------------------------------------------------------------------

const WEIGHT_LABELS: { key: keyof DiscoveryWeights; label: string; description: string }[] = [
  { key: 'source_relevance', label: 'Source Relevance', description: 'Position in retrieval results (1st = highest)' },
  { key: 'topic_score', label: 'Topic Score', description: 'Topic overlap with your rated papers' },
  { key: 'text_similarity', label: 'Text Similarity', description: 'Semantic similarity to your top-rated papers' },
  { key: 'author_affinity', label: 'Author Affinity', description: 'Author overlap with papers you follow' },
  { key: 'journal_affinity', label: 'Journal Affinity', description: 'Published in a journal you read' },
  { key: 'recency_boost', label: 'Recency Boost', description: 'Publication recency (newer = higher)' },
  { key: 'citation_quality', label: 'Citation Quality', description: 'Citation count quality indicator' },
  { key: 'feedback_adj', label: 'Feedback Adjustment', description: 'Adjusted based on your past feedback' },
  { key: 'preference_affinity', label: 'Preference Affinity', description: 'Affinity learned from your accumulated feedback interactions' },
  { key: 'usefulness_boost', label: 'Usefulness Boost', description: 'Rewards timely, credible, and less redundant papers' },
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
]

const RECOMMENDATION_MODES: Array<{ value: string; label: string; description: string }> = [
  { value: 'explore', label: 'Explore', description: 'Increase novelty and recency.' },
  { value: 'balanced', label: 'Balanced', description: 'Keep familiarity and novelty in balance.' },
  { value: 'exploit', label: 'Exploit', description: 'Lean harder on proven taste and continuity.' },
]

// ---------------------------------------------------------------------------
// Defaults + shape hydration.
// ---------------------------------------------------------------------------

const DEFAULT_DISCOVERY: DiscoverySettings = {
  weights: {
    source_relevance: 0.15,
    topic_score: 0.2,
    text_similarity: 0.2,
    author_affinity: 0.15,
    journal_affinity: 0.05,
    recency_boost: 0.1,
    citation_quality: 0.05,
    feedback_adj: 0.1,
    preference_affinity: 0.1,
    usefulness_boost: 0.06,
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
    max_candidates_per_strategy: 20,
    recency_window_years: 10,
    feedback_decay_days_full: 90,
    feedback_decay_days_half: 180,
  },
  schedule: {
    refresh_interval_hours: 0,
    graph_maintenance_interval_hours: 24,
  },
  cache: {
    similarity_ttl_hours: 24,
  },
  sources: {
    openalex: { enabled: true, weight: 1.0 },
    semantic_scholar: { enabled: true, weight: 0.95 },
    crossref: { enabled: true, weight: 0.72 },
    arxiv: { enabled: true, weight: 0.66 },
    biorxiv: { enabled: true, weight: 0.62 },
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
const sourcePolicy = z.object({ enabled: z.boolean(), weight: z.number().min(0).max(2.5) })
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
    usefulness_boost: weightShape,
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
    max_candidates_per_strategy: z.number().int().min(5).max(50),
    recency_window_years: z.number().int().min(1).max(20),
    feedback_decay_days_full: z.number().int().min(1).max(3650),
    feedback_decay_days_half: z.number().int().min(1).max(3650),
  }),
  schedule: z.object({
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
  const weightSum = useMemo(
    () => Object.values(values.weights).reduce((s, v) => s + (v ?? 0), 0),
    [values.weights],
  )
  const weightsBalanced = Math.abs(weightSum - 1.0) < 0.02

  const discSaveMutation = useMutation({
    mutationFn: (data: DiscoveryForm) => api.put<DiscoverySettings>('/discovery/settings', data),
    onSuccess: (data) => {
      const merged = mergeDiscoverySettings(data)
      queryClient.setQueryData(['discovery-settings'], merged)
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

  const headerStat = (
    <StatusBadge tone={weightsBalanced ? 'positive' : 'warning'} size="sm">
      Sum {weightSum.toFixed(2)}
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
        <p className="flex items-center gap-2 text-sm text-red-600">
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
              <div className="grid gap-4 lg:grid-cols-2">
                {WEIGHT_LABELS.map((item) => (
                  <WeightSlider
                    key={item.key}
                    label={item.label}
                    description={item.description}
                    value={values.weights[item.key] ?? 0}
                    onChange={(v) => setWeight(item.key, v)}
                  />
                ))}
              </div>
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
                        <div className="flex items-center gap-2">
                          <span className="text-xs uppercase tracking-wide text-slate-500">
                            Weight
                          </span>
                          <Input
                            type="number"
                            className="h-9 w-24 text-right"
                            min={0}
                            max={2.5}
                            step="0.05"
                            value={values.sources[source.key].weight}
                            onChange={(event) =>
                              form.setValue(
                                `sources.${source.key}.weight` as const,
                                Number(event.target.value),
                                { shouldDirty: true },
                              )
                            }
                          />
                        </div>
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
                  <SettingsNumberField
                    label="Auto-refresh Interval (Hours)"
                    description="0 disables automatic Discovery refresh."
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
          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 pt-4">
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
  onChange,
}: {
  label: string
  description?: string
  value: number
  onChange: (value: number) => void
}) {
  const safeValue = Number.isFinite(value) ? value : 0
  return (
    <div className="space-y-1.5 rounded-sm border border-[var(--color-border)] p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="text-sm font-medium text-slate-800">{label}</p>
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
