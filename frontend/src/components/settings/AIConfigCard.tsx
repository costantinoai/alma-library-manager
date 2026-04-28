import { useCallback, useEffect, useMemo, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  AlertCircle,
  Brain,
  Cloud,
  Cpu,
  Globe,
  Info,
  Loader2,
  Package,
  RefreshCw,
  Save,
  Trash2,
} from 'lucide-react'

import { api, type AIConfig, type AIStatus } from '@/api/client'
import {
  AsyncButton,
  KeyValueRow,
  OptionCard,
  PackageChip,
  SettingsCard,
  SettingsSection,
  SettingsSections,
  StatTile,
} from '@/components/settings/primitives'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Progress } from '@/components/ui/progress'
import { RadioGroup } from '@/components/ui/radio-group'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  StatusBadge,
  type StatusBadgeTone,
} from '@/components/ui/status-badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

type ComputeScope = 'missing' | 'stale' | 'missing_stale' | 'all'

type ComputeEmbeddingsResponse = {
  job_id: string
  status?: string
  message?: string
}

// ---------------------------------------------------------------------------
// Form schema — only fields the user can edit live here. Display-only state
// (capability tiers, feature readiness, dependencies) stays on the AI status
// query and is not part of the form.
// ---------------------------------------------------------------------------

const aiConfigSchema = z.object({
  provider: z.string(),
  local_model: z.string(),
  openai_api_key: z.string(),
  python_env_path: z.string(),
})

type AIConfigForm = z.infer<typeof aiConfigSchema>

const DEFAULT_FORM: AIConfigForm = {
  provider: 'none',
  local_model: 'specter2-base',
  openai_api_key: '',
  python_env_path: '',
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function featureStatusTone(status: string): StatusBadgeTone {
  if (status === 'ready') return 'positive'
  if (status === 'available') return 'info'
  if (status === 'fallback') return 'warning'
  if (status === 'blocked') return 'negative'
  return 'neutral'
}

function ProviderIcon({ icon, className = 'h-4 w-4 text-slate-500' }: { icon?: string; className?: string }) {
  if (icon === 'cpu') return <Cpu className={className} />
  if (icon === 'cloud') return <Cloud className={className} />
  if (icon === 'globe') return <Globe className={className} />
  return <Brain className={className} />
}

function TierHint({ items }: { items: string[] }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          className="inline-flex items-center text-slate-400 transition-colors hover:text-slate-600"
          aria-label="What does this enable?"
        >
          <Info className="h-3 w-3" />
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs bg-alma-chrome p-2.5 text-slate-700 shadow-lg">
        <p className="mb-1 text-xs font-semibold text-slate-700">Enables:</p>
        <ul className="list-inside list-disc space-y-0.5 text-xs text-slate-600">
          {items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </TooltipContent>
    </Tooltip>
  )
}

// ---------------------------------------------------------------------------
// Card
// ---------------------------------------------------------------------------

export function AIConfigCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [embeddingJobId, setEmbeddingJobId] = useState<string | null>(null)
  const [s2FetchJobId, setS2FetchJobId] = useState<string | null>(null)
  const activeEmbeddingJobId = embeddingJobId || s2FetchJobId

  const aiStatusQuery = useQuery({
    queryKey: ['ai-status'],
    queryFn: () => api.get<AIStatus>('/ai/status'),
    staleTime: 0,
    refetchInterval: activeEmbeddingJobId ? 4000 : false,
  })

  const embeddingOpsQuery = useQuery({
    queryKey: ['activity-operations', 'ai-embeddings', activeEmbeddingJobId],
    queryFn: () =>
      api.get<Array<{ job_id: string; status: string; operation_key?: string; message?: string }>>(
        '/activity',
      ),
    enabled: Boolean(activeEmbeddingJobId),
    staleTime: 0,
    refetchInterval: activeEmbeddingJobId ? 3000 : false,
  })

  const form = useForm<AIConfigForm>({
    resolver: zodResolver(aiConfigSchema),
    defaultValues: DEFAULT_FORM,
    mode: 'onBlur',
  })
  const values = form.watch()

  // Hydrate form state from the server once the AI status arrives. We skip
  // hydrating secrets (API keys) because the backend never echoes them back,
  // and overwriting user input with "" would wipe a just-typed key.
  useEffect(() => {
    const status = aiStatusQuery.data
    if (!status) return
    const activeProvider = status.providers.find((p) => p.active)
    const tierProvider = status.capability_tiers?.tier1_embeddings?.active_provider
    form.reset({
      provider: activeProvider?.name ?? tierProvider ?? 'none',
      local_model: status.local_model ?? 'specter2-base',
      openai_api_key: form.getValues('openai_api_key') ?? '',
      python_env_path: status.dependency_environment?.path ?? '',
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aiStatusQuery.data])

  // ── Mutations ───────────────────────────────────────────────────────────

  const aiConfigMutation = useMutation({
    mutationFn: (config: AIConfig) => api.post<AIStatus>('/ai/configure', config),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['ai-status'])
      toast({ title: 'AI settings updated' })
    },
    onError: () => errorToast('Failed to update AI settings'),
  })

  const confirmEnvPathMutation = useMutation({
    mutationFn: (path: string) =>
      api.post<AIStatus>('/ai/configure', { python_env_path: path }),
    onSuccess: async (data) => {
      await invalidateQueries(queryClient, ['ai-status'])
      form.setValue('python_env_path', data.dependency_environment?.path ?? '', {
        shouldDirty: false,
      })
      toast({ title: 'Dependency environment updated' })
    },
    onError: () => errorToast('Invalid dependency environment'),
  })

  const recheckEnvironmentMutation = useMutation({
    mutationFn: () => api.post<AIStatus>('/ai/recheck-environment'),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['ai-status'])
      toast({ title: 'AI environment rechecked' })
    },
    onError: () => errorToast('Failed to recheck AI environment'),
  })

  const computeEmbeddingsMutation = useMutation({
    mutationFn: (scope: ComputeScope = 'missing_stale') =>
      api.post<ComputeEmbeddingsResponse>(`/ai/compute-embeddings?scope=${scope}`),
    onSuccess: async (data) => {
      if (data.job_id) setEmbeddingJobId(data.job_id)
      await invalidateQueries(queryClient, ['ai-status'], ['activity-operations'])
      toast({
        title: data.status === 'noop' ? 'Embedding run not started' : 'Embedding run submitted',
        description: data.message || `Job: ${data.job_id.slice(0, 8)}...`,
      })
    },
    onError: () => errorToast('Failed to start embedding computation'),
  })

  const deleteInactiveEmbeddingsMutation = useMutation({
    mutationFn: () =>
      api.delete<{ status: string; active_model: string; deleted: number; message: string }>(
        '/ai/embeddings/inactive',
      ),
    onSuccess: async (data) => {
      await invalidateQueries(queryClient, ['ai-status'], ['insights'], ['insights-diagnostics'])
      toast({ title: 'Inactive vectors deleted', description: data.message })
    },
    onError: () => errorToast('Failed to delete inactive vectors'),
  })

  const backfillS2VectorsMutation = useMutation({
    mutationFn: () => api.post<ComputeEmbeddingsResponse>('/ai/backfill-s2-vectors'),
    onSuccess: async (data) => {
      if (data.job_id) setS2FetchJobId(data.job_id)
      await invalidateQueries(queryClient, ['ai-status'], ['activity-operations'])
      toast({
        title: 'S2 vector fetch submitted',
        description: data.message || `Job: ${data.job_id.slice(0, 8)}...`,
      })
    },
    onError: () => errorToast('Failed to start S2 vector fetch'),
  })

  // Close out job tracking when the background operation finishes.
  useEffect(() => {
    if (!embeddingJobId || !embeddingOpsQuery.data) return
    const op = embeddingOpsQuery.data.find((item) => item.job_id === embeddingJobId)
    if (!op) return
    if (['queued', 'running', 'cancelling'].includes(op.status)) return
    setEmbeddingJobId(null)
    void invalidateQueries(queryClient, ['ai-status'], ['insights-diagnostics'])
    toast({
      title: op.status === 'completed' ? 'Embedding run completed' : 'Embedding run finished',
      description: op.message || 'Embedding coverage has been refreshed.',
      variant: op.status === 'failed' ? 'destructive' : 'default',
    })
  }, [embeddingJobId, embeddingOpsQuery.data, queryClient, toast])

  useEffect(() => {
    if (!s2FetchJobId || !embeddingOpsQuery.data) return
    const op = embeddingOpsQuery.data.find((item) => item.job_id === s2FetchJobId)
    if (!op) return
    if (['queued', 'running', 'cancelling'].includes(op.status)) return
    setS2FetchJobId(null)
    void invalidateQueries(queryClient, ['ai-status'], ['insights-diagnostics'])
    toast({
      title: op.status === 'completed' ? 'S2 vector fetch completed' : 'S2 vector fetch finished',
      description: op.message || 'Downloaded vector coverage has been refreshed.',
      variant: op.status === 'failed' ? 'destructive' : 'default',
    })
  }, [s2FetchJobId, embeddingOpsQuery.data, queryClient, toast])

  const missingDependencies = useMemo(() => {
    if (!aiStatusQuery.data) return []
    return Object.entries(aiStatusQuery.data.dependencies)
      .filter(([, info]) => !info.installed || info.runtime_importable === false)
      .map(([pkg]) => pkg)
  }, [aiStatusQuery.data])

  const dependencyEnv = aiStatusQuery.data?.dependency_environment
  const savedDependencyPath = (dependencyEnv?.path ?? '').trim()

  const confirmDependencyPath = useCallback(() => {
    const nextPath = (values.python_env_path ?? '').trim()
    if (nextPath === savedDependencyPath) return
    if (confirmEnvPathMutation.isPending) return
    confirmEnvPathMutation.mutate(nextPath)
  }, [values.python_env_path, savedDependencyPath, confirmEnvPathMutation])

  const onSubmit = form.handleSubmit((data) => {
    aiConfigMutation.mutate({
      provider: data.provider,
      local_model: data.provider === 'local' ? data.local_model : undefined,
      openai_api_key: data.openai_api_key || undefined,
      python_env_path: (data.python_env_path ?? '').trim(),
    })
  })

  if (aiStatusQuery.isLoading) {
    return (
      <SettingsCard icon={Brain} title="AI & Embeddings">
        <p className="flex items-center gap-2 text-sm text-slate-500">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading AI status...
        </p>
      </SettingsCard>
    )
  }

  if (aiStatusQuery.isError || !aiStatusQuery.data) {
    return (
      <SettingsCard icon={Brain} title="AI & Embeddings">
        <p className="flex items-center gap-2 text-sm text-red-600">
          <AlertCircle className="h-4 w-4" /> AI endpoints not available.
        </p>
      </SettingsCard>
    )
  }

  const status = aiStatusQuery.data
  const tier1 = status.capability_tiers?.tier1_embeddings
  const coveragePct = status.embeddings.coverage_pct ?? 0
  const selectedLocal = status.providers.find((p) => p.name === 'local')
  const selectedLocalModels = selectedLocal?.local_models ?? []

  return (
    <TooltipProvider>
      <SettingsCard
        icon={Brain}
        title="AI & Embeddings"
        description="Configure AI providers for semantic search, clustering, and auto-tagging. All AI features are optional."
        roomy
      >
        <form className="space-y-6" onSubmit={onSubmit}>
          <SettingsSections>
            {/* Capability tier */}
            <SettingsSection title="Capability" defaultOpen>
              <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-1.5">
                    <span className="text-xs font-semibold text-slate-700">
                      Embeddings
                    </span>
                    <TierHint
                      items={[
                        'Semantic search',
                        'Discovery vector channel',
                        'Graph visualizations',
                      ]}
                    />
                  </div>
                  <StatusBadge tone={tier1?.ready ? 'positive' : 'warning'} size="sm">
                    {tier1?.ready ? 'Ready' : 'Not ready'}
                  </StatusBadge>
                </div>
                <p className="mt-1 text-xs text-slate-500">
                  Provider: {tier1?.active_provider ?? 'none'}
                </p>
              </div>
            </SettingsSection>

            {/* AI feature map */}
            <SettingsSection
              title="AI Feature Map"
              trailing={
                status.features?.summary ? (
                  <span className="text-xs text-slate-500">
                    {status.features.summary.ready ?? 0}/{status.features.summary.total ?? 0} ready
                  </span>
                ) : null
              }
              defaultOpen={false}
            >
              <div className="grid gap-2 lg:grid-cols-2">
                {(status.features?.groups ?? []).map((group) => (
                  <div key={group.id} className="rounded-sm border border-[var(--color-border)] bg-alma-paper p-3 shadow-paper-sm">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                      {group.label}
                    </p>
                    <div className="mt-2 space-y-2">
                      {group.items.map((feature) => (
                        <div key={feature.id} className="grid grid-cols-[1fr_auto] gap-2">
                          <div className="min-w-0">
                            <p className="truncate text-sm font-medium text-slate-800">
                              {feature.label}
                            </p>
                            <p className="line-clamp-2 text-xs text-slate-500">{feature.detail}</p>
                          </div>
                          <StatusBadge tone={featureStatusTone(feature.status)} size="sm">
                            {feature.status}
                          </StatusBadge>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </SettingsSection>

            {/* Embedding provider */}
            <SettingsSection title="Embedding Provider">
              <RadioGroup
                value={values.provider}
                onValueChange={(value) => form.setValue('provider', value, { shouldDirty: true })}
                className="grid gap-2"
              >
                <OptionCard
                  value="none"
                  selected={values.provider === 'none'}
                  title="Disabled"
                  description="No live embedding compute. Downloaded vectors and lexical ranking still work."
                />
                {status.providers.map((p) => {
                  const dimension =
                    p.name === 'local'
                      ? (selectedLocalModels.find((m) => m.key === values.local_model)?.dimension ??
                          p.dimension) + 'd'
                      : p.dimension + 'd'
                  return (
                    <OptionCard
                      key={p.name}
                      value={p.name}
                      selected={values.provider === p.name}
                      disabled={!p.available}
                      icon={<ProviderIcon icon={p.icon} />}
                      title={p.display_name ?? p.name}
                      meta={
                        <>
                          <span className="text-xs text-slate-400">{dimension}</span>
                          <StatusBadge tone={p.available ? 'positive' : 'negative'} size="sm">
                            {p.available ? 'Available' : 'Not available'}
                          </StatusBadge>
                        </>
                      }
                      description={p.description ?? undefined}
                    >
                      {p.model_display_name && (
                        <span className="mt-1 block text-xs text-slate-400">
                          Model: {p.model_display_name}
                        </span>
                      )}
                      {!p.available && p.reason && (
                        <span className="mt-1 block text-xs text-slate-500">{p.reason}</span>
                      )}
                    </OptionCard>
                  )
                })}
              </RadioGroup>

              {values.provider === 'local' && selectedLocal?.local_models && (
                <LocalModelTier
                  models={selectedLocal.local_models}
                  selected={values.local_model}
                  savedSelected={selectedLocal.selected_model ?? 'specter2-base'}
                  onChange={(next) => form.setValue('local_model', next, { shouldDirty: true })}
                />
              )}

              {values.provider === 'openai' && (
                <LabeledField label="OpenAI API Key">
                  <Input
                    type="password"
                    placeholder="Paste OpenAI API key"
                    value={values.openai_api_key}
                    onChange={(e) =>
                      form.setValue('openai_api_key', e.target.value, { shouldDirty: true })
                    }
                  />
                </LabeledField>
              )}
            </SettingsSection>

            {/* Embedding coverage */}
            <SettingsSection title="Embedding Coverage">
              <div className="space-y-3">
                <Alert variant="info" className="px-3 py-2">
                  <Package className="h-4 w-4" />
                  <AlertTitle className="text-xs">Downloaded vectors</AlertTitle>
                  <AlertDescription className="text-xs">
                  <p className="mt-1">
                    Semantic Scholar: {status.embeddings.downloaded_total ?? 0} papers (
                    {(status.embeddings.downloaded_coverage_pct ?? 0).toFixed(1)}%)
                  </p>
                  <p className="mt-1">
                    Local SPECTER2 fill: {status.embeddings.local_total ?? 0} papers (
                    {(status.embeddings.local_coverage_pct ?? 0).toFixed(1)}%)
                  </p>
                  {(status.embeddings.unknown_total ?? 0) > 0 ? (
                    <p className="mt-1">
                      Unknown provenance: {status.embeddings.unknown_total ?? 0} papers (
                      {(status.embeddings.unknown_coverage_pct ?? 0).toFixed(1)}%)
                    </p>
                  ) : null}
                  <p className="mt-1">
                    Canonical SPECTER2 coverage: {status.embeddings.canonical_total ?? 0} papers (
                    {(status.embeddings.canonical_coverage_pct ?? 0).toFixed(1)}%)
                  </p>
                  {status.embeddings.s2_backfill ? (
                    <>
                      <p className="mt-1">
                        S2 fetch queue: {status.embeddings.s2_backfill.total_missing} · fetchable
                        now: {status.embeddings.s2_backfill.eligible_missing} · need DOI/S2 ID:{' '}
                        {status.embeddings.s2_backfill.ineligible_missing}
                      </p>
                      <p className="mt-1">
                        Local SPECTER2 can fill:{' '}
                        {status.embeddings.s2_backfill.local_compute_candidates ??
                          status.embeddings.missing ??
                          0}{' '}
                        · S2 no-match: {status.embeddings.s2_backfill.terminal_unmatched ?? 0} · S2
                        no-vector: {status.embeddings.s2_backfill.terminal_missing_vector ?? 0}
                      </p>
                    </>
                  ) : null}
                  </AlertDescription>
                </Alert>
                <Alert variant="warning" className="px-3 py-2">
                  <Cpu className="h-4 w-4" />
                  <AlertTitle className="text-xs">Manual AI compute</AlertTitle>
                  <AlertDescription className="text-xs">
                    Local embedding compute can use heavy CPU/GPU. It runs only when you press
                    an AI compute button and is tracked in Activity.
                  </AlertDescription>
                </Alert>

                <div className="flex items-center gap-3">
                  <Progress
                    value={coveragePct}
                    className="h-2 flex-1 bg-parchment-100 [&>*]:bg-alma-500"
                  />
                  <span className="font-mono text-xs text-slate-600">
                    {status.embeddings.total} papers ({coveragePct.toFixed(1)}%)
                  </span>
                </div>

                <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
                  <StatTile
                    label="Corpus up-to-date"
                    value={status.embeddings.up_to_date ?? 0}
                    tone="positive"
                  />
                  <StatTile
                    label="Corpus missing"
                    value={status.embeddings.missing ?? 0}
                    tone="warning"
                  />
                  <StatTile label="Stale" value={status.embeddings.stale ?? 0} />
                </div>
                {status.embeddings.coverage_by_status?.library ? (
                  <div className="rounded-md border border-[var(--color-border)] bg-parchment-50 px-2 py-1.5 text-xs text-slate-600">
                    Library: {status.embeddings.coverage_by_status.library.up_to_date}/
                    {status.embeddings.coverage_by_status.library.total} active-model vectors (
                    {status.embeddings.coverage_by_status.library.missing} missing)
                  </div>
                ) : null}
                {(status.embeddings.models ?? []).length > 0 ? (
                  <div className="space-y-1">
                    {(status.embeddings.models ?? []).map((row) => (
                      <div
                        key={row.model}
                        className="grid grid-cols-[1fr_auto_auto_auto] items-center gap-2 rounded-md border border-[var(--color-border)] px-2 py-1.5 text-xs"
                      >
                        <span className="truncate font-mono text-slate-700">{row.model}</span>
                        <span className="text-slate-600">{row.vectors} vectors</span>
                        <span className="text-slate-500">{row.stale ?? 0} stale</span>
                        <span
                          className={row.active ? 'font-semibold text-alma-700' : 'text-slate-500'}
                        >
                          {row.active ? 'active' : `${row.coverage_pct.toFixed(1)}%`}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : null}

                <div className="flex flex-wrap items-center gap-3">
                  <AsyncButton
                    variant="outline"
                    size="sm"
                    icon={<Package className="h-4 w-4" />}
                    pending={backfillS2VectorsMutation.isPending || !!s2FetchJobId}
                    disabled={
                      !!activeEmbeddingJobId ||
                      (status.embeddings.s2_backfill?.eligible_missing ?? 1) === 0
                    }
                    onClick={() => backfillS2VectorsMutation.mutate()}
                  >
                    {s2FetchJobId ? 'S2 Fetch In Progress' : 'Fetch Missing S2 Vectors'}
                  </AsyncButton>
                  <AsyncButton
                    variant="outline"
                    size="sm"
                    icon={<Cpu className="h-4 w-4" />}
                    pending={computeEmbeddingsMutation.isPending || !!embeddingJobId}
                    disabled={values.provider === 'none' || !!activeEmbeddingJobId}
                    onClick={() => computeEmbeddingsMutation.mutate('missing')}
                  >
                    {embeddingJobId ? 'AI Compute In Progress' : 'AI Compute Missing'}
                  </AsyncButton>
                  <AlertDialog>
                    <AlertDialogTrigger asChild>
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={
                          deleteInactiveEmbeddingsMutation.isPending || !!embeddingJobId
                        }
                      >
                        {deleteInactiveEmbeddingsMutation.isPending ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Trash2 className="h-4 w-4" />
                        )}
                        Delete Inactive
                      </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent>
                      <AlertDialogHeader>
                        <AlertDialogTitle>Delete vectors for inactive models?</AlertDialogTitle>
                        <AlertDialogDescription>
                          Removes stored embeddings for every model except the currently active
                          provider. You can recompute them later with "AI Compute Missing".
                        </AlertDialogDescription>
                      </AlertDialogHeader>
                      <AlertDialogFooter>
                        <AlertDialogCancel>Cancel</AlertDialogCancel>
                        <AlertDialogAction
                          onClick={() => deleteInactiveEmbeddingsMutation.mutate()}
                          className="bg-red-600 text-white hover:bg-red-700"
                        >
                          Delete inactive
                        </AlertDialogAction>
                      </AlertDialogFooter>
                    </AlertDialogContent>
                  </AlertDialog>
                  {embeddingJobId ? (
                    <span className="text-xs text-slate-500">
                      Refreshing coverage while job{' '}
                      <span className="font-mono">{embeddingJobId.slice(0, 12)}</span> runs.
                    </span>
                  ) : null}
                </div>
              </div>
            </SettingsSection>

            {/* Dependencies — dense troubleshooting info; collapsed by default
                so the common case (capability tiers + providers + coverage)
                isn't buried under the site-packages diagnostics. */}
            <SettingsSection title="Dependencies" defaultOpen={false}>
              <div className="space-y-3">
                <div className="space-y-1">
                  <label className="text-xs font-medium text-slate-600">
                    Environment Folder or Python Executable
                  </label>
                  <div className="flex gap-2">
                    <Input
                      value={values.python_env_path ?? ''}
                      onChange={(e) =>
                        form.setValue('python_env_path', e.target.value, { shouldDirty: true })
                      }
                      onBlur={() => confirmDependencyPath()}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') {
                          e.preventDefault()
                          confirmDependencyPath()
                        }
                      }}
                      placeholder="Leave empty to use system environment, or set /path/to/environment"
                      disabled={confirmEnvPathMutation.isPending}
                    />
                    <AsyncButton
                      type="button"
                      variant="outline"
                      size="sm"
                      icon={<RefreshCw className="h-4 w-4" />}
                      pending={recheckEnvironmentMutation.isPending}
                      onClick={() => recheckEnvironmentMutation.mutate()}
                    >
                      Recheck
                    </AsyncButton>
                  </div>
                  <p className="text-xs text-slate-400">
                    Type is inferred automatically during validation. Leave empty to use the
                    current server environment.
                  </p>
                  {confirmEnvPathMutation.isPending && (
                    <p className="flex items-center gap-1 text-xs text-slate-500">
                      <Loader2 className="h-3 w-3 animate-spin" />
                      Validating environment and checking installed packages...
                    </p>
                  )}
                </div>

                {dependencyEnv && (
                  <div className="space-y-1 rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <StatusBadge
                        tone={dependencyEnv.valid ? 'positive' : 'negative'}
                        size="sm"
                      >
                        {dependencyEnv.valid ? 'Environment valid' : 'Environment invalid'}
                      </StatusBadge>
                      {dependencyEnv.using_fallback && (
                        <StatusBadge tone="warning" size="sm">
                          Using server fallback
                        </StatusBadge>
                      )}
                    </div>
                    {dependencyEnv.message && (
                      <p className="text-xs text-slate-600">{dependencyEnv.message}</p>
                    )}
                    {dependencyEnv.fallback_reason && (
                      <p className="text-xs text-amber-700">{dependencyEnv.fallback_reason}</p>
                    )}
                    {dependencyEnv.selected_python_executable && (
                      <KeyValueRow
                        label="Selected Python"
                        value={
                          <span className="font-mono text-[11px]">
                            {dependencyEnv.selected_python_executable}
                          </span>
                        }
                      />
                    )}
                    {dependencyEnv.backend_python_executable && (
                      <KeyValueRow
                        label="Backend Python"
                        value={
                          <span className="font-mono text-[11px]">
                            {dependencyEnv.backend_python_executable}
                          </span>
                        }
                      />
                    )}
                    {dependencyEnv.selected_python_version && (
                      <KeyValueRow
                        label="Env Python version"
                        value={dependencyEnv.selected_python_version}
                      />
                    )}
                    {dependencyEnv.python_version_match === false && (
                      <p className="text-xs text-amber-700">
                        Selected env Python differs from the backend runtime. Packages may be
                        installed but not importable until the backend is restarted from that env.
                      </p>
                    )}
                    {(dependencyEnv.active_site_packages?.length ?? 0) > 0 && (
                      <p className="text-xs text-slate-500">
                        Active site-packages paths: {dependencyEnv.active_site_packages?.length}
                      </p>
                    )}
                  </div>
                )}

                <div className="flex flex-wrap gap-2">
                  {Object.entries(status.dependencies).map(([pkg, info]) => {
                    const selectedOk = Boolean(info.installed)
                    const runtimeOk = info.runtime_importable !== false
                    const ready = selectedOk && runtimeOk
                    const tone: StatusBadgeTone = ready
                      ? 'positive'
                      : selectedOk
                        ? 'warning'
                        : 'neutral'
                    return (
                      <PackageChip
                        key={pkg}
                        tone={tone}
                        icon={<Package className="h-3 w-3" />}
                        label={pkg}
                        suffix={
                          <>
                            {info.version ? `v${info.version}` : null}
                            {selectedOk && !runtimeOk ? ' · runtime missing' : null}
                          </>
                        }
                        title={
                          selectedOk && !runtimeOk
                            ? 'Installed in selected environment but not importable by backend runtime'
                            : undefined
                        }
                      />
                    )
                  })}
                </div>
                <p className="text-xs text-slate-400">
                  {missingDependencies.length === 0
                    ? 'All dependency checks passed.'
                    : `${missingDependencies.length} package(s) missing in the active dependency environment.`}
                </p>
                {status.dependency_check_warning && (
                  <p className="text-xs text-amber-700">{status.dependency_check_warning}</p>
                )}
                {status.dependency_setup_suggestions &&
                  status.dependency_setup_suggestions.length > 0 && (
                    <ol className="list-inside list-decimal space-y-1 rounded-lg border border-alma-100 bg-alma-50 p-3 text-xs text-alma-900">
                      {status.dependency_setup_suggestions.map((step, idx) => (
                        <li key={`${idx}-${step}`} className="font-mono text-[11px]">
                          {step}
                        </li>
                      ))}
                    </ol>
                  )}
              </div>
            </SettingsSection>
          </SettingsSections>

          {/* Save */}
          <div className="flex justify-end">
            <AsyncButton
              type="submit"
              icon={<Save className="h-4 w-4" />}
              pending={aiConfigMutation.isPending}
            >
              Save AI Settings
            </AsyncButton>
          </div>
        </form>
      </SettingsCard>
    </TooltipProvider>
  )
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

function LabeledField({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <label className="text-xs font-medium text-slate-600">{label}</label>
      {children}
    </div>
  )
}

function LocalModelTier({
  models,
  selected,
  savedSelected,
  onChange,
}: {
  models: NonNullable<AIStatus['providers'][number]['local_models']>
  selected: string
  savedSelected: string
  onChange: (next: string) => void
}) {
  if (models.length <= 1) {
    const onlyModel = models[0]
    return (
      <div className="ml-6 rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3 text-xs text-slate-600">
        Local model:{' '}
        <span className="font-medium text-slate-800">
          {onlyModel?.display_name ?? 'SPECTER2 Base'}
        </span>
      </div>
    )
  }
  return (
    <div className="ml-6 space-y-2">
      <label className="text-xs font-medium text-slate-600">Model Tier</label>
      <RadioGroup value={selected} onValueChange={onChange} className="space-y-1.5">
        {models.map((m) => (
          <OptionCard
            key={m.key}
            value={m.key}
            selected={selected === m.key}
            title={m.display_name}
            meta={<span className="text-xs text-slate-400">{m.dimension}d</span>}
            description={m.description}
          />
        ))}
      </RadioGroup>
      {selected !== savedSelected && (
        <p className="flex items-center gap-1 text-xs text-amber-600">
          <AlertCircle className="h-3 w-3" />
          Changing model will re-compute all embeddings on next run.
        </p>
      )}
    </div>
  )
}
