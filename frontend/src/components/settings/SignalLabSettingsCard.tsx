import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { FlaskConical, Trash2 } from 'lucide-react'

import {
  getApiErrorMessage,
  getSignalLabEval,
  getSignalLabModel,
  getSignalLabSettings,
  purgeSignalLab,
  updateSignalLabSettings,
  type SignalLabHeadLimits,
  type SignalLabModelSummary,
  type SignalLabReplay,
  type SignalLabSettings,
  type SignalLabSettingsView,
} from '@/api/client'
import { SettingsCard } from '@/components/settings/primitives'
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
import { Button } from '@/components/ui/button'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { Switch } from '@/components/ui/switch'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

/** Mirrors the backend's default head bounds. Used only until the settings
 *  query lands; the served `limits` block replaces it and is the authority. */
const FALLBACK_LIMITS: SignalLabHeadLimits = {
  head_points_max: 10,
  head_points_default: 5,
}

/** Form seed before the server answers. Heads sit at the backend default
 *  (on, not off) so the form never shows a state the server would not. */
const DEFAULTS: SignalLabSettings = {
  enabled: true,
  region_offset_points: FALLBACK_LIMITS.head_points_default,
  utility_points: FALLBACK_LIMITS.head_points_default,
  author_offset_points: FALLBACK_LIMITS.head_points_default,
  venue_offset_points: FALLBACK_LIMITS.head_points_default,
  map_tint_strength: 0.45,
  ring_decay: 0.35,
  exploration_rate: 0.20,
  coverage_target: 20,
  refit_every_rounds: 5,
  holdout_percent: 15,
  override_min_votes: 3,
}

/** The GET view carries a read-only `limits` block the PUT rejects (422), so
 *  the form state is the bare settings and the limits live beside it. */
function splitSettingsView(view: SignalLabSettingsView): {
  settings: SignalLabSettings
  limits: SignalLabHeadLimits
} {
  const { limits, ...settings } = view
  return { settings, limits }
}

/** The replay clause of the status sentence: what the Lab heads at the
 *  current weights actually did to each lens's latest deck. An
 *  `insufficient_evidence` verdict shows its reason verbatim — it is not
 *  "nothing moved". */
function replayClause(replay: SignalLabReplay): string {
  if (replay.status !== 'ok') return `replay: ${replay.reason ?? 'insufficient evidence'}`
  const moved = replay.entered_top ?? '—'
  const shift = replay.mean_rank_displacement == null
    ? ''
    : `, mean rank shift ${replay.mean_rank_displacement.toFixed(1)}`
  return (
    `replay on ${replay.lenses_assessed} of ${replay.lenses_total} lenses moved ` +
    `${moved} papers into the top ${replay.top_n}${shift}`
  )
}

/** Readiness for everyday use; evaluation detail belongs in the disclosure. */
function statusSentence(
  model: SignalLabModelSummary | undefined,
  enabled: boolean,
): string {
  if (!enabled) return 'Switched off. Your previous answers are kept.'
  if (!model?.ready) return 'No fitted model yet — play rounds on Home.'
  const rounds = model.counts?.rounds ?? 0
  return `Learning from ${rounds} rounds.`
}

export function SignalLabSettingsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [form, setForm] = useState(DEFAULTS)
  const [advancedOpen, setAdvancedOpen] = useState(false)
  const [lastPurged, setLastPurged] = useState<number | null>(null)

  const settingsQuery = useQuery({
    queryKey: ['signal-lab', 'settings'],
    queryFn: getSignalLabSettings,
  })
  useEffect(() => {
    if (settingsQuery.data) setForm(splitSettingsView(settingsQuery.data).settings)
  }, [settingsQuery.data])
  const limits = settingsQuery.data?.limits ?? FALLBACK_LIMITS
  const headMax = limits.head_points_max
  const hasUnsavedChanges = settingsQuery.data != null && (
    Object.keys(form) as (keyof SignalLabSettings)[]
  ).some((key) => form[key] !== settingsQuery.data?.[key])
  const settingsUnavailable = !settingsQuery.data || settingsQuery.isError

  const modelQuery = useQuery({
    queryKey: ['signal-lab', 'model'],
    queryFn: getSignalLabModel,
    staleTime: 30_000,
  })
  const evalQuery = useQuery({
    queryKey: ['signal-lab', 'eval'],
    queryFn: getSignalLabEval,
    staleTime: 60_000,
    enabled: (modelQuery.data?.counts?.answered ?? 0) > 0,
  })

  const saveMutation = useMutation({
    mutationFn: (next: SignalLabSettings) => updateSignalLabSettings(next),
    onSuccess: async (saved) => {
      setForm(saved)
      await invalidateQueries(
        queryClient,
        ['signal-lab'],
        ['home'],
        ['home-brief'],
        ['graphs'],
      )
      toast({
        title: saved.enabled ? 'Signal Lab settings saved' : 'Signal Lab switched off',
        description: saved.enabled
          ? 'Retained signals are active.'
          : 'Rounds and model are retained but ignored.',
      })
    },
    onError: (error) => errorToast('Signal Lab settings were not saved', getApiErrorMessage(error)),
  })

  const purgeMutation = useMutation({
    mutationFn: purgeSignalLab,
    onSuccess: async (result) => {
      setLastPurged(result.rounds_deleted)
      await invalidateQueries(queryClient, ['signal-lab'], ['graphs'])
    },
    onError: (error) => errorToast('Signal Lab purge failed', getApiErrorMessage(error)),
  })

  const save = (next = form) => saveMutation.mutate(next)
  const update = <K extends keyof SignalLabSettings>(key: K, value: SignalLabSettings[K]) => {
    setForm((current) => ({ ...current, [key]: value }))
  }
  const holdout = evalQuery.data?.holdout
  const parity = evalQuery.data?.replay?.parity
  const percent = (value: number | null | undefined) => (
    value == null ? '—' : `${Math.round(value * 100)}%`
  )

  return (
    <SettingsCard
      icon={FlaskConical}
      title="Signal Lab"
      description="Optional comparisons that help ALMa learn which papers you prefer."
      action={
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">{settingsQuery.isPending ? 'Loading…' : settingsUnavailable ? 'Unavailable' : form.enabled ? 'On' : 'Off'}</span>
          <Switch
            checked={form.enabled}
            disabled={settingsUnavailable || saveMutation.isPending}
            onCheckedChange={(enabled) => {
              const next = { ...form, enabled }
              setForm(next)
              save(next)
            }}
            aria-label={`${form.enabled ? 'Disable' : 'Enable'} Signal Lab`}
          />
        </div>
      }
      footer={hasUnsavedChanges ? (
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-xs text-slate-500">
            Unsaved changes to advanced settings.
          </span>
          <Button
            size="sm"
            onClick={() => save()}
            disabled={settingsUnavailable || saveMutation.isPending}
          >
            {saveMutation.isPending ? 'Saving…' : 'Save Signal Lab'}
          </Button>
        </div>
      ) : undefined}
    >
      <div className="space-y-4">
        {settingsQuery.isError && (
          <p role="alert" className="text-xs text-critical-600">
            Settings unavailable: {getApiErrorMessage(settingsQuery.error)}
          </p>
        )}

        {/* Keep readiness and actionable failures visible when details close. */}
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
            <p>
              {settingsQuery.isPending ? 'Loading Signal Lab settings…'
                : settingsUnavailable ? 'Your Signal Lab settings could not be loaded yet.'
                  : modelQuery.isError ? 'Learning status is unavailable. Try again shortly.'
                  : modelQuery.isPending ? 'Loading learning status…'
                    : statusSentence(modelQuery.data, form.enabled)}
              {lastPurged != null ? ` Reset ${lastPurged} answers.` : ''}
            </p>
            {(settingsQuery.isError || modelQuery.isError) && (
              <Button
                variant="outline"
                size="sm"
                disabled={settingsQuery.isFetching || modelQuery.isFetching}
                onClick={() => {
                  void settingsQuery.refetch()
                  void modelQuery.refetch()
                }}
              >
                Retry loading Signal Lab
              </Button>
            )}
            {parity && parity.mismatched > 0 && (
              <StatusBadge
                tone="warning"
                size="sm"
                title="The ranker was asked to reproduce the scores it had stored and could not. That is ranker drift, not a property of the Lab."
              >
                Some scores could not be verified. See advanced details.
              </StatusBadge>
            )}
          </div>
        </div>

        <DisclosurePanel
          title="Advanced settings and evidence"
          description="Adjust learning, inspect its effects, or reset your answers."
          open={advancedOpen}
          onOpenChange={setAdvancedOpen}
        >
          <div className="space-y-5">
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="outline" size="sm" disabled={purgeMutation.isPending}>
                  <Trash2 className="h-4 w-4" />
                  Reset Signal Lab
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Reset Signal Lab answers?</AlertDialogTitle>
                  <AlertDialogDescription>
                    Deletes every answered round and the derived model. Enabled
                    state and knobs stay unchanged. Library, ratings, and ordinary
                    feedback remain untouched. This cannot be undone.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction onClick={() => purgeMutation.mutate()}>
                    Reset Signal Lab
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
            <section className="space-y-3">
              <EyebrowLabel tone="muted">Scoring weights</EyebrowLabel>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                <NumberField
                  id="lab-region-points"
                  label="Region nudge (points)"
                  value={form.region_offset_points}
                  min={0}
                  max={headMax}
                  step={0.5}
                  description={`Up to ${headMax} points in Discovery and Feed, scaled down by how much evidence backs the region. 0 switches this head off.`}
                  onChange={(value) => update('region_offset_points', value)}
                />
                <NumberField
                  id="lab-utility-points"
                  label="Utility nudge (points)"
                  value={form.utility_points}
                  min={0}
                  max={headMax}
                  step={0.5}
                  description={`Up to ${headMax} points, scaled by the amount of feedback. This is not a probability of being correct. 0 switches this head off.`}
                  onChange={(value) => update('utility_points', value)}
                />
                <NumberField
                  id="lab-author-points"
                  label="Author nudge (points)"
                  value={form.author_offset_points}
                  min={0}
                  max={headMax}
                  step={0.5}
                  description={`Folds into the author signal your Library already produces. Fitted from same-region comparisons only. Up to ${headMax} points.`}
                  onChange={(value) => update('author_offset_points', value)}
                />
                <NumberField
                  id="lab-venue-points"
                  label="Venue nudge (points)"
                  value={form.venue_offset_points}
                  min={0}
                  max={headMax}
                  step={0.5}
                  description={`Folds into the venue signal your Library already produces. Fitted from same-region comparisons only. Up to ${headMax} points.`}
                  onChange={(value) => update('venue_offset_points', value)}
                />
              </div>
            </section>

            {/* The tint is NOT a scoring weight — it never reaches a score. It
                colours the map at read time, and geometry is corpus-intrinsic:
                taste may tint what you see, never move where a paper sits. */}
            <section className="space-y-3">
              <EyebrowLabel tone="muted">Map</EyebrowLabel>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                <NumberField
                  id="lab-map-tint"
                  label="Map taste tint"
                  value={form.map_tint_strength}
                  min={0}
                  max={1}
                  step={0.05}
                  description="Read-time terrain colour only. Never moves a paper's position and never changes a score."
                  onChange={(value) => update('map_tint_strength', value)}
                />
              </div>
            </section>

            <section className="space-y-3">
              <EyebrowLabel tone="muted">Sampler</EyebrowLabel>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                <NumberField id="lab-ring-decay" label="Ring decay γ" value={form.ring_decay} min={0.01} max={1} step={0.05} onChange={(value) => update('ring_decay', value)} />
                <NumberField id="lab-exploration" label="Exploration ε" value={form.exploration_rate} min={0} max={1} step={0.05} onChange={(value) => update('exploration_rate', value)} />
                <NumberField id="lab-coverage" label="Coverage target" value={form.coverage_target} min={1} max={500} step={1} integer onChange={(value) => update('coverage_target', value)} />
                <NumberField id="lab-refit" label="Refit every rounds" value={form.refit_every_rounds} min={1} max={100} step={1} integer onChange={(value) => update('refit_every_rounds', value)} />
                <NumberField id="lab-holdout" label="Holdout (%)" value={form.holdout_percent} min={0} max={50} step={1} integer onChange={(value) => update('holdout_percent', value)} />
                <NumberField id="lab-override-votes" label="Override votes" value={form.override_min_votes} min={1} max={100} step={1} integer onChange={(value) => update('override_min_votes', value)} />
              </div>
            </section>

            <section className="space-y-1.5">
              <EyebrowLabel tone="muted">Evidence</EyebrowLabel>
              <p className="text-xs text-slate-500">
                {evalQuery.isError ? 'Evaluation is unavailable. Try again shortly.'
                  : evalQuery.data?.replay ? replayClause(evalQuery.data.replay)
                    : 'Evaluation appears when enough information is available.'}
              </p>
              {parity && parity.mismatched > 0 && (
                <p className="text-xs text-critical-600">
                  {parity.mismatched} of {parity.checked} stored scores could not be reproduced.
                </p>
              )}
              <p className="text-xs text-slate-500">
                {modelQuery.data?.ready
                  ? `Holdout accuracy — prior ${percent(holdout?.prior_accuracy)} · regions ${percent(holdout?.offsets_accuracy)} · utility ${percent(holdout?.utility_accuracy)}`
                  : 'Holdout accuracy appears once a model is fitted.'}
              </p>
            </section>
          </div>
        </DisclosurePanel>
      </div>
    </SettingsCard>
  )
}

function NumberField({
  id,
  label,
  value,
  min,
  max,
  step,
  integer = false,
  description,
  onChange,
}: {
  id: string
  label: string
  value: number
  min: number
  max: number
  step: number
  integer?: boolean
  description?: string
  onChange: (value: number) => void
}) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="number"
        value={value}
        min={min}
        max={max}
        step={step}
        onChange={(event) => {
          const parsed = integer
            ? Number.parseInt(event.target.value, 10)
            : Number.parseFloat(event.target.value)
          onChange(Number.isFinite(parsed) ? parsed : min)
        }}
      />
      {description && <p className="text-xs text-slate-500">{description}</p>}
    </div>
  )
}
