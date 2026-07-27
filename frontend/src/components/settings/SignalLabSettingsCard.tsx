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
  type SignalLabSettings,
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
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

const DEFAULTS: SignalLabSettings = {
  enabled: true,
  region_offset_points: 0,
  utility_points: 0,
  author_offset_points: 0,
  map_tint_strength: 0.45,
  ring_decay: 0.35,
  exploration_rate: 0.20,
  coverage_target: 20,
  refit_every_rounds: 5,
  holdout_percent: 15,
  override_min_votes: 3,
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
    if (settingsQuery.data) setForm(settingsQuery.data)
  }, [settingsQuery.data])

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
  const rounds = modelQuery.data?.counts?.rounds ?? 0
  const holdout = evalQuery.data?.holdout
  const percent = (value: number | null | undefined) => (
    value == null ? '—' : `${Math.round(value * 100)}%`
  )

  return (
    <SettingsCard
      icon={FlaskConical}
      title="Signal Lab"
      description="Taste-calibration games are a native ALMa feature. Home owns the round deck; the retained model can tint maps and add bounded Discovery/Feed nudges."
      action={
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">{form.enabled ? 'Active' : 'Ignored'}</span>
          <Switch
            checked={form.enabled}
            disabled={settingsQuery.isLoading || saveMutation.isPending}
            onCheckedChange={(enabled) => {
              const next = { ...form, enabled }
              setForm(next)
              save(next)
            }}
            aria-label={`${form.enabled ? 'Disable' : 'Enable'} Signal Lab`}
          />
        </div>
      }
      footer={
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-xs text-slate-500">
            {form.enabled
              ? 'Home, maps, Discovery, and Feed may consume the retained model.'
              : 'Games and every model effect are off; data remains.'}
          </span>
          <Button
            size="sm"
            onClick={() => save()}
            disabled={settingsQuery.isLoading || saveMutation.isPending}
          >
            {saveMutation.isPending ? 'Saving…' : 'Save Signal Lab'}
          </Button>
        </div>
      }
    >
      <div className="space-y-4">
        {settingsQuery.isError && (
          <p className="text-xs text-critical-600">
            Settings unavailable: {getApiErrorMessage(settingsQuery.error)}
          </p>
        )}
        <div className="grid gap-4 sm:grid-cols-3">
          <NumberField
            id="lab-region-points"
            label="Region nudge (points)"
            value={form.region_offset_points}
            min={0}
            max={2.5}
            step={0.05}
            description="At most 2.5 points in Discovery/Feed; zero keeps this head observational."
            onChange={(value) => update('region_offset_points', value)}
          />
          <NumberField
            id="lab-utility-points"
            label="Utility nudge (points)"
            value={form.utility_points}
            min={0}
            max={2.5}
            step={0.05}
            description="Confidence-scaled, at most 2.5 points. Promote only after holdout evidence."
            onChange={(value) => update('utility_points', value)}
          />
          <NumberField
            id="lab-author-points"
            label="Author nudge (points)"
            value={form.author_offset_points}
            min={0}
            max={2.5}
            step={0.05}
            description="Folds into the author signal your Library already produces. Fitted from same-region comparisons only."
            onChange={(value) => update('author_offset_points', value)}
          />
          <NumberField
            id="lab-map-tint"
            label="Map taste tint"
            value={form.map_tint_strength}
            min={0}
            max={1}
            step={0.05}
            description="Read-time terrain effect. Never moves map positions."
            onChange={(value) => update('map_tint_strength', value)}
          />
        </div>

        <Button variant="ghost" size="sm" onClick={() => setAdvancedOpen((open) => !open)}>
          {advancedOpen ? 'Hide sampler controls' : 'Show sampler controls'}
        </Button>
        {advancedOpen && (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <NumberField id="lab-ring-decay" label="Ring decay γ" value={form.ring_decay} min={0.01} max={1} step={0.05} onChange={(value) => update('ring_decay', value)} />
            <NumberField id="lab-exploration" label="Exploration ε" value={form.exploration_rate} min={0} max={1} step={0.05} onChange={(value) => update('exploration_rate', value)} />
            <NumberField id="lab-coverage" label="Coverage target" value={form.coverage_target} min={1} max={500} step={1} integer onChange={(value) => update('coverage_target', value)} />
            <NumberField id="lab-refit" label="Refit every rounds" value={form.refit_every_rounds} min={1} max={100} step={1} integer onChange={(value) => update('refit_every_rounds', value)} />
            <NumberField id="lab-holdout" label="Holdout (%)" value={form.holdout_percent} min={0} max={50} step={1} integer onChange={(value) => update('holdout_percent', value)} />
            <NumberField id="lab-override-votes" label="Override votes" value={form.override_min_votes} min={1} max={100} step={1} integer onChange={(value) => update('override_min_votes', value)} />
          </div>
        )}

        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-edge-1 pt-3">
          <p className="text-xs text-slate-500">
            {modelQuery.data?.ready
              ? `${rounds} rounds · holdout prior ${percent(holdout?.prior_accuracy)} · regions ${percent(holdout?.offsets_accuracy)} · utility ${percent(holdout?.utility_accuracy)}`
              : 'No fitted calibration model yet.'}
            {lastPurged != null ? ` Purged ${lastPurged}.` : ''}
          </p>
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="outline" size="sm" disabled={purgeMutation.isPending}>
                <Trash2 className="h-4 w-4" />
                Purge signals
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Purge all game signals?</AlertDialogTitle>
                <AlertDialogDescription>
                  Deletes every answered round and the derived model. Enabled
                  state and knobs stay unchanged. Library, ratings, and ordinary
                  feedback remain untouched. This cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={() => purgeMutation.mutate()}>
                  Purge signals
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
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
