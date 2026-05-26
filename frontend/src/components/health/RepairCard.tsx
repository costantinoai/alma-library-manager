/**
 * RepairCard — the single, self-contained Health card: one maintenance
 * operation rendered as **its status AND its action together**. It merges
 * what used to be split across two tabs:
 *   - the data-health *status* (which gaps it repairs, their severity + counts,
 *     drilldown to the affected papers) — formerly `HealthDimensionCard`;
 *   - the *operation* itself (pending count, ETA, run / auto-repair / daily-cap
 *     / scope / dry-run controls, last run) — formerly `MaintenanceOperationCard`.
 *
 * The card unit is the OPERATION, not the dimension, because the mapping is
 * many-to-many (e.g. `corpus_metadata` repairs seven dimensions). The op's
 * dimensions render as clickable status rows; the controls render once in the
 * footer — so one backend config is never shown seven times.
 *
 * Surface: warm `alma-content-elev` — the lightest committed tone, so the card
 * sits clearly forefront of the parchment desk ("more forefront = lighter").
 * Inner status rows + the pending tile recess to the cooler `alma-chrome-elev`
 * (distinct on hue AND lightness). The only saturated element is the severity
 * badge — the triage signal — everything else is alma-grey, per the "saturated
 * tones inside an off-white card read as alarms" lesson.
 */
import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Eye, History, Play } from 'lucide-react'

import { StatusBadge } from '@/components/ui/status-badge'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { MetricTile } from '@/components/shared/MetricTile'
import { AsyncButton, SettingsNumberField } from '@/components/settings/primitives'
import { EtaHint } from '@/components/shared/EtaHint'
import { JargonHint } from '@/components/shared/JargonHint'
import { formatRelativeShort } from '@/lib/utils'
import {
  estimateMaintenanceOperation,
  type HealthDimension,
  type MaintenanceOperation,
} from '@/api/client'
import {
  COST_LABEL,
  dimensionBadgeTone,
  opSeverity,
  runStatusTone,
  severityLabel,
  severityMetricTone,
  sortBySeverity,
} from './healthFormat'
import { DimensionStatusRow } from './DimensionStatusRow'

interface RepairCardProps {
  op: MaintenanceOperation
  /** The health dimensions this op repairs (op.repairs ∩ snapshot.dimensions). */
  dims: HealthDimension[]
  onRun: (key: string, params?: Record<string, unknown>) => void
  onConfig: (key: string, body: { enabled?: boolean; daily_cap?: number; batch_size?: number }) => void
  /** Open the affected-papers drilldown for one dimension. */
  onOpenDim: (dim: HealthDimension) => void
  running: boolean
}

/** "followed_plus_library" → "Followed + library" for the scope dropdown. */
function prettyScope(value: string): string {
  const spaced = value.replace(/_plus_/g, ' + ').replace(/_/g, ' ')
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
}

export function RepairCard({ op, dims, onRun, onConfig, onOpenDim, running }: RepairCardProps) {
  // Local daily-cap state so the input stays responsive; commit on blur.
  const [cap, setCap] = useState(op.daily_cap)
  useEffect(() => setCap(op.daily_cap), [op.daily_cap])

  const scopeSpec = op.params_spec?.scope
  const hasDryRun = op.params_spec?.dry_run != null
  const defaultScope =
    typeof scopeSpec?.default === 'string' ? scopeSpec.default : scopeSpec?.options?.[0] ?? ''
  const [scope, setScope] = useState<string>(defaultScope)

  // Per-op API batch size (overridable ops only — e.g. S2 vectors). Local state
  // for a responsive field; persisted on blur, but the ETA recomputes live as it
  // changes so the user sees the call count / time drop before committing.
  const batchOverridable = op.batch_size_max != null
  const persistedBatch = op.batch_size ?? op.batch_size_default ?? 1
  const [batch, setBatch] = useState<number>(persistedBatch)
  useEffect(() => setBatch(persistedBatch), [persistedBatch])

  // "Dirty" = the chosen scope/batch differs from what the polled op row reflects,
  // so re-query a live count + ETA instead of using the (cached) row values.
  const scopeDirty = !!scopeSpec && scope !== defaultScope
  const batchDirty = batchOverridable && batch !== persistedBatch
  const dirty = scopeDirty || batchDirty
  const estimateQuery = useQuery({
    queryKey: ['health', 'estimate', op.key, scope, batch],
    queryFn: () =>
      estimateMaintenanceOperation(op.key, {
        scope: scopeSpec ? scope : undefined,
        batch_size: batchOverridable ? batch : undefined,
      }),
    enabled: dirty,
    staleTime: 30_000,
  })
  const estimate = dirty ? estimateQuery.data : undefined
  const pending = estimate ? estimate.candidates_pending : op.candidates_pending
  const eta = estimate ? estimate.eta : op.eta

  const runParams = useMemo(() => {
    return (extra?: Record<string, unknown>): Record<string, unknown> | undefined => {
      const p: Record<string, unknown> = { ...extra }
      if (scopeSpec) p.scope = scope
      return Object.keys(p).length ? p : undefined
    }
  }, [scopeSpec, scope])

  // Rolled-up severity = worst across the op's dimensions (null = dimension-less
  // cleanup op, which shows no severity badge — just its pending count).
  const severity = opSeverity(dims)
  const attentionDims = sortBySeverity(dims.filter((d) => d.severity !== 'ok'))
  const healthyCount = dims.length - attentionDims.length
  const last = op.last_run

  // A thin severity-tinted left spine turns a stack of cards into an instantly
  // scannable triage column — an index tab at the frame, not a saturated fill.
  const spine =
    severity === 'critical'
      ? 'border-l-2 border-l-critical-500'
      : severity === 'warning'
        ? 'border-l-2 border-l-warning-500'
        : severity === 'info'
          ? 'border-l-2 border-l-alma-folio'
          : ''

  return (
    <div
      className={`space-y-3 rounded-sm border border-[var(--color-border)] bg-surface-2 p-4 shadow-paper ${spine}`}
    >
      {/* Head: what it is · cost · rolled-up severity · pending · ETA */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-medium text-alma-800">{op.label}</h3>
            <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
              {COST_LABEL[op.cost] ?? op.cost}
            </StatusBadge>
            {severity ? (
              <StatusBadge tone={dimensionBadgeTone(severity)} size="sm" className="capitalize">
                {severityLabel(severity)}
              </StatusBadge>
            ) : null}
          </div>
          <p className="mt-1 text-sm text-slate-500">{op.description}</p>
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          <MetricTile
            label="pending"
            value={pending}
            tone={severity ? severityMetricTone(severity) : 'neutral'}
            align="center"
            className="w-28"
            labelSuffix={
              <JargonHint
                title="Pending"
                description="How many items this task would address right now, for the selected scope."
              />
            }
          />
          <EtaHint eta={eta} />
        </div>
      </div>

      {/* Status rows — the gaps this op repairs (click → affected papers). */}
      {attentionDims.length > 0 ? (
        <div className="space-y-1.5">
          {attentionDims.map((dim) => (
            <DimensionStatusRow key={dim.key} dim={dim} onOpen={() => onOpenDim(dim)} />
          ))}
          {healthyCount > 0 ? (
            <p className="pl-1 text-[11px] text-slate-400">+{healthyCount} healthy</p>
          ) : null}
        </div>
      ) : dims.length > 0 ? (
        <p className="text-xs text-success-700">All {dims.length} repaired dimensions healthy.</p>
      ) : null}

      {/* Last run */}
      <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <span className="inline-flex items-center gap-1.5 font-medium text-slate-600">
          <History className="h-3.5 w-3.5" aria-hidden />
          Last run
        </span>
        {last ? (
          <>
            <StatusBadge tone={runStatusTone(last.status)} size="sm" className="capitalize">
              {last.status}
            </StatusBadge>
            <span>{formatRelativeShort(last.finished_at ?? last.updated_at)}</span>
            {last.duration_seconds != null ? <span>· {last.duration_seconds}s</span> : null}
            {last.trigger_source ? <span>· {last.trigger_source}</span> : null}
          </>
        ) : (
          <span>never run</span>
        )}
      </div>

      {/* Controls footer: auto-repair · scope/cap · preview · run */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[var(--color-border)] pt-3">
        <label className="flex items-center gap-2 text-sm text-slate-700">
          <Switch
            checked={op.enabled}
            onCheckedChange={(value) => onConfig(op.key, { enabled: value })}
          />
          <span className="inline-flex items-center gap-1">
            Auto-repair
            <span className="text-xs text-slate-400">{op.enabled ? '(on)' : '(opt-in)'}</span>
          </span>
        </label>

        <div className="flex flex-wrap items-center gap-3">
          {scopeSpec ? (
            <label className="flex items-center gap-1.5 text-xs text-slate-600">
              Scope
              <Select value={scope} onValueChange={setScope}>
                <SelectTrigger className="h-8 min-w-[9rem] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {(scopeSpec.options ?? []).map((opt) => (
                    <SelectItem key={opt} value={opt} className="text-xs">
                      {prettyScope(opt)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>
          ) : (
            <SettingsNumberField
              label={
                <span className="inline-flex items-center gap-1">
                  Daily cap
                  <JargonHint
                    title="Daily cap"
                    description="The most items the automatic healer will process per day for this task. Run-now also uses this as its batch size where the task supports a limit."
                  />
                </span>
              }
              value={cap}
              min={1}
              onChange={setCap}
              onBlur={() => {
                if (cap !== op.daily_cap) onConfig(op.key, { daily_cap: cap })
              }}
              className="min-w-0"
            />
          )}

          {batchOverridable ? (
            <SettingsNumberField
              label={
                <span className="inline-flex items-center gap-1">
                  Batch size
                  <JargonHint
                    title="Batch size"
                    description={`Items per API request (max ${op.batch_size_max}). A bigger batch means fewer requests — at this endpoint's rate limit that's proportionally faster. The ETA above updates as you change it, and the actual run uses this value.`}
                  />
                </span>
              }
              value={batch}
              min={1}
              max={op.batch_size_max ?? undefined}
              onChange={setBatch}
              onBlur={() => {
                if (batch !== persistedBatch) onConfig(op.key, { batch_size: batch })
              }}
              className="min-w-0"
            />
          ) : null}

          {hasDryRun ? (
            <AsyncButton
              size="sm"
              variant="ghost"
              icon={<Eye className="h-4 w-4" />}
              pending={running}
              className="text-alma-700 hover:bg-alma-50"
              onClick={() => onRun(op.key, runParams({ dry_run: true }))}
            >
              Preview
            </AsyncButton>
          ) : null}
          <AsyncButton
            size="sm"
            variant="outline"
            icon={<Play className="h-4 w-4" />}
            pending={running}
            className="border-alma-200 text-alma-700 hover:bg-alma-50"
            onClick={() => onRun(op.key, runParams(hasDryRun ? { dry_run: false } : undefined))}
          >
            {hasDryRun ? 'Run sweep' : 'Run now'}
          </AsyncButton>
        </div>
      </div>
    </div>
  )
}
