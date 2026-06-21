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
import { useEffect, useState } from 'react'
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
  type MaintenanceRunRequest,
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
import { RepairConfirmDialog } from './RepairConfirmDialog'

interface RepairCardProps {
  op: MaintenanceOperation
  /** The health dimensions this op repairs (op.repairs ∩ snapshot.dimensions). */
  dims: HealthDimension[]
  onRun: (key: string, request: MaintenanceRunRequest) => void
  onConfig: (
    key: string,
    body: {
      auto_enabled?: boolean
      auto_daily_cap?: number
      remembered_manual_limit?: number
      request_batch_size?: number
    },
  ) => void
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
  const [autoCap, setAutoCap] = useState(op.auto_daily_cap)
  useEffect(() => setAutoCap(op.auto_daily_cap), [op.auto_daily_cap])
  const [manualLimit, setManualLimit] = useState(op.manual_limit)
  useEffect(() => setManualLimit(op.manual_limit), [op.manual_limit])

  const scopeSpec = op.params_spec?.scope
  const hasDryRun = op.params_spec?.dry_run != null
  const defaultScope =
    typeof scopeSpec?.default === 'string' ? scopeSpec.default : scopeSpec?.options?.[0] ?? ''
  const [scope, setScope] = useState<string>(defaultScope)

  // Per-op API batch size (overridable ops only — e.g. S2 vectors). Local state
  // for a responsive field; persisted on blur, but the ETA recomputes live as it
  // changes so the user sees the call count / time drop before committing.
  const batchOverridable = op.request_batch_max != null
  const persistedBatch = op.request_batch_size ?? op.request_batch_default ?? 1
  const [batch, setBatch] = useState<number>(persistedBatch)
  useEffect(() => setBatch(persistedBatch), [persistedBatch])

  // Destructive ops route their Run through an explicit review dialog that
  // carries the backend confirmation token; safe ops run on the single click.
  const [confirmOpen, setConfirmOpen] = useState(false)

  // "Dirty" = the chosen scope/batch differs from what the polled op row reflects,
  // so re-query a live count + ETA instead of using the (cached) row values.
  const scopeDirty = !!scopeSpec && scope !== defaultScope
  const batchDirty = batchOverridable && batch !== persistedBatch
  const limitDirty = manualLimit !== op.manual_limit
  const dirty = scopeDirty || batchDirty || limitDirty
  const estimateQuery = useQuery({
    queryKey: ['health', 'estimate', op.key, scope, batch, manualLimit],
    queryFn: () =>
      estimateMaintenanceOperation(op.key, {
        scope: scopeSpec ? scope : undefined,
        max_items: manualLimit,
        request_batch_size: batchOverridable ? batch : undefined,
      }),
    enabled: dirty,
    staleTime: 30_000,
  })
  const estimate = dirty ? estimateQuery.data : undefined
  const pending = estimate ? estimate.candidates_pending : op.candidates_pending
  const eta = estimate ? estimate.eta : op.eta

  const runRequest = (extra?: Partial<MaintenanceRunRequest>): MaintenanceRunRequest => ({
    max_items: manualLimit,
    request_batch_size: batchOverridable ? batch : undefined,
    scope: scopeSpec ? scope : undefined,
    ...extra,
  })

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
            {op.recommended ? <StatusBadge tone="info" size="sm">Recommended next</StatusBadge> : null}
            {op.optional ? <StatusBadge tone="neutral" size="sm">Optional</StatusBadge> : null}
            {op.manual_gate ? <StatusBadge tone="warning" size="sm">Manual gate</StatusBadge> : null}
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

      {op.blocked_by.length > 0 ? (
        <p className="rounded-sm border border-warning-100 bg-warning-50 px-3 py-2 text-xs text-warning-800">
          Blocked by {op.blocked_by.map((item) => `${item.label} (${item.pending})`).join(', ')}.
        </p>
      ) : null}

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
        {!op.destructive ? (
          <label className="flex items-center gap-2 text-sm text-slate-700">
            <Switch
              checked={op.auto_enabled}
              onCheckedChange={(value) => onConfig(op.key, { auto_enabled: value })}
            />
            <span className="inline-flex items-center gap-1">
              Auto-repair
              <span className="text-xs text-slate-400">{op.auto_enabled ? '(on)' : '(opt-in)'}</span>
            </span>
          </label>
        ) : (
          <span className="text-xs font-medium text-warning-700">Never runs automatically</span>
        )}

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
          ) : null}

          <SettingsNumberField
            label={<span className="inline-flex items-center gap-1">Run limit<JargonHint title="Run limit" description={`Maximum ${op.unit.replace(/_/g, ' ')} units for this manual run. This exact visible value is sent atomically with Run.`} /></span>}
            value={manualLimit}
            min={1}
            max={op.max_manual_limit}
            onChange={setManualLimit}
            onBlur={() => {
              if (manualLimit !== op.manual_limit) onConfig(op.key, { remembered_manual_limit: manualLimit })
            }}
            className="min-w-0"
          />

          {!op.destructive ? (
            <SettingsNumberField
              label={<span className="inline-flex items-center gap-1">Auto daily cap<JargonHint title="Auto daily cap" description="Maximum units unattended repair may process per UTC day. It does not change the manual Run limit." /></span>}
              value={autoCap}
              min={1}
              max={op.max_auto_daily_cap}
              onChange={setAutoCap}
              onBlur={() => {
                if (autoCap !== op.auto_daily_cap) onConfig(op.key, { auto_daily_cap: autoCap })
              }}
              className="min-w-0"
            />
          ) : null}

          {batchOverridable ? (
            <SettingsNumberField
              label={
                <span className="inline-flex items-center gap-1">
                  Batch size
                  <JargonHint
                    title="Batch size"
                    description={`${op.request_batch_unit?.replace(/_/g, ' ') ?? 'lookup IDs'} per upstream request (max ${op.request_batch_max}). This is separate from the total run limit.`}
                  />
                </span>
              }
              value={batch}
              min={1}
              max={op.request_batch_max ?? undefined}
              onChange={setBatch}
              onBlur={() => {
                if (batch !== persistedBatch) onConfig(op.key, { request_batch_size: batch })
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
              onClick={() => onRun(op.key, runRequest({ dry_run: true }))}
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
            disabled={op.blocked_by.length > 0}
            onClick={() =>
              op.destructive
                ? setConfirmOpen(true)
                : onRun(op.key, runRequest(hasDryRun ? { dry_run: false } : undefined))
            }
          >
            {op.destructive ? 'Review & run…' : 'Run now'}
          </AsyncButton>
        </div>
      </div>

      {op.destructive ? (
        <RepairConfirmDialog
          op={op}
          open={confirmOpen}
          onOpenChange={setConfirmOpen}
          request={runRequest()}
          running={running}
          onConfirm={(req) => {
            setConfirmOpen(false)
            onRun(op.key, req)
          }}
        />
      ) : null}
    </div>
  )
}
