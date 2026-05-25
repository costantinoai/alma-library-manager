/**
 * MaintenanceOperationCard — one maintenance task: what it repairs, how many
 * items are pending, when it last ran, plus the run-now / auto-repair / daily-
 * cap controls. Warm chrome-elev surface (these are committed tools/config),
 * differentiating them on hue + lightness from the cool "needs attention"
 * cards above.
 */
import { useEffect, useState } from 'react'
import { Play } from 'lucide-react'

import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { Switch } from '@/components/ui/switch'
import { AsyncButton, SettingsNumberField } from '@/components/settings/primitives'
import { JargonHint } from '@/components/shared/JargonHint'
import { formatRelativeShort } from '@/lib/utils'
import type { MaintenanceOperation } from '@/api/client'
import { COST_LABEL } from './healthFormat'

interface MaintenanceOperationCardProps {
  op: MaintenanceOperation
  onRun: (key: string) => void
  onConfig: (key: string, body: { enabled?: boolean; daily_cap?: number }) => void
  running: boolean
}

function runStatusTone(status?: string | null): StatusBadgeTone {
  if (status === 'completed') return 'positive'
  if (status === 'failed') return 'negative'
  if (status === 'running' || status === 'queued' || status === 'scheduled') return 'info'
  return 'neutral'
}

export function MaintenanceOperationCard({
  op,
  onRun,
  onConfig,
  running,
}: MaintenanceOperationCardProps) {
  // Local daily-cap state so the number input stays responsive; commit on blur.
  const [cap, setCap] = useState(op.daily_cap)
  useEffect(() => setCap(op.daily_cap), [op.daily_cap])

  const last = op.last_run

  return (
    <div className="space-y-3 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-4 shadow-paper-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-medium text-alma-800">{op.label}</h3>
            <StatusBadge tone="neutral" size="sm" className="uppercase tracking-wide">
              {COST_LABEL[op.cost] ?? op.cost}
            </StatusBadge>
          </div>
          <p className="mt-1 text-sm text-slate-500">{op.description}</p>
        </div>
        <div className="shrink-0 text-right">
          <p className="font-brand text-xl font-semibold tabular-nums text-alma-800">
            {op.candidates_pending.toLocaleString()}
          </p>
          <p className="flex items-center justify-end gap-1 text-[11px] text-slate-500">
            pending
            <JargonHint
              title="Pending"
              description="How many papers this task would address right now, from the canonical health snapshot."
            />
          </p>
        </div>
      </div>

      {/* Last run */}
      <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <span className="font-medium text-slate-600">Last run</span>
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

      {/* Controls */}
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

        <div className="flex items-center gap-3">
          <SettingsNumberField
            label={
              <span className="inline-flex items-center gap-1">
                Daily cap
                <JargonHint
                  title="Daily cap"
                  description="The most items the automatic healer will process per day for this task. Run-now also uses this as its batch size."
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
          <AsyncButton
            size="sm"
            variant="outline"
            icon={<Play className="h-4 w-4" />}
            pending={running}
            className="border-alma-200 text-alma-700 hover:bg-alma-50"
            onClick={() => onRun(op.key)}
          >
            Run now
          </AsyncButton>
        </div>
      </div>
    </div>
  )
}
