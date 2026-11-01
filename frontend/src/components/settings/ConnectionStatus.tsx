/**
 * One way to say whether a setting is set and whether it works.
 *
 * The backend owns the answer (`application/connection_status.py`): state,
 * severity, label, detail. Nothing here derives a state or picks a colour per
 * field, so Settings, onboarding and Home cannot disagree. Colour comes from
 * `lib/severity`; saved-but-unchecked is never green because it is never `ok`.
 *
 * Checks run on save or on an explicit press, never on page load: a read of this
 * status opens no connection to anyone.
 */
import { Loader2, RefreshCw } from 'lucide-react'

import { useCheckMutation, useConnection, useConnections } from '@/components/settings/useConnections'
import { AsyncButton } from '@/components/settings/primitives'
import { StatusRow } from '@/components/shared/StatusRow'
import { StatusBadge } from '@/components/ui/status-badge'
import { dimensionBadgeTone, type Severity } from '@/lib/severity'
import { formatRelativeShort } from '@/lib/utils'

function StatusPill({
  severity,
  label,
  checking,
}: {
  severity: string
  label: string
  checking?: boolean
}) {
  return (
    <StatusBadge tone={dimensionBadgeTone(severity)} size="sm" className="shrink-0">
      {checking && <Loader2 className="h-3 w-3 animate-spin" aria-hidden />}
      {label}
    </StatusBadge>
  )
}

/** The state alone, for a row or a collapsed header. */
export function ConnectionBadge({ id }: { id: string }) {
  const record = useConnection(id)
  if (!record) return null
  return (
    <StatusPill
      severity={record.severity}
      label={record.status_label}
      checking={record.state === 'checking'}
    />
  )
}

/** A section's worst state, for its header. */
export function SectionStatus({ section }: { section: string }) {
  const summary = useConnections().data?.sections[section]
  if (!summary) return null
  return (
    <StatusPill
      severity={summary.severity}
      label={summary.status_label}
      checking={summary.state === 'checking'}
    />
  )
}

/** State, the sentence behind it, when it was last checked, what to do, Check. */
export function ConnectionLine({ id, named }: { id: string; named?: boolean }) {
  const record = useConnection(id)
  const check = useCheckMutation()
  if (!record) return null

  const checking = check.isPending || record.state === 'checking'
  const when = record.checked_at
    ? `Checked ${formatRelativeShort(record.checked_at)}.`
    : record.last_used_at
      ? `Last used ${formatRelativeShort(record.last_used_at)}.`
      : null
  // "Checked" would claim an answer. Can't check says when it last worked, if ever.
  const worked =
    record.state === 'cannot_check'
      ? record.last_worked_at
        ? `Last worked ${formatRelativeShort(record.last_worked_at)}.`
        : ''
      : null

  return (
    <div className="space-y-1.5" data-connection={record.id} data-state={record.state}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            {named && <span className="text-sm font-medium text-slate-700">{record.label}</span>}
            <StatusPill
              severity={checking ? 'info' : record.severity}
              label={checking ? 'Checking' : record.status_label}
              checking={checking}
            />
            {record.source === 'environment' && (
              <span className="text-xs text-slate-500">Set in the environment, which wins over this page.</span>
            )}
          </div>
          <p className="break-words text-xs text-slate-500">
            {[record.detail, record.action, worked ?? when].filter(Boolean).join(' ')}
          </p>
        </div>
        {record.checkable && (
          <AsyncButton
            type="button"
            variant="outline"
            size="sm"
            icon={<RefreshCw className="h-3 w-3" />}
            pending={checking}
            onClick={() => check.mutate(record.id)}
            className="h-7 shrink-0 px-2 text-xs"
          >
            Check
          </AsyncButton>
        )}
      </div>
      {record.results.length > 0 && (
        <div className="space-y-1.5" aria-label={`${record.label} results`}>
          {record.results.map((row) => (
            <StatusRow
              key={`${row.source ?? ''}:${row.address}`}
              severity={row.severity as Severity}
              label={row.address}
              title={row.source}
              metric={<span className="shrink-0 text-xs text-slate-500">{row.state}</span>}
            />
          ))}
        </div>
      )}
    </div>
  )
}

/**
 * One input's own state, under it. A filled, saved input says nothing here: the
 * dependency's line already speaks for it. What only the field knows is that it
 * is empty, what filling it adds, or that what is typed is not saved yet.
 */
export function FieldStatus({ id, field, dirty }: { id: string; field: string; dirty?: boolean }) {
  const record = useConnection(id)
  const reading = record?.fields.find((f) => f.name === field)
  if (dirty) return <StatusPill severity="unknown" label="Not saved" />
  if (!reading || reading.is_set || record?.state === 'off') return null
  if (reading.required) return <StatusPill severity="warning" label="Needs setting" />
  return (
    <span className="flex flex-wrap items-center gap-2">
      <StatusPill severity="unknown" label="Not set" />
      {reading.adds && <span className="text-xs text-slate-500">Optional: adds {reading.adds}.</span>}
    </span>
  )
}
