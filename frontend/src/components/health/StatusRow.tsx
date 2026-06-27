/**
 * StatusRow — the page's one status-line primitive: a severity badge + a label
 * + an optional right-aligned metric, optionally clickable into a drilldown
 * ("view →"). Shared by the gaps inside a `RepairCard`, the `DiagnosticsSection`
 * rows, and the `SystemStatusSection` subsystems — so every status line on the
 * Health page reads identically.
 *
 * `metric` is a caller-supplied node (a count, a coverage bar, "2 degraded", …)
 * so each surface keeps its own metric shape without forking the row.
 */
import { StatusBadge } from '@/components/ui/status-badge'
import { cn } from '@/lib/utils'
import { dimensionBadgeTone, severityLabel } from './healthFormat'

const BASE =
  'flex w-full items-center gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-3 py-2 text-left'

interface StatusRowProps {
  severity?: string | null
  label: string
  /** Right-aligned metric node (already styled by the caller). */
  metric?: React.ReactNode
  /** When provided the row becomes a button that opens a drilldown. */
  onOpen?: () => void
  /** Native tooltip — used to surface the severity reason (H-7) on hover. */
  title?: string
}

export function StatusRow({ severity, label, metric, onOpen, title }: StatusRowProps) {
  const clickable = !!onOpen
  const inner = (
    <>
      <StatusBadge tone={dimensionBadgeTone(severity)} size="sm" className="shrink-0 capitalize">
        {severityLabel(severity)}
      </StatusBadge>
      <span className="min-w-0 flex-1 truncate text-sm text-alma-800">{label}</span>
      {metric}
      {clickable ? (
        <span className="shrink-0 text-[11px] font-medium text-alma-folio opacity-0 transition-opacity group-hover:opacity-100">
          view →
        </span>
      ) : null}
    </>
  )
  if (!clickable) return <div className={BASE} title={title}>{inner}</div>
  return (
    <button
      type="button"
      onClick={onOpen}
      title={title}
      className={cn(
        BASE,
        'group transition-colors hover:border-alma-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
      )}
    >
      {inner}
    </button>
  )
}
