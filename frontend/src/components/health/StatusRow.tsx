/**
 * StatusRow — the app's ONE status-line primitive: a severity badge + a label
 * + an optional right-aligned metric, optionally clickable into a drilldown
 * ("view →").
 *
 * Used by the gaps inside a `RepairCard`, the `DiagnosticsSection` rows, the
 * subsystem lines on Health, and Home's needs-you panel — so a line that says
 * "here is a thing, here is how bad it is, here is where to fix it" reads
 * identically wherever it appears. It sits on the ink ladder
 * (`control-well` + `control-edge`), so it looks the same at any elevation.
 *
 * `metric` is a caller-supplied node (a count, a coverage bar, "2 degraded", …)
 * so each surface keeps its own metric shape without forking the row.
 *
 * `href` and `onOpen` are alternative destinations: `onOpen` for an in-page
 * drilldown, `href` for another surface. Passing both is a contradiction —
 * `onOpen` wins.
 */
import { StatusBadge } from '@/components/ui/status-badge'
import { cn } from '@/lib/utils'
import { dimensionBadgeTone, severityLabel } from '@/lib/severity'

const BASE =
  'flex w-full items-center gap-3 rounded-sm border border-control-edge bg-control-well px-3 py-2 text-left'

interface StatusRowProps {
  severity?: string | null
  label: string
  /** Right-aligned metric node (already styled by the caller). */
  metric?: React.ReactNode
  /** When provided the row becomes a button that opens an in-page drilldown. */
  onOpen?: () => void
  /** When provided (and `onOpen` is not) the row becomes a link to that route. */
  href?: string
  /** Native tooltip — used to surface the severity reason (H-7) on hover. */
  title?: string
}

export function StatusRow({ severity, label, metric, onOpen, href, title }: StatusRowProps) {
  const clickable = !!onOpen || !!href
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
  const interactive = cn(
    BASE,
    'group transition-colors hover:border-control-edge-strong focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
  )
  if (onOpen) {
    return (
      <button type="button" onClick={onOpen} title={title} className={interactive}>
        {inner}
      </button>
    )
  }
  return (
    <a href={href} title={title} className={interactive}>
      {inner}
    </a>
  )
}
