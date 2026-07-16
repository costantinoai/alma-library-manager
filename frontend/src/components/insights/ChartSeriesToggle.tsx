import { Toggle } from '@/components/ui/toggle'
import type { SeriesToggleSpec } from '@/components/insights/chartSeries'

/**
 * Shared "which series are visible" control for Insights charts that overlay two
 * incompatible units on one figure (I-18: a count vs a per-paper average).
 *
 * Both the Publications Timeline (Papers bars + Avg-Citations line) and Top
 * Journals (Papers + Avg-Citations bars) plot a volume series and an impact
 * series on one chart. Rather than force both on the reader at once, each chart
 * exposes the SAME toggle so volume and impact can be read independently — and
 * this is the one place that logic lives (DRY), including the invariant that at
 * least one series must stay visible so the chart never goes blank.
 */
/** Render the toggle group for a chart's series (place in the card header action). */
export function SeriesToggleGroup({
  specs,
  visible,
  onToggle,
}: {
  specs: SeriesToggleSpec[]
  visible: Record<string, boolean>
  onToggle: (key: string) => void
}) {
  return (
    <div className="flex items-center gap-2">
      {specs.map((s) => (
        <Toggle
          key={s.key}
          pressed={visible[s.key]}
          onPressedChange={() => onToggle(s.key)}
          size="sm"
          variant="outline"
          title={s.title}
          className={s.activeClassName}
        >
          {s.label}
        </Toggle>
      ))}
    </div>
  )
}
