import { useState } from 'react'

import { Toggle } from '@/components/ui/toggle'

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
export interface SeriesToggleSpec {
  /** Stable key, also the visibility map key. */
  key: string
  label: string
  /** `data-[state=on]:…` classes carrying this series' accent color. */
  activeClassName: string
  title?: string
}

/** The Papers / Avg-Citations pair shared by the timeline and journals charts. */
export const PAPERS_AVG_CIT_SERIES: SeriesToggleSpec[] = [
  {
    key: 'papers',
    label: 'Papers',
    title: 'Toggle papers series',
    activeClassName:
      'data-[state=on]:border-alma-700 data-[state=on]:bg-alma-100 data-[state=on]:text-alma-800',
  },
  {
    key: 'avg_citations',
    label: 'Avg Citations',
    title: 'Toggle average citations series',
    activeClassName:
      'data-[state=on]:border-gold-300 data-[state=on]:bg-gold-100 data-[state=on]:text-gold-700',
  },
]

/**
 * Visibility state for a set of chart series. Every series starts visible;
 * `toggle` flips one but refuses to hide the last remaining series, so the
 * chart always renders something meaningful.
 */
export function useSeriesVisibility(keys: string[]) {
  const [visible, setVisible] = useState<Record<string, boolean>>(() =>
    Object.fromEntries(keys.map((k) => [k, true])),
  )
  const toggle = (key: string) => {
    setVisible((prev) => {
      const next = { ...prev, [key]: !prev[key] }
      // Keep at least one series on — an all-hidden chart is useless.
      if (!Object.values(next).some(Boolean)) return prev
      return next
    })
  }
  return { visible, toggle }
}

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
