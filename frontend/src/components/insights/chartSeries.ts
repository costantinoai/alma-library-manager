import { useState } from 'react'

export interface SeriesToggleSpec {
  key: string
  label: string
  activeClassName: string
  title?: string
}

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
 * Timeline series. MEDIAN is the default impact line, not the mean: one runaway
 * paper drags a year's average far from where its papers actually sit, and the
 * timeline's job is to show the typical year. The mean stays one click away for
 * anyone who wants the skew.
 */
export const TIMELINE_SERIES: SeriesToggleSpec[] = [
  PAPERS_AVG_CIT_SERIES[0],
  {
    key: 'median_citations',
    label: 'Median Citations',
    title: 'Toggle the median citations line (the typical paper that year)',
    activeClassName:
      'data-[state=on]:border-gold-300 data-[state=on]:bg-gold-100 data-[state=on]:text-gold-700',
  },
  {
    key: 'avg_citations',
    label: 'Mean',
    title: 'Toggle the mean citations line (sensitive to one runaway paper)',
    activeClassName:
      'data-[state=on]:border-alma-200 data-[state=on]:bg-surface-3 data-[state=on]:text-slate-700',
  },
]

export function useSeriesVisibility(keys: string[], initial?: Record<string, boolean>) {
  const [visible, setVisible] = useState<Record<string, boolean>>(
    () => initial ?? Object.fromEntries(keys.map((key) => [key, true])),
  )
  const toggle = (key: string) => {
    setVisible((previous) => {
      const next = { ...previous, [key]: !previous[key] }
      return Object.values(next).some(Boolean) ? next : previous
    })
  }
  return { visible, toggle }
}
