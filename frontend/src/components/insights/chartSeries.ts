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

export function useSeriesVisibility(keys: string[]) {
  const [visible, setVisible] = useState<Record<string, boolean>>(() =>
    Object.fromEntries(keys.map((key) => [key, true])),
  )
  const toggle = (key: string) => {
    setVisible((previous) => {
      const next = { ...previous, [key]: !previous[key] }
      return Object.values(next).some(Boolean) ? next : previous
    })
  }
  return { visible, toggle }
}
