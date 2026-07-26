import { useMemo } from 'react'
import type { CSSProperties, RefObject } from 'react'

import { useElementWidth } from '@/hooks/useElementWidth'

export interface MeasuredGridOptions {
  /** Narrowest a card may get before its content starts to wrap badly. */
  minItemWidth: number
  /** Gap between tracks, in px. Must match the grid's `gap-*` class. */
  gap: number
  /** Upper bound on tracks, so a wide desk doesn't produce a ribbon of slivers. */
  maxColumns?: number
  /** Tracks assumed for the single frame before the first measurement lands. */
  fallbackColumns?: number
}

export interface MeasuredGrid<T extends HTMLElement> {
  /** Attach to the element whose width decides the column count. */
  ref: RefObject<T | null>
  columns: number
  /** Inline `gridTemplateColumns` — apply so layout can't drift from `columns`. */
  style: CSSProperties
  /** Whole rows only: how many items fit in `rows` rows of this grid. */
  itemsForRows: (rows: number) => number
}

/**
 * Fit as many ≥`minItemWidth` columns into the measured container as possible.
 *
 * Viewport breakpoints can't see a fixed-width modal or a sidebar-squeezed
 * panel; a ResizeObserver can. Deriving the count from the container is also
 * what lets a caller show WHOLE ROWS — a section that shows "5" items leaves a
 * ragged hole in a 3-column grid and overflows a 1-column phone, while
 * `itemsForRows(2)` is 2, 4, or 6 depending on what actually fits.
 */
export function useMeasuredGrid<T extends HTMLElement>({
  minItemWidth,
  gap,
  maxColumns = 6,
  fallbackColumns = 3,
}: MeasuredGridOptions): MeasuredGrid<T> {
  const [ref, width] = useElementWidth<T>()

  const columns = useMemo(() => {
    if (width == null || width <= 0) return fallbackColumns
    return Math.max(1, Math.min(maxColumns, Math.floor((width + gap) / (minItemWidth + gap))))
  }, [width, gap, minItemWidth, maxColumns, fallbackColumns])

  const style = useMemo<CSSProperties>(
    () => ({ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }),
    [columns],
  )

  return {
    ref,
    columns,
    style,
    itemsForRows: (rows: number) => Math.max(1, columns * Math.max(1, rows)),
  }
}
