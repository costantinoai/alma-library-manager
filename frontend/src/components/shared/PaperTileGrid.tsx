import { useState, type ReactNode } from 'react'

import { Button } from '@/components/ui/button'
import { useMeasuredGrid } from '@/hooks/useMeasuredGrid'

/** Narrowest a tile may get before its title and byline start to wrap badly. */
const MIN_TILE_WIDTH = 260
/** Matches the `gap-3` between tiles. */
const GRID_GAP = 12
const MAX_COLUMNS = 4

export interface PaperTileGridProps<T> {
  items: T[]
  getKey: (item: T) => string
  renderTile: (item: T) => ReactNode
  /**
   * Rows visible before "Show more". The item count follows the MEASURED
   * column count, so a section always ends on a full row — never a ragged
   * hole on a wide desk or an overflowing column on a phone. Omit to show
   * every item.
   */
  collapsedRows?: number
  /**
   * Whether the overflow can be revealed in place. `false` makes
   * `collapsedRows` a hard cap with no control — for a section that is meant
   * to be exactly one row of the best items, not a truncated list.
   */
  expandable?: boolean
}

/**
 * The one grid every paper-tile section uses.
 *
 * Owns the three things a section would otherwise re-invent: how many columns
 * fit the available width, how many whole rows are visible while collapsed,
 * and the expansion control. Callers supply only the items and the tile.
 */
export function PaperTileGrid<T>({
  items,
  getKey,
  renderTile,
  collapsedRows,
  expandable = true,
}: PaperTileGridProps<T>) {
  const [expanded, setExpanded] = useState(false)
  const grid = useMeasuredGrid<HTMLDivElement>({
    minItemWidth: MIN_TILE_WIDTH,
    gap: GRID_GAP,
    maxColumns: MAX_COLUMNS,
  })

  const cap =
    collapsedRows === undefined || expanded ? items.length : grid.itemsForRows(collapsedRows)
  const visible = items.slice(0, cap)
  const hidden = items.length - visible.length

  return (
    <div className="space-y-3">
      <div ref={grid.ref} className="grid items-stretch gap-3" style={grid.style}>
        {visible.map((item) => (
          // `h-full` chains the row's stretched height down to the tile, so
          // every tile in a row ends on the same baseline.
          <div key={getKey(item)} className="h-full">
            {renderTile(item)}
          </div>
        ))}
      </div>
      {hidden > 0 && expandable && (
        <Button size="sm" variant="ghost" onClick={() => setExpanded(true)}>
          Show {hidden} more
        </Button>
      )}
    </div>
  )
}
