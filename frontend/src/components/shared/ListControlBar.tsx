import type { ReactNode } from 'react'
import { ArrowDownWideNarrow, type LucideIcon } from 'lucide-react'

import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { Button } from '@/components/ui/button'
import { SubPanel } from '@/components/ui/sub-panel'

export interface ListViewOption {
  value: string
  label: string
  icon: LucideIcon
  title: string
}

/** Always-one-active segmented control. */
export interface ListSegment {
  value: string
  ariaLabel: string
  options: ListViewOption[]
  onChange: (value: string) => void
}

/** Two-way sort pill. The page owns the label/state; the bar owns the look. */
export interface ListSortToggle {
  label: string
  title: string
  ariaLabel: string
  onToggle: () => void
}

export interface ListControlBarProps {
  /** Page-specific controls rendered at the left (e.g. Feed's All/New filter). */
  leading?: ReactNode
  /** A pill for two orders, a segmented control for more. */
  sort: ListSortToggle | ListSegment
  count: number
  /** Noun after the count. Default "in view". */
  countLabel?: string
  /** Inline select-all toggle, shown only when `show` is true. */
  selectAll?: { allSelected: boolean; onToggle: () => void; show: boolean }
}

function SegmentedChoice({ segment }: { segment: ListSegment }) {
  return (
    <ToggleGroup
      type="single"
      value={segment.value}
      onValueChange={(value) => {
        // Radix lets the user deselect the active item; we require one
        // always-active so the list always renders.
        if (value) segment.onChange(value)
      }}
      aria-label={segment.ariaLabel}
      variant="segment"
    >
      {segment.options.map(({ value, label, icon: Icon, title }) => (
        <ToggleGroupItem
          key={value}
          value={value}
          title={title}
          className="h-7 min-w-0 gap-1 px-2.5 text-xs font-medium"
        >
          <Icon className="h-3.5 w-3.5" />
          <span className="hidden md:inline">{label}</span>
        </ToggleGroupItem>
      ))}
    </ToggleGroup>
  )
}

/**
 * U-10: the single canonical control bar for the Feed + Discovery lists.
 *
 * Both surfaces previously hand-rolled the same strip (sort pill · counter +
 * select-all) and had started to drift on styling/ARIA. This
 * owns the container + every shared control so the two read as the same product
 * by construction; pages pass only their data + a `leading` slot for the
 * Feed-only All/New filter.
 */
export function ListControlBar({
  leading,
  sort,
  count,
  countLabel = 'in view',
  selectAll,
}: ListControlBarProps) {
  return (
    <SubPanel variant="flat" padded={false}  className="flex flex-wrap items-center gap-3 px-3 py-2 shadow-paper-sm">
      {leading}

      {/* Sort: a pill for two orders, a segmented control for more. */}
      {'options' in sort ? (
        <SegmentedChoice segment={sort} />
      ) : (
        <Button variant="outline" size="xs"
          onClick={sort.onToggle}
          title={sort.title}
          aria-label={sort.ariaLabel}
          className="gap-1.5 px-3"
        >
          <ArrowDownWideNarrow className="h-3.5 w-3.5 text-slate-500" />
          {sort.label}
        </Button>
      )}

      {/* Right cluster: counter with inline select-all. */}
      <div className="ml-auto flex items-center gap-3">
        <div className="hidden items-center gap-1.5 text-xs text-slate-500 sm:inline-flex">
          <span className="tabular-nums font-medium text-slate-700">{count}</span>
          <span>{countLabel}</span>
          {selectAll?.show && (
            <>
              <span className="text-slate-300" aria-hidden>·</span>
              <Button variant="link-ink" size="inline"
                onClick={selectAll.onToggle}
                className=""
              >
                {selectAll.allSelected ? 'Clear selection' : 'Select all'}
              </Button>
            </>
          )}
        </div>

      </div>
    </SubPanel>
  )
}
