import type { ReactNode } from 'react'
import { ArrowDownWideNarrow, type LucideIcon } from 'lucide-react'

import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'

export interface ListViewOption {
  value: string
  label: string
  icon: LucideIcon
  title: string
}

export interface ListControlBarProps {
  /** Page-specific controls rendered at the left (e.g. Feed's All/New filter). */
  leading?: ReactNode
  /** Binary sort pill. The page owns the label/state; the bar owns the look. */
  sort: { label: string; title: string; ariaLabel: string; onToggle: () => void }
  count: number
  /** Noun after the count. Default "in view". */
  countLabel?: string
  /** Inline select-all toggle, shown only when `show` is true. */
  selectAll?: { allSelected: boolean; onToggle: () => void; show: boolean }
  /** Always-one-active view-mode segmented control. */
  view: {
    value: string
    ariaLabel: string
    options: ListViewOption[]
    onChange: (value: string) => void
  }
}

/**
 * U-10: the single canonical control bar for the Feed + Discovery lists.
 *
 * Both surfaces previously hand-rolled the same strip (sort pill · counter +
 * select-all · view-mode toggle) and had started to drift on styling/ARIA. This
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
  view,
}: ListControlBarProps) {
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-sm border border-[var(--color-border)] bg-surface-1 px-3 py-2 shadow-sm">
      {leading}

      {/* Sort toggle — pill button, binary state. */}
      <button
        type="button"
        onClick={sort.onToggle}
        title={sort.title}
        aria-label={sort.ariaLabel}
        className="inline-flex h-7 items-center gap-1.5 rounded-sm border border-[var(--color-border)] bg-surface-1 px-3 text-xs font-medium text-alma-800 transition-colors hover:bg-surface-2"
      >
        <ArrowDownWideNarrow className="h-3.5 w-3.5 text-slate-500" />
        {sort.label}
      </button>

      {/* Right cluster: counter with inline select-all, then view mode. */}
      <div className="ml-auto flex items-center gap-3">
        <div className="hidden items-center gap-1.5 text-xs text-slate-500 sm:inline-flex">
          <span className="tabular-nums font-medium text-slate-700">{count}</span>
          <span>{countLabel}</span>
          {selectAll?.show && (
            <>
              <span className="text-slate-300" aria-hidden>·</span>
              <button
                type="button"
                onClick={selectAll.onToggle}
                className="text-alma-700 underline-offset-2 transition-colors hover:text-alma-800 hover:underline"
              >
                {selectAll.allSelected ? 'Clear selection' : 'Select all'}
              </button>
            </>
          )}
        </div>

        <ToggleGroup
          type="single"
          value={view.value}
          onValueChange={(value) => {
            // Radix lets the user deselect the active item; we require one
            // always-active so the list always renders.
            if (value) view.onChange(value)
          }}
          aria-label={view.ariaLabel}
          className="gap-0 rounded-sm bg-surface-2/80 p-0.5"
        >
          {view.options.map(({ value, label, icon: Icon, title }) => (
            <ToggleGroupItem
              key={value}
              value={value}
              title={title}
              className="h-7 min-w-0 gap-1 rounded-sm px-2.5 text-xs font-medium text-slate-600 hover:bg-transparent hover:text-alma-800 data-[state=on]:bg-surface-1 data-[state=on]:text-alma-800 data-[state=on]:shadow-paper-sm data-[state=on]:ring-1 data-[state=on]:ring-[var(--color-border)]"
            >
              <Icon className="h-3.5 w-3.5" />
              <span className="hidden md:inline">{label}</span>
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </div>
    </div>
  )
}
