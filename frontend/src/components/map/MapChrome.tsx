/**
 * MapChrome — the ONE toolbar / legend / toggle vocabulary for every map
 * host (task 50-K). Discovery frontier, the Map page, and the Authors
 * network all mount THESE shells, so a user landing on any map recognises
 * every control from the last map they used:
 *
 *   - `MapToolbar`   — the bar ABOVE the plate (never buttons floating on it)
 *   - `MapToggle`    — the one pill-toggle idiom (accent = on)
 *   - `MapLegend`    — the section BELOW the plate
 *   - `ClusterLegendChips` — cluster chips as VIEW-ONLY dim toggles
 *     (dimmed = 15% opacity, never hidden, never a discovery signal)
 */
import { cn } from '@/lib/utils'

export function MapToolbar({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-[var(--color-border)] bg-surface-2 px-3 py-2">
      {children}
    </div>
  )
}

export function MapToggle({
  active,
  onClick,
  title,
  children,
}: {
  active?: boolean
  onClick: () => void
  title?: string
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      title={title}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-sm border px-2.5 py-1 text-xs font-medium transition-colors',
        active
          ? 'border-accent-edge bg-accent-soft text-alma-folio'
          : 'border-control-edge bg-control-well text-slate-600 hover:bg-control-quiet',
      )}
    >
      {children}
    </button>
  )
}

/** The 47-H style either/or switch (one grouping at a time, etc.). */
export function MapModeSwitch<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T
  options: Array<{ value: T; label: string; title?: string }>
  onChange: (value: T) => void
}) {
  return (
    <div className="inline-flex overflow-hidden rounded-sm border border-[var(--color-border)]">
      {options.map((opt) => (
        <button
          key={opt.value}
          type="button"
          onClick={() => onChange(opt.value)}
          title={opt.title}
          className={cn(
            'px-2 py-1 text-xs font-medium transition-colors',
            value === opt.value
              ? 'bg-accent-soft text-alma-folio'
              : 'bg-control-well text-slate-600 hover:bg-control-quiet',
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

export function MapLegend({ children }: { children: React.ReactNode }) {
  return (
    <div className="border-t border-[var(--color-border)] bg-surface-2 px-3 py-2.5 text-xs">
      {children}
    </div>
  )
}

export interface ClusterChipEntry {
  id: number
  label: string
  count: number
  color: string
}

/** Cluster chips as view-only dim toggles — identical on every host. */
export function ClusterLegendChips({
  clusters,
  dimmed,
  onToggle,
  limit = 8,
}: {
  clusters: ClusterChipEntry[]
  dimmed: ReadonlySet<number>
  onToggle: (id: number) => void
  limit?: number
}) {
  if (clusters.length === 0) return null
  return (
    <div className="mt-2 flex flex-wrap gap-1.5 border-t border-[var(--color-border)] pt-2">
      {clusters.slice(0, limit).map((c) => {
        const isDim = dimmed.has(c.id)
        return (
          <button
            key={c.id}
            type="button"
            onClick={() => onToggle(c.id)}
            aria-pressed={!isDim}
            className={cn(
              'inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-control-edge bg-control-quiet px-1.5 py-0.5 text-slate-600 transition-opacity hover:bg-control-quiet-hover',
              isDim && 'opacity-40',
            )}
            title={isDim ? `Show "${c.label}" (${c.count})` : `Dim "${c.label}" (${c.count})`}
          >
            <span className="inline-block h-2 w-2 shrink-0 rounded-full" style={{ background: c.color }} />
            <span className={cn('truncate', isDim && 'line-through')}>{c.label}</span>
            <span className="shrink-0 text-slate-400">· {c.count}</span>
          </button>
        )
      })}
    </div>
  )
}
