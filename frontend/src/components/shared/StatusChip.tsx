import type { ComponentType, ReactNode } from 'react'

import { severityDot } from '@/lib/severity'
import { cn } from '@/lib/utils'

export interface StatusChipProps {
  /** Category glyph — leading, quiet. Says what this chip is ABOUT. */
  icon?: ComponentType<{ className?: string }>
  /** Fills the status dot from the shared severity vocabulary. */
  severity?: string | null
  /** Line 1 — the thing's own name. Keep it short; it truncates. */
  name: ReactNode
  /** Line 2 — the one number or phrase that qualifies it. Truncates. */
  metric?: ReactNode
  /** Opens an in-page detail. Renders a `<button>`. */
  onClick?: () => void
  /** Navigates. Renders an `<a>`. Ignored when `onClick` is given. */
  href?: string
  /** Hover / AT text. Say what is wrong and what happens next. */
  title?: string
  /** Screen-reader name when the visible text is too terse on its own. */
  ariaLabel?: string
  /**
   * How much room the chip takes.
   *
   *   `chip` (default) — the boxed two-line well: glyph, dot, name, metric
   *                      under it. For a grid/strip that IS the content
   *                      (Health's system-status band).
   *   `slim`           — one inline line, no box: dot, name, and the metric as
   *                      a quiet trailing clause. For a status rail that must
   *                      not compete with the content below it (Home's header).
   *
   * Both carry the same dot, the same link target and the same tooltip, so a
   * chip does not change MEANING when it changes weight.
   */
  variant?: 'chip' | 'slim'
  className?: string
}

/**
 * StatusChip — the app's ONE "component and how it's doing" chip.
 *
 * Two stacked lines in a neutral ink well: a category glyph, a severity dot,
 * the thing's name, and one qualifying metric under it. Colour lands only in
 * the dot, so the chip reads the same at every severity and a row of them
 * scans as a strip rather than a traffic light.
 *
 * Health's system-status strip is where this shape was invented; it is now
 * shared with Home's connections rail and needs-you panel, so "here is a
 * component, here is its state" looks identical on both pages. Extracting it
 * was the alternative to Home re-deriving the same markup — the version that
 * had already been hand-rolled once was 8 nested elements with its own
 * severity→dot map.
 *
 * Two weights: the boxed `chip` (default) and the boxless `slim` inline line —
 * same dot, same link, same tooltip, so weight changes without meaning
 * changing. Home's header rail is slim so it cannot compete with the figures
 * below it; Health's band is the content, so it is boxed.
 *
 * Distinct from `StatusBadge` (a one-line pill stating a single fact) and from
 * `StatusRow` (a full-width line with a right-aligned metric, for dense lists).
 * Reach for this when the unit is a *thing with a state*.
 */
export function StatusChip({
  icon: Icon,
  severity,
  name,
  metric,
  onClick,
  href,
  title,
  ariaLabel,
  variant = 'chip',
  className,
}: StatusChipProps) {
  const isSlim = variant === 'slim'
  const shell = isSlim
    ? cn(
        'flex items-center gap-1.5 rounded-sm text-xs text-slate-500 transition-colors',
        'hover:text-alma-folio',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
        className,
      )
    : cn(
    'group flex min-w-[150px] items-start gap-2 rounded-sm border border-control-edge',
    'bg-control-well px-3 py-2 text-left transition-colors',
    'hover:border-control-edge-strong hover:bg-control-quiet',
    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
    className,
  )
  const inner = isSlim ? (
    <>
      <span
        className={cn('h-1.5 w-1.5 shrink-0 rounded-full', severityDot(severity))}
        aria-hidden
      />
      <span className="truncate font-medium text-alma-800">{name}</span>
      {metric != null && <span className="truncate tabular-nums">{metric}</span>}
    </>
  ) : (
    <>
      {Icon && <Icon className="mt-0.5 h-4 w-4 shrink-0 text-alma-500" aria-hidden />}
      <span className="min-w-0 flex-1">
        <span className="flex items-center gap-1.5">
          <span
            className={cn('h-2 w-2 shrink-0 rounded-full', severityDot(severity))}
            aria-hidden
          />
          <span className="truncate text-sm font-medium text-alma-800">{name}</span>
        </span>
        {metric != null && (
          <span className="mt-0.5 block truncate text-xs tabular-nums text-slate-500">
            {metric}
          </span>
        )}
      </span>
    </>
  )

  if (onClick) {
    return (
      <button
        type="button"
        onClick={onClick}
        title={title}
        aria-label={ariaLabel}
        className={shell}
      >
        {inner}
      </button>
    )
  }
  // Static when neither destination is given: a chip that only reports.
  if (!href) {
    return (
      <span title={title} aria-label={ariaLabel} className={shell}>
        {inner}
      </span>
    )
  }
  return (
    <a href={href} title={title} aria-label={ariaLabel} className={shell}>
      {inner}
    </a>
  )
}
