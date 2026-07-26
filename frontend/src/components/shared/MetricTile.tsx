import { Card, CardContent } from '@/components/ui/card'
import { cn, formatNumber } from '@/lib/utils'

export type MetricTileTone =
  | 'neutral'
  | 'success'
  | 'warning'
  | 'critical'
  | 'info'
  | 'accent'

const VALUE_TONE_CLASS: Record<MetricTileTone, string> = {
  neutral: 'text-alma-800',
  success: 'text-success-700',
  warning: 'text-warning-700',
  critical: 'text-critical-700',
  info: 'text-info-600',
  accent: 'text-alma-folio',
}

export interface MetricTileProps {
  label: string
  value: number | string
  /** Optional secondary hint. */
  hint?: string
  /** Tone-led coloring of the value. Ignored when `icon` is provided. */
  tone?: MetricTileTone
  /**
   * Optional leading icon. When provided, renders the prominent "icon-led"
   * variant (used by Insights Overview summary cards). When absent, renders
   * the compact bordered tile (used across Diagnostics).
   */
  icon?: React.ComponentType<{ className?: string; style?: React.CSSProperties }>
  /** Accent color for icon-led variant. Expects a hex or CSS color value. */
  iconColor?: string
  /**
   * Optional inline node rendered immediately after the label — typical home
   * for a `JargonHint` info button so the tile can carry an explanation
   * without forking a bespoke shell. Only honored on the bordered (no-icon)
   * variant.
   */
  labelSuffix?: React.ReactNode
  /**
   * Text alignment inside the tile. Default `'left'` matches the
   * v2 reading-paper convention. Use `'center'` for dense summary
   * grids (Branch Studio overview row, Settings OpenAlex usage,
   * Insights ratio strips) where the tiles read like a scoreboard.
   * Only honored on the bordered (no-icon) variant.
   */
  align?: 'left' | 'center'
  /**
   * Chrome around the number.
   *
   *   `bordered` (default) — the nested lifted tile: hairline + surface + inset
   *                          shadow. For a grid of tiles inside a card.
   *   `bare`               — no box at all: numeral, label, hint. For an
   *                          editorial scoreboard STRIP, where a row of boxes
   *                          reads as five competing cards and the eye has to
   *                          cross a border per figure. The parent row owns any
   *                          dividers and spacing.
   *
   * Only honored on the bordered (no-icon) shape.
   */
  variant?: 'bordered' | 'bare'
  /**
   * When set, the tile becomes an interactive control that drills into the
   * entities behind its number (e.g. the papers behind a summary count). Adds a
   * hover affordance + full keyboard activation (Enter / Space) and the right
   * ARIA role. Honored on BOTH variants.
   */
  onClick?: () => void
  className?: string
}

/**
 * Unified metric tile used across Insights (Overview, Diagnostics, Reports).
 *
 * - Without `icon`: compact bordered tile with tone-aware value color.
 *   Replaces the ad-hoc `rounded-lg border p-3 + text-xl font-bold + text-xs`
 *   pattern duplicated ~20 times in Diagnostics. Pass `variant="bare"` for a
 *   boxless scoreboard strip (Home's "Today in ALMa").
 * - With `icon`: prominent Card with large tinted icon square on the left.
 *   Replaces the Overview-only `StatCard` local helper.
 *
 * Numbers are formatted via `formatNumber` for consistency; strings render
 * as-is.
 */
export function MetricTile({
  label,
  value,
  hint,
  tone = 'neutral',
  icon: Icon,
  iconColor,
  labelSuffix,
  align = 'left',
  variant = 'bordered',
  onClick,
  className,
}: MetricTileProps) {
  const formatted = typeof value === 'number' ? formatNumber(value) : value

  // Shared interactive contract so both variants drill the same way: button
  // semantics + Enter/Space activation. Spread onto whichever root renders.
  const interactiveProps = onClick
    ? {
        onClick,
        role: 'button' as const,
        tabIndex: 0,
        'aria-label': `${label}: ${formatted}`,
        onKeyDown: (e: React.KeyboardEvent) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            onClick()
          }
        },
      }
    : {}

  if (Icon) {
    return (
      <Card
        interactive={!!onClick}
        className={cn('relative overflow-hidden', className)}
        {...interactiveProps}
      >
        <CardContent className="p-5">
          <div className="flex items-center gap-4">
            <div
              className="flex h-12 w-12 shrink-0 items-center justify-center rounded-sm"
              style={{ backgroundColor: iconColor ? `${iconColor}15` : undefined }}
            >
              <Icon className="h-6 w-6" style={{ color: iconColor }} />
            </div>
            <div className="min-w-0">
              <p className="font-brand text-2xl font-semibold text-alma-800 tabular-nums">{formatted}</p>
              <p className="text-sm font-medium text-slate-500">{label}</p>
              {hint && <p className="text-xs text-slate-400">{hint}</p>}
            </div>
          </div>
        </CardContent>
      </Card>
    )
  }

  const isCentered = align === 'center'
  const isBare = variant === 'bare'
  return (
    <div
      className={cn(
        'min-w-0',
        // Chrome-elev sits inside a chrome card (e.g. an Insights tab),
        // so the tile reads as a lifted nested surface — slightly
        // brighter than its host. min-w-0 + truncate guards against the
        // narrow-column overflow that the v2 tiles suffered from on
        // sub-400px grids.
        !isBare && 'rounded-sm border border-[var(--color-border)] bg-surface-2 p-3 shadow-paper-sm',
        // Interactive (drilldown) tiles get the folio accent on hover/focus —
        // the single interactive identity (no new color). A bare tile has no
        // border to tint, so it moves its numeral instead.
        onClick &&
          'cursor-pointer transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
        onClick && !isBare && 'hover:border-alma-folio',
        onClick && isBare && 'group rounded-sm',
        className,
      )}
      {...interactiveProps}
    >
      <p
        className={cn(
          'truncate font-brand tabular-nums',
          // A bare figure is set LARGER but LIGHTER: with no box to hold it, a
          // semibold numeral at this size reads as a headline competing with
          // the page's own. Size carries the emphasis, weight stays quiet.
          isBare ? 'text-[1.75rem] font-normal leading-none' : 'text-xl font-semibold',
          VALUE_TONE_CLASS[tone],
          onClick && isBare && 'transition-colors group-hover:text-alma-folio',
          isCentered && 'text-center',
        )}
      >
        {formatted}
      </p>
      <div
        className={cn(
          'flex items-center gap-1 text-xs',
          isBare ? 'mt-1.5 text-slate-600' : 'mt-0.5 text-slate-500',
          isCentered && 'justify-center',
        )}
      >
        <span className="truncate">{label}</span>
        {labelSuffix}
      </div>
      {hint && (
        <p
          className={cn(
            'mt-0.5 line-clamp-2 text-[11px] text-slate-400',
            isCentered && 'text-center',
          )}
        >
          {hint}
        </p>
      )}
    </div>
  )
}
