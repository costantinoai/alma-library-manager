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
  success: 'text-emerald-700',
  warning: 'text-amber-700',
  critical: 'text-rose-700',
  info: 'text-sky-600',
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
  className?: string
}

/**
 * Unified metric tile used across Insights (Overview, Diagnostics, Reports).
 *
 * - Without `icon`: compact bordered tile with tone-aware value color.
 *   Replaces the ad-hoc `rounded-lg border p-3 + text-xl font-bold + text-xs`
 *   pattern duplicated ~20 times in Diagnostics.
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
  className,
}: MetricTileProps) {
  const formatted = typeof value === 'number' ? formatNumber(value) : value

  if (Icon) {
    return (
      <Card className={cn('relative overflow-hidden', className)}>
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
  return (
    <div
      className={cn(
        // Chrome-elev sits inside a chrome card (e.g. an Insights tab),
        // so the tile reads as a lifted nested surface — slightly
        // brighter than its host. min-w-0 + truncate guards against the
        // narrow-column overflow that the v2 tiles suffered from on
        // sub-400px grids.
        'min-w-0 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-3 shadow-paper-sm',
        className,
      )}
    >
      <p
        className={cn(
          'truncate font-brand text-xl font-semibold tabular-nums',
          VALUE_TONE_CLASS[tone],
          isCentered && 'text-center',
        )}
      >
        {formatted}
      </p>
      <div
        className={cn(
          'mt-0.5 flex items-center gap-1 text-xs text-slate-500',
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
