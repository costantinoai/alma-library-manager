import * as React from 'react'
import { cn } from '@/lib/utils'
import { Surface } from '@/components/ui/surface'
import { useSurfaceLevel, nextLevel, type SurfaceLevel } from '@/components/ui/surface-level'

/**
 * SubPanel — recessed inner frame on the single neutral ladder.
 *
 * Like Card, a SubPanel climbs one level (it's lighter than its host), but it
 * adds the inset paper shadow so it reads as pressed INTO the page rather than
 * lifted off it — two kinds of physical hierarchy from one ladder. Use it to
 * group content inside a card: abstract frames, score breakdowns, config
 * sub-sections.
 *
 *   variant 'default'  ladder surface + inset shadow (the recessed frame).
 *           'accent'   folio-soft tint + accent hairline. Highlight only
 *                      ("top contributor", "selected lane", "winner") —
 *                      keep it rare so it stays meaningful.
 *           'flat'     ladder surface, hairline only, no inset — plain
 *                      grouping box.
 *
 *   cool     recessed box on the COOL surface variant — the one deliberate
 *            exception, used ONLY inside the Activity panel (telemetry).
 *   level    force an absolute level (0–4) instead of the relational host+1.
 */
type SubPanelVariant = 'default' | 'accent' | 'flat'

export interface SubPanelProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: SubPanelVariant
  level?: SurfaceLevel
  /** Apply default p-4 spacing. Disable for tight compositions. */
  padded?: boolean
  /** Cool telemetry variant — Activity panel only (the one cool surface). */
  cool?: boolean
}

const SubPanel = React.forwardRef<HTMLDivElement, SubPanelProps>(
  ({ className, variant = 'default', level, padded = true, cool = false, ...props }, ref) => {
    const host = useSurfaceLevel()
    const isFlat = variant === 'flat'
    const isAccent = variant === 'accent'

    // Cool variant is off the warm ladder (Activity panel telemetry): a cool
    // recessed box. Rendered directly rather than via the (warm) Surface.
    if (cool) {
      return (
        <div
          ref={ref}
          className={cn(
            'rounded-sm border border-[var(--color-border-cool)] bg-surface-cool-2',
            padded && 'p-4',
            !isFlat && 'shadow-paper-inset-cool',
            className,
          )}
          {...props}
        />
      )
    }

    return (
      <Surface
        ref={ref}
        level={level ?? nextLevel(host)}
        // accent paints its own tint over the ladder, so suppress the ladder
        // fill + border and supply the accent ones below.
        filled={!isAccent}
        bordered={!isAccent}
        className={cn(
          'rounded-sm',
          padded && 'p-4',
          !isFlat && !isAccent && 'shadow-paper-inset',
          isAccent && 'border border-accent-edge bg-accent-soft',
          className,
        )}
        {...props}
      />
    )
  },
)
SubPanel.displayName = 'SubPanel'

export { SubPanel }
export type { SubPanelVariant }
