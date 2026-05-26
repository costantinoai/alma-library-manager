import * as React from 'react'
import { cn } from '@/lib/utils'
import { Surface, useSurfaceLevel, nextLevel, type SurfaceLevel } from '@/components/ui/surface'

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
 *   level    force an absolute level (0–4) instead of the relational host+1.
 */
type SubPanelVariant = 'default' | 'accent' | 'flat'

/** @deprecated v3 tones. Kept so existing call sites compile during the
 * migration; map onto the single-ladder variants. The cool "ops" telemetry
 * surface is folded into the neutral ladder. Removed after the sweep. */
type DeprecatedSubTone = 'content' | 'chrome' | 'ops' | 'accent' | 'parchment' | 'paper' | 'cool'

const TONE_TO_VARIANT: Record<DeprecatedSubTone, SubPanelVariant> = {
  content: 'default',
  parchment: 'default',
  ops: 'default',
  cool: 'default',
  chrome: 'flat',
  paper: 'flat',
  accent: 'accent',
}

export interface SubPanelProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: SubPanelVariant
  level?: SurfaceLevel
  /** Apply default p-4 spacing. Disable for tight compositions. */
  padded?: boolean
  /** @deprecated use `variant` / `level`. */
  tone?: DeprecatedSubTone
}

const SubPanel = React.forwardRef<HTMLDivElement, SubPanelProps>(
  ({ className, variant, level, padded = true, tone, ...props }, ref) => {
    const resolved: SubPanelVariant = variant ?? (tone ? TONE_TO_VARIANT[tone] : 'default')
    const host = useSurfaceLevel()
    const isFlat = resolved === 'flat'
    const isAccent = resolved === 'accent'
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
/** @deprecated alias kept for back-compat; use SubPanelVariant. */
export type SubPanelTone = DeprecatedSubTone
