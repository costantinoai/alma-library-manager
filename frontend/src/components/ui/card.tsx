import * as React from 'react'
import { cn } from '@/lib/utils'
import { Surface, useSurfaceLevel, nextLevel, type SurfaceLevel } from '@/components/ui/surface'

/**
 * Card — paper-sheet surface on the single neutral elevation ladder.
 *
 * A Card reads as a real sheet of paper lifting off whatever it sits on: it
 * renders ONE level lighter than its host surface (relational — see
 * `surface.tsx`), paints `bg-surface-N` + `border-edge-N`, and stacks the
 * paper-sheet shadow for physical lift. It also hands its level down, so a
 * Card inside a Card inside a dialog all climb the same ladder with no
 * per-call-site color choices. Depth alone decides the color.
 *
 *   variant 'default'   the everyday card: relational lift + paper-sheet shadow.
 *           'elevated'  same level, larger `shadow-paper-sheet-lg`. Hero /
 *                       feature cards only (paper-detail header, anchored seed)
 *                       — one or two per page; rarity keeps it loud.
 *           'flat'      no lift, transparent fill, hairline only — does NOT
 *                       burn a rung of the ladder (children stay at the host
 *                       level). Dense meta-tile grids where individual lift
 *                       would compete with the parent card.
 *
 *   level     force an absolute level (0–4) instead of the relational host+1.
 *   interactive  whole card is clickable: hover lifts it a touch (the spring
 *                upgrade lives in the motion layer).
 */
type CardVariant = 'default' | 'flat' | 'elevated'

export interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  interactive?: boolean
  level?: SurfaceLevel
  variant?: CardVariant
}

const Card = React.forwardRef<HTMLDivElement, CardProps>(
  ({ className, interactive, level, variant = 'default', ...props }, ref) => {
    const resolvedVariant: CardVariant = variant

    const host = useSurfaceLevel()
    const isFlat = resolvedVariant === 'flat'
    const isElevated = resolvedVariant === 'elevated'

    return (
      <Surface
        ref={ref}
        // flat paints nothing and adds no depth — render at the host level so
        // descendants don't climb; otherwise lift one rung (or honor `level`).
        level={isFlat ? host : level ?? nextLevel(host)}
        filled={!isFlat}
        bordered
        className={cn(
          'relative rounded-sm transition-[box-shadow,transform] duration-200 ease-out',
          !isFlat && !isElevated && 'shadow-paper-sheet',
          isElevated && 'shadow-paper-sheet-lg',
          // Interactive: lifts off the desk on hover, presses back flush on
          // click (tactile paper feel). The transform/shadow transition is
          // already declared above; press is a touch quicker via active:.
          interactive &&
            'cursor-pointer hover:-translate-y-0.5 hover:shadow-paper-sheet-hover active:translate-y-0 active:shadow-paper-sm active:duration-75',
          className,
        )}
        {...props}
      />
    )
  },
)
Card.displayName = 'Card'

/**
 * CardHeader — section title block. Pair with `<BrandRule />` inside the card
 * body for an editorial gold separator between header and content.
 */
const CardHeader = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn('flex flex-col space-y-1.5 p-6', className)} {...props} />
  ),
)
CardHeader.displayName = 'CardHeader'

const CardTitle = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div
      ref={ref}
      className={cn('font-brand text-lg font-semibold text-alma-800', className)}
      {...props}
    />
  ),
)
CardTitle.displayName = 'CardTitle'

const CardDescription = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn('text-sm text-slate-500', className)} {...props} />
  ),
)
CardDescription.displayName = 'CardDescription'

const CardContent = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn('p-6 pt-0', className)} {...props} />
  ),
)
CardContent.displayName = 'CardContent'

const CardFooter = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn('flex items-center p-6 pt-0', className)} {...props} />
  ),
)
CardFooter.displayName = 'CardFooter'

export { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter }
