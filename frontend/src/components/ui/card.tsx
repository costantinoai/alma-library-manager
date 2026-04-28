import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Card — paper-sheet surface primitive (v3 brand, "two papers, one desk").
 *
 * The card reads as a real piece of paper resting on the warm parchment
 * desk: face catching ambient light at the top edge, hairline parchment
 * border, a tight close shadow for paper thickness, and a soft far
 * ambient drop for depth. Stacked into one `--shadow-paper-sheet` token
 * so a single `shadow-paper-sheet` utility delivers the whole effect —
 * single-layer drop shadows look synthetic, layered ones read as
 * physical material.
 *
 * Two paper qualities lift from the same desk. The tone prop says which.
 *
 *   - `chrome`   (DEFAULT)  cooler off-white working paper — the
 *                everyday card: settings, lens controls, branch UI
 *                chrome, alerts automation, insights tiles, dialogs,
 *                popovers, every form panel. Most cards are chrome.
 *   - `content`             warmer ivory cream reading paper — reserved
 *                for cards that BE the content the user came to read:
 *                a paper row, an author tile, a recommendation, the
 *                inner branch result tiles inside Branch Studio, the
 *                paper-detail panel body. The warmer tone is the
 *                primitive's quiet way of saying "this is the work."
 *   - `elevated`            warm content gradient + the larger
 *                paper-sheet-lg shadow. Hero / feature cards only
 *                (paper-detail header, anchored seed). One or two per
 *                page max — rarity keeps it loud.
 *   - `flat`                hairline border only, no shadow, no surface
 *                fill (transparent so it inherits its host). Dense grids
 *                of meta tiles where individual lift would compete with
 *                the parent card. Reads like a small index card.
 *
 * Pass `interactive` when the whole card is clickable. Hover deepens
 * the shadow and lifts the card 1px — a brief, calm gesture, not a
 * leap. The lift cooperates with the bottom-edge close shadow so the
 * paper appears to be peeled fractionally off the desk, not levitating.
 *
 * Migration note (v2 → v3, 2026-04-26):
 *   The v2 default tone was `paper` (cream surface, generic). v3 splits
 *   that into `chrome` and `content` — a Card that wants to be the
 *   thing-to-read explicitly opts in via tone="content". `paper` is
 *   accepted as a deprecated alias of `chrome` during the migration
 *   sweep; new callers should use `chrome` (or omit, since it's the
 *   default).
 */
type CardTone = 'chrome' | 'content' | 'elevated' | 'flat' | 'paper'

const Card = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & {
    interactive?: boolean
    tone?: CardTone
  }
>(({ className, interactive, tone = 'chrome', ...props }, ref) => {
  const resolvedTone = tone === 'paper' ? 'chrome' : tone
  return (
    <div
      ref={ref}
      className={cn(
        // Paper-sheet geometry — 2px corner reads as crisp paper edge,
        // not bubbly chip. The depth comes from the shadow stack, not
        // from radius.
        'relative rounded-sm border border-[var(--color-border)]',
        'transition-[box-shadow,transform] duration-200 ease-out',
        resolvedTone === 'chrome' && 'bg-alma-chrome shadow-paper-sheet',
        resolvedTone === 'content' && 'bg-alma-content shadow-paper-sheet',
        resolvedTone === 'elevated' && 'alma-paper-sheet-bg shadow-paper-sheet-lg',
        resolvedTone === 'flat' && 'bg-transparent',
        interactive &&
          'cursor-pointer hover:-translate-y-px hover:shadow-paper-sheet-hover',
        className,
      )}
      {...props}
    />
  )
})
Card.displayName = 'Card'

/**
 * CardHeader — sets up a section title block. Pair with `<BrandRule />`
 * inside the card body when you want an editorial gold separator
 * between header and content (mirrors the wordmark's own rule).
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
