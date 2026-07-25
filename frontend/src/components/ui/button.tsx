import * as React from 'react'
import { Slot } from '@radix-ui/react-slot'
import { Loader2 } from 'lucide-react'
import {
  buttonVariants,
  type ButtonVariantProps,
} from '@/components/ui/button-variants'
import { cn } from '@/lib/utils'

/**
 * Button — ALMa v2 brand button primitive.
 *
 * Design rationale (post-rebrand, 2026-04-25):
 * - **Shape** — `rounded-sm` (2px) letterpress edge. The earlier
 *   `rounded-md` (6px) still felt too soft; paper buttons want a
 *   crisp, almost-square corner. The ALMa identity is "strength of
 *   knowledge + softness of a library", not sportswear pill.
 * - **Filled variants** (default / accent / destructive / success / gold)
 *   keep a cool flat fill with a faint paper-warm shadow at rest
 *   (`shadow-paper-sm`) and lift slightly on hover (`shadow-paper-md`).
 *   No colored-glow halo, no inset white ring, no gradient. The fill
 *   does the work; the shadow just suggests paper resting on paper.
 * - **Outline / secondary / ghost** fill from the CONTROL INK ladder
 *   (`control-well` / `control-quiet`), not from the paper ramps — see
 *   "controls are ink, surfaces are paper" in index.css. That keeps an
 *   unfilled button reading as the same object whether it sits on a card,
 *   a panel, or inside a near-white popover, instead of borrowing (and
 *   dissolving into) whatever cream is under it.
 * - **`default` (navy `alma-800`) is the ONE heavy button fill.** Every
 *   filled primary action — Follow, Save, Create, Confirm — takes it, so
 *   the same verb is never two different blues on two different surfaces.
 *   `accent` (folio) is the *interactive identity*: links, active nav,
 *   focus rings, and selected / active / checked / on states. Reaching for
 *   it as a second CTA fill is what let Follow render folio on the
 *   suggested-author card and navy in the author hover card (fixed
 *   2026-07-25). Filled `accent` buttons are for surfaces that are ALL
 *   accent by design — the onboarding flow's step CTAs.
 * - **Gold** is reserved for fine accents / premium actions (export,
 *   citation copy, decorative CTAs). It is NOT a default CTA.
 * - **Focus** — teal halo (the v2 accent) on the warm paper offset, so
 *   keyboard navigation feels editorial, not generic-blue.
 * - Auto-icon sizing + `loading` spinner unchanged from the previous
 *   primitive contract.
 */
export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    ButtonVariantProps {
  asChild?: boolean
  loading?: boolean
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild = false, loading = false, children, disabled, ...props }, ref) => {
    const Comp = asChild ? Slot : 'button'
    return (
      <Comp
        className={cn(buttonVariants({ variant, size, className }))}
        ref={ref}
        disabled={disabled || loading}
        aria-busy={loading || undefined}
        {...props}
      >
        {/* asChild → Slot requires exactly ONE child; passing the spinner
            expression alongside (even when it's `false`) makes the children an
            array and trips `React.Children.only`. So hand Slot the single child
            untouched, and only compose the spinner on the real <button>. */}
        {asChild ? (
          children
        ) : (
          <>
            {loading && <Loader2 className="size-4 animate-spin" />}
            {children}
          </>
        )}
      </Comp>
    )
  },
)
Button.displayName = 'Button'

export { Button }
