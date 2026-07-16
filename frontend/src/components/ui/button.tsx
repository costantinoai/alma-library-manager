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
 * - **Outline / secondary / ghost** read on the warm `cream` surface
 *   with parchment-tinted hover states. The outline variant uses the
 *   v2 hairline border (`var(--color-border)`); ghost stays chrome-free
 *   at rest.
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
