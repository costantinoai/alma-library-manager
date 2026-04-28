import * as React from 'react'
import { Slot } from '@radix-ui/react-slot'
import { cva, type VariantProps } from 'class-variance-authority'
import { Loader2 } from 'lucide-react'
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
const buttonVariants = cva(
  [
    // 2px corner — letterpress / index-card feel, not pill or bubbly.
    'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-sm text-sm font-medium',
    'transition-[color,background-color,border-color,box-shadow] duration-200 ease-out',
    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-alma-paper',
    'disabled:pointer-events-none disabled:opacity-50',
    "[&_svg:not([class*='size-']):not([class*='h-']):not([class*='w-'])]:size-4",
    '[&_svg]:shrink-0',
    'cursor-pointer select-none',
  ].join(' '),
  {
    variants: {
      variant: {
        // Primary — brand navy, ink-on-paper feel.
        default:
          'bg-alma-800 text-alma-cream shadow-paper-sm hover:bg-alma-700 hover:shadow-paper-md active:bg-alma-900',
        // Accent — brand teal. Used sparingly for "discover" / decorative CTAs.
        accent:
          'bg-alma-folio text-alma-cream shadow-paper-sm hover:bg-[#0a5957] hover:shadow-paper-md active:bg-[#073e3d]',
        // Destructive — critical-600. Soft red ink, not shouty SaaS-saturation.
        destructive:
          'bg-critical-600 text-white shadow-paper-sm hover:bg-critical-700 hover:shadow-paper-md',
        // Success — success-600.
        success:
          'bg-success-600 text-white shadow-paper-sm hover:bg-success-700 hover:shadow-paper-md',
        // Gold — fine accent / premium actions only. Deep gold on cream;
        // text is brand ink for legibility.
        gold:
          'bg-gold-400 text-alma-900 shadow-paper-sm hover:bg-gold-500 hover:text-alma-cream hover:shadow-paper-md',
        // Outline — paper surface + warm hairline border + ink text.
        // Paper bg (not cream) so the button reads as a distinct
        // surface when sitting inside a cream Card. Hover tints to
        // soft parchment + deepens border.
        outline:
          'border border-[var(--color-border)] bg-alma-paper text-alma-900 shadow-paper-sm hover:border-parchment-400 hover:bg-parchment-100',
        // Secondary — soft parchment fill.
        secondary:
          'bg-parchment-100 text-alma-900 hover:bg-parchment-200',
        // Ghost — no chrome at rest, parchment tint on hover.
        ghost:
          'text-alma-700 hover:bg-parchment-100 hover:text-alma-900',
        // Link — text-only with teal underline on hover. Drops the radius
        // and shadow so it can sit inline with prose.
        link:
          'rounded-none px-0 text-alma-folio underline-offset-4 shadow-none hover:underline hover:text-[#0a5957]',
      },
      size: {
        default: 'h-9 px-4',
        sm: 'h-8 px-3 text-xs gap-1.5',
        xs: 'h-7 px-2 text-xs gap-1',
        lg: 'h-11 px-6 text-base gap-2.5',
        icon: 'size-9',
        'icon-sm': 'size-8',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
)

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
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
        {loading && !asChild && <Loader2 className="size-4 animate-spin" />}
        {children}
      </Comp>
    )
  },
)
Button.displayName = 'Button'

export { Button, buttonVariants }
