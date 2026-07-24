import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

/**
 * Legacy badge shell, kept because ~40 call sites use its `variant` API.
 *
 * It is now a strict ALIAS of the `StatusBadge` colour contract (see
 * `status-badge.tsx`) — same pill radius, same metrics, same washes — so a
 * `<Badge variant="success">` and a `<StatusBadge tone="positive">` are
 * visually identical. Before this alignment the two shells disagreed on both
 * radius (`rounded-sm` vs `rounded-full`) and fill, which is why pills looked
 * subtly different from screen to screen.
 *
 *   default / secondary → neutral   quiet metadata (the common case)
 *   outline             → outlined  a label/tag, same family, lighter weight
 *   success             → positive  good news
 *   warning             → warning   proceed with care
 *   destructive         → negative  a hard negative state
 *
 * NEW CODE SHOULD NOT USE THIS. Reach for `SignalChip` (which picks colour
 * from the shared semantic registry) or `StatusBadge` directly. This exists so
 * the migration can be incremental without leaving mismatched pills behind.
 */
const badgeVariants = cva(
  'inline-flex items-center gap-1 rounded-full font-medium transition-colors',
  {
    variants: {
      variant: {
        // Quiet metadata — matches StatusBadge `neutral`.
        default: 'border border-edge-2 bg-surface-2 text-slate-600',
        secondary: 'border border-edge-2 bg-surface-2 text-slate-600',
        // Outlined label — matches StatusBadge `info` in weight (transparent
        // fill + hairline) but stays neutral-hued, since most `outline`
        // callers are descriptive counts and tags, not brand signal.
        outline: 'border border-edge-3 bg-transparent text-slate-600',
        // Semantic valence — identical washes to StatusBadge.
        success: 'border border-transparent bg-success-700/10 text-success-800',
        destructive: 'border border-transparent bg-critical-700/10 text-critical-700',
        warning: 'border border-transparent bg-warning-700/12 text-warning-800',
      },
      size: {
        sm: 'px-2 py-px text-[0.65rem]',
        default: 'px-2.5 py-0.5 text-xs',
        lg: 'px-3 py-1 text-sm',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
)

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, size, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant, size }), className)} {...props} />
}

export { Badge }
