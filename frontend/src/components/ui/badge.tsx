import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

// 2026-04-26 night palette consolidation: bubble badges collapse to
// two visuals — white card chip (default / secondary / outline) or
// Folio-blue translucent (success / destructive / warning, all
// signal-bearing). See `tasks/lessons.md` "Surface contrast" for the
// rationale. The `variant` API is preserved so existing callers
// don't need to migrate.
const badgeVariants = cva(
  'inline-flex items-center rounded-sm font-medium transition-colors',
  {
    variants: {
      variant: {
        default: 'border border-alma-100 bg-white text-alma-800',
        secondary: 'border border-alma-100 bg-white text-alma-800',
        outline: 'border border-alma-100 bg-white text-alma-800',
        success: 'border border-transparent bg-emerald-700/10 text-emerald-700',
        destructive: 'border border-transparent bg-rose-700/10 text-rose-700',
        warning: 'border border-transparent bg-amber-700/10 text-amber-700',
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
