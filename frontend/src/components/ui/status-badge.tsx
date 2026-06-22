import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

// 2026-04-26 night palette consolidation: every bubble badge now
// resolves to one of TWO visuals — neutral white-with-border, or
// Folio-blue translucent (the same chip we use inside the suggested-
// author card). Saturated semantic tones (emerald / rose / amber /
// sky) were noisy across the v3 off-white surfaces, so the meaning
// stays in the *label* and the visual stays calm. The `tone` API is
// preserved so callers don't need to migrate.
const statusBadgeVariants = cva(
  'inline-flex items-center gap-1 rounded-full font-medium transition-colors',
  {
    variants: {
      tone: {
        // Neutral chip — stable structural state ("ready", "active",
        // neutral metadata). No saturation; the top-of-ladder white.
        neutral: 'border border-alma-100 bg-surface-4 text-alma-800',
        // Semantic chips — a calm translucent wash of the matching
        // semantic token. Reads as "signal", not alarm-bright.
        positive: 'border border-transparent bg-success-700/10 text-success-700',
        negative: 'border border-transparent bg-critical-700/10 text-critical-700',
        warning: 'border border-transparent bg-warning-700/10 text-warning-700',
        // Info / accent (and any non-semantic signal) ride the brand
        // accent — Folio binding blue.
        info: 'border border-transparent bg-alma-folio/10 text-alma-folio',
        accent: 'border border-transparent bg-alma-folio/10 text-alma-folio',
      },
      size: {
        sm: 'px-2 py-px text-[0.65rem]',
        default: 'px-2.5 py-0.5 text-xs',
        lg: 'px-3 py-1 text-sm',
      },
    },
    defaultVariants: {
      tone: 'neutral',
      size: 'default',
    },
  },
)

export type StatusBadgeTone = NonNullable<VariantProps<typeof statusBadgeVariants>['tone']>

export interface StatusBadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof statusBadgeVariants> {}

function StatusBadge({ className, tone, size, ...props }: StatusBadgeProps) {
  return <span className={cn(statusBadgeVariants({ tone, size }), className)} {...props} />
}

export function monitorHealthTone(health?: string | null): StatusBadgeTone {
  if (health === 'ready') return 'positive'
  if (health === 'disabled') return 'neutral'
  return 'warning'
}

export function severityTone(severity?: string | null): StatusBadgeTone {
  if (severity === 'critical') return 'negative'
  if (severity === 'warning') return 'warning'
  return 'info'
}

export function scoreStatusTone(status?: string | null): StatusBadgeTone {
  if (status === 'good') return 'positive'
  if (status === 'critical') return 'negative'
  // I-23/I-26: "insufficient_data" (empty population) and "observed" (a
  // measures-only card with no composite grade) are NOT problems — render them
  // as calm neutral chips, not alarm-amber.
  if (status === 'insufficient_data' || status === 'observed') return 'neutral'
  return 'warning'
}

export { StatusBadge }
