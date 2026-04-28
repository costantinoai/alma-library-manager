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
        // White card chip — stable structural state ("ready", "active",
        // neutral metadata). No saturation.
        neutral: 'border border-alma-100 bg-white text-alma-800',
        // Semantic chips — same translucent treatment as Folio blue,
        // but pulled toward the matching deep semantic hue. Reads as
        // "calm signal" against the off-white surface, NOT alarm-bright.
        positive: 'border border-transparent bg-emerald-700/10 text-emerald-700',
        negative: 'border border-transparent bg-rose-700/10 text-rose-700',
        warning: 'border border-transparent bg-amber-700/10 text-amber-700',
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
  return 'warning'
}

export { StatusBadge }
