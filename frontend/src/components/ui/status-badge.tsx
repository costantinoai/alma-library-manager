import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

/**
 * The ONE badge/pill visual in the app (CLAUDE.md: "StatusBadge tone is the
 * only badge path"). Every chip — Discovery evidence, Feed monitors, paper
 * metadata, source provenance — resolves through this component, so a pill
 * looks the same everywhere and its colour always means the same thing.
 *
 * ── The colour contract (2026-07-25) ────────────────────────────────────
 * Colour encodes VALENCE — is this fact arguing for or against the thing? —
 * and nothing else. Category ("is this a citation fact or an author fact?")
 * is carried by the `icon` slot, not by hue. The split is deliberate: hue
 * has only a few legible steps, and the old scheme spent them on category
 * (indigo / emerald / orange / cyan / violet) which left the reader unable
 * to tell good news from bad. Now one glance answers "for or against", and
 * the glyph answers "about what".
 *
 *   positive  success wash   argues FOR, strongly — matches what you save
 *   accent    folio fill     the engine's own reasoning — why this surfaced
 *   info      folio outline  an informational tag (same family, quieter)
 *   warning   warning wash   argues AGAINST / proceed with care
 *   negative  critical wash  a hard negative state — retracted, failed
 *   neutral   quiet surface  descriptive metadata + retrieval plumbing
 *
 * `neutral` is deliberately recessive — a surface step with a hairline, not
 * the stark white lozenge it used to be — because most chips are metadata
 * and should sit *under* the coloured ones that carry real signal. `accent`
 * (filled) vs `info` (outlined) is a fill/outline pair, not a second hue:
 * same brand blue, different weight. They were literally identical CSS
 * before, which made the distinction meaningless.
 */
const statusBadgeVariants = cva(
  'inline-flex items-center gap-1 rounded-full font-medium transition-colors',
  {
    variants: {
      tone: {
        // Quiet metadata + mechanism. One surface step up from the card it
        // sits on, hairline edge — reads as a chip, not as a white sticker.
        neutral: 'border border-edge-2 bg-surface-2 text-slate-600',
        // Semantic valence — a calm translucent wash of the matching token.
        // Signal, not alarm: these sit on off-white paper surfaces.
        positive: 'border border-transparent bg-success-700/10 text-success-800',
        negative: 'border border-transparent bg-critical-700/10 text-critical-700',
        warning: 'border border-transparent bg-warning-700/12 text-warning-800',
        // Engine reasoning — brand accent, FILLED.
        accent: 'border border-transparent bg-alma-folio/10 text-alma-folio',
        // Informational tag — same accent family, OUTLINED so it reads as a
        // "label" rather than a "signal" without introducing another hue.
        info: 'border border-accent-edge bg-transparent text-alma-folio',
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

const ICON_SIZE = {
  sm: 'h-2.5 w-2.5',
  default: 'h-3 w-3',
  lg: 'h-3.5 w-3.5',
} as const

export type StatusBadgeTone = NonNullable<VariantProps<typeof statusBadgeVariants>['tone']>

export interface StatusBadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof statusBadgeVariants> {
  /** Category glyph. Colour says for/against; this says what the fact is
   *  about — so both read at a glance without spending a second hue. */
  icon?: React.ComponentType<{ className?: string }>
}

function StatusBadge({
  className,
  tone,
  size,
  icon: Icon,
  children,
  ...props
}: StatusBadgeProps) {
  return (
    <span className={cn(statusBadgeVariants({ tone, size }), className)} {...props}>
      {Icon && (
        <Icon className={cn('shrink-0 opacity-80', ICON_SIZE[size ?? 'default'])} aria-hidden />
      )}
      {children}
    </span>
  )
}

export { StatusBadge }
