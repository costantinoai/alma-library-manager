import type { ComponentType, ReactNode } from 'react'

import { StatusBadge, type StatusBadgeProps } from '@/components/ui/status-badge'
import { cn } from '@/lib/utils'

export interface IdentityChipProps {
  /**
   * The identity fill, taken from a `lib/palette.ts` map
   * (`MONITOR_TYPE_CHIP`, `SOURCE_COLORS`, `CAPTURE_CHANNEL_CHIP`, …).
   *
   * Empty string is the documented "no hue for this one" value in every such
   * map, and it falls through to the shell's own neutral tone — never a
   * hand-copied neutral, which would be free to drift from the real one.
   */
  chipClassName?: string
  /** Category glyph, exactly as on `StatusBadge`. */
  icon?: ComponentType<{ className?: string }>
  /** Hover/AT text spelling out what the hue encodes. */
  title?: string
  size?: StatusBadgeProps['size']
  children: ReactNode
  className?: string
}

/**
 * The ONE identity-coloured chip: hue answers *which one*, never *how good*.
 *
 * This is the documented exception to the valence colour contract (CLAUDE.md →
 * "Chips & pills"): a Feed monitor type, a Library provenance source and an
 * Inbox capture channel all encode WHICH, so they need a categorical hue that
 * `SignalChip`'s valence registry has no way to express.
 *
 * It exists so that exception has exactly one implementation. Three call sites
 * had each re-derived the same `<StatusBadge className={cn(chip &&
 * 'border-transparent', chip)}>` incantation, and one of them had already
 * drifted onto the legacy `Badge` shell with a different text size. The hues
 * still live in `lib/palette.ts` — this owns only the shell.
 *
 * For a chip whose colour means good-or-bad, use `SignalChip` instead.
 */
export function IdentityChip({
  chipClassName,
  icon,
  title,
  size,
  children,
  className,
}: IdentityChipProps) {
  return (
    <StatusBadge
      icon={icon}
      title={title}
      size={size}
      // The identity fill replaces the tone's own border as well as its
      // background; leaving the neutral hairline on would ring a coloured
      // wash in grey.
      className={cn(chipClassName && 'border-transparent', chipClassName, className)}
    >
      {children}
    </StatusBadge>
  )
}
