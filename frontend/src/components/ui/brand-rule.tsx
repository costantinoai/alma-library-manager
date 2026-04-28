import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * BrandRule — editorial separator that mirrors the wordmark's own rule.
 *
 * The ALMa wordmark (`branding/logo/alma-wordmark.svg`) flanks the
 * subtitle "ANOTHER LIBRARY MANAGER" with two thin gold rules and a
 * centered gold dot. This primitive lifts that exact pattern into the
 * UI so section breaks read in the same visual language as the brand.
 *
 * Variants:
 *   - `center` — `none | dot | diamond`. Adds a centered gold accent
 *     between two rule segments. Default `none` (plain gold rule).
 *   - `tone` — `gold | navy`. Default `gold`. Switch to `navy` when
 *     placed on a dark surface (sidebar, hero), where the gold rule
 *     would disappear.
 *   - `variant` — `single | double`. Default `single`. The `double`
 *     variant is two parallel rules with a 3px gap, an old-book
 *     touch for hero pages.
 *
 * Accessibility: rendered as `role="separator"` with `aria-hidden`,
 * since the rule is purely decorative.
 */
type BrandRuleCenter = 'none' | 'dot' | 'diamond'
type BrandRuleTone = 'gold' | 'navy'
type BrandRuleVariant = 'single' | 'double'

interface BrandRuleProps extends React.HTMLAttributes<HTMLDivElement> {
  center?: BrandRuleCenter
  tone?: BrandRuleTone
  variant?: BrandRuleVariant
}

const TONE_CLASS: Record<BrandRuleTone, { line: string; accent: string }> = {
  gold: { line: 'bg-gold-400', accent: 'bg-gold-400' },
  navy: { line: 'bg-alma-700', accent: 'bg-alma-300' },
}

export function BrandRule({
  center = 'none',
  tone = 'gold',
  variant = 'single',
  className,
  ...props
}: BrandRuleProps) {
  const palette = TONE_CLASS[tone]

  const accent =
    center === 'none' ? null : (
      <span
        className={cn(
          'shrink-0',
          center === 'dot' && cn('h-1.5 w-1.5 rounded-full', palette.accent),
          center === 'diamond' && cn('h-2 w-2 rotate-45', palette.accent),
        )}
        aria-hidden
      />
    )

  if (variant === 'double') {
    return (
      <div
        role="separator"
        aria-hidden
        className={cn('flex w-full flex-col items-center gap-[3px]', className)}
        {...props}
      >
        <span className={cn('h-px w-full', palette.line)} aria-hidden />
        <span className={cn('h-px w-full', palette.line)} aria-hidden />
      </div>
    )
  }

  if (center === 'none') {
    return (
      <div
        role="separator"
        aria-hidden
        className={cn('h-px w-full', palette.line, className)}
        {...props}
      />
    )
  }

  return (
    <div
      role="separator"
      aria-hidden
      className={cn('flex w-full items-center gap-2', className)}
      {...props}
    >
      <span className={cn('h-px flex-1', palette.line)} aria-hidden />
      {accent}
      <span className={cn('h-px flex-1', palette.line)} aria-hidden />
    </div>
  )
}
