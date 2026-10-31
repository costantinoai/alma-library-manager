import * as React from 'react'

/**
 * Brand-element icons — distinctive line illustrations from the v2
 * brand kit (`branding/logo/alma-brand-elements.svg`). Reserved for
 * "first impression" / empty-state moments, not functional UI.
 *
 * Lucide icons stay everywhere else (toolbar buttons, inline action
 * affordances). These hand-drawn-style line marks add a small
 * editorial touch where the user lands on a blank surface — Library
 * empty state, Imports empty state, Discovery empty state, etc.
 *
 * Each icon uses `stroke="currentColor"` so it picks up `text-…`
 * classes from the parent. The `EmptyState` primitive renders icons
 * inside a small parchment circle on the v2 surface, so a navy /
 * teal / muted current-color reads cleanly.
 */
type IconProps = Omit<React.SVGAttributes<SVGSVGElement>, 'viewBox' | 'fill' | 'stroke'> & {
  className?: string
}

const baseProps = {
  viewBox: '0 0 220 220',
  fill: 'none' as const,
  stroke: 'currentColor',
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  'aria-hidden': true,
}

export function DiscoverIcon({ className, ...props }: IconProps) {
  return (
    <svg {...baseProps} strokeWidth={12} className={className} {...props}>
      <circle cx="92" cy="92" r="55" />
      <circle cx="92" cy="92" r="36" />
      <path d="M132 132 L185 185" />
      <path d="M168 158 L195 185" />
    </svg>
  )
}
