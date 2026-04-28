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

export function BookmarkIcon({ className, ...props }: IconProps) {
  return (
    <svg {...baseProps} strokeWidth={14} className={className} {...props}>
      <path d="M55 30 L165 30 L165 195 L110 155 L55 195 Z" />
    </svg>
  )
}

export function PaperIcon({ className, ...props }: IconProps) {
  return (
    <svg {...baseProps} strokeWidth={12} className={className} {...props}>
      <path d="M50 25 L130 25 L185 80 L185 195 L50 195 Z" />
      <path d="M130 25 L130 80 L185 80" />
      <path d="M75 110 L160 110 M75 140 L160 140 M75 170 L135 170" />
    </svg>
  )
}

export function LibraryIcon({ className, ...props }: IconProps) {
  return (
    <svg {...baseProps} strokeWidth={12} className={className} {...props}>
      <path d="M40 195 L40 75 L70 75 L70 195" />
      <path d="M75 195 L75 50 L110 50 L110 195" />
      <path d="M115 195 L115 90 L150 90 L150 195" />
      <path d="M30 195 L195 195" />
      <path d="M48 110 L62 110 M85 95 L100 95 M125 130 L140 130" />
    </svg>
  )
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
