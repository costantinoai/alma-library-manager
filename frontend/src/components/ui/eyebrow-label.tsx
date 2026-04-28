import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

/**
 * EyebrowLabel — small-caps overline for section announcements.
 *
 * Replaces the proliferating
 *   `text-[11px] font-semibold uppercase tracking-wide text-slate-500`
 * inline pattern (~30+ call sites: Discovery profile-list titles,
 * Insights diagnostics headers, Settings card subtitles, PaperCard
 * score-breakdown header, etc.).
 *
 * Tones:
 *   - `accent` (default) — teal, draws the eye toward editorial
 *     callouts ("FAVORITE TOPICS", "NEW THIS WEEK")
 *   - `muted` — slate, neutral subdivision in dense settings/forms
 *   - `ink` — navy, used on warm parchment/cream when teal would
 *     compete with adjacent tone
 *
 * Tracking and weight match the v2 spec
 * (`branding/type.md` § "Small caps / labels"):
 *   `font-weight: 700; letter-spacing: 0.16em; text-transform: uppercase`.
 */
const eyebrowVariants = cva(
  'block text-[11px] font-bold uppercase tracking-[0.16em]',
  {
    variants: {
      tone: {
        accent: 'text-alma-folio',
        muted:  'text-slate-500',
        ink:    'text-alma-800',
      },
    },
    defaultVariants: {
      tone: 'accent',
    },
  },
)

export interface EyebrowLabelProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof eyebrowVariants> {}

export function EyebrowLabel({ className, tone, ...props }: EyebrowLabelProps) {
  return <span className={cn(eyebrowVariants({ tone }), className)} {...props} />
}
