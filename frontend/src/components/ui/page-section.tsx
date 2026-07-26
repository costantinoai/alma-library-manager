import type { ComponentType, ReactNode } from 'react'

import { StatusBadge } from '@/components/ui/status-badge'
import { cn } from '@/lib/utils'

export interface PageSectionProps {
  /** Anchor for `aria-labelledby`; also the heading's DOM id. */
  id: string
  title: string
  /**
   * Category glyph beside the title — what this band is ABOUT.
   *
   * One flat folio tint for every section, deliberately: on a page where
   * colour already means valence (chips, severity rows, action buttons), a
   * per-section hue would be a second, contradictory code. The icon carries
   * category; the accent carries "this is a heading".
   */
  icon?: ComponentType<{ className?: string }>
  /**
   * How many items the section holds, as a pill beside the title.
   *
   * The count is the WHOLE set, not what happens to be rendered — the point is
   * to tell the reader how deep the section goes before they scan it. Omit it
   * when the number is unknown or meaningless; a pill reading "0" would be an
   * empty section that should not have rendered at all.
   */
  count?: number
  /** One quiet line under the title. Say what the section contains. */
  description?: ReactNode
  /** Right-aligned control on the title row — usually a link to the owner page. */
  action?: ReactNode
  children: ReactNode
  className?: string
}

/**
 * PageSection — the top-level band on a page: heading, one line of context,
 * an optional owner-page action, and its content.
 *
 * This is the page-level counterpart to `SectionHeader` (which is a card's
 * internal header). It exists so a page never hand-rolls the h2 + description
 * + action row: the type scale, the spacing rhythm, and the heading/landmark
 * wiring are decided once here.
 */
export function PageSection({
  id,
  title,
  icon: Icon,
  count,
  description,
  action,
  children,
  className,
}: PageSectionProps) {
  return (
    <section className={cn('space-y-3', className)} aria-labelledby={id}>
      <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-end sm:justify-between sm:gap-3">
        <div className="min-w-0">
          {/* The pill is a SIBLING of the heading, never inside it: nesting it
              would fold the number into the heading's accessible name, so
              "Inbox" would announce and be findable only as "Inbox 4". */}
          <div className="flex items-center gap-2">
            {Icon && <Icon className="h-[1.15rem] w-[1.15rem] shrink-0 text-alma-folio" aria-hidden />}
            <h2 id={id} className="font-brand text-lg font-semibold text-alma-800">
              {title}
            </h2>
            {count != null && count > 0 && (
              <StatusBadge
                tone="neutral"
                size="sm"
                className="font-mono tabular-nums"
                aria-label={`${count} in total`}
              >
                {count}
              </StatusBadge>
            )}
          </div>
          {description && <p className="text-sm text-slate-500">{description}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  )
}
