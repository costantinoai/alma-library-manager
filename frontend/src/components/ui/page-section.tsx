import type { ReactNode } from 'react'

import { cn } from '@/lib/utils'

export interface PageSectionProps {
  /** Anchor for `aria-labelledby`; also the heading's DOM id. */
  id: string
  title: string
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
  description,
  action,
  children,
  className,
}: PageSectionProps) {
  return (
    <section className={cn('space-y-3', className)} aria-labelledby={id}>
      <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-end sm:justify-between sm:gap-3">
        <div className="min-w-0">
          <h2 id={id} className="font-brand text-lg font-semibold text-alma-800">
            {title}
          </h2>
          {description && <p className="text-sm text-slate-500">{description}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  )
}
