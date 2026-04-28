import { CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'

export interface SectionHeaderProps {
  icon: React.ComponentType<{ className?: string }>
  title: string
  description?: string
  /** Tailwind class for the icon color (e.g. "text-alma-700"). */
  accent?: string
  className?: string
}

/**
 * Shared card header with an icon anchor + title + optional description.
 *
 * Used across Insights tabs (Overview / Reports / Diagnostics) to give
 * dense scroll views a consistent visual rhythm. Underlined with a thin
 * gold hairline — the same trim the wordmark uses around its subtitle
 * — so every section break reads like a chapter rule in a Folio book,
 * not a SaaS card heading.
 */
export function SectionHeader({
  icon: Icon,
  title,
  description,
  accent = 'text-alma-700',
  className,
}: SectionHeaderProps) {
  return (
    <CardHeader className={cn('border-b border-gold-300/60 pb-3', className)}>
      <CardTitle className="flex items-center gap-2 text-lg">
        <Icon className={cn('h-5 w-5', accent)} />
        {title}
      </CardTitle>
      {description && <p className="text-sm text-slate-500">{description}</p>}
    </CardHeader>
  )
}
