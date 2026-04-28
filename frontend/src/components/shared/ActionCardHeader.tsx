import { CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'

export interface ActionCardHeaderProps {
  icon: React.ComponentType<{ className?: string }>
  title: string
  description?: string
  /** Tailwind color class for the icon (e.g. "text-blue-500"). */
  accent?: string
  /** Right-aligned primary action. Usually a `<Button>` or spinner state. */
  action?: React.ReactNode
  className?: string
}

/**
 * CardHeader variant with an icon-anchored title on the left and a primary
 * action (usually a "Generate" / "Open" / "Refresh" button) on the right.
 *
 * Collapses the `flex flex-row items-center justify-between gap-3` +
 * icon + title + description + Button pattern that was duplicated 5× in
 * InsightsReportsTab. Sibling to `SectionHeader`, which has no action slot.
 */
export function ActionCardHeader({
  icon: Icon,
  title,
  description,
  accent = 'text-alma-700',
  action,
  className,
}: ActionCardHeaderProps) {
  return (
    <CardHeader
      className={cn('flex flex-row items-center justify-between gap-3 border-b border-gold-300/60 pb-3', className)}
    >
      <div className="min-w-0">
        <CardTitle className="flex items-center gap-2 text-lg">
          <Icon className={cn('h-5 w-5', accent)} />
          {title}
        </CardTitle>
        {description && (
          <p className="mt-1 text-sm text-slate-500">{description}</p>
        )}
      </div>
      {action && <div className="shrink-0">{action}</div>}
    </CardHeader>
  )
}
