import * as React from 'react'
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from '@/components/ui/empty'
import { cn } from '@/lib/utils'

interface EmptyStateProps {
  icon?: React.ComponentType<{ className?: string }>
  title: string
  description?: string
  action?: React.ReactNode
  className?: string
}

export function EmptyState({ icon: Icon, title, description, action, className }: EmptyStateProps) {
  return (
    <Empty
      className={cn(
        // Override shadcn Empty defaults to keep ALMa's existing compact look.
        // Callers that want the airier default can pass their own padding/gap classes.
        'gap-3 rounded-sm border border-dashed border-[var(--color-border)] bg-alma-chrome px-6 py-10 md:p-10',
        className,
      )}
    >
      <EmptyHeader className="gap-2">
        {Icon && (
          <EmptyMedia
            variant="icon"
            className="size-12 rounded-full bg-parchment-100 text-alma-700 ring-1 ring-[var(--color-border)]"
          >
            <Icon className="size-5" />
          </EmptyMedia>
        )}
        <EmptyTitle className="font-brand text-base font-semibold text-alma-800">{title}</EmptyTitle>
        {description && (
          <EmptyDescription className="text-xs text-slate-500">{description}</EmptyDescription>
        )}
      </EmptyHeader>
      {action && <EmptyContent className="mt-1">{action}</EmptyContent>}
    </Empty>
  )
}
