import { Button } from '@/components/ui/button'
import {
  ResearchState,
  type ResearchStateScope,
} from '@/components/ui/research-state'

interface ErrorStateProps {
  message: string
  title?: string
  actionLabel?: string
  onAction?: () => void
  actionPending?: boolean
  scope?: ResearchStateScope
  className?: string
}

export function ErrorState({
  message,
  title = 'This section could not be loaded',
  actionLabel,
  onAction,
  actionPending = false,
  scope = 'section',
  className,
}: ErrorStateProps) {
  return (
    <ResearchState
      kind="error"
      scope={scope}
      title={title}
      message={message}
      className={className}
      action={
        actionLabel && onAction ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={actionPending}
          onClick={onAction}
        >
          {actionPending ? 'Trying…' : actionLabel}
        </Button>
        ) : undefined
      }
    />
  )
}
