import { AlertCircle } from 'lucide-react'

import { Button } from '@/components/ui/button'

interface ErrorStateProps {
  message: string
  actionLabel?: string
  onAction?: () => void
  actionPending?: boolean
}

export function ErrorState({
  message,
  actionLabel,
  onAction,
  actionPending = false,
}: ErrorStateProps) {
  return (
    <div className="flex flex-wrap items-center justify-center gap-3 rounded border border-critical-200 bg-critical-50 px-4 py-8">
      <span className="flex items-center gap-2">
        <AlertCircle className="h-5 w-5 text-critical-500" />
        <span className="text-sm text-critical-700">{message}</span>
      </span>
      {actionLabel && onAction ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={actionPending}
          onClick={onAction}
        >
          {actionPending ? 'Trying…' : actionLabel}
        </Button>
      ) : null}
    </div>
  )
}
