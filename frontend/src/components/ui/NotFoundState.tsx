import type { ReactNode } from 'react'

import {
  ResearchState,
  type ResearchStateScope,
} from '@/components/ui/research-state'

interface NotFoundStateProps {
  title?: string
  message: string
  action?: ReactNode
  scope?: ResearchStateScope
  className?: string
}

export function NotFoundState({
  title = 'That page is not in this catalogue',
  message,
  action,
  scope = 'section',
  className,
}: NotFoundStateProps) {
  return (
    <ResearchState
      kind="not-found"
      scope={scope}
      title={title}
      message={message}
      action={action}
      className={className}
    />
  )
}
