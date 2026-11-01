import * as React from 'react'

import { cn } from '@/lib/utils'

export interface RowButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  /** The row is the current choice: the shared selected wash, folio text. */
  selected?: boolean
}

/**
 * RowButton — a whole row you press: a list entry, an expander, a picker line.
 *
 * It owns what every such row must have and `Button` must not impose: full
 * width, left-aligned content, the focus ring and the selected wash. Padding,
 * borders and hover stay with the caller, because a row belongs to its list.
 */
export const RowButton = React.forwardRef<HTMLButtonElement, RowButtonProps>(
  ({ className, selected = false, type = 'button', ...props }, ref) => (
    <button
      ref={ref}
      type={type}
      className={cn(
        'flex w-full cursor-pointer items-center gap-2 text-left transition-colors focus-ring disabled:cursor-default',
        selected && 'bg-accent-soft text-alma-folio',
        className,
      )}
      {...props}
    />
  ),
)
RowButton.displayName = 'RowButton'
