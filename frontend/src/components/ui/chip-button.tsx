import * as React from 'react'

import { cn } from '@/lib/utils'

export interface ChipButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  /** The chip is on: same accent state as every other control. */
  selected?: boolean
}

/**
 * ChipButton — a pill you press. `StatusBadge` reports; this one acts, so it
 * takes a control's ink fill and focus ring rather than a colour of its own.
 */
export const ChipButton = React.forwardRef<HTMLButtonElement, ChipButtonProps>(
  ({ className, selected = false, type = 'button', ...props }, ref) => (
    <button
      ref={ref}
      type={type}
      className={cn(
        'inline-flex cursor-pointer items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs transition-colors focus-ring',
        selected
          ? 'border-alma-folio bg-accent-soft text-alma-folio'
          : 'border-control-edge bg-control-well text-alma-700 hover:border-control-edge-strong',
        className,
      )}
      {...props}
    />
  ),
)
ChipButton.displayName = 'ChipButton'
