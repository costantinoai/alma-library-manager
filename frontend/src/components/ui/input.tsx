import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Input — ink-recessed text field.
 *
 * Fills with `control-well` (a translucent ink wash) rather than a step on
 * the paper ladder, so the field reads as a well stamped INTO whatever page
 * it lands on. The earlier `bg-surface-0` pinned it to the desk's cream:
 * correct inside a Card, a dark slab inside a near-white popover. A faint
 * inset shadow reinforces the indent; `control-edge` is the control
 * hairline. See "controls are ink, surfaces are paper" in index.css.
 */
const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          'flex h-10 w-full rounded-sm border border-control-edge bg-control-well px-3 py-2 text-sm text-alma-900 placeholder:text-slate-400 shadow-paper-inset-cool focus:outline-none focus:ring-2 focus:ring-alma-folio focus:border-transparent disabled:cursor-not-allowed disabled:opacity-50 aria-[invalid=true]:border-critical-500 aria-[invalid=true]:ring-2 aria-[invalid=true]:ring-critical-500/30',
          className,
        )}
        ref={ref}
        {...props}
      />
    )
  },
)
Input.displayName = 'Input'

export { Input }
