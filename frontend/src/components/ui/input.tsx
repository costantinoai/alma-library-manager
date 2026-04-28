import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Input — paper-recessed text field.
 *
 * Sits on `paper` (#FFFCF7) — the body bg — so when an input is dropped
 * into a cream Card it reads as a well stamped INTO the page, not as a
 * second cream surface flush with the card. A faint cool inset shadow
 * reinforces the indented feel; the parchment hairline border ties it
 * to the surrounding paper system.
 */
const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(
  ({ className, type, ...props }, ref) => {
    return (
      <input
        type={type}
        className={cn(
          'flex h-10 w-full rounded-sm border border-[var(--color-border)] bg-alma-paper px-3 py-2 text-sm text-alma-900 placeholder:text-slate-400 shadow-paper-inset-cool focus:outline-none focus:ring-2 focus:ring-alma-folio focus:border-transparent disabled:cursor-not-allowed disabled:opacity-50',
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
