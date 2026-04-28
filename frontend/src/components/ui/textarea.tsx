import * as React from "react"

import { cn } from "@/lib/utils"

const Textarea = React.forwardRef<
  HTMLTextAreaElement,
  React.ComponentProps<"textarea">
>(({ className, ...props }, ref) => {
  return (
    <textarea
      className={cn(
        // Recessed paper well — matches Input/Select/Checkbox so form
        // controls indent consistently into cream cards instead of
        // disappearing into them.
        "flex min-h-[80px] w-full rounded-sm border border-[var(--color-border)] bg-alma-paper px-3 py-2 text-base placeholder:text-slate-400 shadow-paper-inset-cool focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-alma-paper disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
        className
      )}
      ref={ref}
      {...props}
    />
  )
})
Textarea.displayName = "Textarea"

export { Textarea }
