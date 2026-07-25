import * as React from "react"

import { cn } from "@/lib/utils"

const Textarea = React.forwardRef<
  HTMLTextAreaElement,
  React.ComponentProps<"textarea">
>(({ className, ...props }, ref) => {
  return (
    <textarea
      className={cn(
        // Recessed INK well — matches Input/Select/Checkbox so form controls
        // indent consistently into any host surface instead of borrowing its
        // cream. See "controls are ink, surfaces are paper" in index.css.
        "flex min-h-[80px] w-full rounded-sm border border-control-edge bg-control-well px-3 py-2 text-base placeholder:text-slate-400 shadow-paper-inset-cool focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-surface-1 disabled:cursor-not-allowed disabled:opacity-50 aria-[invalid=true]:border-critical-500 aria-[invalid=true]:ring-2 aria-[invalid=true]:ring-critical-500/30 md:text-sm",
        className
      )}
      ref={ref}
      {...props}
    />
  )
})
Textarea.displayName = "Textarea"

export { Textarea }
