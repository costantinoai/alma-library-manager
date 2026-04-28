import * as React from "react"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

// Tone variants mirror the `StatusBadge` tone vocabulary so a warning
// callout and a warning chip land on the same palette. Forked inline
// markup like `<Alert className="border-amber-200 bg-amber-50">` is
// exactly the drift this vocabulary is meant to prevent; reach for
// `variant="warning"` instead.
const alertVariants = cva(
  "relative w-full rounded border border-[var(--color-border)] p-4 [&>svg~*]:pl-7 [&>svg+div]:translate-y-[-3px] [&>svg]:absolute [&>svg]:left-4 [&>svg]:top-4 [&>svg]:text-alma-800",
  {
    variants: {
      variant: {
        default: "bg-alma-chrome text-alma-900",
        destructive:
          "border-red-500/50 text-red-500 dark:border-red-500 [&>svg]:text-red-500 dark:border-red-900/50 dark:text-red-900 dark:dark:border-red-900 dark:[&>svg]:text-red-900",
        // Soft "negative" tone matching the StatusBadge `negative` palette —
        // rose-200/rose-50/rose-700. Use for anti-affordance content (top
        // negative preferences, suppressed entities, dislike summaries) where
        // `destructive` red is too loud.
        negative:
          "border-rose-200 bg-rose-50 text-rose-800 [&>svg]:text-rose-700",
        warning:
          "border-amber-200 bg-amber-50 text-amber-800 [&>svg]:text-amber-700",
        success:
          "border-emerald-200 bg-emerald-50 text-emerald-800 [&>svg]:text-emerald-700",
        info:
          "border-sky-200 bg-sky-50 text-sky-800 [&>svg]:text-sky-700",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
)

const Alert = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & VariantProps<typeof alertVariants>
>(({ className, variant, ...props }, ref) => (
  <div
    ref={ref}
    role="alert"
    className={cn(alertVariants({ variant }), className)}
    {...props}
  />
))
Alert.displayName = "Alert"

const AlertTitle = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLHeadingElement>
>(({ className, ...props }, ref) => (
  <h5
    ref={ref}
    className={cn("mb-1 font-medium leading-none tracking-tight", className)}
    {...props}
  />
))
AlertTitle.displayName = "AlertTitle"

const AlertDescription = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    className={cn("text-sm [&_p]:leading-relaxed", className)}
    {...props}
  />
))
AlertDescription.displayName = "AlertDescription"

export { Alert, AlertTitle, AlertDescription }
