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
        default: "bg-surface-1 text-alma-900",
        destructive:
          "border-critical-500/50 text-critical-500 [&>svg]:text-critical-500",
        // Soft "negative" tone matching the StatusBadge `negative` palette.
        // Use for anti-affordance content (top negative preferences, suppressed
        // entities, dislike summaries) where `destructive` is too loud.
        negative:
          "border-critical-200 bg-critical-50 text-critical-800 [&>svg]:text-critical-700",
        warning:
          "border-warning-200 bg-warning-50 text-warning-800 [&>svg]:text-warning-700",
        success:
          "border-success-200 bg-success-50 text-success-800 [&>svg]:text-success-700",
        info:
          "border-info-200 bg-info-50 text-info-800 [&>svg]:text-info-700",
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
