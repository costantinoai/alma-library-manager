"use client"

import * as React from "react"
import * as TogglePrimitive from "@radix-ui/react-toggle"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const toggleVariants = cva(
  "inline-flex items-center justify-center rounded-sm text-sm font-medium transition-colors hover:bg-parchment-100 hover:text-alma-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-alma-paper disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-parchment-200 data-[state=on]:text-alma-900 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 gap-2",
  {
    variants: {
      variant: {
        default: "bg-transparent",
        outline:
          "border border-[var(--color-border)] bg-transparent hover:bg-parchment-100 hover:text-alma-900",
        // Chip-shaped toggle with StatusBadge-accent selected state. Use for
        // filter / selector rows (tag filters) where the selection is
        // list-scoped. Do *not* override the pressed palette with inline
        // `data-[state=on]:bg-*` overrides — the tone is shared with
        // `StatusBadge tone="accent"` on purpose. Paper-edge corner.
        pill:
          "rounded-sm border border-[var(--color-border)] bg-alma-chrome text-alma-700 hover:border-parchment-400 hover:bg-parchment-100 data-[state=on]:border-alma-folio data-[state=on]:bg-alma-100 data-[state=on]:text-alma-900 data-[state=on]:shadow-paper-sm",
      },
      size: {
        default: "h-10 px-3 min-w-10",
        sm: "h-9 px-2.5 min-w-9",
        lg: "h-11 px-5 min-w-11",
        // Chip height — matches the visual mass of `StatusBadge` at
        // `size="default"` so pill Toggles nest cleanly in chip rows.
        chip: "h-7 gap-1.5 px-2.5 text-xs font-medium",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

const Toggle = React.forwardRef<
  React.ElementRef<typeof TogglePrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof TogglePrimitive.Root> &
    VariantProps<typeof toggleVariants>
>(({ className, variant, size, ...props }, ref) => (
  <TogglePrimitive.Root
    ref={ref}
    className={cn(toggleVariants({ variant, size, className }))}
    {...props}
  />
))

Toggle.displayName = TogglePrimitive.Root.displayName

export { Toggle, toggleVariants }
