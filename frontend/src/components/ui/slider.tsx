import * as React from "react"
import * as SliderPrimitive from "@radix-ui/react-slider"

import { cn } from "@/lib/utils"

const Slider = React.forwardRef<
  React.ElementRef<typeof SliderPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SliderPrimitive.Root>
>(({ className, ...props }, ref) => (
  <SliderPrimitive.Root
    ref={ref}
    className={cn(
      "relative flex w-full touch-none select-none items-center",
      className
    )}
    {...props}
  >
    {/* Ink rail + folio range + raised white knob — the same three parts as
        Switch, Progress and Meter, so every track in the app matches. */}
    <SliderPrimitive.Track className="relative h-1.5 w-full grow overflow-hidden rounded-full bg-control-track">
      <SliderPrimitive.Range className="absolute h-full bg-alma-folio" />
    </SliderPrimitive.Track>
    <SliderPrimitive.Thumb className="block h-4 w-4 rounded-full border-2 border-alma-folio bg-surface-4 shadow-paper-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-surface-1 disabled:pointer-events-none disabled:opacity-50" />
  </SliderPrimitive.Root>
))
Slider.displayName = SliderPrimitive.Root.displayName

export { Slider }
