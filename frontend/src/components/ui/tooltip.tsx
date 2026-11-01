import * as React from 'react'
import * as TooltipPrimitive from '@radix-ui/react-tooltip'
import { cn } from '@/lib/utils'

const TooltipProvider = TooltipPrimitive.Provider
const Tooltip = TooltipPrimitive.Root
const TooltipTrigger = TooltipPrimitive.Trigger

/**
 * The primitive owns both looks, so a call site never paints a tooltip itself.
 * - `ink`: the default dark chip. `tooltip-ink` (index.css) re-points the slate
 *   TEXT ramp, which is tuned for paper, so nested `text-slate-*` stays legible.
 * - `paper`: a small card for tooltips with a list or a breakdown inside.
 */
const TOOLTIP_TONES = {
  ink: 'tooltip-ink bg-alma-800 text-alma-cream shadow-paper-md',
  paper: 'border border-edge-3 bg-surface-3 text-slate-700 shadow-paper-md',
} as const

interface TooltipContentProps
  extends React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content> {
  tone?: keyof typeof TOOLTIP_TONES
}

const TooltipContent = React.forwardRef<
  React.ComponentRef<typeof TooltipPrimitive.Content>,
  TooltipContentProps
>(({ className, sideOffset = 4, tone = 'ink', ...props }, ref) => (
  <TooltipPrimitive.Portal>
    <TooltipPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      className={cn(
        'z-tooltip overflow-hidden rounded-control px-2.5 py-1.5 text-xs font-medium data-[state=delayed-open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=delayed-open]:fade-in-0',
        TOOLTIP_TONES[tone],
        className,
      )}
      {...props}
    />
  </TooltipPrimitive.Portal>
))
TooltipContent.displayName = TooltipPrimitive.Content.displayName

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider }
