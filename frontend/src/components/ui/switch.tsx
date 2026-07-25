import * as React from 'react'
import * as SwitchPrimitive from '@radix-ui/react-switch'
import { cn } from '@/lib/utils'

const Switch = React.forwardRef<
  React.ComponentRef<typeof SwitchPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitive.Root>
>(({ className, ...props }, ref) => (
  <SwitchPrimitive.Root
    ref={ref}
    className={cn(
      // Off = the control ink track; on = folio, the single "active" colour.
      'peer inline-flex h-5 w-9 shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-alma-folio data-[state=unchecked]:bg-control-track',
      className,
    )}
    {...props}
  >
    <SwitchPrimitive.Thumb
      className={cn(
        // Raised-knob exception: the thumb sits at the TOP of the paper
        // ladder so it stays legible against both the ink track and the
        // folio fill. See index.css → "controls are ink, surfaces are paper".
        'pointer-events-none block h-4 w-4 rounded-full bg-surface-4 shadow-paper-sm ring-0 transition-transform data-[state=checked]:translate-x-4 data-[state=unchecked]:translate-x-0',
      )}
    />
  </SwitchPrimitive.Root>
))
Switch.displayName = SwitchPrimitive.Root.displayName

export { Switch }
