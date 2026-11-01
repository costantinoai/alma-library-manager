import * as React from 'react'
import * as TabsPrimitive from '@radix-ui/react-tabs'
import { cn } from '@/lib/utils'

const Tabs = TabsPrimitive.Root

/** `rail` is the raised segment on an ink rail. `underline` is the quieter
 *  strip a page draws along a hairline of its own. */
type TabsVariant = 'rail' | 'underline'
const TabsVariantContext = React.createContext<TabsVariant>('rail')

const TabsList = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.List>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.List> & { variant?: TabsVariant }
>(({ className, variant = 'rail', ...props }, ref) => (
  <TabsVariantContext.Provider value={variant}>
    <TabsPrimitive.List
      ref={ref}
      className={cn(
        // The list is a recessed INK rail; the active trigger is a raised
        // white segment on top of it (see TabsTrigger). A cream rail on a
        // cream card had no edge to lift off.
        variant === 'rail'
          ? 'inline-flex h-10 items-center justify-center rounded-control bg-control-track p-1 text-alma-700'
          : 'flex items-center gap-1',
        className,
      )}
      {...props}
    />
  </TabsVariantContext.Provider>
))
TabsList.displayName = TabsPrimitive.List.displayName

const TabsTrigger = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.Trigger>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>
>(({ className, ...props }, ref) => {
  const variant = React.useContext(TabsVariantContext)
  return (
    <TabsPrimitive.Trigger
      ref={ref}
      className={cn(
        'inline-flex items-center justify-center gap-1.5 whitespace-nowrap text-sm font-medium focus-ring disabled:pointer-events-none disabled:opacity-50 cursor-pointer',
        variant === 'rail'
          ? // Raised-knob exception: the active tab lifts to the TOP of the paper
            // ladder so it reads as a segment sitting on the ink rail at any
            // elevation, and takes folio text — the shared active signal.
            'rounded-control px-3 py-1.5 transition-all data-[state=active]:bg-surface-4 data-[state=active]:text-alma-folio data-[state=active]:shadow-paper-sm'
          : 'relative -mb-px border-b-2 border-transparent px-3 py-2 text-slate-500 transition-colors hover:text-alma-800 data-[state=active]:border-accent data-[state=active]:text-alma-900',
        className,
      )}
      {...props}
    />
  )
})
TabsTrigger.displayName = TabsPrimitive.Trigger.displayName

const TabsContent = React.forwardRef<
  React.ComponentRef<typeof TabsPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Content>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.Content
    ref={ref}
    className={cn(
      'mt-2 focus-ring',
      className,
    )}
    {...props}
  />
))
TabsContent.displayName = TabsPrimitive.Content.displayName

export { Tabs, TabsList, TabsTrigger, TabsContent }
