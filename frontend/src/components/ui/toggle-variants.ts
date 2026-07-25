import { cva } from 'class-variance-authority'

export const toggleVariants = cva(
  'inline-flex items-center justify-center rounded-sm text-sm font-medium transition-colors hover:bg-control-quiet hover:text-alma-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-surface-1 disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-accent-soft data-[state=on]:text-alma-folio [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 gap-2',
  {
    variants: {
      variant: {
        default: 'bg-transparent',
        outline:
          'border border-control-edge bg-transparent hover:bg-control-quiet hover:text-alma-900',
        pill:
          'rounded-sm border border-control-edge bg-control-well text-alma-700 hover:border-control-edge-strong hover:bg-control-quiet data-[state=on]:border-alma-folio data-[state=on]:bg-accent-soft data-[state=on]:text-alma-folio data-[state=on]:shadow-paper-sm',
        // Segmented control: pair with `<ToggleGroup variant="segment">`,
        // which supplies the recessed ink rail. The ON segment is a RAISED
        // KNOB — top of the paper ladder + shadow — so it lifts off the rail
        // at any elevation. Every hand-rolled version of this before was a
        // cream segment on a cream rail on a cream card.
        segment:
          'rounded-sm border border-transparent text-slate-600 hover:bg-transparent hover:text-alma-800 data-[state=on]:bg-surface-4 data-[state=on]:text-alma-800 data-[state=on]:shadow-paper-sm',
      },
      size: {
        default: 'h-10 px-3 min-w-10',
        sm: 'h-9 px-2.5 min-w-9',
        lg: 'h-11 px-5 min-w-11',
        chip: 'h-7 gap-1.5 px-2.5 text-xs font-medium',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
)
