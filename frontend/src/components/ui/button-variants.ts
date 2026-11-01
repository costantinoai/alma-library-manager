import { cva, type VariantProps } from 'class-variance-authority'

export const buttonVariants = cva(
  [
    'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-control text-sm font-medium',
    'transition-[color,background-color,border-color,box-shadow] duration-200 ease-out',
    'focus-ring',
    'disabled:pointer-events-none disabled:opacity-50',
    "[&_svg:not([class*='size-']):not([class*='h-']):not([class*='w-'])]:size-4",
    '[&_svg]:shrink-0',
    'cursor-pointer select-none',
  ].join(' '),
  {
    variants: {
      variant: {
        default:
          'bg-alma-800 text-alma-cream shadow-paper-sm hover:bg-alma-700 hover:shadow-paper-md active:bg-alma-900',
        accent:
          'bg-alma-folio text-alma-cream shadow-paper-sm hover:bg-alma-folio-600 hover:shadow-paper-md active:bg-alma-folio-700',
        destructive:
          'bg-critical-600 text-on-fill shadow-paper-sm hover:bg-critical-700 hover:shadow-paper-md active:bg-critical-700',
        success:
          'bg-success-600 text-on-fill shadow-paper-sm hover:bg-success-700 hover:shadow-paper-md active:bg-success-700',
        // Unfilled variants fill from the CONTROL INK ladder, never from the
        // paper ramps: an outline button on a cream card used to wear the
        // desk's own cream, and inside a near-white popover the same button
        // read as a dark slab. Ink composites the same on every host.
        outline:
          'border border-control-edge bg-control-well text-alma-900 shadow-paper-sm hover:border-control-edge-strong hover:bg-control-quiet',
        secondary: 'bg-control-quiet text-alma-900 hover:bg-control-quiet-hover',
        ghost: 'text-alma-700 hover:bg-control-quiet hover:text-alma-900',
        // Ghost at text-slate weight: the quiet icon or label beside content.
        muted: 'text-slate-500 hover:bg-control-quiet hover:text-slate-700',
        // The quiet way to say something destructive: a row's Remove, a Dismiss.
        'danger-quiet': 'text-critical-600 hover:bg-critical-700/10 hover:text-critical-700',
        // Grey until you reach for it, so a list of rows is not a list of red.
        'danger-muted': 'text-slate-400 hover:bg-critical-700/10 hover:text-critical-700',
        'warning-quiet': 'text-warning-600 hover:bg-warning-700/12 hover:text-warning-800',
        'success-quiet': 'text-success-700 hover:bg-success-700/10',
        link:
          'rounded-none px-0 text-alma-folio underline-offset-4 shadow-none hover:underline hover:text-alma-folio-600',
        // A link inside running text or a status line, in ink.
        'link-ink': 'rounded-none px-0 text-alma-700 underline-offset-2 hover:text-alma-800 hover:underline',
      },
      size: {
        default: 'h-9 px-4',
        sm: 'h-8 px-3 text-xs gap-1.5',
        xs: 'h-7 px-2 text-xs gap-1',
        lg: 'h-11 px-6 text-base gap-2.5',
        icon: 'size-9',
        'icon-sm': 'size-8',
        'icon-xs': 'size-5',
        // No box: the button takes its size from the text around it.
        inline: 'h-auto gap-1 whitespace-normal p-0 text-[length:inherit]',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
)

export type ButtonVariantProps = VariantProps<typeof buttonVariants>
