import { cva, type VariantProps } from 'class-variance-authority'

// A filled or sheet button goes flat when disabled instead of fading: a faded
// navy reads as a muddy grey slab. Text-only variants simply dim.
const DISABLED_FLAT = 'disabled:border-transparent disabled:bg-control-quiet disabled:text-slate-500 disabled:shadow-none'
const DISABLED_DIM = 'disabled:opacity-50'

export const buttonVariants = cva(
  [
    'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-control text-sm font-medium',
    'transition-[color,background-color,border-color,box-shadow] duration-200 ease-out',
    'focus-ring',
    'disabled:pointer-events-none',
    "[&_svg:not([class*='size-']):not([class*='h-']):not([class*='w-'])]:size-4",
    '[&_svg]:shrink-0',
    'cursor-pointer select-none',
  ].join(' '),
  {
    variants: {
      variant: {
        default:
          `bg-primary text-alma-cream shadow-paper-sm hover:bg-primary-hover hover:shadow-paper-md active:bg-primary-active ${DISABLED_FLAT}`,
        accent:
          `bg-alma-folio text-alma-cream shadow-paper-sm hover:bg-alma-folio-600 hover:shadow-paper-md active:bg-alma-folio-700 ${DISABLED_FLAT}`,
        destructive:
          `bg-critical-700 text-on-fill shadow-paper-sm hover:bg-critical-800 hover:shadow-paper-md active:bg-critical-800 ${DISABLED_FLAT}`,
        success:
          `bg-success-700 text-on-fill shadow-paper-sm hover:bg-success-800 hover:shadow-paper-md active:bg-success-800 ${DISABLED_FLAT}`,
        // A sheet of lighter paper with a warm hairline: it composites the
        // same on every host, and the weight sits between the fills and the
        // quiet text variants.
        outline:
          `border border-control-edge bg-control-well text-alma-900 shadow-paper-sm hover:border-control-edge-strong hover:bg-control-well-hover active:bg-control-quiet ${DISABLED_FLAT}`,
        secondary: `bg-control-quiet text-alma-900 hover:bg-control-quiet-hover ${DISABLED_DIM}`,
        ghost: `text-alma-700 hover:bg-control-quiet-hover hover:text-alma-900 ${DISABLED_DIM}`,
        // Ghost at text-slate weight: the quiet icon or label beside content.
        muted: `text-slate-500 hover:bg-control-quiet-hover hover:text-slate-700 ${DISABLED_DIM}`,
        // The quiet way to say something destructive: a row's Remove, a Dismiss.
        'danger-quiet': `text-critical-700 hover:bg-critical-700/10 hover:text-critical-800 ${DISABLED_DIM}`,
        // Grey until you reach for it, so a list of rows is not a list of red.
        'danger-muted': `text-slate-400 hover:bg-critical-700/10 hover:text-critical-800 ${DISABLED_DIM}`,
        'warning-quiet': `text-warning-700 hover:bg-warning-700/12 hover:text-warning-800 ${DISABLED_DIM}`,
        'success-quiet': `text-success-700 hover:bg-success-700/10 hover:text-success-800 ${DISABLED_DIM}`,
        link:
          `rounded-none px-0 text-alma-folio underline-offset-4 shadow-none hover:underline hover:text-alma-folio-600 ${DISABLED_DIM}`,
        // A link inside running text or a status line, in ink.
        'link-ink': `rounded-none px-0 text-alma-700 underline-offset-2 hover:text-alma-800 hover:underline ${DISABLED_DIM}`,
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
