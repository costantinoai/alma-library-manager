import { cva } from 'class-variance-authority'

export const toggleVariants = cva(
  'inline-flex items-center justify-center rounded-sm text-sm font-medium transition-colors hover:bg-parchment-100 hover:text-alma-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-surface-1 disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-accent-soft data-[state=on]:text-alma-folio [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0 gap-2',
  {
    variants: {
      variant: {
        default: 'bg-transparent',
        outline:
          'border border-[var(--color-border)] bg-transparent hover:bg-parchment-100 hover:text-alma-900',
        pill:
          'rounded-sm border border-[var(--color-border)] bg-surface-1 text-alma-700 hover:border-parchment-400 hover:bg-parchment-100 data-[state=on]:border-alma-folio data-[state=on]:bg-accent-soft data-[state=on]:text-alma-folio data-[state=on]:shadow-paper-sm',
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
