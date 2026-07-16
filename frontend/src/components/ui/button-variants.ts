import { cva, type VariantProps } from 'class-variance-authority'

export const buttonVariants = cva(
  [
    'inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-sm text-sm font-medium',
    'transition-[color,background-color,border-color,box-shadow] duration-200 ease-out',
    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio focus-visible:ring-offset-2 focus-visible:ring-offset-surface-1',
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
          'bg-critical-600 text-white shadow-paper-sm hover:bg-critical-700 hover:shadow-paper-md active:bg-critical-700',
        success:
          'bg-success-600 text-white shadow-paper-sm hover:bg-success-700 hover:shadow-paper-md active:bg-success-700',
        gold:
          'bg-gold-400 text-alma-900 shadow-paper-sm hover:bg-gold-500 hover:text-alma-cream hover:shadow-paper-md',
        outline:
          'border border-[var(--color-border)] bg-surface-0 text-alma-900 shadow-paper-sm hover:border-parchment-400 hover:bg-parchment-100',
        secondary: 'bg-parchment-100 text-alma-900 hover:bg-parchment-200',
        ghost: 'text-alma-700 hover:bg-parchment-100 hover:text-alma-900',
        link:
          'rounded-none px-0 text-alma-folio underline-offset-4 shadow-none hover:underline hover:text-alma-folio-600',
      },
      size: {
        default: 'h-9 px-4',
        sm: 'h-8 px-3 text-xs gap-1.5',
        xs: 'h-7 px-2 text-xs gap-1',
        lg: 'h-11 px-6 text-base gap-2.5',
        icon: 'size-9',
        'icon-sm': 'size-8',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
)

export type ButtonVariantProps = VariantProps<typeof buttonVariants>
