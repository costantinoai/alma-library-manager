import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * DotLoader — three gold dots fading in sequence.
 *
 * Editorial replacement for `<Loader2 className="animate-spin" />` in
 * full-page or large-block loading states. The dots echo the wordmark's
 * centered gold dot; the staggered fade reads like a subtle pulse, not
 * a shouty spinner.
 *
 * Inline button spinners stay on `Loader2` (consistency with shadcn);
 * this primitive is reserved for stand-alone loading moments where
 * giving the page a beat before content lands feels right.
 *
 * Sizes: `sm | md | lg`.
 *
 * Accessibility: marks the wrapper with `role="status"` so screen
 * readers announce "Loading…"; pass an `aria-label` to override.
 */
type DotLoaderSize = 'sm' | 'md' | 'lg'

interface DotLoaderProps extends Omit<React.HTMLAttributes<HTMLDivElement>, 'aria-label'> {
  size?: DotLoaderSize
  'aria-label'?: string
  message?: string
}

const SIZE_CLASS: Record<DotLoaderSize, { dot: string; gap: string }> = {
  sm: { dot: 'h-1.5 w-1.5', gap: 'gap-1.5' },
  md: { dot: 'h-2 w-2',     gap: 'gap-2' },
  lg: { dot: 'h-2.5 w-2.5', gap: 'gap-2.5' },
}

export function DotLoader({
  size = 'md',
  message,
  className,
  'aria-label': ariaLabel = 'Loading…',
  ...props
}: DotLoaderProps) {
  const palette = SIZE_CLASS[size]
  return (
    <div
      role="status"
      aria-label={ariaLabel}
      className={cn(
        'inline-flex items-center justify-center py-12',
        message ? 'flex-col gap-3' : palette.gap,
        className,
      )}
      {...props}
    >
      <div className={cn('inline-flex items-center', palette.gap)} aria-hidden>
        <span className={cn('rounded-full bg-gold-400 alma-dot-pulse-1', palette.dot)} />
        <span className={cn('rounded-full bg-gold-400 alma-dot-pulse-2', palette.dot)} />
        <span className={cn('rounded-full bg-gold-400 alma-dot-pulse-3', palette.dot)} />
      </div>
      {message && <span className="text-xs text-slate-500">{message}</span>}
      <style>{`
        @keyframes alma-dot-pulse {
          0%, 80%, 100% { opacity: 0.25; }
          40%           { opacity: 1; }
        }
        .alma-dot-pulse-1 { animation: alma-dot-pulse 1.2s infinite ease-in-out; }
        .alma-dot-pulse-2 { animation: alma-dot-pulse 1.2s infinite ease-in-out; animation-delay: 0.15s; }
        .alma-dot-pulse-3 { animation: alma-dot-pulse 1.2s infinite ease-in-out; animation-delay: 0.30s; }
        @media (prefers-reduced-motion: reduce) {
          .alma-dot-pulse-1, .alma-dot-pulse-2, .alma-dot-pulse-3 { animation: none; opacity: 0.6; }
        }
      `}</style>
    </div>
  )
}
