import type { ReactNode } from 'react'
import { AlertCircle, FileQuestion } from 'lucide-react'

import { BrandRule } from '@/components/ui/brand-rule'
import { Surface } from '@/components/ui/surface'
import { cn } from '@/lib/utils'

export type ResearchStateKind = 'error' | 'not-found'
export type ResearchStateScope = 'section' | 'page'

interface ResearchStateProps {
  kind: ResearchStateKind
  scope?: ResearchStateScope
  title: string
  message: string
  action?: ReactNode
  className?: string
}

/**
 * Shared ALMa failure/404 primitive.
 *
 * The same academic-paper grammar serves embedded sections and whole pages:
 * folio notation, brand rule, warm stock, restrained semantic ink. Callers
 * provide recovery actions; they do not invent another red warning box.
 */
export function ResearchState({
  kind,
  scope = 'section',
  title,
  message,
  action,
  className,
}: ResearchStateProps) {
  const Icon = kind === 'not-found' ? FileQuestion : AlertCircle
  const notation = kind === 'not-found' ? '404 · record not found' : 'Load note · unavailable'

  return (
    <Surface
      role={kind === 'error' ? 'alert' : 'status'}
      className={cn(
        'relative overflow-hidden rounded-sm shadow-paper-sheet',
        scope === 'page'
          ? 'mx-auto flex min-h-[52vh] max-w-3xl items-center px-7 py-14 sm:px-12'
          : 'px-5 py-8 sm:px-7',
        className,
      )}
    >
      <div
        aria-hidden
        className={cn(
          'absolute inset-y-0 left-0 w-1',
          kind === 'error' ? 'bg-critical-400/70' : 'bg-gold-400',
        )}
      />
      <div className={cn('w-full', scope === 'page' ? 'max-w-xl' : 'max-w-2xl')}>
        <div className="flex items-center gap-2">
          <Icon
            className={cn(
              'h-4 w-4',
              kind === 'error' ? 'text-critical-600' : 'text-alma-folio',
            )}
          />
          <span className="font-mono text-[10px] uppercase tracking-[0.14em] text-slate-500">
            {notation}
          </span>
        </div>
        <BrandRule center="dot" className="my-4 max-w-52 opacity-80" />
        <h2
          className={cn(
            'font-brand font-semibold tracking-tight text-alma-800',
            scope === 'page' ? 'text-2xl sm:text-3xl' : 'text-base',
          )}
        >
          {title}
        </h2>
        <p className="mt-2 max-w-prose text-sm leading-relaxed text-slate-600">{message}</p>
        {action ? <div className="mt-5 flex flex-wrap gap-2">{action}</div> : null}
      </div>
    </Surface>
  )
}
