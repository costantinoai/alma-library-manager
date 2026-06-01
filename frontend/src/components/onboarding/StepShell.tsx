import * as React from 'react'
import { ChevronLeft } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { cn } from '@/lib/utils'

/**
 * StepShell — the per-step layout used by every onboarding step.
 *
 * A flex column inside the OnboardingShell card: a fixed heading, a scrollable
 * body (so long lists — papers, recommendations — scroll without moving the
 * chrome), and a fixed footer for navigation. Steps render
 * `<StepShell heading={...} footer={<StepNav .../>}>{body}</StepShell>`.
 */
export function StepShell({
  eyebrow,
  title,
  lead,
  footer,
  children,
  bodyClassName,
}: {
  eyebrow?: string
  title: React.ReactNode
  lead?: React.ReactNode
  footer?: React.ReactNode
  children: React.ReactNode
  bodyClassName?: string
}) {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="shrink-0 px-6 pt-5 sm:px-8">
        {eyebrow ? <EyebrowLabel className="mb-2">{eyebrow}</EyebrowLabel> : null}
        <h2 className="font-brand text-2xl font-semibold leading-tight text-alma-800 sm:text-[28px]">
          {title}
        </h2>
        {lead ? (
          <p className="mt-2 max-w-prose text-sm leading-relaxed text-slate-600">{lead}</p>
        ) : null}
      </div>
      <div className={cn('min-h-0 flex-1 overflow-y-auto px-6 py-5 sm:px-8', bodyClassName)}>
        {children}
      </div>
      {footer ? (
        <div className="shrink-0 border-t border-[var(--color-border)] px-6 py-4 sm:px-8">
          {footer}
        </div>
      ) : null}
    </div>
  )
}

/**
 * StepNav — the footer navigation row. Back on the left; an optional soft Skip
 * and the primary Continue on the right. Pass `primary` to replace the default
 * Continue button entirely (e.g. the gated "Run discovery" action).
 */
export function StepNav({
  onBack,
  onSkip,
  skipLabel = 'Skip for now',
  onContinue,
  continueLabel = 'Continue',
  continueDisabled,
  continueLoading,
  primary,
  hint,
}: {
  onBack?: () => void
  onSkip?: () => void
  skipLabel?: string
  onContinue?: () => void
  continueLabel?: string
  continueDisabled?: boolean
  continueLoading?: boolean
  /** Replace the default Continue button (keeps Back + Skip). */
  primary?: React.ReactNode
  /** Small muted note shown above the buttons (e.g. encouragement). */
  hint?: React.ReactNode
}) {
  return (
    <div className="space-y-3">
      {hint ? <p className="text-xs text-slate-500">{hint}</p> : null}
      <div className="flex items-center justify-between gap-3">
        <div>
          {onBack ? (
            <Button variant="ghost" size="sm" onClick={onBack}>
              <ChevronLeft className="h-4 w-4" />
              Back
            </Button>
          ) : null}
        </div>
        <div className="flex items-center gap-2">
          {onSkip ? (
            <Button variant="ghost" size="sm" onClick={onSkip}>
              {skipLabel}
            </Button>
          ) : null}
          {primary ??
            (onContinue ? (
              <Button
                variant="accent"
                onClick={onContinue}
                disabled={continueDisabled}
                loading={continueLoading}
              >
                {continueLabel}
              </Button>
            ) : null)}
        </div>
      </div>
    </div>
  )
}

/**
 * GoalMeter — a soft "3 of 5" progress chip for the encouraged-minimum steps
 * (follow authors, save papers). Reads done/target; turns folio when met.
 */
export function GoalMeter({
  done,
  target,
  noun,
}: {
  done: number
  target: number
  noun: string
}) {
  const met = done >= target
  const pct = Math.min(100, Math.round((done / Math.max(1, target)) * 100))
  return (
    <div className="flex items-center gap-3">
      <div className="h-1.5 w-28 overflow-hidden rounded-full bg-surface-3">
        <div
          className={cn('h-full rounded-full transition-all', met ? 'bg-success-500' : 'bg-alma-folio')}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={cn('text-xs font-medium', met ? 'text-success-700' : 'text-slate-500')}>
        {met ? `${done} ${noun} — nice` : `${done} of ${target} ${noun}`}
      </span>
    </div>
  )
}
