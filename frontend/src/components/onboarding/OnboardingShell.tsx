import * as React from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { BrandRule } from '@/components/ui/brand-rule'
import { SurfaceProvider } from '@/components/ui/surface'

const EASE = [0.22, 0.61, 0.36, 1] as const

/**
 * OnboardingShell — the full-screen first-run takeover.
 *
 * A soft scrim over the parchment desk, a centred elevated card carrying the
 * persistent brand header + step progress, and the active step rendered into a
 * flex body that owns its own scroll. The step content cross-fades on `step`
 * change (calm, no slide-pop), matching the app's `reveal` motion language.
 */
export function OnboardingShell({
  step,
  total,
  onClose,
  closeDisabled,
  children,
}: {
  step: number
  total: number
  onClose?: () => void
  closeDisabled?: boolean
  children: React.ReactNode
}) {
  const reduced = useReducedMotion()
  const pct = total > 1 ? Math.round((step / (total - 1)) * 100) : 0

  return (
    <motion.div
      className="fixed inset-0 z-[60] grid place-items-center overflow-y-auto bg-black/40 p-4 backdrop-blur-sm"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: reduced ? 0 : 0.3, ease: EASE }}
      role="dialog"
      aria-modal="true"
      aria-label="Welcome to ALMa"
    >
      <SurfaceProvider level={1}>
        <motion.div
          className="w-full max-w-3xl"
          initial={{ opacity: 0, y: reduced ? 0 : 14, scale: reduced ? 1 : 0.99 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          transition={{ duration: reduced ? 0 : 0.36, ease: EASE }}
        >
          <Card variant="elevated" className="flex max-h-[92vh] flex-col overflow-hidden">
            {/* Brand header — quiet chrome above the step content. */}
            <div className="flex shrink-0 items-center gap-3 px-6 pt-5 sm:px-8">
              <img
                src="/brand/alma-mark-source.svg"
                alt=""
                aria-hidden
                className="h-9 w-9 shrink-0"
              />
              <div className="flex flex-col leading-none">
                <span className="font-brand text-xl font-semibold tracking-[0.01em] text-alma-800">
                  ALMa
                </span>
                <span className="mt-0.5 text-[9px] font-bold uppercase tracking-[0.16em] text-alma-folio">
                  <span className="text-alma-800">A</span>nother{' '}
                  <span className="text-alma-800">L</span>ibrary{' '}
                  <span className="text-alma-800">Ma</span>nager
                </span>
              </div>
              <div className="ml-auto flex items-center gap-2">
                <div className="text-right">
                  <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-slate-400">
                    Step {Math.min(step + 1, total)} of {total}
                  </span>
                </div>
                {onClose ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={onClose}
                    disabled={closeDisabled}
                    aria-label="Close onboarding"
                    title="Close onboarding"
                    className="shrink-0 text-slate-500 hover:text-alma-900"
                  >
                    <X className="h-4 w-4" aria-hidden />
                  </Button>
                ) : null}
              </div>
            </div>

            <div className="shrink-0 px-6 pb-1 pt-3 sm:px-8">
              <BrandRule center="dot" tone="gold" className="mb-3" />
              <Progress value={pct} aria-label="Onboarding progress" />
            </div>

            {/* Step body — cross-fades on step change; owns its own scroll. */}
            <AnimatePresence mode="wait" initial={false}>
              <motion.div
                key={step}
                className="flex min-h-0 flex-1 flex-col"
                initial={{ opacity: 0, y: reduced ? 0 : 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: reduced ? 0 : -6 }}
                transition={{ duration: reduced ? 0 : 0.26, ease: EASE }}
              >
                {children}
              </motion.div>
            </AnimatePresence>
          </Card>
        </motion.div>
      </SurfaceProvider>
    </motion.div>
  )
}
