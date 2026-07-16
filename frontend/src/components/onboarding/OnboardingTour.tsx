import * as React from 'react'
import { createPortal } from 'react-dom'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { useQuery } from '@tanstack/react-query'
import { HelpCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { SurfaceProvider } from '@/components/ui/surface'
import { getBootstrap } from '@/api/client'
import { useFirstVisitTour } from '@/components/onboarding/useFirstVisitTour'
import { cn } from '@/lib/utils'

const EASE = [0.22, 0.61, 0.36, 1] as const
const PAD = 6 // spotlight padding around the target
const GAP = 12 // gap between target and the card
const CARD_W = 320

export interface TourStep {
  /** CSS selector for the element to spotlight. Omit (or no match) → centred card. */
  target?: string
  title: string
  body: React.ReactNode
  /** Preferred placement of the card relative to the target. Default 'bottom'. */
  side?: 'top' | 'bottom'
}

type Rect = { top: number; left: number; width: number; height: number }

function readRect(selector?: string): Rect | null {
  if (!selector) return null
  const el = document.querySelector(selector)
  if (!el) return null
  const r = el.getBoundingClientRect()
  if (r.width === 0 && r.height === 0) return null
  return { top: r.top, left: r.left, width: r.width, height: r.height }
}

/**
 * OnboardingTour — a lightweight coachmark walkthrough: a dimmed full-screen
 * overlay with a "spotlight" cut around the current target (box-shadow trick)
 * and a small card with Skip / Next / Got it. Steps without a resolvable target
 * render as a centred card. Calm fades; collapses under reduced-motion.
 *
 * Controlled: parent owns `open` and `onClose` (which should mark the tour done).
 */
export function OnboardingTour({
  steps,
  open,
  onClose,
}: {
  steps: TourStep[]
  open: boolean
  onClose: () => void
}) {
  const reduced = useReducedMotion()
  const [index, setIndex] = React.useState(0)
  const [rect, setRect] = React.useState<Rect | null>(null)
  const [cardPos, setCardPos] = React.useState<{ top: number; left: number } | null>(null)
  const cardRef = React.useRef<HTMLDivElement>(null)

  // Reset to the first step each time the tour opens.
  React.useEffect(() => {
    if (open) setIndex(0)
  }, [open])

  const step = steps[index]

  // Resolve + track the target rect (scroll the target into view first).
  React.useEffect(() => {
    if (!open || !step) return
    let raf = 0
    const el = step.target ? document.querySelector(step.target) : null
    el?.scrollIntoView({ block: 'center', behavior: reduced ? 'auto' : 'smooth' })
    const update = () => setRect(readRect(step.target))
    // Let the smooth-scroll settle before first measure.
    const t = window.setTimeout(update, reduced ? 0 : 220)
    const onMove = () => {
      cancelAnimationFrame(raf)
      raf = requestAnimationFrame(update)
    }
    window.addEventListener('resize', onMove)
    window.addEventListener('scroll', onMove, true)
    return () => {
      window.clearTimeout(t)
      cancelAnimationFrame(raf)
      window.removeEventListener('resize', onMove)
      window.removeEventListener('scroll', onMove, true)
    }
  }, [open, step, index, reduced])

  // Position the card relative to the target (with viewport clamping + flip).
  React.useLayoutEffect(() => {
    if (!open) return
    const cardH = cardRef.current?.offsetHeight ?? 160
    const vw = window.innerWidth
    const vh = window.innerHeight
    if (!rect) {
      setCardPos({ top: Math.max(24, vh / 2 - cardH / 2), left: Math.max(24, vw / 2 - CARD_W / 2) })
      return
    }
    const preferTop = step?.side === 'top'
    const belowTop = rect.top + rect.height + GAP
    const aboveTop = rect.top - GAP - cardH
    let top = preferTop ? aboveTop : belowTop
    // Flip if the preferred side overflows.
    if (!preferTop && top + cardH > vh - 16) top = aboveTop
    if (preferTop && top < 16) top = belowTop
    top = Math.min(Math.max(16, top), vh - cardH - 16)
    let left = rect.left + rect.width / 2 - CARD_W / 2
    left = Math.min(Math.max(16, left), vw - CARD_W - 16)
    setCardPos({ top, left })
  }, [rect, open, step, index])

  React.useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  if (typeof document === 'undefined') return null

  const isLast = index >= steps.length - 1
  const next = () => (isLast ? onClose() : setIndex((i) => i + 1))

  return createPortal(
    <AnimatePresence>
      {open && step ? (
        <motion.div
          className="fixed inset-0 z-[55]"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: reduced ? 0 : 0.2, ease: EASE }}
        >
          {/* Dimmer + spotlight. The lit div casts a huge shadow = the dim. */}
          {rect ? (
            <div
              className="pointer-events-none absolute rounded-md"
              style={{
                top: rect.top - PAD,
                left: rect.left - PAD,
                width: rect.width + PAD * 2,
                height: rect.height + PAD * 2,
                boxShadow: '0 0 0 9999px rgba(20, 35, 58, 0.55)',
                transition: reduced ? undefined : 'all 0.2s ease',
              }}
              aria-hidden
            />
          ) : (
            <div className="absolute inset-0" style={{ backgroundColor: 'rgba(20, 35, 58, 0.55)' }} aria-hidden />
          )}
          {/* Click-catcher to block the app behind (card stops propagation). */}
          <div className="absolute inset-0" onClick={onClose} aria-hidden />

          <SurfaceProvider level={1}>
            <motion.div
              ref={cardRef}
              key={index}
              role="dialog"
              aria-label={step.title}
              className={cn(
                'absolute w-[320px] rounded-sm border border-[var(--color-border)] bg-surface-1 p-4 shadow-paper-lg',
              )}
              style={{ top: cardPos?.top ?? -9999, left: cardPos?.left ?? -9999 }}
              initial={{ opacity: 0, y: reduced ? 0 : 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: reduced ? 0 : 0.22, ease: EASE }}
              onClick={(e) => e.stopPropagation()}
            >
              <EyebrowLabel className="mb-1.5">
                {index + 1} / {steps.length}
              </EyebrowLabel>
              <p className="font-brand text-base font-semibold text-alma-800">{step.title}</p>
              <div className="mt-1 text-sm leading-relaxed text-slate-600">{step.body}</div>
              <div className="mt-4 flex items-center justify-between">
                <Button variant="ghost" size="sm" onClick={onClose}>
                  Skip tour
                </Button>
                <Button variant="accent" size="sm" onClick={next}>
                  {isLast ? 'Got it' : 'Next'}
                </Button>
              </div>
            </motion.div>
          </SurfaceProvider>
        </motion.div>
      ) : null}
    </AnimatePresence>,
    document.body,
  )
}

/**
 * PageTour — drop-in for a page header: renders a small "?" relaunch button and
 * the tour itself. First visit auto-runs; the button replays it anytime.
 */
export function PageTour({ pageKey, steps }: { pageKey: string; steps: TourStep[] }) {
  // Don't auto-run page tours while the first-run wizard is still active —
  // the wizard (z-60) sits above the tour (z-55), and they'd fight on boot.
  const { data: bootstrap } = useQuery({
    queryKey: ['bootstrap'],
    queryFn: () => getBootstrap(),
    staleTime: 60_000,
  })
  const onboardingActive = bootstrap?.onboarding?.completed === false
  const { open, complete, relaunch } = useFirstVisitTour(pageKey, !onboardingActive)
  return (
    <>
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={relaunch}
        aria-label="Show page tour"
        title="Show page tour"
      >
        <HelpCircle className="h-4 w-4" />
      </Button>
      <OnboardingTour steps={steps} open={open} onClose={complete} />
    </>
  )
}
