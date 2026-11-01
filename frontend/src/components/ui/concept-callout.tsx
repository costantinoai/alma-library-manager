import * as React from 'react'
import { ChevronDown, Info } from 'lucide-react'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import { cn } from '@/lib/utils'
import { nextLevel, SURFACE_BG, useSurfaceLevel } from '@/components/ui/surface-level'

/**
 * ConceptCallout — an in-page explainer for a complex feature or
 * concept the user might not yet understand.
 *
 * The pattern: a quiet chrome-elev card with an "info" icon, a small
 * eyebrow ("What is this?", "How does this work?", etc.), a single-
 * line summary that's always visible, and a chevron that expands to
 * reveal the full explanation. Built on top of shadcn's Collapsible
 * primitive so animation, focus management, and ARIA are all handled.
 *
 * When to use:
 *   - Whenever a feature has its own vocabulary (Branch Studio's "core
 *     pull" / "explore push", Discovery's "lenses", etc.) and the user
 *     has no obvious place to learn what the words mean.
 *   - Whenever a non-obvious computation drives what the user sees
 *     ("how are these branches computed?", "where does this score
 *     come from?").
 *   - Once per surface, near the top, NEVER inside another
 *     ConceptCallout (don't nest explanations).
 *
 * When NOT to use:
 *   - For a small piece of jargon inside a paragraph — that's what
 *     `JargonHint` is for (a per-term info popover).
 *   - For an error / warning state — those have their own primitives
 *     (`Alert`, `StatusBadge`).
 *
 * Visual: chrome-elev surface (it's chrome-on-chrome — a quiet
 * sub-frame) with the v3 hairline border. Eyebrow uses the Folio-blue
 * accent so the callout reads as "informational, click for more"
 * without competing with primary CTAs.
 */
interface ConceptCalloutProps {
  /** Eyebrow label. Default: "What is this?" */
  eyebrow?: string
  /** One-line teaser shown next to the eyebrow — the TL;DR. */
  summary: React.ReactNode
  /** Full explanation revealed when expanded. */
  children: React.ReactNode
  /** Start expanded. Default: false. */
  defaultOpen?: boolean
  /**
   * Weld the callout to the bottom edge of its host instead of floating as
   * its own card: hairline on top only, no radius. Used by `PageIntro`, where
   * the guide is part of the masthead object rather than a second box under
   * it — a page whose first two elements are both bordered cards reads as two
   * things to get past before the content.
   */
  flush?: boolean
  className?: string
}

export function ConceptCallout({
  eyebrow = 'What is this?',
  summary,
  children,
  defaultOpen = false,
  flush = false,
  className,
}: ConceptCalloutProps) {
  const host = useSurfaceLevel()
  const level = nextLevel(host)
  // Inside a card it is a line and a fold, not a box in a box.
  const plain = !flush && host >= 1
  return (
    <Collapsible defaultOpen={defaultOpen} className={cn('w-full', className)}>
      <div
        className={cn(
          flush && 'border-t border-divider',
          !flush && !plain && cn(SURFACE_BG[level], 'rounded-card border border-divider'),
        )}
      >
        <CollapsibleTrigger
          className={cn(
            'group flex w-full cursor-pointer select-none items-start gap-3 py-3 text-left',
            plain ? 'hover:text-alma-800' : 'px-4 transition-colors hover:bg-control-quiet',
            'focus-ring',
          )}
        >
          <Info className="mt-0.5 h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
          <div className="min-w-0 flex-1 text-sm">
            <span className="mr-2 text-caption font-bold uppercase tracking-[0.16em] text-alma-folio">
              {eyebrow}
            </span>
            <span className="text-slate-600">{summary}</span>
          </div>
          <ChevronDown
            className="mt-0.5 h-4 w-4 shrink-0 text-slate-400 transition-transform duration-200 group-data-[state=open]:rotate-180"
            aria-hidden
          />
        </CollapsibleTrigger>
        <CollapsibleContent>
          <div
            className={cn(
              'space-y-2 text-sm leading-relaxed text-slate-600',
              plain ? 'pb-3 pl-7' : 'border-t border-divider px-4 py-3',
            )}
          >
            {children}
          </div>
        </CollapsibleContent>
      </div>
    </Collapsible>
  )
}
