import * as React from 'react'
import { ChevronDown, Info } from 'lucide-react'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import { cn } from '@/lib/utils'

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
  className?: string
}

export function ConceptCallout({
  eyebrow = 'What is this?',
  summary,
  children,
  defaultOpen = false,
  className,
}: ConceptCalloutProps) {
  return (
    <Collapsible defaultOpen={defaultOpen} className={cn('w-full', className)}>
      <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev">
        <CollapsibleTrigger
          className={cn(
            'group flex w-full cursor-pointer select-none items-start gap-3 px-4 py-3 text-left',
            'transition-colors hover:bg-parchment-50/40',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio',
          )}
        >
          <Info className="mt-0.5 h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
          <div className="min-w-0 flex-1 text-sm">
            <span className="mr-2 text-[11px] font-bold uppercase tracking-[0.16em] text-alma-folio">
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
          <div className="space-y-2 border-t border-[var(--color-border)] px-4 py-3 text-sm leading-relaxed text-slate-600">
            {children}
          </div>
        </CollapsibleContent>
      </div>
    </Collapsible>
  )
}
