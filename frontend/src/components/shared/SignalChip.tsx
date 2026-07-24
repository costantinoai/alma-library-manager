import type { ComponentType } from 'react'
import {
  AlertTriangle,
  Bookmark,
  Calendar,
  Database,
  FileText,
  GitBranch,
  GitMerge,
  Globe,
  Heart,
  Languages,
  Layers,
  Link2,
  Quote,
  Route,
  Target,
  TrendingUp,
  Type,
  Users,
} from 'lucide-react'

import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'

/**
 * THE registry of every chip the app can show, and what its colour means.
 *
 * A pill has to answer two questions in one glance: *what kind of fact is
 * this* and *is it good news or bad news*. We answer them on two separate
 * channels so neither has to compete for the same few legible hues:
 *
 *   COLOUR → valence, on three families with distinct authority
 *     · success / warning  — YOUR OWN feedback loop speaking (what you save,
 *       what you pass on). The strongest for/against the app can state.
 *     · accent (folio)     — the ENGINE's evidence: why it surfaced this.
 *     · neutral            — plumbing and description. Deliberately quiet.
 *   ICON   → category: authors, citations, topic, source, venue, …
 *
 * Everything else follows: any new chip picks a `SignalKind` here rather
 * than hand-rolling a colour, so a chip can never mean two things in two
 * places. Categorical *identity* chips (feed monitors, discovery branches)
 * are the one exception and are documented at MONITOR_TYPE_CHIP in
 * `lib/palette.ts` — they encode which-one, not how-good, and still render
 * through this same shell so the shape and metrics stay identical.
 */
export type SignalKind =
  // ── Your feedback loop — the strongest for/against we can state ────────
  | 'taste-match' // near papers you save / rate highly
  | 'taste-avoid' // near papers you dismissed or disliked
  | 'retracted' // hard negative fact about the work itself
  // ── Engine evidence — why this candidate surfaced ─────────────────────
  | 'consensus' // several independent sources found it
  | 'topic' // semantic (SPECTER2) closeness
  | 'wording' // lexical / keyword overlap
  | 'authors' // shared authors with your library
  | 'coupling' // shares references with a saved paper
  | 'cocitation' // cited together with a saved paper
  | 'branch' // the lens branch that pursued it
  | 'trending' // citation momentum
  // ── Learned preference profile: what a lens chases vs steers away from ──
  | 'pref-topic'
  | 'pref-author'
  | 'pref-venue'
  | 'pref-query' // a query that recently produced saves
  | 'suppressed-topic'
  | 'suppressed-author'
  | 'suppressed-venue'
  // ── Plumbing + description — quiet by design ──────────────────────────
  | 'channel' // retrieval lane: vector / lexical / graph / external
  | 'source' // which API returned it: openalex, semantic scholar, …
  | 'venue' // journal / conference
  | 'year' // publication year
  | 'language'
  | 'work-type'
  | 'collection'
  | 'meta' // anything else descriptive

interface SignalSpec {
  tone: StatusBadgeTone
  icon?: ComponentType<{ className?: string }>
  /** Plain-language gloss, used as the chip's hover title when the caller
   *  doesn't supply a more specific one (truthful-UI: never a bare number). */
  hint?: string
}

export const SIGNAL_KINDS: Record<SignalKind, SignalSpec> = {
  // Your feedback loop.
  'taste-match': {
    tone: 'positive',
    icon: Heart,
    hint: 'Close to papers you save and rate highly',
  },
  'taste-avoid': {
    tone: 'warning',
    icon: AlertTriangle,
    hint: 'Close to papers you dismissed or disliked',
  },
  retracted: { tone: 'negative', icon: AlertTriangle, hint: 'This work has been retracted' },

  // Engine evidence.
  consensus: { tone: 'accent', icon: Layers, hint: 'Independently found by several sources' },
  topic: { tone: 'accent', icon: Target, hint: 'Semantic (SPECTER2) closeness to your library' },
  wording: { tone: 'accent', icon: Type, hint: 'Keyword / phrasing overlap with your library' },
  authors: { tone: 'accent', icon: Users, hint: 'Shares authors with papers you have' },
  coupling: { tone: 'accent', icon: Link2, hint: 'Cites the same works as a paper you saved' },
  cocitation: { tone: 'accent', icon: GitMerge, hint: 'Cited alongside a paper you saved' },
  branch: { tone: 'accent', icon: GitBranch, hint: 'The lens branch that pursued this' },
  trending: { tone: 'accent', icon: TrendingUp, hint: 'Citation momentum' },

  // A lens's learned preferences. Chased and suppressed are genuinely
  // different signal kinds — not one kind with a colour override — so the
  // registry owns both and the pair reads green-for / amber-away anywhere
  // it's used.
  'pref-topic': { tone: 'positive', icon: Target, hint: 'A topic this lens pulls toward' },
  'pref-author': { tone: 'positive', icon: Users, hint: 'An author this lens pulls toward' },
  'pref-venue': { tone: 'positive', icon: Quote, hint: 'A venue this lens pulls toward' },
  'pref-query': { tone: 'positive', icon: Type, hint: 'A query that recently earned saves' },
  'suppressed-topic': { tone: 'warning', icon: Target, hint: 'A topic this lens steers away from' },
  'suppressed-author': { tone: 'warning', icon: Users, hint: 'An author this lens steers away from' },
  'suppressed-venue': { tone: 'warning', icon: Quote, hint: 'A venue this lens steers away from' },

  // Plumbing + description.
  channel: { tone: 'neutral', icon: Route, hint: 'Which retrieval lane returned it' },
  source: { tone: 'neutral', icon: Globe, hint: 'Which API returned it' },
  venue: { tone: 'neutral', icon: Quote, hint: 'Journal or conference' },
  year: { tone: 'neutral', icon: Calendar },
  language: { tone: 'neutral', icon: Languages },
  'work-type': { tone: 'neutral', icon: FileText },
  collection: { tone: 'neutral', icon: Bookmark },
  meta: { tone: 'neutral', icon: Database },
}

export interface SignalChipProps {
  kind: SignalKind
  children: React.ReactNode
  /** Overrides the registry hint. Use for evidence strings that carry real
   *  figures ("Shares 12 references with …"). */
  title?: string
  size?: 'sm' | 'default' | 'lg'
  /** Drop the glyph when the chip sits in a dense row that already has one
   *  (e.g. a list of source APIs under a single Globe heading). */
  hideIcon?: boolean
  className?: string
}

/**
 * Render one chip through the shared registry. Prefer this over reaching for
 * `StatusBadge` + a hand-picked tone: it keeps colour meaning single-owner.
 */
export function SignalChip({
  kind,
  children,
  title,
  size = 'sm',
  hideIcon = false,
  className,
}: SignalChipProps) {
  const spec = SIGNAL_KINDS[kind] ?? SIGNAL_KINDS.meta
  return (
    <StatusBadge
      tone={spec.tone}
      size={size}
      icon={hideIcon ? undefined : spec.icon}
      title={title ?? spec.hint}
      className={className}
    >
      {children}
    </StatusBadge>
  )
}
