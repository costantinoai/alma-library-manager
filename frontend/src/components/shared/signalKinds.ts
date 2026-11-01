import type { ComponentType } from 'react'
import {
  AlertTriangle,
  Bookmark,
  Calendar,
  Database,
  FileCheck,
  FileQuestion,
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

import type { StatusBadgeTone } from '@/components/ui/status-badge'

/**
 * THE registry of every chip the app can show, and what its colour means.
 *
 * Colour communicates valence; the icon communicates category. Keeping this
 * registry outside the component module lets React Fast Refresh treat
 * `SignalChip.tsx` as a component-only boundary.
 */
export type SignalKind =
  | 'taste-match'
  | 'taste-avoid'
  | 'retracted'
  | 'consensus'
  | 'topic'
  | 'wording'
  | 'authors'
  | 'coupling'
  | 'cocitation'
  | 'branch'
  | 'trending'
  | 'pref-topic'
  | 'pref-author'
  | 'pref-venue'
  | 'pref-query'
  | 'suppressed-topic'
  | 'suppressed-author'
  | 'suppressed-venue'
  | 'channel'
  | 'source'
  | 'venue'
  | 'year'
  | 'language'
  | 'work-type'
  | 'collection'
  | 'meta'

  // The paper's kept PDF (task 81). Plumbing, not a verdict: neutral, with
  // the icon saying whether the file's own text confirmed the paper.
  | 'pdf'
  | 'pdf-unverified'
  | 'pdf-mismatch'

interface SignalSpec {
  tone: StatusBadgeTone
  icon?: ComponentType<{ className?: string }>
  hint?: string
  /** The chip's words, kept as short as a scan needs: the icon already says the
   *  category, and the full meaning opens from `SignalChip`'s `explain`. */
  label?: (value: string | number) => string
}

/** A kind's short label for `value`. Kinds without one show the value itself. */
export function signalLabel(kind: SignalKind, value: string | number): string {
  return SIGNAL_KINDS[kind].label?.(value) ?? String(value)
}

/** Rarest first: a chip on nearly every card tells two papers apart on nothing.
 *  `counts` holds how many items in the visible list carry each key; without it
 *  the items keep their order. Ties keep their order too. */
export function byRarity<T>(items: T[], key: (item: T) => string, counts?: Map<string, number>): T[] {
  if (!counts) return items
  return [...items].sort((a, b) => (counts.get(key(a)) ?? 0) - (counts.get(key(b)) ?? 0))
}

/** How many of `lists` carry each key, for `byRarity`. */
export function countChipKeys(lists: string[][]): Map<string, number> {
  const counts = new Map<string, number>()
  for (const list of lists) for (const key of new Set(list)) counts.set(key, (counts.get(key) ?? 0) + 1)
  return counts
}

export const SIGNAL_KINDS: Record<SignalKind, SignalSpec> = {
  'taste-match': {
    tone: 'positive',
    icon: Heart,
    hint: 'Close to papers you save and rate highly',
    label: () => 'Your taste',
  },
  'taste-avoid': {
    tone: 'warning',
    icon: AlertTriangle,
    hint: 'Close to papers you dismissed or disliked',
    label: () => 'Near dislike',
  },
  retracted: { tone: 'negative', icon: AlertTriangle, hint: 'This work has been retracted' },

  consensus: { tone: 'accent', icon: Layers, hint: 'Independently found by several sources', label: (n) => `${n} sources` },
  topic: { tone: 'accent', icon: Target, hint: 'Semantic (SPECTER2) closeness to your library', label: (band) => `${band} topic` },
  wording: { tone: 'accent', icon: Type, hint: 'Keyword / phrasing overlap with your library', label: (band) => `${band} words` },
  authors: { tone: 'accent', icon: Users, hint: 'Shares authors with papers you have', label: (n) => (n === 1 ? 'Co-author' : `${n} authors`) },
  coupling: { tone: 'accent', icon: Link2, hint: 'Cites the same works as a paper you saved', label: (n) => (Number(n) === 1 ? '1 ref' : `${n} refs`) },
  cocitation: { tone: 'accent', icon: GitMerge, hint: 'Cited alongside a paper you saved', label: (n) => `${n} co-cited` },
  branch: { tone: 'accent', icon: GitBranch, hint: 'The lens branch that pursued it' },
  trending: { tone: 'accent', icon: TrendingUp, hint: 'Citation momentum' },

  'pref-topic': { tone: 'positive', icon: Target, hint: 'A topic this lens pulls toward' },
  'pref-author': { tone: 'positive', icon: Users, hint: 'An author this lens pulls toward' },
  'pref-venue': { tone: 'positive', icon: Quote, hint: 'A venue this lens pulls toward' },
  'pref-query': { tone: 'positive', icon: Type, hint: 'A query that recently earned saves' },
  'suppressed-topic': { tone: 'warning', icon: Target, hint: 'A topic this lens steers away from' },
  'suppressed-author': { tone: 'warning', icon: Users, hint: 'An author this lens steers away from' },
  'suppressed-venue': { tone: 'warning', icon: Quote, hint: 'A venue this lens steers away from' },

  channel: { tone: 'neutral', icon: Route, hint: 'Which retrieval lane returned it' },
  source: { tone: 'neutral', icon: Globe, hint: 'Which API returned it' },
  venue: { tone: 'neutral', icon: Quote, hint: 'Journal or conference' },
  year: { tone: 'neutral', icon: Calendar },
  language: { tone: 'neutral', icon: Languages },
  'work-type': { tone: 'neutral', icon: FileText },
  collection: { tone: 'neutral', icon: Bookmark },
  meta: { tone: 'neutral', icon: Database },


  pdf: { tone: 'neutral', icon: FileCheck, hint: 'A PDF is kept for this paper; its text names this paper' },
  'pdf-unverified': {
    tone: 'neutral',
    icon: FileQuestion,
    hint: 'A PDF is kept, but it has no readable text to confirm which paper it is',
  },
  'pdf-mismatch': {
    tone: 'neutral',
    icon: AlertTriangle,
    hint: 'You attached this PDF, but its text names a different paper',
  },
}
