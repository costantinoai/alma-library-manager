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

interface SignalSpec {
  tone: StatusBadgeTone
  icon?: ComponentType<{ className?: string }>
  hint?: string
}

export const SIGNAL_KINDS: Record<SignalKind, SignalSpec> = {
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

  consensus: { tone: 'accent', icon: Layers, hint: 'Independently found by several sources' },
  topic: { tone: 'accent', icon: Target, hint: 'Semantic (SPECTER2) closeness to your library' },
  wording: { tone: 'accent', icon: Type, hint: 'Keyword / phrasing overlap with your library' },
  authors: { tone: 'accent', icon: Users, hint: 'Shares authors with papers you have' },
  coupling: { tone: 'accent', icon: Link2, hint: 'Cites the same works as a paper you saved' },
  cocitation: { tone: 'accent', icon: GitMerge, hint: 'Cited alongside a paper you saved' },
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
}
