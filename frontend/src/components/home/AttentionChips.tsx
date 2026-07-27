import { FileUp, HeartPulse, Inbox, Radio, Users } from 'lucide-react'
import type { ComponentType } from 'react'

import type { HomeBrief } from '@/api/client'
import { StatusChip } from '@/components/shared/StatusChip'
import { buildHashRoute } from '@/lib/hashRoute'
import { severityRank, type Severity } from '@/lib/severity'

type AttentionKey = keyof HomeBrief['attention']

interface AttentionSpec {
  /** Category glyph — severity says how urgent, the icon says about what. */
  icon: ComponentType<{ className?: string }>
  /**
   * How badly this wants you, in the app's one severity vocabulary
   * (`lib/severity`), so a Home chip grades on the same scale as a Health
   * dimension and its colour means the same thing.
   *
   * `critical` = broken and staying broken; `warning` = a decision ALMa cannot
   * make for you; `info` = a queue waiting on you but harming nothing.
   */
  severity: Severity
  /** Chip line 1 — what needs you. */
  label: (count: number) => string
  /** Chip line 2 — the count, phrased so the number carries a unit. */
  metric: (count: number) => string
  /** The full explanation, on hover — what happened and what happens next. */
  title: (count: number) => string
  href: string
}

/**
 * THE registry of everything that can ask for you on Home.
 *
 * One entry owns a kind's icon, severity, wording and destination together.
 * These were five hand-written rows whose tone was hard-coded amber for all of
 * them — which spent a colour to say "attention", a thing the label already
 * said.
 */
const ATTENTION: Record<AttentionKey, AttentionSpec> = {
  critical_health: {
    icon: HeartPulse,
    severity: 'critical',
    label: () => 'Health',
    metric: (n) => `${n} critical ${n === 1 ? 'issue' : 'issues'}`,
    title: (n) =>
      `${n} Health ${n === 1 ? 'finding is' : 'findings are'} critical and actionable. Open Health to run the repair.`,
    href: buildHashRoute('health'),
  },
  monitors_need_resolution: {
    icon: Radio,
    // A monitor that lost its link stops delivering SILENTLY — a break, not a
    // pending decision, which is why it grades alongside critical health.
    severity: 'critical',
    label: () => 'Feed monitors',
    metric: (n) => `${n} not delivering`,
    title: (n) =>
      `${n} Feed ${n === 1 ? 'monitor has' : 'monitors have'} lost their link and are silently delivering nothing. Relink them in Settings.`,
    href: buildHashRoute('settings', { anchor: 'feed-monitors' }),
  },
  inbox_unresolved: {
    icon: Inbox,
    severity: 'warning',
    label: () => 'Captures',
    metric: (n) => `${n} not identified`,
    title: (n) =>
      `${n} captured ${n === 1 ? 'message' : 'messages'} reached ALMa but resolved to no paper — a link with no DOI, or an upstream failure. Recorded rather than dropped.`,
    href: buildHashRoute('settings', { anchor: 'plugins' }),
  },
  author_decisions: {
    icon: Users,
    severity: 'warning',
    label: () => 'Author identities',
    metric: (n) => `${n} to review`,
    title: (n) =>
      `${n} author ${n === 1 ? 'identity needs' : 'identities need'} a decision ALMa cannot make for you.`,
    href: buildHashRoute('authors', { focus: 'needs-attention' }),
  },
  imports_pending: {
    icon: FileUp,
    severity: 'info',
    label: () => 'Imports',
    metric: (n) => `${n} to review`,
    title: (n) =>
      `${n} imported ${n === 1 ? 'paper is' : 'papers are'} waiting in the staging panel. Nothing is harmed while they wait.`,
    href: buildHashRoute('library', { tab: 'imports' }),
  },
}

export interface AttentionChipsProps {
  attention: HomeBrief['attention']
}

/**
 * Everything that needs a decision from you — the second half of "what is my
 * situation", directly under today's numbers.
 *
 * **Just the chips.** No panel, no heading: a chip whose dot, name and count
 * already say "Health · 1 critical issue" needs no label announcing that things
 * need you, and a frame around them only pushes the research further down. They
 * are the shared `StatusChip` in its slim weight — the same chip Health's
 * system-status band uses boxed — and they sit on the SAME line as the
 * connection dots, because "is the machinery up" and "does anything want me"
 * are one glance. Each links to the surface that owns the fix.
 *
 * Ordered by severity, so the worst thing is always first, and a kind with a
 * zero count is absent entirely. When nothing is waiting, nothing renders —
 * which makes their presence the signal.
 */
export function AttentionChips({ attention }: AttentionChipsProps) {
  const items = (Object.keys(ATTENTION) as AttentionKey[])
    .map((key) => ({ key, count: attention[key] ?? 0, spec: ATTENTION[key] }))
    .filter((item) => item.count > 0)
    .sort((a, b) => severityRank(a.spec.severity) - severityRank(b.spec.severity))

  if (items.length === 0) return null

  // A fragment, not a row: the host places these on one line with the
  // connection dots.
  return (
    <>
      {items.map(({ key, count, spec }) => (
        <StatusChip
          key={key}
          variant="slim"
          severity={spec.severity}
          name={spec.label(count)}
          href={spec.href}
          title={`${spec.metric(count)}. ${spec.title(count)}`}
          ariaLabel={`${spec.label(count)}: ${spec.metric(count)}`}
        />
      ))}
    </>
  )
}
