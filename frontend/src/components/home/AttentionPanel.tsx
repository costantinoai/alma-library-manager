import { FileUp, HeartPulse, Inbox, Radio, Users } from 'lucide-react'
import type { ComponentType } from 'react'

import type { HomeBrief } from '@/api/client'
import { StatusRow } from '@/components/shared/StatusRow'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { SubPanel } from '@/components/ui/sub-panel'
import { buildHashRoute } from '@/lib/hashRoute'
import { severityRank, type Severity } from '@/lib/severity'

type AttentionKey = keyof HomeBrief['attention']

interface AttentionSpec {
  /** Category glyph — severity says how urgent, the icon says about what. */
  icon: ComponentType<{ className?: string }>
  /**
   * How badly this wants you, in the app's one severity vocabulary
   * (`lib/severity`), so a Home row grades exactly like a Health row.
   *
   * `critical` = broken and staying broken; `warning` = a decision ALMa cannot
   * make for you; `info` = a queue waiting on you but harming nothing.
   */
  severity: Severity
  /** What is wrong, as a sentence. The count rides in the metric slot. */
  label: (count: number) => string
  /** The full explanation, on hover — what happened and what happens next. */
  title: (count: number) => string
  href: string
}

/**
 * THE registry of everything that can ask for you on Home.
 *
 * One entry owns a kind's icon, severity, wording and destination together.
 * These were five hand-written JSX rows whose tone was hard-coded amber for
 * all of them — which spent a colour to say "attention", a thing the panel
 * heading already said.
 */
const ATTENTION: Record<AttentionKey, AttentionSpec> = {
  critical_health: {
    icon: HeartPulse,
    severity: 'critical',
    label: (n) => (n === 1 ? 'Critical health issue' : 'Critical health issues'),
    title: (n) =>
      `${n} Health ${n === 1 ? 'finding is' : 'findings are'} critical and actionable. Open Health to run the repair.`,
    href: buildHashRoute('health'),
  },
  monitors_need_resolution: {
    icon: Radio,
    // A monitor that lost its link stops delivering SILENTLY — a break, not a
    // pending decision, which is why it grades alongside critical health.
    severity: 'critical',
    label: (n) => (n === 1 ? 'Monitor not delivering' : 'Monitors not delivering'),
    title: (n) =>
      `${n} Feed ${n === 1 ? 'monitor has' : 'monitors have'} lost their link and are silently delivering nothing. Relink them in Settings.`,
    href: buildHashRoute('settings', { anchor: 'feed-monitors' }),
  },
  inbox_unresolved: {
    icon: Inbox,
    severity: 'warning',
    label: (n) => (n === 1 ? 'Capture not identified' : 'Captures not identified'),
    title: (n) =>
      `${n} captured ${n === 1 ? 'message' : 'messages'} reached ALMa but resolved to no paper — a link with no DOI, or an upstream failure. Recorded rather than dropped.`,
    href: buildHashRoute('settings', { anchor: 'channels' }),
  },
  author_decisions: {
    icon: Users,
    severity: 'warning',
    label: (n) => (n === 1 ? 'Author identity to review' : 'Author identities to review'),
    title: (n) =>
      `${n} author ${n === 1 ? 'identity needs' : 'identities need'} a decision ALMa cannot make for you.`,
    href: buildHashRoute('authors', { focus: 'needs-attention' }),
  },
  imports_pending: {
    icon: FileUp,
    severity: 'info',
    label: (n) => (n === 1 ? 'Import to review' : 'Imports to review'),
    title: (n) =>
      `${n} imported ${n === 1 ? 'paper is' : 'papers are'} waiting in the staging panel. Nothing is harmed while they wait.`,
    href: buildHashRoute('library', { tab: 'imports' }),
  },
}

export interface AttentionPanelProps {
  attention: HomeBrief['attention']
}

/**
 * Everything that needs a decision from you — the second half of "what is my
 * situation", directly under today's numbers.
 *
 * Built from the app's shared `StatusRow`, the same line Health uses for a
 * failing dimension or a degraded subsystem: severity badge, what it is, how
 * many, where to fix it. Home does not get its own dialect for this — a row
 * that means "something needs you" should read identically on both pages.
 *
 * Rows are ordered by severity, so the worst thing is always first, and a kind
 * with a zero count is absent entirely. When nothing is waiting the panel does
 * not render, which makes its presence the signal.
 */
export function AttentionPanel({ attention }: AttentionPanelProps) {
  const items = (Object.keys(ATTENTION) as AttentionKey[])
    .map((key) => ({ key, count: attention[key] ?? 0, spec: ATTENTION[key] }))
    .filter((item) => item.count > 0)
    .sort((a, b) => severityRank(a.spec.severity) - severityRank(b.spec.severity))

  if (items.length === 0) return null

  return (
    <SubPanel className="space-y-2.5 p-3.5">
      <EyebrowLabel tone="muted">Needs you</EyebrowLabel>
      {/* Two columns on a wide desk: five obligations as five full-width bars
          would out-weigh the research they sit above. */}
      <div className="grid gap-2 lg:grid-cols-2">
        {items.map(({ key, count, spec }) => {
          const Icon = spec.icon
          return (
            <StatusRow
              key={key}
              severity={spec.severity}
              href={spec.href}
              title={spec.title(count)}
              label={spec.label(count)}
              metric={
                <span className="flex shrink-0 items-center gap-1.5 text-xs font-medium text-alma-700">
                  <Icon className="h-3.5 w-3.5 text-slate-400" aria-hidden />
                  <span className="tabular-nums">{count}</span>
                </span>
              }
            />
          )
        })}
      </div>
    </SubPanel>
  )
}
