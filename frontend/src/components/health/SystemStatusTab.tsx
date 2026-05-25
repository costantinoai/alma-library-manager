/**
 * SystemStatusTab — the Health page's **Status** tab: the operational health of
 * the running system, *fully* bisected here so Insights → Activity stays pure
 * analytics. Reads the shared diagnostics sections and surfaces ONLY the
 * actionable health: operational issues (via OperationalStatusCard), degraded
 * feed monitors, source errors, AI recommendations, and degraded authors.
 *
 * Per the "every card has a drilldown" rule, each health card is clickable into
 * a centered detail popup listing the affected items. No charts live here.
 */
import { useState } from 'react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { MetricTile } from '@/components/shared/MetricTile'
import { StatusBadge } from '@/components/ui/status-badge'
import { OperationalStatusCard } from '@/components/settings/OperationalStatusCard'
import { useDiagnosticsSections } from '@/components/insights/useDiagnosticsSections'
import { dimensionBadgeTone, severityLabel, severityMetricTone } from './healthFormat'

// Light, defensive row shapes — the diagnostics payloads are dynamic JSON whose
// nested types are opaque, so we read just the fields we render.
interface MonitorRow {
  author_name?: string
  label?: string
  health?: string
  health_reason?: string
  last_error?: string
}
interface AuthorRow {
  author_name?: string
  health_reason?: string
  last_error?: string
}
interface RecRow {
  label?: string
  detail?: string
  severity?: string
}
interface SourceRow {
  source?: string
  http_errors?: number
  transport_errors?: number
  retries?: number
  last_error?: string
}

interface StatusItem {
  id: string
  primary: string
  secondary?: string
  severity?: string
}
interface HealthCard {
  key: string
  title: string
  metric: number
  metricLabel: string
  severity: string // ok | info | warning | critical
  blurb: string
  items: StatusItem[]
  emptyLabel: string
}

export function SystemStatusTab() {
  const sections = useDiagnosticsSections()
  const [openCard, setOpenCard] = useState<HealthCard | null>(null)

  const feed = sections.feed.data
  const authors = sections.authors.data
  const ai = sections.ai.data
  const discovery = sections.discovery.data

  const monitors = ((feed?.monitors ?? []) as unknown as MonitorRow[]).filter(
    (m) => m.health && m.health !== 'ready' && m.health !== 'disabled',
  )
  const degradedAuthors = (authors?.degraded ?? []) as unknown as AuthorRow[]
  const recs = (((ai as Record<string, unknown> | undefined)?.recommendations ?? []) as unknown as RecRow[])
  const badSources = ((discovery?.source_diagnostics ?? []) as unknown as SourceRow[]).filter(
    (s) => (s.http_errors ?? 0) > 0 || (s.transport_errors ?? 0) > 0,
  )

  const worst = (items: { severity?: string }[]): string => {
    if (items.some((i) => i.severity === 'critical')) return 'critical'
    if (items.length > 0) return 'warning'
    return 'ok'
  }

  const cards: HealthCard[] = [
    {
      key: 'monitors',
      title: 'Feed monitors',
      metric: monitors.length,
      metricLabel: 'degraded',
      severity: monitors.length > 0 ? 'warning' : 'ok',
      blurb: 'Monitors that can’t refresh cleanly stop new papers arriving.',
      emptyLabel: 'All monitors healthy.',
      items: monitors.map((m, i) => ({
        id: String(i),
        primary: m.author_name || m.label || 'Monitor',
        secondary: m.last_error || m.health_reason || m.health,
        severity: 'warning',
      })),
    },
    {
      key: 'sources',
      title: 'Upstream sources',
      metric: badSources.length,
      metricLabel: 'with errors',
      severity: badSources.length > 0 ? 'warning' : 'ok',
      blurb: 'HTTP / transport errors on OpenAlex, Crossref, or Semantic Scholar.',
      emptyLabel: 'All sources responding cleanly.',
      items: badSources.map((s, i) => ({
        id: String(i),
        primary: s.source || 'Source',
        secondary:
          s.last_error ||
          `${s.http_errors ?? 0} HTTP / ${s.transport_errors ?? 0} transport errors`,
        severity: 'warning',
      })),
    },
    {
      key: 'ai',
      title: 'AI provider',
      metric: recs.length,
      metricLabel: 'recommendations',
      severity: worst(recs),
      blurb: 'Embedding / similarity issues that degrade Discovery ranking.',
      emptyLabel: 'AI pipeline healthy.',
      items: recs.map((r, i) => ({
        id: String(i),
        primary: r.label || 'Recommendation',
        secondary: r.detail,
        severity: r.severity,
      })),
    },
    {
      key: 'authors',
      title: 'Tracked authors',
      metric: degradedAuthors.length,
      metricLabel: 'degraded',
      severity: degradedAuthors.length > 0 ? 'warning' : 'ok',
      blurb: 'Followed authors whose identity or refresh needs attention.',
      emptyLabel: 'All tracked authors resolving cleanly.',
      items: degradedAuthors.map((a, i) => ({
        id: String(i),
        primary: a.author_name || 'Author',
        secondary: a.last_error || a.health_reason,
        severity: 'warning',
      })),
    },
  ]

  return (
    <div className="space-y-6">
      {/* Operational issues + remediation (the canonical operational view). */}
      <OperationalStatusCard />

      <section className="space-y-3">
        <h2 className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">
          Subsystem health
        </h2>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {cards.map((card) => {
            const clickable = card.items.length > 0
            return (
              <div
                key={card.key}
                role={clickable ? 'button' : undefined}
                tabIndex={clickable ? 0 : undefined}
                onClick={clickable ? () => setOpenCard(card) : undefined}
                onKeyDown={
                  clickable
                    ? (e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault()
                          setOpenCard(card)
                        }
                      }
                    : undefined
                }
                className={
                  'flex flex-col gap-2 rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-4 shadow-paper-sm transition-colors' +
                  (clickable
                    ? ' cursor-pointer hover:border-alma-300 hover:shadow-paper focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio'
                    : '')
                }
              >
                <div className="flex items-start justify-between gap-2">
                  <h3 className="text-sm font-medium text-alma-800">{card.title}</h3>
                  <StatusBadge tone={dimensionBadgeTone(card.severity)} size="sm" className="capitalize">
                    {severityLabel(card.severity)}
                  </StatusBadge>
                </div>
                <MetricTile
                  label={card.metricLabel}
                  value={card.metric}
                  tone={severityMetricTone(card.severity)}
                />
                <p className="text-xs text-slate-500">
                  {card.metric > 0 ? card.blurb : card.emptyLabel}
                </p>
                {clickable ? (
                  <p className="text-[11px] font-medium text-alma-folio">View details →</p>
                ) : null}
              </div>
            )
          })}
        </div>
      </section>

      {/* Per-card detail popup. */}
      <Dialog open={openCard != null} onOpenChange={(o) => !o && setOpenCard(null)}>
        <DialogContent className="max-h-[80vh] max-w-2xl overflow-y-auto bg-alma-chrome">
          {openCard ? (
            <>
              <DialogHeader>
                <DialogTitle className="text-alma-900">{openCard.title}</DialogTitle>
                <DialogDescription className="text-slate-600">{openCard.blurb}</DialogDescription>
              </DialogHeader>
              <div className="space-y-2">
                {openCard.items.map((item) => (
                  <div
                    key={item.id}
                    className="rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-3"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <p className="min-w-0 text-sm font-medium text-alma-800">{item.primary}</p>
                      {item.severity ? (
                        <StatusBadge tone={dimensionBadgeTone(item.severity)} size="sm" className="shrink-0 capitalize">
                          {severityLabel(item.severity)}
                        </StatusBadge>
                      ) : null}
                    </div>
                    {item.secondary ? (
                      <p className="mt-1 text-xs text-slate-500">{item.secondary}</p>
                    ) : null}
                  </div>
                ))}
              </div>
            </>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
