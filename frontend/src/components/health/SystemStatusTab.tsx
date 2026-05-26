/**
 * SystemStatusTab — the Health page's **System status** band: the operational
 * health of the running system, in the SAME card language as the repair groups
 * above. The four subsystems (feed monitors, upstream sources, AI provider,
 * tracked authors) render as `StatusRow`s in one card; each opens a detail
 * popup listing the affected items. Below sits `OperationalStatusCard` — the
 * "degraded right now" issues with their one-click remediation.
 *
 * No charts live here (subsystem trends + analytics → Insights → Activity).
 */
import { useState } from 'react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { StatusBadge } from '@/components/ui/status-badge'
import { OperationalStatusCard } from '@/components/settings/OperationalStatusCard'
import { useDiagnosticsSections } from '@/components/insights/useDiagnosticsSections'
import { dimensionBadgeTone, severityLabel } from './healthFormat'
import { StatusRow } from './StatusRow'

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
  const recs = ((ai as Record<string, unknown> | undefined)?.recommendations ?? []) as unknown as RecRow[]
  const badSources = ((discovery?.source_diagnostics ?? []) as unknown as SourceRow[]).filter(
    (s) => (s.http_errors ?? 0) > 0 || (s.transport_errors ?? 0) > 0,
  )

  const worst = (items: { severity?: string }[]): string => {
    if (items.some((i) => i.severity === 'critical')) return 'critical'
    if (items.length > 0) return 'warning'
    return 'ok'
  }

  // Spell out raw HTTP status codes so a glance explains the warning instead of
  // showing a bare "HTTP 429".
  const humanizeSourceError = (s: SourceRow): string => {
    const raw = (s.last_error ?? '').trim()
    if (/\b429\b/.test(raw))
      return 'Rate-limited (HTTP 429): too many requests — the source is throttling us. ALMa backs off and retries; adding/verifying an API key raises the limit.'
    if (/\b50\d\b/.test(raw)) return `Source server error (${raw}) — usually transient; ALMa retries automatically.`
    if (/\b40[13]\b/.test(raw)) return `Access rejected (${raw}) — check the API key for this source.`
    if (raw) return raw
    const total = (s.http_errors ?? 0) + (s.transport_errors ?? 0)
    return `${s.http_errors ?? 0} HTTP / ${s.transport_errors ?? 0} transport error${total === 1 ? '' : 's'}`
  }

  const cards: HealthCard[] = [
    {
      key: 'monitors',
      title: 'Feed monitors',
      metric: monitors.length,
      metricLabel: 'degraded',
      severity: monitors.length > 0 ? 'warning' : 'ok',
      blurb: 'Monitors that can’t refresh cleanly stop new papers arriving.',
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
      items: badSources.map((s, i) => ({
        id: String(i),
        primary: s.source || 'Source',
        secondary: humanizeSourceError(s),
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
      items: degradedAuthors.map((a, i) => ({
        id: String(i),
        primary: a.author_name || 'Author',
        secondary: a.last_error || a.health_reason,
        severity: 'warning',
      })),
    },
  ]

  return (
    <div className="space-y-3">
      {/* Subsystem health — one card of status rows, same language as repairs. */}
      <div className="space-y-1.5 rounded-sm border border-[var(--color-border)] bg-alma-content-elev p-4 shadow-paper">
        {cards.map((card) => (
          <StatusRow
            key={card.key}
            severity={card.severity}
            label={card.title}
            metric={
              <span className="shrink-0 text-xs tabular-nums text-slate-600">
                {card.metric > 0 ? `${card.metric} ${card.metricLabel}` : 'healthy'}
              </span>
            }
            onOpen={card.items.length > 0 ? () => setOpenCard(card) : undefined}
          />
        ))}
      </div>

      {/* Operational issues + one-click remediation (degraded right now). */}
      <OperationalStatusCard />

      {/* Per-subsystem detail popup. */}
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
                        <StatusBadge
                          tone={dimensionBadgeTone(item.severity)}
                          size="sm"
                          className="shrink-0 capitalize"
                        >
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
