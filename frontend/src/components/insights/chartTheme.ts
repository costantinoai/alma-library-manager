/**
 * Shared chart palette for Insights + the Health page's System Diagnostics.
 *
 * Pinned to the ALMa brand (navy anchor, Folio-blue accent, two chart series,
 * gold trim, status semantics). Extracted from
 * InsightsPage so the diagnostics scorecards render identically whether shown
 * under Insights (historically) or under Health (current home).
 */

export const COLORS = {
  blue: 'var(--color-alma-800)', // brand navy
  purple: 'var(--color-alma-700)', // deeper navy
  green: 'var(--color-alma-folio)', // Folio binding blue
  amber: 'var(--color-gold-400)', // brand gold
  cyan: 'var(--color-chart-sky)',
  pink: 'var(--color-chart-sand)',
  indigo: 'var(--color-alma-500)', // mid navy
  orange: 'var(--color-gold-500)', // warm trim
  red: 'var(--color-critical-500)', // critical
  slate: 'var(--color-slate-500)',
}

/** The chart frame: grid lines, axis lines, tick labels. */
export const CHART_FRAME = {
  grid: 'var(--color-border-cool)',
  axis: 'var(--color-border)',
  tick: 'var(--color-alma-700)',
}

export const TOOLTIP_STYLE = {
  contentStyle: {
    background: 'var(--color-alma-800)',
    border: '1px solid var(--color-gold-400)', // gold trim, editorial card edge
    borderRadius: 6,
    color: 'var(--color-alma-cream)',
    fontSize: 13,
    padding: '6px 10px',
  },
  itemStyle: { color: 'var(--color-alma-cream)' },
  labelStyle: { color: 'var(--color-gold-400)', fontWeight: 600, marginBottom: 4 },
}
