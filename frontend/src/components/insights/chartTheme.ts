/**
 * Shared chart palette for Insights + the Health page's System Diagnostics.
 *
 * Pinned to the ALMa brand (navy anchor, Folio-blue accent, pale-blue +
 * parchment supporting tones, gold trim, status semantics). Extracted from
 * InsightsPage so the diagnostics scorecards render identically whether shown
 * under Insights (historically) or under Health (current home).
 */

export const COLORS = {
  blue: '#0F1E36', // alma-800 (brand navy)
  purple: '#152642', // alma-700 (deeper navy)
  green: '#1E5B86', // alma-folio (Folio binding blue)
  amber: '#C49A45', // gold-400 (brand gold)
  cyan: '#6F98BB', // pale-500 (mid pale-blue)
  pink: '#C2A86B', // parchment-500 (warm parchment)
  indigo: '#344E7C', // alma-500 (mid navy)
  orange: '#A77E36', // gold-500 (warm trim)
  red: '#f43f5e', // critical (semantic token)
  slate: '#64748b',
}

export const PIE_COLORS = [
  COLORS.blue,
  COLORS.green,
  COLORS.amber,
  COLORS.cyan,
  COLORS.indigo,
  COLORS.pink,
  COLORS.purple,
  COLORS.orange,
]

export const TOOLTIP_STYLE = {
  contentStyle: {
    background: '#0F1E36', // alma-800 (brand navy)
    border: '1px solid #C49A45', // gold trim — editorial card edge
    borderRadius: 2,
    color: '#FFF9F0', // alma-cream
    fontSize: 13,
    padding: '6px 10px',
  },
  itemStyle: { color: '#FFF9F0' },
  labelStyle: { color: '#C49A45', fontWeight: 600, marginBottom: 4 },
}
