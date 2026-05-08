/**
 * Formatting helpers shared across pages.
 *
 * The repeated `(x * 100).toFixed(N)` and `toLocaleDateString('en-GB',
 * { month: 'short', year: 'numeric' })` patterns lived in 8+ components
 * with no single source — divergence in fraction digits / locale was
 * just waiting to happen.
 */

/**
 * Format a unit-interval ratio (0..1) as a percent string with the
 * given number of fraction digits. The trailing `%` is included.
 *
 * Example: `formatPercent(0.387, 1)` → `"38.7%"`.
 *
 * Pass `withSign=false` to drop the trailing `%` (useful inside table
 * cells that already render the unit separately).
 */
export function formatPercent(
  ratio: number | null | undefined,
  fractionDigits = 0,
  { withSign = true }: { withSign?: boolean } = {},
): string {
  const value = typeof ratio === 'number' && Number.isFinite(ratio) ? ratio : 0
  const text = (value * 100).toFixed(fractionDigits)
  return withSign ? `${text}%` : text
}

/**
 * Format an ISO date / `YYYY-MM-DD` string as `"Mar 2024"`. Returns
 * the empty string when the input is falsy or unparseable.
 */
export function formatYearMonth(value: string | null | undefined): string {
  if (!value) return ''
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })
}
