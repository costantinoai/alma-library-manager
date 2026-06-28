/**
 * InsightsActivity — the subsystem scorecards (feed / discovery / ai / authors /
 * alerts / feedback / evaluation): trends, distributions, and quality metrics
 * over time. The analytics half of the old "Diagnostics" tab; lives under
 * **Insights** (Stats / Graph / Activity / Reports).
 *
 * Operational *health*, repair recommendations, automation setup, AND branch
 * tuning are mutations/operations that live on the Health / Alerts / Discovery
 * pages — this tab is read-only analytics (I-27 / D7). It just reads the shared
 * `useDiagnosticsSections` hook and renders the presentational tab.
 */
import { InsightsDiagnosticsTab } from '@/components/insights/InsightsDiagnosticsTab'
import { useDiagnosticsSections } from '@/components/insights/useDiagnosticsSections'
import { COLORS, TOOLTIP_STYLE } from '@/components/insights/chartTheme'

export function InsightsActivity() {
  const sections = useDiagnosticsSections()
  return <InsightsDiagnosticsTab sections={sections} colors={COLORS} tooltipStyle={TOOLTIP_STYLE} />
}
