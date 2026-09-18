/**
 * useDiagnosticsSections — the shared data hook behind the Health page's
 * **System status** band and its **Activity** section. One materialised view
 * per subsystem; each streams in independently and caches for 60s, so both
 * surfaces read the SAME section data (DRY) instead of wiring eight queries
 * twice. It also owns the section-state TYPES, which used to live in the
 * (now deleted) Insights diagnostics tab.
 */
import { useAsyncRefresh } from '@/hooks/useAsyncRefresh'
import { useQueries } from '@tanstack/react-query'

import {
  getDiagnosticsSection,
  refreshDiagnostics,
  type DiagnosticsSectionKey,
  type DiagnosticsAiSection,
  type DiagnosticsAlertsSection,
  type DiagnosticsAuthorsSection,
  type DiagnosticsDiscoverySection,
  type DiagnosticsEvaluationSection,
  type DiagnosticsFeedSection,
  type DiagnosticsFeedbackSection,
  type DiagnosticsOperationalSection,
} from '@/api/client'

/**
 * Per-section load state. Sections stream independently so a fast one paints
 * while a slow one still shows a skeleton. `loading` is true while the first
 * response is in flight; `stale` means a cached payload is being served while
 * a background rebuild runs (surfaced as a "Refreshing…" pill, never as a
 * blocking spinner).
 */
export interface SectionState<T> {
  data?: T
  loading: boolean
  error: boolean
  stale?: boolean
}

export interface InsightsDiagnosticsSections {
  feed: SectionState<DiagnosticsFeedSection>
  discovery: SectionState<DiagnosticsDiscoverySection>
  ai: SectionState<DiagnosticsAiSection>
  authors: SectionState<DiagnosticsAuthorsSection>
  alerts: SectionState<DiagnosticsAlertsSection>
  feedback: SectionState<DiagnosticsFeedbackSection>
  operational: SectionState<DiagnosticsOperationalSection>
  evaluation: SectionState<DiagnosticsEvaluationSection>
}

const SECTIONS: DiagnosticsSectionKey[] = ['feed', 'discovery', 'ai', 'authors', 'alerts', 'feedback', 'operational', 'evaluation']

export function useDiagnosticsSections() {
  const { building, buildError, refresh } = useAsyncRefresh({
    queryKey: ['insights-diag'],
    request: refreshDiagnostics,
  })
  const queries = useQueries({ queries: SECTIONS.map(section => ({
    queryKey: ['insights-diag', section],
    queryFn: () => getDiagnosticsSection(section),
    staleTime: 60_000,
    retry: 1,
    refetchInterval: building ? 1500 : false as const,
  })) })

  const sections = Object.fromEntries(SECTIONS.map((section, index) => {
    const query = queries[index]
    return [section, {
      data: query.data ?? undefined,
      loading: !query.data && !buildError && (query.isLoading || building),
      error: !query.data && (query.isError || Boolean(buildError)),
      stale: Boolean(query.data && building),
    }]
  })) as unknown as InsightsDiagnosticsSections
  return {
    ...sections,
    building,
    refreshError: buildError || (queries.some(query => query.isError) ? 'Could not load diagnostic snapshots.' : null),
    refresh,
  }
}
