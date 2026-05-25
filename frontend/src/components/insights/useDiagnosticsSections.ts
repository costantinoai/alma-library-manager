/**
 * useDiagnosticsSections — the shared data hook behind both the Insights
 * **Activity** tab (analytics) and the Health **Status** tab (operational
 * health). One materialised view per subsystem; each streams in independently
 * and caches for 60s. Extracted so the two surfaces read the SAME section data
 * (DRY) instead of each wiring its own eight queries.
 */
import { useQuery } from '@tanstack/react-query'

import {
  getDiagnosticsSection,
  type DiagnosticsAiSection,
  type DiagnosticsAlertsSection,
  type DiagnosticsAuthorsSection,
  type DiagnosticsDiscoverySection,
  type DiagnosticsEvaluationSection,
  type DiagnosticsFeedSection,
  type DiagnosticsFeedbackSection,
  type DiagnosticsOperationalSection,
} from '@/api/client'
import {
  type InsightsDiagnosticsSections,
  type SectionState,
} from '@/components/insights/InsightsDiagnosticsTab'

function toSectionState<T extends { stale?: boolean }>(query: {
  data?: T
  isLoading: boolean
  isError: boolean
}): SectionState<T> {
  return {
    data: query.data,
    loading: query.isLoading,
    error: query.isError,
    stale: query.data?.stale ?? false,
  }
}

export function useDiagnosticsSections(): InsightsDiagnosticsSections {
  const feed = useQuery({
    queryKey: ['insights-diag', 'feed'],
    queryFn: () => getDiagnosticsSection('feed'),
    staleTime: 60_000,
    retry: 1,
  })
  const discovery = useQuery({
    queryKey: ['insights-diag', 'discovery'],
    queryFn: () => getDiagnosticsSection('discovery'),
    staleTime: 60_000,
    retry: 1,
  })
  const ai = useQuery({
    queryKey: ['insights-diag', 'ai'],
    queryFn: () => getDiagnosticsSection('ai'),
    staleTime: 60_000,
    retry: 1,
  })
  const authors = useQuery({
    queryKey: ['insights-diag', 'authors'],
    queryFn: () => getDiagnosticsSection('authors'),
    staleTime: 60_000,
    retry: 1,
  })
  const alerts = useQuery({
    queryKey: ['insights-diag', 'alerts'],
    queryFn: () => getDiagnosticsSection('alerts'),
    staleTime: 60_000,
    retry: 1,
  })
  const feedback = useQuery({
    queryKey: ['insights-diag', 'feedback'],
    queryFn: () => getDiagnosticsSection('feedback'),
    staleTime: 60_000,
    retry: 1,
  })
  const operational = useQuery({
    queryKey: ['insights-diag', 'operational'],
    queryFn: () => getDiagnosticsSection('operational'),
    staleTime: 60_000,
    retry: 1,
  })
  const evaluation = useQuery({
    queryKey: ['insights-diag', 'evaluation'],
    queryFn: () => getDiagnosticsSection('evaluation'),
    staleTime: 60_000,
    retry: 1,
  })

  return {
    feed: toSectionState<DiagnosticsFeedSection>(feed),
    discovery: toSectionState<DiagnosticsDiscoverySection>(discovery),
    ai: toSectionState<DiagnosticsAiSection>(ai),
    authors: toSectionState<DiagnosticsAuthorsSection>(authors),
    alerts: toSectionState<DiagnosticsAlertsSection>(alerts),
    feedback: toSectionState<DiagnosticsFeedbackSection>(feedback),
    operational: toSectionState<DiagnosticsOperationalSection>(operational),
    evaluation: toSectionState<DiagnosticsEvaluationSection>(evaluation),
  }
}
