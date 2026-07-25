/**
 * useDiagnosticsSections — the shared data hook behind the Health page's
 * **System status** band and its **Activity** section. One materialised view
 * per subsystem; each streams in independently and caches for 60s, so both
 * surfaces read the SAME section data (DRY) instead of wiring eight queries
 * twice. It also owns the section-state TYPES, which used to live in the
 * (now deleted) Insights diagnostics tab.
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
