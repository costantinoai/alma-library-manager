export async function mountHealthFixture() {
  const refresh = (await import('/@react-refresh')).default
  refresh.injectIntoGlobalHook(window)
  window.$RefreshReg$ = () => {}
  window.$RefreshSig$ = () => (type) => type
  window.__vite_plugin_react_preamble_installed__ = true
  await import('/src/index.css')
  const reactModule = await import('react')
  const React = reactModule.default ?? reactModule
  const domModule = await import('react-dom/client')
  const { createRoot } = domModule.default ?? domModule
  const { QueryClient, QueryClientProvider } = await import('@tanstack/react-query')
  const { RepairCard } = await import('/src/components/health/RepairCard.tsx')
  const { DiagnosticsSection } = await import('/src/components/health/DiagnosticsSection.tsx')
  const root = createRoot(document.getElementById('root'))
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const base = {
    key: 'repair', label: 'Repair metadata', description: 'Fill missing metadata',
    cost: 'cheap', sources: [], local_compute: false, destructive: false,
    stage: 'metadata', order: 1, unit: 'paper', target_kind: 'paper',
    supports_targets: false, prerequisites: [], dependencies: [], blocked_by: [],
    unlocks: [], optional: false, manual_gate: false, readiness: 'assessment_failed',
    recommended: false, repairs: [], operation_key: 'repair', candidates_pending: null,
    eta: null, quota: null, params_spec: { dry_run: { default: true } },
    request_batch_size: null, request_batch_default: null, request_batch_max: null,
    request_batch_unit: null, auto_enabled: false, default_auto_enabled: false,
    paused_by_user_until: null, auto_daily_cap: 10, max_auto_daily_cap: 100,
    manual_limit: 10, default_manual_limit: 10, max_manual_limit: 100,
    last_run: null, last_success_at: null,
    assessment_error: {
      task_key: 'repair', message: 'Could not assess Repair metadata.',
      cause: 'OperationalError: assessment storage unavailable',
      recovery: 'Use Health → Re-assess to retry. If this persists, report the task and cause with the server log; repair eligibility is unknown.',
    },
  }
  window.showAssessment = (recovered) => root.render(React.createElement(
    QueryClientProvider, { client }, React.createElement(RepairCard, {
      op: recovered ? { ...base, candidates_pending: 5, readiness: 'ready', assessment_error: null } : base,
      dims: [], onRun: () => {}, onConfig: () => {}, onResume: () => {},
      onOpenDim: () => {}, running: false,
    }),
  ))
  window.showAssessment(false)
  window.showSeedPlacement = () => root.render(React.createElement(
    QueryClientProvider, { client }, React.createElement('div', { className: 'space-y-6' },
      React.createElement(RepairCard, {
        op: { ...base, label: 'Seed under-covered suggested authors', description: 'Fetch papers for suggested authors with fewer than two papers. Map placement also requires vectors and a layout.', candidates_pending: 0, readiness: 'healthy', assessment_error: null },
        dims: [], onRun: () => {}, onConfig: () => {}, onResume: () => {}, onOpenDim: () => {}, running: false,
      }),
      React.createElement(DiagnosticsSection, {
        dims: [{ key: 'authors.unplaceable', label: 'Suggested authors awaiting map placement', count: 1, total: 1, severity: 'info', state: 'measured', actions: [], repair_task: null,
          explanation: '1 suggested author already has at least two papers, but fewer than two are embedded and placed on the map.',
          impact: 'No more paper seeding is needed. Check vector-fetch and local-embedding eligibility; when vectors are available, rebuild map layouts.' }],
        onOpenDim: () => {},
      }),
    ),
  ))
}
