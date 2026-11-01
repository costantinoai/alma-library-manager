import { Component, type ErrorInfo, type ReactNode } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  message: string | null
}

/**
 * Last line of defence around the whole app. Without it React unmounts the tree
 * on a render error and the user sees a blank page with nothing to report. It
 * uses plain markup on purpose: anything richer could be what just failed.
 */
export class AppErrorBoundary extends Component<Props, State> {
  state: State = { message: null }

  static getDerivedStateFromError(error: unknown): State {
    return { message: error instanceof Error ? error.message : String(error) }
  }

  componentDidCatch(error: unknown, info: ErrorInfo) {
    console.error('ALMa failed to render', error, info.componentStack)
  }

  render() {
    if (this.state.message === null) return this.props.children
    return (
      <main style={{ maxWidth: 560, margin: '15vh auto', padding: 24, fontFamily: 'system-ui, sans-serif' }}>
        <h1 style={{ fontSize: 20 }}>ALMa could not start this page</h1>
        <p>Reloading usually fixes it after an update. If it does not, this is the error:</p>
        <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontSize: 13 }}>{this.state.message}</pre>
        <button type="button" onClick={() => window.location.reload()} style={{ padding: '8px 16px' }}>
          Reload
        </button>
      </main>
    )
  }
}
