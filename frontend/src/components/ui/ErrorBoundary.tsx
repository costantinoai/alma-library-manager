import { Component, type ReactNode } from 'react'

interface ErrorBoundaryProps {
  /** Rendered in place of the subtree when a descendant throws. */
  fallback: ReactNode
  children: ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
}

/**
 * Minimal class error boundary (React has no functional equivalent). Wrap
 * subtrees that can throw at render time — notably the lazy WebGL graph, where
 * a driver/context failure would otherwise unmount the whole app — so the
 * failure is contained to a local fallback instead of crashing the page.
 *
 * Boundaries reset on unmount, so closing + reopening the host (e.g. a dialog
 * that unmounts its content) gives a fresh attempt.
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false }

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true }
  }

  componentDidCatch(error: unknown) {
    console.error('ErrorBoundary caught a render error:', error)
  }

  render() {
    return this.state.hasError ? this.props.fallback : this.props.children
  }
}
