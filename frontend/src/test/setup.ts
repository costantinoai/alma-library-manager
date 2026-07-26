// Vitest global setup: register @testing-library/jest-dom matchers
// (toBeInTheDocument, toHaveClass, toHaveTextContent, …) and auto-clean
// the rendered DOM between tests.
import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

// jsdom ships no ResizeObserver, and container-measured layout
// (`useElementWidth` → `useMeasuredGrid`) mounts one on every measured grid.
// The stub observes nothing, so measured components render at their declared
// fallback column count — deterministic for assertions.
if (!('ResizeObserver' in globalThis)) {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
}

afterEach(() => {
  cleanup()
})
