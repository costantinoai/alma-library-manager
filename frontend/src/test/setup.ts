// Vitest global setup: register @testing-library/jest-dom matchers
// (toBeInTheDocument, toHaveClass, toHaveTextContent, …) and auto-clean
// the rendered DOM between tests.
import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

afterEach(() => {
  cleanup()
})
