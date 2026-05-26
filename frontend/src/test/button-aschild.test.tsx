import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'

import { Button } from '@/components/ui/button'

/**
 * Regression: `<Button asChild>` renders through Radix `Slot`, which calls
 * `React.Children.only`. The primitive used to emit a falsy spinner sibling
 * next to `{children}` even when `asChild` was set, so Slot received
 * `[false, child]` (an array) and threw "expected to receive a single React
 * element child" — crashing every surface with a `<Button asChild>` link
 * (e.g. the author detail panel's Scholar / OpenAlex buttons).
 */
describe('Button asChild', () => {
  it('hands Slot a single child (link with nested icon) without crashing', () => {
    render(
      <Button asChild variant="outline" size="xs">
        <a href="https://example.com">
          Open <span data-testid="icon">↗</span>
        </a>
      </Button>,
    )
    const link = screen.getByRole('link')
    expect(link).toHaveTextContent('Open')
    expect(screen.getByTestId('icon')).toBeInTheDocument()
  })

  it('still composes a spinner on the real button when loading', () => {
    render(
      <Button loading>
        Saving
      </Button>,
    )
    expect(screen.getByRole('button')).toHaveTextContent('Saving')
  })
})
