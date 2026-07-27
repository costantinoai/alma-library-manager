import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { ErrorState } from './ErrorState'
import { NotFoundState } from './NotFoundState'

describe('research states', () => {
  it('renders a recoverable embedded error through the shared paper primitive', () => {
    const retry = vi.fn()
    render(
      <ErrorState
        message="The request failed."
        actionLabel="Try again"
        onAction={retry}
      />,
    )

    expect(screen.getByRole('alert')).toHaveTextContent('Load note · unavailable')
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(retry).toHaveBeenCalledOnce()
  })

  it('renders a page-level 404 without pretending it is Home', () => {
    render(
      <NotFoundState
        scope="page"
        message="The address is not part of this catalogue."
      />,
    )

    expect(screen.getByRole('status')).toHaveTextContent('404 · record not found')
    expect(screen.getByRole('heading')).toHaveTextContent(
      'That page is not in this catalogue',
    )
  })
})
