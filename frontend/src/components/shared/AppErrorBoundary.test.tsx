import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { AppErrorBoundary } from './AppErrorBoundary'

function Broken(): never {
  throw new Error('the bundle disagrees with the API')
}

describe('AppErrorBoundary', () => {
  it('shows the error and a reload instead of a blank page', () => {
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {})
    render(
      <AppErrorBoundary>
        <Broken />
      </AppErrorBoundary>,
    )
    expect(screen.getByText('the bundle disagrees with the API')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Reload' })).toBeTruthy()
    quiet.mockRestore()
  })
})
