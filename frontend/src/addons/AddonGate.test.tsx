import { lazy, Suspense } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { AddonGate } from './AddonGate'
import type { AddonUi } from './registry'

let enabled = true

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => ({
    isLoading: false,
    data: { addons: [{ id: 'fake', enabled, badge: 0 }] },
  }),
}))

function fakeAddon(): AddonUi {
  const load = async () => ({ default: () => <p>fake page</p> })
  return {
    id: 'fake',
    label: 'Fake',
    navGroup: 'Manage',
    icon: () => null,
    load,
    page: lazy(load),
  }
}

describe('AddonGate', () => {
  it('loads the page without suspending to the shell', async () => {
    // A shell fallback here would hide the page motion and leave it blank.
    enabled = true
    render(
      <Suspense fallback={<p>shell fallback</p>}>
        <AddonGate addon={fakeAddon()} />
      </Suspense>,
    )
    expect(screen.queryByText('shell fallback')).not.toBeInTheDocument()
    expect(await screen.findByText('fake page')).toBeInTheDocument()
  })

  it('explains itself while the add-on is off and never loads the page', () => {
    enabled = false
    const addon = fakeAddon()
    render(<AddonGate addon={addon} />)
    expect(screen.getByText('Fake is switched off')).toBeInTheDocument()
    expect(screen.queryByText('fake page')).not.toBeInTheDocument()
  })
})
