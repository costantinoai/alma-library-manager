import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { lazy } from 'react'

import { AddonSlot } from './AddonSlot'

const state = vi.hoisted(() => ({ enabled: true }))

vi.mock('@/hooks/useBootstrap', () => ({
  useBootstrap: () => ({ data: { addons: [{ id: 'fake', enabled: state.enabled, badge: 0 }] } }),
}))

vi.mock('./registry', () => ({
  slotEntries: () => [
    { addonId: 'fake', Component: lazy(async () => ({ default: () => <p>fake part</p> })) },
  ],
}))

describe('AddonSlot', () => {
  beforeEach(() => {
    state.enabled = true
  })

  it('renders a part of an add-on that is on, suspending by itself', async () => {
    render(<AddonSlot name="tags.tune" />)
    expect(await screen.findByText('fake part')).toBeInTheDocument()
  })

  it('renders nothing for an add-on that is off', () => {
    state.enabled = false
    const { container } = render(<AddonSlot name="tags.tune" />)
    expect(container).toBeEmptyDOMElement()
  })
})
