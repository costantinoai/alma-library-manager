import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBadge, monitorHealthTone, severityTone } from './status-badge'

describe('StatusBadge', () => {
  it('renders its children', () => {
    render(<StatusBadge>Ready</StatusBadge>)
    expect(screen.getByText('Ready')).toBeInTheDocument()
  })

  it('applies the neutral tone classes by default', () => {
    render(<StatusBadge>Idle</StatusBadge>)
    // Neutral = white chip with an alma border (see status-badge.tsx).
    expect(screen.getByText('Idle')).toHaveClass('bg-white')
  })

  it('switches visual treatment with the tone prop', () => {
    render(<StatusBadge tone="positive">Healthy</StatusBadge>)
    expect(screen.getByText('Healthy')).toHaveClass('text-emerald-700')
  })

  it('merges a caller className', () => {
    render(<StatusBadge className="ml-2">X</StatusBadge>)
    expect(screen.getByText('X')).toHaveClass('ml-2')
  })
})

describe('tone mapping helpers', () => {
  it('monitorHealthTone maps health → tone', () => {
    expect(monitorHealthTone('ready')).toBe('positive')
    expect(monitorHealthTone('disabled')).toBe('neutral')
    expect(monitorHealthTone('degraded')).toBe('warning')
    expect(monitorHealthTone(undefined)).toBe('warning')
  })

  it('severityTone returns a valid StatusBadge tone', () => {
    const tone = severityTone('critical')
    expect(['neutral', 'positive', 'negative', 'warning', 'info', 'accent']).toContain(tone)
  })
})
