import { fireEvent, render, screen } from '@testing-library/react'
import { beforeAll, describe, expect, it, vi } from 'vitest'

import { SemanticMap } from './SemanticMap'

const drawContext = {
  arc: vi.fn(),
  beginPath: vi.fn(),
  drawImage: vi.fn(),
  fill: vi.fn(),
  fillRect: vi.fn(),
  fillText: vi.fn(),
  lineTo: vi.fn(),
  moveTo: vi.fn(),
  putImageData: vi.fn(),
  scale: vi.fn(),
  setLineDash: vi.fn(),
  stroke: vi.fn(),
  strokeRect: vi.fn(),
  strokeText: vi.fn(),
}

beforeAll(() => {
  vi.stubGlobal(
    'ResizeObserver',
    class ResizeObserver {
      observe() {}
      disconnect() {}
    },
  )
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(
    () => drawContext as unknown as CanvasRenderingContext2D,
  )
  Object.defineProperty(HTMLCanvasElement.prototype, 'setPointerCapture', {
    configurable: true,
    value: vi.fn(),
  })
})

describe('SemanticMap click card', () => {
  it('anchors a node card and dismisses it with Escape or a background click', () => {
    const onClickNode = vi.fn()
    render(
      <SemanticMap
        nodes={[{ id: 'paper-1', x: 0.5, y: 0.5, kind: 'library' }]}
        height={200}
        showToponyms={false}
        onClickNode={onClickNode}
        renderClick={(id, close) => (
          <div role="dialog">
            {id}
            <button type="button" onClick={close}>Close</button>
          </div>
        )}
      />,
    )

    const canvas = screen.getByRole('img')
    vi.spyOn(canvas, 'getBoundingClientRect').mockReturnValue({
      x: 0,
      y: 0,
      left: 0,
      top: 0,
      right: 800,
      bottom: 200,
      width: 800,
      height: 200,
      toJSON: () => ({}),
    })

    fireEvent.pointerDown(canvas, { pointerId: 1, clientX: 400, clientY: 100 })
    fireEvent.pointerUp(canvas, { pointerId: 1, clientX: 400, clientY: 100 })
    expect(screen.getByRole('dialog')).toHaveTextContent('paper-1')
    expect(onClickNode).toHaveBeenLastCalledWith('paper-1')

    fireEvent.keyDown(window, { key: 'Escape' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()

    fireEvent.pointerDown(canvas, { pointerId: 1, clientX: 400, clientY: 100 })
    fireEvent.pointerUp(canvas, { pointerId: 1, clientX: 400, clientY: 100 })
    expect(screen.getByRole('dialog')).toBeInTheDocument()

    fireEvent.pointerDown(canvas, { pointerId: 1, clientX: 20, clientY: 20 })
    fireEvent.pointerUp(canvas, { pointerId: 1, clientX: 20, clientY: 20 })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(onClickNode).toHaveBeenLastCalledWith(null)
  })
})
