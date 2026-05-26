import * as React from 'react'
import { toast as sonnerToast } from 'sonner'

type ToastVariant = 'default' | 'destructive'

interface ToastAction {
  /** Button copy, e.g. "Undo". */
  label: string
  /** Fired when the action button is clicked. Sonner auto-dismisses after. */
  onClick: () => void
}

interface ToastInput {
  title?: React.ReactNode
  description?: React.ReactNode
  variant?: ToastVariant
  /** Optional inline action button (e.g. an "Undo" affordance). */
  action?: ToastAction
}

interface ToastHandle {
  id: string | number
  dismiss: () => void
  update: (updateProps: Partial<ToastInput>) => void
}

function emit(input: ToastInput, id?: string | number): string | number {
  const options: Parameters<typeof sonnerToast.success>[1] = {
    description: input.description,
    ...(id !== undefined ? { id } : {}),
    ...(input.action ? { action: input.action } : {}),
  }
  return input.variant === 'destructive'
    ? sonnerToast.error(input.title as React.ReactNode, options)
    : sonnerToast.success(input.title as React.ReactNode, options)
}

function toast(input: ToastInput): ToastHandle {
  const id = emit(input)
  return {
    id,
    dismiss: () => sonnerToast.dismiss(id),
    update: (updateProps) => {
      emit({ ...input, ...updateProps }, id)
    },
  }
}

function useToast() {
  return {
    toast,
    dismiss: (id?: string | number) => sonnerToast.dismiss(id),
  }
}

/**
 * Shorthand for the destructive-variant toast used in mutation `onError`
 * handlers. Collapses the ~110 copies of
 * `toast({ title, description, variant: 'destructive' })` across the app.
 */
function errorToast(title: React.ReactNode, description?: React.ReactNode): ToastHandle {
  return toast({ title, description, variant: 'destructive' })
}

export { useToast, toast, errorToast }
