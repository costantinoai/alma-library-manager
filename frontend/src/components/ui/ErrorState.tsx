import { AlertCircle } from 'lucide-react'

interface ErrorStateProps {
  message: string
}

export function ErrorState({ message }: ErrorStateProps) {
  return (
    <div className="flex items-center justify-center gap-2 rounded border border-critical-200 bg-critical-50 px-4 py-8">
      <AlertCircle className="h-5 w-5 text-critical-500" />
      <span className="text-sm text-critical-700">{message}</span>
    </div>
  )
}
