import { motion, useReducedMotion } from 'framer-motion'
import { Plug, X } from 'lucide-react'
import type { ConnectorNotice } from '@/lib/connector'

const COPY: Record<ConnectorNotice['kind'], { title: string; body: string }> = {
  connector_outdated: {
    title: 'Connector update available',
    body: 'Update your ALMa connector to keep saving papers straight from your browser.',
  },
  alma_outdated: {
    title: 'ALMa is behind your connector',
    body: 'Update ALMa so it matches your browser connector.',
  },
}

export function ConnectorToastCard({
  notice,
  onClose,
}: {
  notice: ConnectorNotice
  onClose: () => void
}) {
  const reduceMotion = useReducedMotion()
  const copy = COPY[notice.kind]

  return (
    <div className="relative flex w-[360px] items-start gap-3 overflow-hidden rounded border border-[var(--color-border)] border-l-4 border-l-gold-400 bg-surface-1 p-4 shadow-paper-lg">
      <div className="relative mt-0.5 shrink-0">
        {!reduceMotion && (
          <motion.span
            aria-hidden
            className="absolute inset-0 rounded-full bg-gold-400/30"
            initial={{ scale: 1, opacity: 0.5 }}
            animate={{ scale: 1.9, opacity: 0 }}
            transition={{ duration: 0.9, ease: 'easeOut' }}
          />
        )}
        <motion.span
          className="relative flex h-9 w-9 items-center justify-center rounded-full bg-gold-400/15 text-gold-600"
          initial={reduceMotion ? false : { scale: 0.6, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ type: 'spring', stiffness: 420, damping: 22 }}
        >
          <Plug className="h-[18px] w-[18px]" strokeWidth={2} />
        </motion.span>
      </div>

      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium text-alma-900">{copy.title}</p>
          <span className="rounded border border-[var(--color-border)] bg-surface-2 px-1.5 py-0.5 font-mono text-[10px] leading-none tracking-tight text-alma-700">
            v{notice.connectorVersion}
          </span>
        </div>
        <p className="mt-1 text-xs leading-relaxed text-slate-500">{copy.body}</p>
        <button
          type="button"
          onClick={onClose}
          className="mt-2.5 rounded-sm border border-alma-200 px-2.5 py-1 text-xs font-medium text-alma-700 transition-colors hover:bg-alma-50"
        >
          Got it
        </button>
      </div>

      <button
        type="button"
        aria-label="Dismiss"
        onClick={onClose}
        className="absolute right-2 top-2 rounded-sm p-1 text-alma-400 transition-colors hover:bg-surface-2 hover:text-alma-700"
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  )
}
