import { FileText } from 'lucide-react'

import { usePdfFetchEnabled } from '@/hooks/usePdfSources'
import { buildReaderHref } from '@/lib/hashRoute'

interface PaperPdfLinkProps {
  paperId: string | null | undefined
  className?: string
  iconClassName?: string
}

/**
 * The one "open this paper's PDF" link, for card headers and table title
 * cells alike. Renders nothing unless a PDF source plugin is on.
 *
 * A real link, not a button: it opens the PDF handoff page in a NEW tab from
 * the tap itself (phones block tabs opened after an async wait). That page
 * opens the kept PDF at once, or finds it first.
 */
export function PaperPdfLink({ paperId, className, iconClassName = 'h-3 w-3' }: PaperPdfLinkProps) {
  const enabled = usePdfFetchEnabled()
  if (!enabled || !paperId) return null
  return (
    <a
      href={buildReaderHref(paperId)}
      target="_blank"
      rel="noopener"
      onClick={(event) => event.stopPropagation()}
      title="Open the PDF — ALMa finds it first if it has none yet"
      aria-label="Open PDF"
      className={className}
    >
      <FileText className={iconClassName} aria-hidden />
    </a>
  )
}
