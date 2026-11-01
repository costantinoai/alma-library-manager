import { FileCheck2, FileSearch } from 'lucide-react'

import { useHasPdf } from '@/hooks/usePaperPdf'
import { buildReaderHref } from '@/lib/hashRoute'
import { cn } from '@/lib/utils'

interface PaperPdfLinkProps {
  paperId: string | null | undefined
  className?: string
  iconClassName?: string
  /** Added when the file is here, so a host can give "have" its own weight. */
  heldClassName?: string
}

/**
 * The one "open this paper's PDF" link, for card headers and table title
 * cells alike. Local files remain accessible with every fetch plugin off.
 *
 * Two states, told apart by glyph as well as colour: a file with a tick, in
 * folio, means the PDF is here; a file with a lens, in quiet ink, means ALMa
 * would have to look for one.
 *
 * A real link, not a button: it opens the PDF handoff page in a NEW tab from
 * the tap itself (phones block tabs opened after an async wait). That page
 * opens the kept PDF at once, or finds it first.
 */
export function PaperPdfLink({
  paperId,
  className,
  iconClassName = 'h-3 w-3',
  heldClassName,
}: PaperPdfLinkProps) {
  const held = useHasPdf(paperId)
  if (!paperId) return null
  const Icon = held ? FileCheck2 : FileSearch
  const label = held ? 'PDF on file, open it' : 'Find a PDF for this paper'
  return (
    <a
      href={buildReaderHref(paperId)}
      target="_blank"
      rel="noopener"
      onClick={(event) => event.stopPropagation()}
      title={label}
      aria-label={label}
      data-pdf={held ? 'held' : 'missing'}
      className={cn(className, held && (heldClassName ?? 'text-alma-folio'))}
    >
      <Icon className={iconClassName} aria-hidden />
    </a>
  )
}
