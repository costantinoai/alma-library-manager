import type { PaperPdfAttempt } from '@/api/client'
import { attemptPlaces, describeAttempt } from '@/lib/pdf'

/**
 * One source's latest answer, as a list item — and, when it tried several
 * places (mirrors, listed locations), what each one said, indented under it.
 * The detail panel and the reader page both list attempts through this.
 */
export function PdfAttemptItem({ attempt }: { attempt: PaperPdfAttempt }) {
  const places = attemptPlaces(attempt)
  return (
    <li>
      {describeAttempt(attempt)}
      {places.length > 0 && (
        <ul className="mt-0.5 space-y-0.5 pl-3 text-[11px] text-slate-500">
          {places.map((place) => (
            <li key={place} className="break-words">{place}</li>
          ))}
        </ul>
      )}
    </li>
  )
}
