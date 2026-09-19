import { useRef, type DragEvent } from 'react'
import { FileText, Upload, X } from 'lucide-react'

import { Button } from '@/components/ui/button'

interface FileDropZoneProps {
  /** The file input's `accept` (the browser's picker filter). */
  accept: string
  /** Pick / drop several files at once (the PDF import); one otherwise. */
  multiple?: boolean
  /** Whether a dropped or picked file is one this zone takes. */
  isAccepted: (file: File) => boolean
  /** The accepted files, in the order given. */
  onFiles: (files: File[]) => void
  /** Called with `rejectMessage` when nothing dropped was acceptable. */
  onRejected?: (message: string) => void
  rejectMessage: string
  /** The main line ("Drop a .bib file here or click to browse"). */
  prompt: string
  /** A quieter second line (format note). */
  hint?: string
  /** Single-file mode: the chosen file, shown with a clear button. */
  file?: File | null
  onClear?: () => void
}

/**
 * The one "drop a file here or click to browse" zone of the Import dialog.
 *
 * The BibTeX and Zotero RDF tabs each carried a copy and they had already
 * drifted (one cleared with a Button, the other with a bare `<button>`); the
 * PDF tab would have been a third. A control, so it fills from the ink well,
 * never from the paper ladder.
 */
export function FileDropZone({
  accept,
  multiple = false,
  isAccepted,
  onFiles,
  onRejected,
  rejectMessage,
  prompt,
  hint,
  file,
  onClear,
}: FileDropZoneProps) {
  const inputRef = useRef<HTMLInputElement>(null)

  const take = (list: FileList | null | undefined) => {
    const all = Array.from(list ?? [])
    const accepted = all.filter(isAccepted)
    if (accepted.length > 0) onFiles(multiple ? accepted : accepted.slice(0, 1))
    else if (all.length > 0) onRejected?.(rejectMessage)
  }

  const onDrop = (event: DragEvent) => {
    event.preventDefault()
    take(event.dataTransfer.files)
  }

  return (
    <div
      onDrop={onDrop}
      onDragOver={(event) => event.preventDefault()}
      onClick={() => inputRef.current?.click()}
      className="flex cursor-pointer flex-col items-center gap-3 rounded-lg border-2 border-dashed border-control-edge bg-control-well p-8 text-center transition-colors hover:border-control-edge-strong hover:bg-control-quiet"
    >
      <Upload className="h-8 w-8 text-slate-400" />
      {file ? (
        <div className="flex items-center gap-2">
          <FileText className="h-4 w-4 text-alma-600" />
          <span className="text-sm font-medium text-slate-700">{file.name}</span>
          {onClear && (
            <Button
              size="icon-sm"
              variant="ghost"
              onClick={(event) => {
                event.stopPropagation()
                onClear()
              }}
              aria-label="Clear selected file"
            >
              <X className="size-3.5 text-slate-400" />
            </Button>
          )}
        </div>
      ) : (
        <>
          <p className="text-sm font-medium text-slate-600">{prompt}</p>
          {hint && <p className="text-xs text-slate-400">{hint}</p>}
        </>
      )}
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        multiple={multiple}
        className="hidden"
        onChange={(event) => {
          take(event.target.files)
          event.target.value = '' // picking the same file again still fires
        }}
      />
    </div>
  )
}
