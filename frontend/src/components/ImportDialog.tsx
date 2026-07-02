import { useState, useCallback, useRef } from 'react'
import {
  Upload,
  FileText,
  BookOpen,
  Loader2,
  CheckCircle,
  AlertCircle,
  X,
  FolderOpen,
  Globe,
} from 'lucide-react'

import { OnlineSearchTab } from '@/components/OnlineSearchTab'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import {
  type ImportResult,
  type ImportPreflight,
  type ImportOperationEnvelope,
  type ZoteroCollection,
  importBibtexFile,
  importBibtexText,
  importZotero,
  importZoteroRdfFile,
  isImportQueued,
  listZoteroCollections,
  preflightBibtexFile,
  preflightBibtexText,
  preflightZotero,
  preflightZoteroRdfFile,
} from '@/api/client'

type TabId = 'bibtex' | 'zotero' | 'zotero-rdf' | 'online'

interface ImportDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onImportComplete?: () => void
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function ImportDialog({ open, onOpenChange, onImportComplete }: ImportDialogProps) {
  const [activeTab, setActiveTab] = useState<TabId>('bibtex')

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Import Papers</DialogTitle>
          <DialogDescription>
            Import from BibTeX or Zotero, or search across OpenAlex and triage
            results directly into Saved Library.
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)}>
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="bibtex">
              <FileText />
              BibTeX
            </TabsTrigger>
            <TabsTrigger value="zotero">
              <BookOpen />
              Zotero
            </TabsTrigger>
            <TabsTrigger value="zotero-rdf">
              <FolderOpen />
              Zotero RDF
            </TabsTrigger>
            <TabsTrigger value="online">
              <Globe />
              Online
            </TabsTrigger>
          </TabsList>
          <TabsContent value="bibtex"><BibtexTab onImportComplete={onImportComplete} /></TabsContent>
          <TabsContent value="zotero"><ZoteroTab onImportComplete={onImportComplete} /></TabsContent>
          <TabsContent value="zotero-rdf"><ZoteroRdfTab onImportComplete={onImportComplete} /></TabsContent>
          <TabsContent value="online"><OnlineSearchTab onImportComplete={onImportComplete} /></TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  )
}

// ---------------------------------------------------------------------------
// BibTeX tab
// ---------------------------------------------------------------------------

function BibtexTab({ onImportComplete }: { onImportComplete?: () => void }) {
  const [mode, setMode] = useState<'file' | 'text'>('file')
  const [file, setFile] = useState<File | null>(null)
  const [text, setText] = useState('')
  const [loading, setLoading] = useState(false)
  const [preflightLoading, setPreflightLoading] = useState(false)
  const [preflight, setPreflight] = useState<ImportPreflight | null>(null)
  const [result, setResult] = useState<ImportResult | null>(null)
  const [queued, setQueued] = useState<ImportOperationEnvelope | null>(null)
  const [error, setError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const dropped = e.dataTransfer.files[0]
    if (dropped && (dropped.name.endsWith('.bib') || dropped.type === 'application/x-bibtex')) {
      setFile(dropped)
      setPreflight(null)
      setError(null)
    } else {
      setError('Please drop a .bib file')
    }
  }, [])

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0]
    if (selected) {
      setFile(selected)
      setPreflight(null)
      setError(null)
    }
  }

  const handleReview = async () => {
    setPreflightLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      let forecast: ImportPreflight
      if (mode === 'file' && file) {
        forecast = await preflightBibtexFile(file)
      } else if (mode === 'text' && text.trim()) {
        forecast = await preflightBibtexText(text)
      } else {
        setError(mode === 'file' ? 'Please select a .bib file' : 'Please paste BibTeX content')
        return
      }
      setPreflight(forecast)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Import review failed')
    } finally {
      setPreflightLoading(false)
    }
  }

  const handleImport = async () => {
    if (!preflight) {
      await handleReview()
      return
    }
    setLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      const res = mode === 'file' && file
        ? await importBibtexFile(file)
        : await importBibtexText(text)
      if (isImportQueued(res)) {
        setQueued(res)
        // Notify parent so it can refresh Library views when the background
        // job eventually completes (useOperationToasts also invalidates).
        onImportComplete?.()
      } else {
        setResult(res)
        if (res.imported + res.staged > 0) onImportComplete?.()
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Import failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      {/* Mode toggle */}
      <ToggleGroup
        type="single"
        value={mode}
        onValueChange={(v) => {
          if (v) {
            setMode(v as 'file' | 'text')
            setPreflight(null)
          }
        }}
        aria-label="Import source"
        className="justify-start"
      >
        <ToggleGroupItem value="file" variant="outline">Upload file</ToggleGroupItem>
        <ToggleGroupItem value="text" variant="outline">Paste text</ToggleGroupItem>
      </ToggleGroup>

      {mode === 'file' ? (
        <div
          onDrop={handleDrop}
          onDragOver={(e) => e.preventDefault()}
          onClick={() => fileInputRef.current?.click()}
          className="flex cursor-pointer flex-col items-center gap-3 rounded-lg border-2 border-dashed border-[var(--color-border)] bg-surface-2 p-8 text-center transition-colors hover:border-alma-400 hover:bg-alma-50/50"
        >
          <Upload className="h-8 w-8 text-slate-400" />
          {file ? (
            <div className="flex items-center gap-2">
              <FileText className="h-4 w-4 text-alma-600" />
              <span className="text-sm font-medium text-slate-700">{file.name}</span>
              <Button
                size="icon-sm"
                variant="ghost"
                onClick={(e) => {
                  e.stopPropagation()
                  setFile(null)
                  setPreflight(null)
                }}
                aria-label="Clear selected file"
              >
                <X className="size-3.5 text-slate-400" />
              </Button>
            </div>
          ) : (
            <>
              <p className="text-sm font-medium text-slate-600">
                Drop a .bib file here or click to browse
              </p>
              <p className="text-xs text-slate-400">Supports standard BibTeX format</p>
            </>
          )}
          <input
            ref={fileInputRef}
            type="file"
            accept=".bib,application/x-bibtex"
            className="hidden"
            onChange={handleFileSelect}
          />
        </div>
      ) : (
        <textarea
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            setPreflight(null)
          }}
          placeholder={`@article{doe2024,\n  title = {My Paper Title},\n  author = {Doe, John and Smith, Jane},\n  year = {2024},\n  journal = {Nature},\n}`}
          className="h-48 w-full resize-y rounded-sm border border-[var(--color-border)] bg-surface-1 p-3 font-mono text-sm text-alma-800 shadow-paper-inset-cool placeholder:text-slate-400 focus:border-transparent focus:outline-none focus:ring-2 focus:ring-alma-500"
        />
      )}

      {preflight && <ImportPreflightDisplay preflight={preflight} />}

      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 rounded-lg border border-critical-100 bg-critical-50 px-4 py-3">
          <AlertCircle className="h-4 w-4 text-critical-500" />
          <span className="text-sm text-critical-700">{error}</span>
        </div>
      )}

      {/* Result */}
      {queued && <ImportQueuedDisplay envelope={queued} />}
      {result && <ImportResultDisplay result={result} />}

      {/* Actions */}
      <DialogFooter>
        <Button onClick={handleImport} disabled={loading || preflightLoading}>
          {loading || preflightLoading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              {preflightLoading ? 'Reviewing...' : 'Importing...'}
            </>
          ) : !preflight ? (
            'Review import'
          ) : (
            <>
              <Upload className="mr-2 h-4 w-4" />
              Import
            </>
          )}
        </Button>
      </DialogFooter>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Zotero tab
// ---------------------------------------------------------------------------

function ZoteroTab({ onImportComplete }: { onImportComplete?: () => void }) {
  const [libraryId, setLibraryId] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [libraryType, setLibraryType] = useState<'user' | 'group'>('user')
  const [collections, setCollections] = useState<ZoteroCollection[] | null>(null)
  const [selectedCollectionKey, setSelectedCollectionKey] = useState<string | null>(null)
  const [connecting, setConnecting] = useState(false)
  const [loading, setLoading] = useState(false)
  const [preflightLoading, setPreflightLoading] = useState(false)
  const [preflight, setPreflight] = useState<ImportPreflight | null>(null)
  const [result, setResult] = useState<ImportResult | null>(null)
  const [queued, setQueued] = useState<ImportOperationEnvelope | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleConnect = async () => {
    if (!libraryId.trim() || !apiKey.trim()) {
      setError('Library ID and API key are required')
      return
    }
    setConnecting(true)
    setError(null)
    setCollections(null)
    try {
      const colls = await listZoteroCollections({
        library_id: libraryId.trim(),
        api_key: apiKey.trim(),
        library_type: libraryType,
      })
      setCollections(colls)
      setPreflight(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to connect to Zotero')
    } finally {
      setConnecting(false)
    }
  }

  const handleReview = async () => {
    setPreflightLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      setPreflight(await preflightZotero({
        library_id: libraryId.trim(),
        api_key: apiKey.trim(),
        library_type: libraryType,
        collection_key: selectedCollectionKey,
      }))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Import review failed')
    } finally {
      setPreflightLoading(false)
    }
  }

  const handleImport = async () => {
    if (!preflight) {
      await handleReview()
      return
    }
    setLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      const res = await importZotero({
        library_id: libraryId.trim(),
        api_key: apiKey.trim(),
        library_type: libraryType,
        collection_key: selectedCollectionKey,
      })
      if (isImportQueued(res)) {
        setQueued(res)
        onImportComplete?.()
      } else {
        setResult(res)
        if (res.imported + res.staged > 0) onImportComplete?.()
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Zotero import failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      {/* Connection form */}
      <div className="space-y-3">
        <div>
          <label className="mb-1 block text-sm font-medium text-slate-700">Library ID</label>
          <Input
            value={libraryId}
            onChange={(e) => {
              setLibraryId(e.target.value)
              setPreflight(null)
            }}
            placeholder="e.g. 123456"
          />
          <p className="mt-1 text-xs text-slate-400">
            Find this in Zotero Settings &rarr; Feeds/API.
          </p>
        </div>

        <div>
          <label className="mb-1 block text-sm font-medium text-slate-700">API Key</label>
          <Input
            type="password"
            value={apiKey}
            onChange={(e) => {
              setApiKey(e.target.value)
              setPreflight(null)
            }}
            placeholder="Enter your Zotero API key"
          />
          <p className="mt-1 text-xs text-slate-400">
            Generate one at{' '}
            <a
              href="https://www.zotero.org/settings/keys/new"
              target="_blank"
              rel="noopener noreferrer"
              className="text-alma-600 hover:underline"
            >
              zotero.org/settings/keys
            </a>
          </p>
        </div>

        <div>
          <label className="mb-1 block text-sm font-medium text-slate-700">Library Type</label>
          <ToggleGroup
            type="single"
            value={libraryType}
            onValueChange={(v) => {
              if (v) {
                setLibraryType(v as 'user' | 'group')
                setPreflight(null)
              }
            }}
            aria-label="Library type"
            className="justify-start"
          >
            <ToggleGroupItem value="user" variant="outline" className="capitalize">user</ToggleGroupItem>
            <ToggleGroupItem value="group" variant="outline" className="capitalize">group</ToggleGroupItem>
          </ToggleGroup>
        </div>

        <Button onClick={handleConnect} disabled={connecting} variant="outline">
          {connecting ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Connecting...
            </>
          ) : (
            <>
              <BookOpen className="mr-2 h-4 w-4" />
              Connect
            </>
          )}
        </Button>
      </div>

      {/* Collection picker */}
      {collections !== null && (
        <div>
          <label className="mb-1 block text-sm font-medium text-slate-700">
            Zotero Collection (optional)
          </label>
          <div className="max-h-48 overflow-y-auto rounded-sm border border-[var(--color-border)]">
            <button
              onClick={() => {
                setSelectedCollectionKey(null)
                setPreflight(null)
              }}
              className={`flex w-full items-center gap-2 px-3 py-2 text-left text-sm transition-colors ${
                selectedCollectionKey === null
                  ? 'bg-alma-50 text-alma-700'
                  : 'text-slate-600 hover:bg-surface-2'
              }`}
            >
              <FolderOpen className="h-4 w-4" />
              All items
            </button>
            {collections.map((c) => (
              <button
                key={c.key}
                onClick={() => {
                  setSelectedCollectionKey(c.key)
                  setPreflight(null)
                }}
                className={`flex w-full items-center justify-between px-3 py-2 text-left text-sm transition-colors ${
                  selectedCollectionKey === c.key
                    ? 'bg-alma-50 text-alma-700'
                    : 'text-slate-600 hover:bg-surface-2'
                }`}
              >
                <span className="flex items-center gap-2">
                  <FolderOpen className="h-4 w-4" />
                  {c.name}
                </span>
                <Badge variant="secondary">{c.num_items}</Badge>
              </button>
            ))}
            {collections.length === 0 && (
              <p className="px-3 py-2 text-sm text-slate-400">No collections found</p>
            )}
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 rounded-lg border border-critical-100 bg-critical-50 px-4 py-3">
          <AlertCircle className="h-4 w-4 text-critical-500" />
          <span className="text-sm text-critical-700">{error}</span>
        </div>
      )}

      {/* Result */}
      {preflight && <ImportPreflightDisplay preflight={preflight} />}
      {queued && <ImportQueuedDisplay envelope={queued} />}
      {result && <ImportResultDisplay result={result} />}

      {/* Import button */}
      {collections !== null && (
        <DialogFooter>
          <Button onClick={handleImport} disabled={loading || preflightLoading}>
            {loading || preflightLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                {preflightLoading ? 'Reviewing...' : 'Importing...'}
              </>
            ) : !preflight ? (
              'Review import'
            ) : (
              <>
                <Upload className="mr-2 h-4 w-4" />
                Import from Zotero
              </>
            )}
          </Button>
        </DialogFooter>
      )}
    </div>
  )
}

function ZoteroRdfTab({ onImportComplete }: { onImportComplete?: () => void }) {
  const [file, setFile] = useState<File | null>(null)
  const [loading, setLoading] = useState(false)
  const [preflightLoading, setPreflightLoading] = useState(false)
  const [preflight, setPreflight] = useState<ImportPreflight | null>(null)
  const [result, setResult] = useState<ImportResult | null>(null)
  const [queued, setQueued] = useState<ImportOperationEnvelope | null>(null)
  const [error, setError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const dropped = e.dataTransfer.files[0]
    if (dropped && dropped.name.toLowerCase().endsWith('.rdf')) {
      setFile(dropped)
      setPreflight(null)
      setError(null)
    } else {
      setError('Please drop a .rdf file exported from Zotero')
    }
  }, [])

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0]
    if (selected) {
      setFile(selected)
      setPreflight(null)
      setError(null)
    }
  }

  const handleReview = async () => {
    if (!file) {
      setError('Please select a .rdf file')
      return
    }
    setPreflightLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      setPreflight(await preflightZoteroRdfFile(file))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Import review failed')
    } finally {
      setPreflightLoading(false)
    }
  }

  const handleImport = async () => {
    if (!preflight) {
      await handleReview()
      return
    }
    setLoading(true)
    setResult(null)
    setQueued(null)
    setError(null)
    try {
      const res = await importZoteroRdfFile(file as File)
      if (isImportQueued(res)) {
        setQueued(res)
        onImportComplete?.()
      } else {
        setResult(res)
        if (res.imported + res.staged > 0) onImportComplete?.()
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Zotero RDF import failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        onClick={() => fileInputRef.current?.click()}
        className="flex cursor-pointer flex-col items-center gap-3 rounded-lg border-2 border-dashed border-[var(--color-border)] bg-surface-2 p-8 text-center transition-colors hover:border-alma-400 hover:bg-alma-50/50"
      >
        <Upload className="h-8 w-8 text-slate-400" />
        {file ? (
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-alma-600" />
            <span className="text-sm font-medium text-slate-700">{file.name}</span>
            <button
              onClick={(e) => {
                e.stopPropagation()
                setFile(null)
                setPreflight(null)
              }}
              className="rounded p-0.5 hover:bg-slate-200"
            >
              <X className="h-3.5 w-3.5 text-slate-400" />
            </button>
          </div>
        ) : (
          <>
            <p className="text-sm font-medium text-slate-600">
              Drop a Zotero RDF export here or click to browse
            </p>
            <p className="text-xs text-slate-400">File extension: .rdf</p>
          </>
        )}
        <input
          ref={fileInputRef}
          type="file"
          accept=".rdf,application/rdf+xml,text/xml,application/xml"
          className="hidden"
          onChange={handleFileSelect}
        />
      </div>

      {preflight && <ImportPreflightDisplay preflight={preflight} />}

      {error && (
        <div className="flex items-center gap-2 rounded-lg border border-critical-100 bg-critical-50 px-4 py-3">
          <AlertCircle className="h-4 w-4 text-critical-500" />
          <span className="text-sm text-critical-700">{error}</span>
        </div>
      )}

      {queued && <ImportQueuedDisplay envelope={queued} />}
      {result && <ImportResultDisplay result={result} />}

      <DialogFooter>
        <Button onClick={handleImport} disabled={loading || preflightLoading}>
          {loading || preflightLoading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              {preflightLoading ? 'Reviewing...' : 'Importing...'}
            </>
          ) : !preflight ? (
            'Review import'
          ) : (
            <>
              <Upload className="mr-2 h-4 w-4" />
              Import Zotero RDF
            </>
          )}
        </Button>
      </DialogFooter>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Import result display
// ---------------------------------------------------------------------------

function ImportPreflightDisplay({ preflight }: { preflight: ImportPreflight }) {
  const etaLabels = [
    preflight.eta.openalex?.label && `OpenAlex ${preflight.eta.openalex.label}`,
    preflight.eta.title_resolution?.label && `title search ${preflight.eta.title_resolution.label}`,
    preflight.eta.s2_vector?.label && `S2 vectors ${preflight.eta.s2_vector.label}`,
  ].filter(Boolean)
  const sourceCalls = preflight.likely_source_calls

  return (
    <div className="rounded-sm border border-alma-200 bg-alma-50 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <CheckCircle className="h-5 w-5 text-alma-700" />
          <span className="font-medium text-alma-900">Import review ready</span>
        </div>
        <Badge variant="secondary">{preflight.valid_entries} entries</Badge>
      </div>
      <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2">
        <div>DOI-backed: <span className="font-medium">{preflight.identifiers.doi}</span></div>
        <div>Title search: <span className="font-medium">{preflight.identifiers.title_search_needed}</span></div>
        <div>Likely new rows: <span className="font-medium">{preflight.dedup.likely_new_rows}</span></div>
        <div>Existing matches: <span className="font-medium">{preflight.dedup.existing_matches}</span></div>
        <div>With abstracts: <span className="font-medium">{preflight.metadata.with_abstract}</span></div>
        <div>Rich metadata: <span className="font-medium">{preflight.metadata.rich_enough_to_skip_most_hydration}</span></div>
      </div>
      <p className="mt-3 text-xs text-slate-600">
        Forecast: {sourceCalls.openalex} OpenAlex request{sourceCalls.openalex === 1 ? '' : 's'},
        {' '}{sourceCalls.semantic_scholar_title_search} title-search request{sourceCalls.semantic_scholar_title_search === 1 ? '' : 's'},
        {' '}{sourceCalls.semantic_scholar_vector_batch_candidates} S2 vector candidate{sourceCalls.semantic_scholar_vector_batch_candidates === 1 ? '' : 's'}.
        {etaLabels.length ? ` ${etaLabels.join(' · ')}.` : ''}
      </p>
      {preflight.parse_errors > 0 && (
        <p className="mt-2 text-xs text-critical-700">
          {preflight.parse_errors} malformed entr{preflight.parse_errors === 1 ? 'y was' : 'ies were'} skipped in this forecast.
        </p>
      )}
    </div>
  )
}

function ImportQueuedDisplay({ envelope }: { envelope: ImportOperationEnvelope }) {
  const alreadyRunning = envelope.status === 'already_running'
  return (
    <div className="rounded-lg border border-alma-200 bg-alma-50 p-4">
      <div className="mb-2 flex items-center gap-2">
        <Loader2 className="h-5 w-5 animate-spin text-alma-600" />
        <span className="font-medium text-alma-800">
          {alreadyRunning ? 'Import already running' : 'Import queued'}
        </span>
      </div>
      <p className="text-sm text-slate-600">
        {envelope.message ||
          'Your import is running in the background. You can keep using the app — Library will refresh automatically when it finishes.'}
      </p>
      <p className="mt-2 text-xs text-slate-500">
        Track progress in the{' '}
        <a href="/activity" className="font-medium text-alma-700 hover:underline">
          Activity panel
        </a>
        .
      </p>
    </div>
  )
}

function ImportResultDisplay({ result }: { result: ImportResult }) {
  return (
    <div className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-4">
      <div className="mb-2 flex items-center gap-2">
        <CheckCircle className="h-5 w-5 text-success-600" />
        <span className="font-medium text-alma-800">Import Complete</span>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
        <StatBox label="Total" value={result.total} />
        <StatBox label="Imported" value={result.imported} color="green" />
        <StatBox label="Staged" value={result.staged} color="yellow" />
        <StatBox label="Skipped" value={result.skipped} />
        <StatBox label="Failed" value={result.failed} color="red" />
      </div>
      {result.errors.length > 0 && (
        <div className="mt-3">
          <p className="mb-1 text-xs font-medium text-critical-700">Errors:</p>
          <ul className="max-h-32 space-y-0.5 overflow-y-auto text-xs text-critical-600">
            {result.errors.slice(0, 20).map((err, i) => (
              <li key={i} className="flex items-start gap-1">
                <AlertCircle className="mt-0.5 h-3 w-3 shrink-0" />
                {err}
              </li>
            ))}
            {result.errors.length > 20 && (
              <li className="text-slate-500">...and {result.errors.length - 20} more</li>
            )}
          </ul>
        </div>
      )}
    </div>
  )
}

function StatBox({
  label,
  value,
  color,
}: {
  label: string
  value: number
  color?: 'green' | 'yellow' | 'red'
}) {
  const colorMap = {
    green: 'text-success-700',
    yellow: 'text-warning-700',
    red: 'text-critical-700',
  }
  return (
    <div className="rounded-md bg-surface-1 p-2 text-center shadow-sm">
      <div className={`text-lg font-bold ${color ? colorMap[color] : 'text-alma-800'}`}>
        {value}
      </div>
      <div className="text-xs text-slate-500">{label}</div>
    </div>
  )
}
