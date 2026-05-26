import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { GitMerge, Loader2, Search } from 'lucide-react'

import {
  api,
  mergeAuthorProfiles,
  type Author,
  type AuthorMergeFieldChoice,
} from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Input } from '@/components/ui/input'
import { LoadingState } from '@/components/ui/LoadingState'
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group'
import { StatusBadge } from '@/components/ui/status-badge'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

type MergeFieldKey =
  | 'name'
  | 'affiliation'
  | 'email_domain'
  | 'orcid'
  | 'scholar_id'
  | 'url_picture'
  | 'citedby'
  | 'h_index'
  | 'works_count'

interface MergeMetadataField {
  key: MergeFieldKey
  label: string
}

interface MergeDiscrepancy {
  key: MergeFieldKey
  label: string
  primaryValue: string
  altValue: string
  defaultChoice: AuthorMergeFieldChoice
}

interface AuthorMergeDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  /**
   * Normal mode: the FIXED survivor. The user picks the duplicate to absorb
   * into it. (The opened author keeps its id / follow / ratings.)
   */
  primaryAuthor?: Author | null
  /**
   * Absorb mode: the FIXED author to be ABSORBED (soft-removed). The user picks
   * the SURVIVOR from the corpus / library. Use when merging a suggestion into
   * an author you already curate — your existing author survives, the suggestion
   * folds in. Mutually exclusive with `primaryAuthor`.
   */
  absorbAuthor?: Author | null
  allowedTargetIds?: string[]
  initialTargetId?: string | null
  title?: string
  description?: ReactNode
  emptyCandidateMessage?: string
  contextSlot?: ReactNode
  onMerged?: () => void
}

const MERGE_METADATA_FIELDS: MergeMetadataField[] = [
  { key: 'name', label: 'Name' },
  { key: 'affiliation', label: 'Affiliation' },
  { key: 'email_domain', label: 'Email domain' },
  { key: 'orcid', label: 'ORCID' },
  { key: 'scholar_id', label: 'Google Scholar ID' },
  { key: 'url_picture', label: 'Profile image' },
  { key: 'citedby', label: 'Citation count' },
  { key: 'h_index', label: 'h-index' },
  { key: 'works_count', label: 'Works count' },
]

function metadataValue(author: Author | null | undefined, key: MergeFieldKey): string {
  const raw = author?.[key]
  if (raw === null || raw === undefined || raw === '') return ''
  if (typeof raw === 'number') return String(raw)
  return String(raw).trim()
}

function displayMetadataValue(value: string): string {
  return value || 'Not recorded'
}

// Discrepancies are always computed survivor(primary) vs absorbed(alt); the
// merge contract keeps `primary` values unless a choice says `alt`.
function mergeDiscrepancies(primary: Author, alt: Author): MergeDiscrepancy[] {
  return MERGE_METADATA_FIELDS.flatMap((field) => {
    const primaryValue = metadataValue(primary, field.key)
    const altValue = metadataValue(alt, field.key)
    if (primaryValue.toLowerCase() === altValue.toLowerCase()) return []
    return [
      {
        ...field,
        primaryValue,
        altValue,
        defaultChoice: primaryValue ? 'primary' : 'alt',
      },
    ]
  })
}

function defaultMergeChoices(primary: Author, alt: Author): Record<string, AuthorMergeFieldChoice> {
  const choices: Record<string, AuthorMergeFieldChoice> = {}
  for (const discrepancy of mergeDiscrepancies(primary, alt)) {
    choices[discrepancy.key] = discrepancy.defaultChoice
  }
  return choices
}

export function AuthorMergeDialog({
  open,
  onOpenChange,
  primaryAuthor,
  absorbAuthor,
  allowedTargetIds,
  initialTargetId,
  title,
  description,
  emptyCandidateMessage = 'No matching corpus authors.',
  contextSlot,
  onMerged,
}: AuthorMergeDialogProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [mergeSearch, setMergeSearch] = useState('')
  const [mergeTargetId, setMergeTargetId] = useState<string | null>(null)
  const [mergeChoices, setMergeChoices] = useState<Record<string, AuthorMergeFieldChoice>>({})

  const authorsQuery = useQuery({
    queryKey: ['authors'],
    queryFn: () => api.get<Author[]>('/authors'),
    enabled: open,
    retry: 1,
  })

  const allowedIds = useMemo(
    () => new Set((allowedTargetIds ?? []).filter(Boolean)),
    [allowedTargetIds],
  )
  const hasAllowedTargetFilter = allowedTargetIds !== undefined
  const allAuthors = authorsQuery.data ?? []

  // Direction. Normal mode locks the SURVIVOR (primaryAuthor); the user picks
  // the duplicate to absorb. Absorb mode locks the author to ABSORB
  // (absorbAuthor); the user picks the SURVIVOR. Either way the backend merge
  // is merge(survivor.id, [absorbed.id]) — only which side is locked flips.
  const absorbMode = !!absorbAuthor
  const fixedProp = absorbMode ? absorbAuthor : primaryAuthor
  // Resolve the locked author against the fetched list so we compare on the
  // richest copy (the prop may be a thin row / synthesized suggestion).
  const fixed = fixedProp
    ? allAuthors.find((candidate) => candidate.id === fixedProp.id) ?? fixedProp
    : null
  const selected = allAuthors.find((candidate) => candidate.id === mergeTargetId) ?? null
  const primary = absorbMode ? selected : fixed
  const alt = absorbMode ? fixed : selected

  const mergeCandidates = useMemo(() => {
    const needle = mergeSearch.trim().toLowerCase()
    return allAuthors
      .filter((candidate) => candidate.id !== fixed?.id)
      .filter((candidate) => !hasAllowedTargetFilter || allowedIds.has(candidate.id))
      .filter((candidate) => {
        if (!needle) return true
        return [
          candidate.name,
          candidate.affiliation,
          candidate.openalex_id,
          candidate.orcid,
          candidate.scholar_id,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(needle))
      })
      .sort((a, b) => a.name.localeCompare(b.name))
      .slice(0, 30)
  }, [allAuthors, allowedIds, hasAllowedTargetFilter, mergeSearch, fixed?.id])

  const mergeDiffs = useMemo(
    () => (primary && alt ? mergeDiscrepancies(primary, alt) : []),
    [primary, alt],
  )

  useEffect(() => {
    if (!open) {
      setMergeSearch('')
      setMergeTargetId(null)
      setMergeChoices({})
    }
  }, [open])

  useEffect(() => {
    if (open && initialTargetId && !mergeTargetId) {
      setMergeTargetId(initialTargetId)
    }
  }, [initialTargetId, mergeTargetId, open])

  useEffect(() => {
    if (open && primary && alt && Object.keys(mergeChoices).length === 0) {
      setMergeChoices(defaultMergeChoices(primary, alt))
    }
  }, [mergeChoices, primary, alt, open])

  const mergeMutation = useMutation({
    mutationFn: () => {
      if (!primary) throw new Error('Choose the surviving author first.')
      if (!alt) throw new Error('Choose an author to merge first.')
      return mergeAuthorProfiles(primary.id, [alt.id], { [alt.id]: mergeChoices })
    },
    onSuccess: (data) => {
      const survivorId = primary?.id
      const keys: unknown[][] = [
        ['authors'],
        ['authors-needs-attention'],
        ['library-followed-authors'],
        ['author-suggestions'],
        ['feed-monitors'],
      ]
      if (survivorId) {
        keys.push(['author-detail', survivorId], ['author-publications', survivorId])
      }
      void invalidateQueries(queryClient, ...keys)
      toast({
        title: 'Authors merged',
        description:
          `${data.alts_processed} profile${data.alts_processed === 1 ? '' : 's'} collapsed · ` +
          `${data.papers_reassigned} paper${data.papers_reassigned === 1 ? '' : 's'} reattached`,
      })
      onOpenChange(false)
      onMerged?.()
    },
    onError: () => errorToast('Error', 'Failed to merge authors.'),
  })

  const effectiveTitle =
    title ?? (absorbMode ? 'Merge this author into another' : 'Merge author profiles')
  const effectiveDescription =
    description ??
    (absorbMode
      ? 'Pick the author in your corpus or library this one should merge into — that author survives, this one folds in. Then choose which metadata value to keep.'
      : 'Choose the existing author row that represents the same person, then pick which metadata value survives on the merged profile.')

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <DialogTitle>{effectiveTitle}</DialogTitle>
          <DialogDescription>{effectiveDescription}</DialogDescription>
        </DialogHeader>

        {contextSlot}

        <div className="grid gap-4 md:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <EyebrowLabel tone="muted">
                {absorbMode ? 'Choose surviving author' : 'Choose duplicate'}
              </EyebrowLabel>
              <span className="truncate text-[10px] text-slate-400">
                {fixed
                  ? absorbMode
                    ? `absorbing ${fixed.name}`
                    : `→ ${fixed.name}`
                  : null}
              </span>
            </div>
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
              <Input
                value={mergeSearch}
                onChange={(event) => setMergeSearch(event.target.value)}
                placeholder="Search by name, affiliation, ORCID..."
                className="pl-9"
              />
            </div>
            <div className="max-h-[46vh] space-y-2 overflow-y-auto pr-1">
              {authorsQuery.isLoading ? (
                <LoadingState message="Loading authors..." />
              ) : mergeCandidates.length === 0 ? (
                <div className="rounded-sm border border-dashed border-[var(--color-border)] bg-surface-2 p-4 text-sm text-slate-500">
                  {emptyCandidateMessage}
                </div>
              ) : (
                mergeCandidates.map((candidate) => {
                  const isSelected = candidate.id === mergeTargetId
                  return (
                    <button
                      key={candidate.id}
                      type="button"
                      onClick={() => {
                        setMergeTargetId(candidate.id)
                        // Recompute defaults survivor-vs-absorbed for the new pick.
                        const p = absorbMode ? candidate : fixed
                        const a = absorbMode ? fixed : candidate
                        if (p && a) setMergeChoices(defaultMergeChoices(p, a))
                      }}
                      className={`w-full rounded-sm border p-3 text-left shadow-paper-sm transition ${
                        isSelected
                          ? 'border-alma-folio bg-alma-folio-soft'
                          : 'border-[var(--color-border)] bg-surface-2 hover:bg-surface-2'
                      }`}
                    >
                      <div className="flex items-start justify-between gap-2">
                        <span className="text-sm font-medium text-alma-800">
                          {candidate.name}
                        </span>
                        {candidate.author_type === 'followed' ? (
                          <StatusBadge tone="positive" size="sm">
                            Followed
                          </StatusBadge>
                        ) : null}
                      </div>
                      <p className="mt-1 line-clamp-1 text-xs text-slate-500">
                        {candidate.affiliation || candidate.openalex_id || 'No affiliation on record'}
                      </p>
                    </button>
                  )
                })
              )}
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <EyebrowLabel tone="muted">Pick winning values</EyebrowLabel>
              {selected ? (
                <span className="truncate text-[10px] text-slate-400">← {selected.name}</span>
              ) : null}
            </div>
            {primary && alt ? (
              mergeDiffs.length === 0 ? (
                <Alert variant="info" className="px-3 py-2">
                  <AlertDescription className="text-xs">
                    No visible metadata discrepancies in the fields shown here. The merge will
                    still reassign publications and record the alternate OpenAlex ID.
                  </AlertDescription>
                </Alert>
              ) : (
                <div className="max-h-[46vh] space-y-2 overflow-y-auto pr-1">
                  {mergeDiffs.map((diff) => {
                    const choice = mergeChoices[diff.key] ?? diff.defaultChoice
                    return (
                      <div
                        key={diff.key}
                        className="rounded-sm border border-[var(--color-border)] bg-surface-2 p-3"
                      >
                        <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
                          {diff.label}
                        </p>
                        <RadioGroup
                          value={choice}
                          onValueChange={(next) =>
                            setMergeChoices((prev) => ({
                              ...prev,
                              [diff.key]: next as AuthorMergeFieldChoice,
                            }))
                          }
                          className="mt-2 grid gap-2 sm:grid-cols-2"
                        >
                          {(['primary', 'alt'] as AuthorMergeFieldChoice[]).map((side) => {
                            const value = side === 'primary' ? diff.primaryValue : diff.altValue
                            const sideName = side === 'primary' ? primary.name : alt.name
                            const verb = side === 'primary' ? 'Keep' : 'Use'
                            const itemId = `merge-${diff.key}-${side}`
                            const isSelected = choice === side
                            return (
                              <label
                                key={side}
                                htmlFor={itemId}
                                className={`flex cursor-pointer items-start gap-2 rounded-sm border p-2 text-sm transition ${
                                  isSelected
                                    ? 'border-alma-folio bg-alma-folio-soft'
                                    : 'border-[var(--color-border)] bg-surface-2 hover:border-alma-200'
                                }`}
                              >
                                <RadioGroupItem
                                  id={itemId}
                                  value={side}
                                  className="mt-0.5"
                                  aria-label={`${verb} ${sideName}'s ${diff.label.toLowerCase()}`}
                                />
                                <span className="min-w-0">
                                  <span
                                    className={`block text-[11px] font-medium uppercase tracking-wide ${
                                      isSelected ? 'text-alma-folio' : 'text-slate-500'
                                    }`}
                                  >
                                    {verb} · {sideName}
                                  </span>
                                  <span className="mt-0.5 block break-words text-alma-800">
                                    {displayMetadataValue(value)}
                                  </span>
                                </span>
                              </label>
                            )
                          })}
                        </RadioGroup>
                      </div>
                    )
                  })}
                </div>
              )
            ) : (
              <div className="flex min-h-[240px] items-center justify-center rounded-sm border border-dashed border-[var(--color-border)] bg-surface-2 p-6 text-center text-sm text-slate-500">
                {absorbMode
                  ? 'Select the surviving author to compare metadata before merging.'
                  : 'Select an author from the corpus to compare metadata before merging.'}
              </div>
            )}
          </div>
        </div>

        <DialogFooter className="flex-col gap-2 sm:flex-row">
          <Button variant="ghost" onClick={() => onOpenChange(false)} disabled={mergeMutation.isPending}>
            Cancel
          </Button>
          <Button
            onClick={() => mergeMutation.mutate()}
            disabled={!primary || !alt || mergeMutation.isPending}
            title={
              primary && alt
                ? `Merge ${alt.name} into ${primary.name}.`
                : absorbMode
                  ? 'Choose the surviving author first.'
                  : 'Choose an author to merge first.'
            }
          >
            {mergeMutation.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <GitMerge className="h-4 w-4" />
            )}
            Merge profiles
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
