import { useCallback, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Archive,
  Database,
  HardDrive,
  Loader2,
  RotateCcw,
  ShieldAlert,
  Trash2,
  UploadCloud,
} from 'lucide-react'

import {
  api,
  resetEmbeddings,
  resetFeedbackLearning,
  type BackupInfo,
  type LibraryInfo,
} from '@/api/client'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { ImportDialog } from '@/components/ImportDialog'
import { AsyncButton, SettingsCard, StatTile } from '@/components/settings/primitives'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { parseAlmaTimestamp } from '@/lib/utils'

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

export function LibraryManagementCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const libraryInfoQuery = useQuery({
    queryKey: ['library-info'],
    queryFn: () => api.get<LibraryInfo>('/library-mgmt/info'),
    retry: 1,
  })
  const libraryDatabase = libraryInfoQuery.data?.database

  const backupMutation = useMutation({
    mutationFn: () => api.post('/library-mgmt/backup'),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-info'])
      toast({ title: 'Backup Created', description: 'Database backup created successfully.' })
    },
  })

  const restoreMutation = useMutation({
    mutationFn: (name: string) => api.post(`/library-mgmt/restore/${encodeURIComponent(name)}`),
    onSuccess: () => {
      queryClient.invalidateQueries()
      toast({ title: 'Restored', description: 'Database restored from backup.' })
    },
  })

  const deleteBackupMutation = useMutation({
    mutationFn: (name: string) => api.delete(`/library-mgmt/backup/${encodeURIComponent(name)}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-info'])
      toast({ title: 'Backup Deleted', description: 'Backup deleted successfully.' })
    },
  })

  // The reset confirm dialog is controlled (rather than uncontrolled
  // via ConfirmAction) so it can host the optional feedback-learning
  // reset in the same destructive flow.
  const [resetDialogOpen, setResetDialogOpen] = useState(false)
  const [alsoResetSignal, setAlsoResetSignal] = useState(false)

  const resetMutation = useMutation({
    mutationFn: async ({ alsoSignal }: { alsoSignal: boolean }) => {
      const dbResult = await api.delete<{ job_id: string }>('/library-mgmt/reset')
      // Feedback reset is synchronous and small (≤low thousands of rows
      // across the feedback tables) — runs in parallel with the queued
      // Activity job and returns its own counts. Both writes are
      // intentionally destructive so doing them in either order is
      // safe; the Activity job will not race the feedback wipe because
      // the two operate on disjoint tables.
      const signalResult = alsoSignal ? await resetFeedbackLearning() : null
      return { dbResult, signalResult }
    },
    onSuccess: ({ dbResult, signalResult }) => {
      toast({
        title: 'Reset queued',
        description: signalResult
          ? `DB reset job ${dbResult.job_id} queued · cleared ${signalResult.total_rows_cleared.toLocaleString()} feedback-learning rows.`
          : `Job ${dbResult.job_id} is running in Activity.`,
      })
      void invalidateQueries(
        queryClient,
        ['activity-operations'],
        ['library-info'],
        ['author-suggestions'],
      )
      setResetDialogOpen(false)
      setAlsoResetSignal(false)
    },
    onError: () => errorToast('Error', 'Failed to queue library reset.'),
  })

  // Standalone feedback reset — independent of the DB reset above.
  // Users sometimes want to restart the learned ranking state without
  // touching their corpus.
  const resetFeedbackLearningMutation = useMutation({
    mutationFn: () => resetFeedbackLearning(),
    onSuccess: (data) => {
      toast({
        title: 'Feedback learning reset',
        description:
          data.total_rows_cleared === 0
            ? 'No feedback-learning rows to clear.'
            : `Cleared ${data.total_rows_cleared.toLocaleString()} rows across ${
                Object.keys(data.cleared).length
              } tables.`,
      })
      void invalidateQueries(
        queryClient,
        ['author-suggestions'],
        ['lens-signals'],
        ['recommendations'],
        ['discovery-recommendations'],
      )
    },
    onError: () => errorToast('Error', 'Failed to reset learned feedback.'),
  })

  const resetEmbeddingsMutation = useMutation({
    mutationFn: () => resetEmbeddings(),
    onSuccess: (data) => {
      toast({
        title: 'Embeddings deleted',
        description:
          data.total_rows_cleared === 0
            ? 'No saved embeddings were present.'
            : `Deleted ${data.total_rows_cleared.toLocaleString()} rows across ${
                Object.keys(data.cleared).length
              } embedding tables.`,
      })
      void invalidateQueries(
        queryClient,
        ['ai-status'],
        ['insights'],
        ['insights-diagnostics'],
        ['graph-paper-map'],
        ['graph-author-network'],
        ['activity-operations'],
      )
    },
    onError: () => errorToast('Error', 'Failed to delete saved embeddings.'),
  })

  const deduplicateMutation = useMutation({
    mutationFn: () => api.post<{ job_id: string }>('/library-mgmt/deduplicate'),
    onSuccess: (data) => {
      toast({
        title: 'Deduplication started',
        description: `Job ${data.job_id} is now visible in Activity.`,
      })
      void invalidateQueries(queryClient, ['activity-operations'])
    },
    onError: () => errorToast('Error', 'Failed to start deduplication.'),
  })

  const [importDialogOpen, setImportDialogOpen] = useState(false)

  const handleImportComplete = useCallback(() => {
    void invalidateQueries(
      queryClient,
      ['library-info'],
      ['papers'],
      ['library-collections'],
      ['library-tags'],
    )
    toast({ title: 'Import complete', description: 'Library data has been updated.' })
  }, [queryClient, toast])

  return (
    <>
      <SettingsCard
        icon={HardDrive}
        title="Library Management"
        description="Backup, restore, or fully reset your library data."
        roomy
      >
        {libraryInfoQuery.isLoading && (
          <div className="flex items-center gap-2 text-sm text-slate-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading library info...
          </div>
        )}
        {libraryInfoQuery.isError && (
          <p className="text-sm text-red-600">Failed to load library info.</p>
        )}
        {libraryInfoQuery.data && libraryDatabase && (
          <>
            {/* Database info */}
            <div className="space-y-3">
              <div className="space-y-0.5">
                <h4 className="text-xs font-semibold text-slate-700">Unified Database</h4>
                <p className="truncate font-mono text-[11px] text-slate-500" title={libraryDatabase.path}>
                  {libraryDatabase.path}
                </p>
              </div>
              <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                <StatTile label="Size" value={formatBytes(libraryDatabase.size_bytes)} />
                {libraryDatabase.publications_count != null && (
                  <StatTile label="Publications" value={libraryDatabase.publications_count} />
                )}
                {libraryDatabase.authors_count != null && (
                  <StatTile label="Authors" value={libraryDatabase.authors_count} />
                )}
                {libraryDatabase.topics_count != null && (
                  <StatTile label="Topics" value={libraryDatabase.topics_count} />
                )}
              </div>
            </div>

            {/* Primary maintenance actions */}
            <div className="flex flex-wrap gap-2">
              <AsyncButton
                variant="outline"
                icon={<UploadCloud className="h-4 w-4" />}
                onClick={() => setImportDialogOpen(true)}
              >
                Import Papers (BibTeX/Zotero)
              </AsyncButton>
              <AsyncButton
                variant="outline"
                icon={<Archive className="h-4 w-4" />}
                pending={backupMutation.isPending}
                onClick={() => backupMutation.mutate()}
              >
                Create Backup
              </AsyncButton>
              <AsyncButton
                variant="outline"
                icon={<Database className="h-4 w-4" />}
                pending={deduplicateMutation.isPending}
                onClick={() => deduplicateMutation.mutate()}
              >
                Deduplicate Database
              </AsyncButton>
            </div>

            {/* Existing backups */}
            <div className="space-y-2">
              <h4 className="text-sm font-semibold text-slate-800">Existing Backups</h4>
              {libraryInfoQuery.data.backups.length === 0 ? (
                <p className="text-sm text-slate-500">No backups yet.</p>
              ) : (
                <div className="space-y-2">
                  {libraryInfoQuery.data.backups.map((backup: BackupInfo) => (
                    <div
                      key={backup.name}
                      className="flex items-center justify-between rounded-sm border border-[var(--color-border)] px-3 py-2"
                    >
                      <div className="space-y-0.5">
                        <p className="text-sm font-medium text-slate-700">{backup.name}</p>
                        <p className="text-xs text-slate-500">
                          {parseAlmaTimestamp(backup.created_at).toLocaleString()} ·{' '}
                          {formatBytes(backup.size_bytes)}
                        </p>
                      </div>
                      <div className="ml-auto flex items-center gap-1">
                        <ConfirmAction
                          trigger={
                            <Button
                              variant="outline"
                              size="icon-sm"
                              title="Restore backup"
                              disabled={restoreMutation.isPending}
                            >
                              {restoreMutation.isPending ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : (
                                <UploadCloud className="h-3 w-3" />
                              )}
                            </Button>
                          }
                          title={`Restore backup "${backup.name}"?`}
                          description="This will replace the current database file. The running database will be overwritten with the backup's contents."
                          confirmLabel="Restore"
                          onConfirm={() => restoreMutation.mutate(backup.name)}
                        />
                        <ConfirmAction
                          trigger={
                            <Button
                              variant="destructive"
                              size="icon-sm"
                              title="Delete backup"
                              disabled={deleteBackupMutation.isPending}
                            >
                              {deleteBackupMutation.isPending ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : (
                                <Trash2 className="h-3 w-3" />
                              )}
                            </Button>
                          }
                          title={`Delete backup "${backup.name}"?`}
                          description="This cannot be undone."
                          confirmLabel="Delete backup"
                          destructive
                          onConfirm={() => deleteBackupMutation.mutate(backup.name)}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Danger zone — collapsed by default so the destructive
                actions don't dominate the card. Two side-by-side rows
                of (description, button) keep the buttons aligned to
                the right edge instead of stacking under the prose. */}
            <details className="group rounded-lg border border-red-300 bg-red-50">
              <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3">
                <div className="flex items-center gap-2">
                  <ShieldAlert className="h-4 w-4 text-red-600" />
                  <h4 className="text-sm font-semibold text-red-700">Danger Zone</h4>
                </div>
                <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-red-700 group-open:hidden">Show</span>
                <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-red-700 group-open:inline">Hide</span>
              </summary>
              <div className="space-y-3 border-t border-red-200 px-4 pb-4 pt-3">
                <p className="text-xs text-red-700">
                  Destructive actions here are queued in Activity so you can track the job, but they
                  rewrite the database in place. Take a fresh backup first if you're not sure.
                </p>
                <div className="flex items-start justify-between gap-3 border-t border-red-200 pt-3">
                  <p className="flex-1 text-xs text-red-700">
                    <span className="font-medium">Reset all feedback learning.</span> Wipes feedback events,
                    lens weights, author dismissals, author centroids, suggestion cache, and
                    recommendation actions. Library, followed authors, lenses, and corpus stay.
                  </p>
                  <ConfirmAction
                    trigger={
                      <Button variant="destructive" disabled={resetFeedbackLearningMutation.isPending}>
                        {resetFeedbackLearningMutation.isPending ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <RotateCcw className="h-4 w-4" />
                        )}
                        Reset feedback learning
                      </Button>
                    }
                    title="Reset all learned feedback?"
                    description="Wipes feedback events, lens weights, author dismissals, author centroids, suggestion cache, and recommendation actions. Library, followed authors, lenses, and the corpus are preserved. The ranker starts from zero. This cannot be undone."
                    confirmLabel="Reset feedback"
                    destructive
                    onConfirm={() => resetFeedbackLearningMutation.mutate()}
                  />
                </div>
                <div className="flex items-start justify-between gap-3 border-t border-red-200 pt-3">
                  <p className="flex-1 text-xs text-red-700">
                    <span className="font-medium">Delete saved embeddings.</span> Wipes cached
                    paper vectors, author centroids, and per-paper vector fetch markers only.
                    Papers, Library state, feedback, tags, collections, monitors, and sources stay.
                  </p>
                  <ConfirmAction
                    trigger={
                      <Button variant="destructive" disabled={resetEmbeddingsMutation.isPending}>
                        {resetEmbeddingsMutation.isPending ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Trash2 className="h-4 w-4" />
                        )}
                        Delete embeddings
                      </Button>
                    }
                    title="Delete every saved embedding?"
                    description="Wipes cached paper vectors, author centroids, and per-paper vector fetch markers only. Papers, Library state, feedback, tags, collections, monitors, and sources are preserved. Re-run S2 vector fetch or AI compute to repopulate them."
                    confirmLabel="Delete embeddings"
                    destructive
                    onConfirm={() => resetEmbeddingsMutation.mutate()}
                  />
                </div>
                <div className="flex items-start justify-between gap-3 border-t border-red-200 pt-3">
                  <p className="flex-1 text-xs text-red-700">
                    <span className="font-medium">Reset publications database.</span> Deletes every
                    paper, author, and feed item. Saved collections, tags, and topics are kept.
                    Backups are not touched. Optional: also reset learned feedback in the same step.
                  </p>
                  <AlertDialog open={resetDialogOpen} onOpenChange={setResetDialogOpen}>
                <AlertDialogTrigger asChild>
                  <Button variant="destructive" disabled={resetMutation.isPending}>
                    {resetMutation.isPending ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Trash2 className="h-4 w-4" />
                    )}
                    Reset Publications Database
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Reset publications database?</AlertDialogTitle>
                    <AlertDialogDescription>
                      Deletes every paper, author, and feed item. Saved
                      collections, tags, and topics are kept. Backups
                      are not touched.
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <div className="flex items-start gap-2 rounded-md border border-rose-200 bg-rose-50 p-3">
                    <Checkbox
                      id="reset-also-feedback"
                      checked={alsoResetSignal}
                      onCheckedChange={(v) => setAlsoResetSignal(v === true)}
                      className="mt-0.5"
                    />
                    <div className="space-y-0.5">
                      <Label
                        htmlFor="reset-also-feedback"
                        className="text-sm font-medium text-rose-800"
                      >
                        Also reset learned feedback
                      </Label>
                      <p className="text-[11px] leading-snug text-rose-700/80">
                        Wipes feedback events, lens weights, author
                        dismissals, author centroids, suggestion cache,
                        and recommendation actions. The ranker starts
                        from zero. Followed authors and lenses are
                        preserved.
                      </p>
                    </div>
                  </div>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction
                      onClick={() => resetMutation.mutate({ alsoSignal: alsoResetSignal })}
                      className="bg-red-600 text-white hover:bg-red-700"
                    >
                      {alsoResetSignal ? 'Reset DB + feedback' : 'Yes, reset DB only'}
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
                </div>
              </div>
            </details>
          </>
        )}
      </SettingsCard>
      <ImportDialog
        open={importDialogOpen}
        onOpenChange={setImportDialogOpen}
        onImportComplete={handleImportComplete}
      />
    </>
  )
}

/**
 * Local helper that wraps shadcn `AlertDialog` with the minimum API surface
 * we need: a trigger, a title/description, a destructive-or-default confirm
 * button, and a callback. Keeps the settings card from juggling open-state
 * for every destructive action in its body.
 */
function ConfirmAction({
  trigger,
  title,
  description,
  confirmLabel,
  destructive = false,
  onConfirm,
}: {
  trigger: React.ReactNode
  title: React.ReactNode
  description?: React.ReactNode
  confirmLabel: string
  destructive?: boolean
  onConfirm: () => void
}) {
  return (
    <AlertDialog>
      <AlertDialogTrigger asChild>{trigger}</AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          {description ? (
            <AlertDialogDescription>{description}</AlertDialogDescription>
          ) : null}
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            onClick={onConfirm}
            className={destructive ? 'bg-red-600 text-white hover:bg-red-700' : undefined}
          >
            {confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
