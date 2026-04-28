import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Database, Download, FileJson, FileText, RefreshCw, Zap } from 'lucide-react'

import {
  api,
  listUnresolvedImportedPublications,
  resolveImportedPublicationsOpenAlex,
} from '@/api/client'
import { AsyncButton, SettingsCard } from '@/components/settings/primitives'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { downloadFromUrl, downloadJson } from '@/lib/utils'

export function DataManagementCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const exportMutation = useMutation({
    mutationFn: () => api.get<Record<string, unknown>>('/settings/export'),
    onSuccess: (data) => {
      downloadJson(data, 'alma-export.json')
      toast({ title: 'Exported', description: 'Data exported successfully.' })
    },
    onError: () => errorToast('Error', 'Failed to export data.'),
  })

  const unresolvedImportsQuery = useQuery({
    queryKey: ['unresolved-imported-publications'],
    queryFn: () => listUnresolvedImportedPublications(200),
    retry: 1,
  })

  const resolveUnresolvedImportsMutation = useMutation({
    mutationFn: () =>
      resolveImportedPublicationsOpenAlex({
        unresolved_only: true,
        limit: 5000,
        background: true,
      }),
    onSuccess: async (data) => {
      await invalidateQueries(
        queryClient,
        ['unresolved-imported-publications'],
        ['activity-operations'],
      )
      toast({
        title: 'Resolution job started',
        description: data.job_id
          ? `Job ${data.job_id} queued for unresolved publications.`
          : 'OpenAlex resolution triggered.',
      })
    },
    onError: () => errorToast('Error', 'Failed to start unresolved publication resolution.'),
  })

  const backupDatabaseMutation = useMutation({
    mutationFn: () => downloadFromUrl(`${api.baseURL}/backup/export`, 'alma-backup.db'),
    onSuccess: () => toast({ title: 'Database backup downloaded' }),
    onError: () => errorToast('Error', 'Failed to download database backup.'),
  })

  const exportBibtexMutation = useMutation({
    mutationFn: () => downloadFromUrl(`${api.baseURL}/backup/export/bibtex`, 'alma-library.bib'),
    onSuccess: () => toast({ title: 'BibTeX export downloaded' }),
    onError: () => errorToast('Error', 'Failed to export BibTeX.'),
  })

  const exportJsonMutation = useMutation({
    mutationFn: () => downloadFromUrl(`${api.baseURL}/backup/export/json`, 'alma-library.json'),
    onSuccess: () => toast({ title: 'JSON export downloaded' }),
    onError: () => errorToast('Error', 'Failed to export JSON.'),
  })

  return (
    <SettingsCard
      icon={Database}
      title="Data Management"
      description="Manage your data: deep refresh, backup, and export."
    >
      <div className="space-y-2">
        <h4 className="text-xs font-semibold text-slate-700">Operations</h4>
        <div className="flex flex-wrap items-start gap-3">
          <AsyncButton
            variant="outline"
            icon={<Download className="h-4 w-4" />}
            pending={exportMutation.isPending}
            onClick={() => exportMutation.mutate()}
          >
            Export All Data
          </AsyncButton>
          <AsyncButton
            variant="outline"
            icon={<Zap className="h-4 w-4" />}
            pending={resolveUnresolvedImportsMutation.isPending}
            onClick={() => resolveUnresolvedImportsMutation.mutate()}
          >
            Resolve Unresolved Imports (OpenAlex)
          </AsyncButton>
        </div>
        <p className="text-[11px] leading-snug text-slate-500">
          Author refresh has moved to <span className="font-medium">Corpus maintenance</span> below —
          it exposes Library / Followed / Whole-corpus scopes on the same pipeline.
        </p>
      </div>

      <div className="space-y-2">
        <h4 className="text-xs font-semibold text-slate-700">Backup & Export</h4>
        <div className="flex flex-wrap gap-3">
          <AsyncButton
            variant="outline"
            icon={<Database className="h-4 w-4" />}
            pending={backupDatabaseMutation.isPending}
            onClick={() => backupDatabaseMutation.mutate()}
          >
            Download Database
          </AsyncButton>
          <AsyncButton
            variant="outline"
            icon={<FileText className="h-4 w-4" />}
            pending={exportBibtexMutation.isPending}
            onClick={() => exportBibtexMutation.mutate()}
          >
            Export BibTeX
          </AsyncButton>
          <AsyncButton
            variant="outline"
            icon={<FileJson className="h-4 w-4" />}
            pending={exportJsonMutation.isPending}
            onClick={() => exportJsonMutation.mutate()}
          >
            Export JSON
          </AsyncButton>
        </div>
      </div>

      <div className="space-y-2 rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
        <div className="flex items-center justify-between">
          <h4 className="text-xs font-semibold text-slate-700">OpenAlex Resolution Queue</h4>
          <AsyncButton
            variant="ghost"
            size="icon-sm"
            icon={<RefreshCw className="h-3.5 w-3.5" />}
            pending={unresolvedImportsQuery.isFetching}
            onClick={() => unresolvedImportsQuery.refetch()}
          />

        </div>
        {unresolvedImportsQuery.isLoading ? (
          <p className="text-xs text-slate-500">Loading unresolved publications...</p>
        ) : unresolvedImportsQuery.isError ? (
          <p className="text-xs text-red-600">Failed to load unresolved publication list.</p>
        ) : (
          <>
            <p className="text-xs text-slate-600">
              Unresolved publications:{' '}
              <span className="font-mono">{unresolvedImportsQuery.data?.total ?? 0}</span>
            </p>
            {(unresolvedImportsQuery.data?.items ?? []).slice(0, 3).map((item) => (
              <p key={item.id} className="truncate text-xs text-slate-500" title={item.title}>
                {item.openalex_resolution_reason
                  ? `[${item.openalex_resolution_reason}] `
                  : ''}
                {item.title}
              </p>
            ))}
          </>
        )}
      </div>
    </SettingsCard>
  )
}
