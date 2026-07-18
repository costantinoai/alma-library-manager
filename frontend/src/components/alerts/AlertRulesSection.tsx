import { useEffect, useMemo, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Bell,
  Plus,
  Trash2,
  Edit3,
  Power,
  AlertCircle,
  FlaskConical,
  Loader2,
} from 'lucide-react'
import {
  api,
  getInsightsDiagnostics,
  listCollections,
  listFollowedAuthors,
  testFireRule,
  type Alert,
  type AlertRule,
  type Collection,
  type FeedMonitor,
  type FollowedAuthor,
  type Lens,
  type TestFireResult,
} from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { LoadingState } from '@/components/ui/LoadingState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatDate, formatMonitorTypeLabel } from '@/lib/utils'
import { RuleTypeBadge } from './AlertBadges'
import { orphanRuleIds } from './alertDerive'
import {
  EMPTY_FORM_VALUES,
  RULE_TYPES,
  RULE_TYPE_LABEL,
  buildRuleConfig,
  describeRuleConfig,
  ruleFormSchema,
  ruleToFormValues,
  type RuleFormValues,
  type RuleType,
} from './ruleFormLogic'

/** Sentinel for the "type an ID by hand" entry in the author Select —
 * Radix forbids empty-string item values. */
const CUSTOM_AUTHOR_VALUE = '__custom__'

// ── Rule form dialog (shared between Create and Edit) ─────────────────────

interface RuleFormDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: string
  description: string
  submitLabel: string
  initialValues: RuleFormValues
  isPending: boolean
  isError: boolean
  errorMessage: string
  onSubmit: (values: RuleFormValues) => void
  // Option lists (lazy-filled by the parent's queries). Empty arrays are
  // rendered as "no options yet" messages inside the relevant selects.
  lenses: Lens[]
  collections: Collection[]
  monitors: FeedMonitor[]
  authors: FollowedAuthor[]
  // The parent passes `BranchTuningRow[]` from the insights API, which has
  // `branch_id?: string | null` (the API returns explicit nulls). The
  // dialog only reads `branch_id`, `branch_label`, `count`, so we widen
  // the prop to accept the broader shape rather than filtering nulls
  // upstream.
  branches: Array<{ branch_id?: string | null; branch_label?: string; count?: number }>
}

function RuleFormDialog({
  open,
  onOpenChange,
  title,
  description,
  submitLabel,
  initialValues,
  isPending,
  isError,
  errorMessage,
  onSubmit,
  lenses,
  collections,
  monitors,
  authors,
  branches,
}: RuleFormDialogProps) {
  const form = useForm<RuleFormValues>({
    resolver: zodResolver(ruleFormSchema),
    defaultValues: initialValues,
    mode: 'onChange',
  })

  // "Custom ID…" escape hatch for the author Select. Auto-engaged when the
  // rule being edited targets an author we don't follow (the Select can't
  // represent that id).
  const [customAuthor, setCustomAuthor] = useState(false)

  // Re-seed the form whenever the dialog opens or its initial values change
  // (e.g. switching from one rule to another via Edit).
  useEffect(() => {
    if (open) {
      form.reset(initialValues)
      setCustomAuthor(
        initialValues.author_id !== '' &&
          !authors.some((a) => a.author_id === initialValues.author_id),
      )
    }
    // `authors` deliberately excluded: a background refetch of the followed
    // list must not flip an open form between select/custom modes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, initialValues, form])

  const ruleType = form.watch('rule_type')

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form
            id="alert-rule-form"
            onSubmit={form.handleSubmit(onSubmit)}
            className="space-y-4 py-4"
          >
            <FormField
              control={form.control}
              name="name"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Name</FormLabel>
                  <FormControl>
                    <Input placeholder="e.g., New papers from Prof. Smith" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <FormField
              control={form.control}
              name="rule_type"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Type</FormLabel>
                  <Select
                    value={field.value}
                    onValueChange={(value) => field.onChange(value as RuleType)}
                  >
                    <FormControl>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      {RULE_TYPES.map((type) => (
                        <SelectItem key={type} value={type}>
                          {RULE_TYPE_LABEL[type]}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* Type-specific config field. Only the branch for the current
                rule_type renders, so there is exactly one input visible and
                exactly one validation message slot active at a time. */}
            {ruleType === 'author' && (
              <FormField
                control={form.control}
                name="author_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Author</FormLabel>
                    {authors.length > 0 && (
                      <Select
                        value={
                          customAuthor
                            ? CUSTOM_AUTHOR_VALUE
                            : authors.some((a) => a.author_id === field.value)
                              ? field.value
                              : ''
                        }
                        onValueChange={(value) => {
                          if (value === CUSTOM_AUTHOR_VALUE) {
                            setCustomAuthor(true)
                            field.onChange('')
                          } else {
                            setCustomAuthor(false)
                            field.onChange(value)
                          }
                        }}
                      >
                        <FormControl>
                          <SelectTrigger>
                            <SelectValue placeholder="Select a followed author" />
                          </SelectTrigger>
                        </FormControl>
                        <SelectContent>
                          {authors.map((a) => (
                            <SelectItem key={a.author_id} value={a.author_id}>
                              {a.name || a.author_id}
                            </SelectItem>
                          ))}
                          <SelectItem value={CUSTOM_AUTHOR_VALUE}>Custom ID…</SelectItem>
                        </SelectContent>
                      </Select>
                    )}
                    {(customAuthor || authors.length === 0) && (
                      <FormControl>
                        <Input placeholder="e.g. MG9cVagAAAAJ" {...field} />
                      </FormControl>
                    )}
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'collection' && (
              <FormField
                control={form.control}
                name="collection_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Collection</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Select a collection" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {collections.map((c) => (
                          <SelectItem key={c.id} value={c.id}>
                            {c.name} ({c.item_count} papers)
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'keyword' && (
              <FormField
                control={form.control}
                name="keywords"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Keywords</FormLabel>
                    <FormControl>
                      <Input placeholder="Comma-separated, e.g. machine learning, NLP" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'topic' && (
              <FormField
                control={form.control}
                name="topic"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Topic</FormLabel>
                    <FormControl>
                      <Input placeholder="e.g. deep learning" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'similarity' && (
              <FormField
                control={form.control}
                name="min_score"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Minimum score</FormLabel>
                    <FormControl>
                      <Input type="number" min={0} max={100} placeholder="0–100" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'discovery_lens' && (
              <FormField
                control={form.control}
                name="lens_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Discovery lens</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Select a discovery lens" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {lenses.map((lens) => (
                          <SelectItem key={lens.id} value={lens.id}>
                            {lens.name} ({lens.context_type})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'feed_monitor' && (
              <FormField
                control={form.control}
                name="monitor_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Feed monitor</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Select a feed monitor" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {monitors.map((m) => (
                          <SelectItem key={m.id} value={m.id}>
                            {m.label} ({formatMonitorTypeLabel(m.monitor_type)})
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'branch' && (
              <FormField
                control={form.control}
                name="branch_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Branch</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Select a branch" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {branches.map((b) => (
                          <SelectItem
                            key={b.branch_id ?? b.branch_label}
                            value={String(b.branch_id ?? b.branch_label ?? '')}
                          >
                            {b.branch_label ?? b.branch_id}
                            {b.count != null ? ` (${b.count} recs)` : ''}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            {ruleType === 'library_workflow' && (
              <FormField
                control={form.control}
                name="workflow"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Workflow state</FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Select a workflow state" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="reading">On reading list</SelectItem>
                        <SelectItem value="done">Finished</SelectItem>
                        <SelectItem value="excluded">Excluded</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            )}

            <FormField
              control={form.control}
              name="enabled"
              render={({ field }) => (
                <FormItem>
                  <label className="flex items-center gap-2">
                    <FormControl>
                      <Checkbox
                        checked={field.value}
                        onCheckedChange={(v) => field.onChange(v === true)}
                      />
                    </FormControl>
                    <span className="text-sm text-slate-700">Enabled</span>
                  </label>
                </FormItem>
              )}
            />

            {isError && (
              <div className="flex items-center gap-2 rounded-lg border border-critical-100 bg-critical-50 p-3">
                <AlertCircle className="h-4 w-4 text-critical-500" />
                <span className="text-sm text-critical-700">{errorMessage}</span>
              </div>
            )}
          </form>
        </Form>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            type="submit"
            form="alert-rule-form"
            loading={isPending}
            disabled={isPending}
          >
            {submitLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ── Main section ──────────────────────────────────────────────────────────

interface AlertRulesSectionProps {
  /** Jump to the Digests tab (where rules get assigned). */
  onGoToDigests?: () => void
}

export function AlertRulesSection({ onGoToDigests }: AlertRulesSectionProps) {
  const [createOpen, setCreateOpen] = useState(false)
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [testFireResult, setTestFireResult] = useState<TestFireResult | null>(null)

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const rulesQuery = useQuery({
    queryKey: ['alert-rules'],
    queryFn: () => api.get<AlertRule[]>('/alerts/rules'),
    retry: 1,
  })
  // Digest assignments, to flag orphan rules (a rule outside every digest
  // never runs). Shares the ['alerts'] cache with the Digests tab.
  const alertsQuery = useQuery({
    queryKey: ['alerts'],
    queryFn: () => api.get<Alert[]>('/alerts/'),
    retry: 1,
  })
  const authorsQuery = useQuery({
    queryKey: ['followed-authors', 'rules-form'],
    queryFn: listFollowedAuthors,
    retry: 1,
    staleTime: 60_000,
  })
  const lensesQuery = useQuery({
    queryKey: ['lenses', 'rules-form'],
    queryFn: () => api.get<Lens[]>('/lenses?is_active=true&limit=200'),
    retry: 1,
  })
  const monitorsQuery = useQuery({
    queryKey: ['feed-monitors', 'rules-form'],
    queryFn: () => api.get<FeedMonitor[]>('/feed/monitors'),
    retry: 1,
  })
  const collectionsQuery = useQuery({
    queryKey: ['library-collections', 'rules-form'],
    queryFn: listCollections,
    retry: 1,
  })
  const diagnosticsQuery = useQuery({
    queryKey: ['insights-diagnostics', 'rules-form'],
    queryFn: getInsightsDiagnostics,
    retry: 1,
    staleTime: 60_000,
  })

  interface AlertRuleBody {
    name: string
    rule_type: string
    rule_config: Record<string, unknown>
    channels: string[]
    enabled: boolean
  }

  const createMutation = useMutation({
    mutationFn: (body: AlertRuleBody) => api.post<AlertRule>('/alerts/rules', body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alert-rules'])
      setCreateOpen(false)
      toast({ title: 'Created', description: 'Alert rule created successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create alert rule.')
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, body }: { id: string; body: AlertRuleBody }) =>
      api.put<AlertRule>(`/alerts/rules/${id}`, body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alert-rules'])
      setEditingRule(null)
      toast({ title: 'Updated', description: 'Alert rule updated.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to update alert rule.')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/alerts/rules/${id}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alert-rules'])
      setDeleteId(null)
      toast({ title: 'Deleted', description: 'Alert rule deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete alert rule.')
    },
  })

  const toggleMutation = useMutation({
    mutationFn: (id: string) => api.post<AlertRule>(`/alerts/rules/${id}/toggle`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['alert-rules'])
      toast({ title: 'Toggled', description: 'Alert rule toggled.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to toggle alert rule.')
    },
  })

  const testFireMutation = useMutation({
    mutationFn: (id: string) => testFireRule(id),
    onSuccess: (result) => {
      setTestFireResult(result)
    },
    onError: () => {
      errorToast('Error', 'Test-fire failed.')
    },
  })

  const rules = rulesQuery.data ?? []
  const lenses = lensesQuery.data ?? []
  const monitors = monitorsQuery.data ?? []
  const collections = collectionsQuery.data ?? []
  const authors = authorsQuery.data ?? []
  const branches = diagnosticsQuery.data?.discovery.branch_quality ?? []

  const orphanIds = useMemo(
    () => orphanRuleIds(rules, alertsQuery.data ?? []),
    [rules, alertsQuery.data],
  )
  // id → human label maps for the per-card "what does this rule watch" line.
  const describeLookups = useMemo(
    () => ({
      monitors: new Map(monitors.map((m) => [m.id, m.label])),
      lenses: new Map(lenses.map((l) => [l.id, l.name])),
      collections: new Map(collections.map((c) => [c.id, c.name])),
      authors: new Map(
        authors.filter((a) => a.name).map((a) => [a.author_id, a.name as string]),
      ),
    }),
    [monitors, lenses, collections, authors],
  )

  // Memoized so the edit dialog's initialValues keep a stable identity across
  // parent re-renders (query refetches, other mutations). Without this, the
  // dialog's reset effect fires on every render and wipes in-progress edits.
  const editInitialValues = useMemo(
    () => (editingRule ? ruleToFormValues(editingRule) : EMPTY_FORM_VALUES),
    [editingRule],
  )

  function bodyFromValues(values: RuleFormValues, baseRule?: AlertRule | null): AlertRuleBody {
    // When editing without switching type, pass the existing rule_config as
    // the merge base so extras the form doesn't surface (lookback_days,
    // lens_id on a similarity rule, ...) survive the edit.
    const baseConfig =
      baseRule && baseRule.rule_type === values.rule_type ? baseRule.rule_config : {}
    return {
      name: values.name.trim(),
      rule_type: values.rule_type,
      rule_config: buildRuleConfig(values, baseConfig),
      // Delivery channels belong to the digest; rule-level channels are
      // vestigial (evaluation never reads them) and stay empty.
      channels: [],
      enabled: values.enabled,
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-alma-800">Alert Rules</h2>
        <Button onClick={() => setCreateOpen(true)}>
          <Plus className="h-4 w-4" />
          New Rule
        </Button>
      </div>

      {rulesQuery.isLoading ? (
        <LoadingState />
      ) : rulesQuery.isError ? (
        <ErrorState message="Failed to load alert rules." />
      ) : rules.length === 0 ? (
        <div className="py-12 text-center">
          <Bell className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm text-slate-500">No alert rules yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Create a rule to define what triggers alerts.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {rules.map((rule) => (
            <Card key={rule.id} className="transition-shadow hover:shadow-md">
              <CardContent className="p-5">
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <h3 className="font-medium text-alma-800">{rule.name}</h3>
                      {!rule.enabled && <Badge variant="secondary">Disabled</Badge>}
                    </div>
                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      <RuleTypeBadge type={rule.rule_type} />
                      {orphanIds.has(rule.id) && (
                        <button
                          type="button"
                          onClick={onGoToDigests}
                          title="Rules only run inside a digest. Click to open Digests and assign it."
                          className="cursor-pointer"
                        >
                          <Badge variant="warning">
                            <AlertCircle className="mr-1 h-3 w-3" />
                            Not in any digest
                          </Badge>
                        </button>
                      )}
                    </div>
                    <p className="mt-1.5 text-sm text-slate-600">
                      {describeRuleConfig(rule, describeLookups)}
                    </p>
                    <p className="mt-1 text-xs text-slate-400">
                      Created {formatDate(rule.created_at)}
                    </p>
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => testFireMutation.mutate(rule.id)}
                      disabled={testFireMutation.isPending}
                      title="Test rule (dry-run, nothing sent)"
                    >
                      {testFireMutation.isPending && testFireMutation.variables === rule.id ? (
                        <Loader2 className="h-4 w-4 animate-spin text-alma-500" />
                      ) : (
                        <FlaskConical className="h-4 w-4 text-alma-500" />
                      )}
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => toggleMutation.mutate(rule.id)}
                      disabled={toggleMutation.isPending}
                      title={rule.enabled ? 'Disable' : 'Enable'}
                    >
                      <Power className={`h-4 w-4 ${rule.enabled ? 'text-success-500' : 'text-slate-400'}`} />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setEditingRule(rule)}
                      title="Edit rule"
                    >
                      <Edit3 className="h-4 w-4 text-slate-500" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setDeleteId(rule.id)}
                      title="Delete rule"
                    >
                      <Trash2 className="h-4 w-4 text-critical-500" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create dialog */}
      <RuleFormDialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open)
          // Opening fresh should not show a stale error banner from the
          // previous failed attempt.
          if (open) createMutation.reset()
        }}
        title="Create Alert Rule"
        description="Configure a new alert rule to detect publications."
        submitLabel="Create Rule"
        initialValues={EMPTY_FORM_VALUES}
        isPending={createMutation.isPending}
        isError={createMutation.isError}
        errorMessage="Failed to create rule."
        onSubmit={(values) => createMutation.mutate(bodyFromValues(values))}
        lenses={lenses}
        collections={collections}
        monitors={monitors}
        authors={authors}
        branches={branches}
      />

      {/* Edit dialog — open when editingRule is non-null */}
      <RuleFormDialog
        open={editingRule !== null}
        onOpenChange={(open) => {
          if (!open) {
            setEditingRule(null)
            updateMutation.reset()
          }
        }}
        title="Edit Alert Rule"
        description="Update the alert rule configuration."
        submitLabel="Save Changes"
        initialValues={editInitialValues}
        isPending={updateMutation.isPending}
        isError={updateMutation.isError}
        errorMessage="Failed to update rule."
        onSubmit={(values) => {
          if (!editingRule) return
          updateMutation.mutate({ id: editingRule.id, body: bodyFromValues(values, editingRule) })
        }}
        lenses={lenses}
        collections={collections}
        monitors={monitors}
        authors={authors}
        branches={branches}
      />

      {/* Test-fire result — dry-run matches for one rule */}
      <Dialog open={testFireResult !== null} onOpenChange={(open) => !open && setTestFireResult(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Test Results</DialogTitle>
            <DialogDescription>
              Dry-run of this rule against the current corpus — nothing was sent.
            </DialogDescription>
          </DialogHeader>
          {testFireResult && (
            <div className="space-y-3 py-2">
              <p className="text-sm text-slate-700">
                <span className="font-medium">{testFireResult.matches_found}</span> matching paper
                {testFireResult.matches_found !== 1 ? 's' : ''}
                {testFireResult.matches.length < testFireResult.matches_found
                  ? ` (showing first ${testFireResult.matches.length})`
                  : ''}
              </p>
              {testFireResult.matches.length > 0 && (
                <ul className="max-h-64 space-y-1 overflow-y-auto rounded-lg bg-surface-2 p-3">
                  {testFireResult.matches.map((title, i) => (
                    <li key={`${i}-${title}`} className="text-sm text-slate-700">
                      {title}
                    </li>
                  ))}
                </ul>
              )}
              {testFireResult.rule_type === 'feed_monitor' && (
                <p className="text-xs text-slate-500">
                  A digest may deliver fewer papers: its cold-start watermark only counts feed
                  items fetched after the digest was created.
                </p>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setTestFireResult(null)}>Close</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation */}
      <Dialog open={deleteId !== null} onOpenChange={(open) => !open && setDeleteId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Alert Rule</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this alert rule? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteId(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => deleteId && deleteMutation.mutate(deleteId)}
              loading={deleteMutation.isPending}
              disabled={deleteMutation.isPending}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
