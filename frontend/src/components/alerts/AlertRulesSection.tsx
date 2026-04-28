import { useEffect, useState } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Bell,
  Plus,
  Trash2,
  Edit3,
  Power,
  Loader2,
  AlertCircle,
  Hash,
} from 'lucide-react'
import {
  api,
  getInsightsDiagnostics,
  listCollections,
  type AlertRule,
  type Collection,
  type FeedMonitor,
  type Lens,
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

// ── Rule-type registry ─────────────────────────────────────────────────────
// One source of truth for the 9 rule types: human label, placeholder, and
// config-field key. Used by RULE_TYPE_OPTIONS (the Select dropdown) and by
// the dynamic config-field renderer inside the form.

const RULE_TYPES = [
  'author',
  'collection',
  'keyword',
  'topic',
  'similarity',
  'discovery_lens',
  'feed_monitor',
  'branch',
  'library_workflow',
] as const
type RuleType = (typeof RULE_TYPES)[number]

const RULE_TYPE_LABEL: Record<RuleType, string> = {
  author: 'Author',
  collection: 'Collection',
  keyword: 'Keyword',
  topic: 'Topic',
  similarity: 'Similarity Score',
  discovery_lens: 'Discovery Lens',
  feed_monitor: 'Feed Monitor',
  branch: 'Branch',
  library_workflow: 'Library Workflow',
}

// ── Schema ────────────────────────────────────────────────────────────────
// Flat form schema with per-type validation via superRefine. A
// z.discriminatedUnion would give us a tighter compile-time guarantee but
// makes react-hook-form's `reset` awkward (the branch switches shape on
// rule_type change); flat + superRefine is the pragmatic tradeoff for
// what the form needs.
//
// All type-specific fields default to '' so switching rule_type never
// leaves a prior field in an inconsistent state — the form instance just
// validates the field that the current rule_type actually reads, and
// buildRuleConfig() (below) picks that single field on submit.

const ruleFormSchema = z
  .object({
    name: z.string().trim().min(1, 'Name is required'),
    rule_type: z.enum(RULE_TYPES),
    slack: z.boolean(),
    enabled: z.boolean(),
    // Per-type config fields. All optional in the schema; required only
    // for whichever rule_type the user picked (enforced in superRefine).
    author_id: z.string().default(''),
    collection_id: z.string().default(''),
    keywords: z.string().default(''),
    topic: z.string().default(''),
    min_score: z.string().default(''),
    lens_id: z.string().default(''),
    monitor_id: z.string().default(''),
    branch_id: z.string().default(''),
    workflow: z.string().default(''),
  })
  .superRefine((data, ctx) => {
    const addRequired = (field: keyof typeof data, message: string) => {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [field], message })
    }
    switch (data.rule_type) {
      case 'author':
        if (!data.author_id.trim()) addRequired('author_id', 'Author ID is required.')
        break
      case 'collection':
        if (!data.collection_id) addRequired('collection_id', 'Pick a collection.')
        break
      case 'keyword': {
        const kws = data.keywords.split(',').map((k) => k.trim()).filter(Boolean)
        if (kws.length === 0) addRequired('keywords', 'At least one keyword is required.')
        break
      }
      case 'topic':
        if (!data.topic.trim()) addRequired('topic', 'Topic text is required.')
        break
      case 'similarity': {
        const raw = data.min_score.trim()
        if (!raw) {
          addRequired('min_score', 'Minimum score is required.')
        } else {
          const n = Number(raw)
          if (Number.isNaN(n) || n < 0 || n > 100) {
            addRequired('min_score', 'Score must be a number between 0 and 100.')
          }
        }
        break
      }
      case 'discovery_lens':
        if (!data.lens_id) addRequired('lens_id', 'Pick a discovery lens.')
        break
      case 'feed_monitor':
        if (!data.monitor_id) addRequired('monitor_id', 'Pick a feed monitor.')
        break
      case 'branch':
        if (!data.branch_id) addRequired('branch_id', 'Pick a branch.')
        break
      case 'library_workflow':
        if (!data.workflow) addRequired('workflow', 'Pick a workflow state.')
        break
    }
  })

type RuleFormValues = z.infer<typeof ruleFormSchema>

const EMPTY_FORM_VALUES: RuleFormValues = {
  name: '',
  rule_type: 'author',
  slack: true,
  enabled: true,
  author_id: '',
  collection_id: '',
  keywords: '',
  topic: '',
  min_score: '',
  lens_id: '',
  monitor_id: '',
  branch_id: '',
  workflow: '',
}

// ── Form → API shape conversion ───────────────────────────────────────────
// The API accepts a single `rule_config` object whose shape depends on
// `rule_type`. This switch is the ONLY place we translate form fields into
// that shape — so if a form field goes stale after a rule_type switch, it
// is never sent to the API. The branch-narrowing means values.min_score
// etc. are only read on the matching rule_type branch.

function buildRuleConfig(values: RuleFormValues): Record<string, unknown> {
  switch (values.rule_type) {
    case 'author':
      return { author_id: values.author_id.trim() }
    case 'collection':
      return { collection_id: values.collection_id }
    case 'keyword':
      return {
        keywords: values.keywords.split(',').map((k) => k.trim()).filter(Boolean),
      }
    case 'topic':
      return { topic: values.topic.trim() }
    case 'similarity':
      return { min_score: Number(values.min_score) }
    case 'discovery_lens':
      return { lens_id: values.lens_id }
    case 'feed_monitor':
      return { monitor_id: values.monitor_id, include_statuses: ['new'], lookback_days: 14 }
    case 'branch':
      return { branch_id: values.branch_id, min_score: 0.55 }
    case 'library_workflow':
      return { workflow: values.workflow, limit: 20 }
  }
}

// Reverse direction: populate form state from an existing AlertRule when
// opening the edit dialog. Any fields the rule's rule_type doesn't use are
// left at their empty defaults.
function ruleToFormValues(rule: AlertRule): RuleFormValues {
  const cfg = rule.rule_config
  const base: RuleFormValues = {
    ...EMPTY_FORM_VALUES,
    name: rule.name,
    rule_type: rule.rule_type as RuleType,
    slack: rule.channels.includes('slack'),
    enabled: rule.enabled,
  }
  switch (rule.rule_type as RuleType) {
    case 'author':
      return { ...base, author_id: String(cfg.author_id ?? '') }
    case 'collection':
      return { ...base, collection_id: String(cfg.collection_id ?? cfg.collection_name ?? '') }
    case 'keyword':
      return {
        ...base,
        keywords: Array.isArray(cfg.keywords) ? (cfg.keywords as unknown[]).join(', ') : '',
      }
    case 'topic':
      return { ...base, topic: String(cfg.topic ?? '') }
    case 'similarity':
      return { ...base, min_score: String(cfg.min_score ?? '') }
    case 'discovery_lens':
      return { ...base, lens_id: String(cfg.lens_id ?? '') }
    case 'feed_monitor':
      return { ...base, monitor_id: String(cfg.monitor_id ?? '') }
    case 'branch':
      return { ...base, branch_id: String(cfg.branch_id ?? cfg.branch_label ?? '') }
    case 'library_workflow':
      return { ...base, workflow: String(cfg.workflow ?? cfg.state ?? '') }
  }
}

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
  branches: Array<{ branch_id?: string; branch_label?: string; count?: number }>
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
  branches,
}: RuleFormDialogProps) {
  const form = useForm<RuleFormValues>({
    resolver: zodResolver(ruleFormSchema),
    defaultValues: initialValues,
    mode: 'onChange',
  })

  // Re-seed the form whenever the dialog opens or its initial values change
  // (e.g. switching from one rule to another via Edit).
  useEffect(() => {
    if (open) {
      form.reset(initialValues)
    }
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
                    <FormLabel>Author ID</FormLabel>
                    <FormControl>
                      <Input placeholder="e.g. MG9cVagAAAAJ" {...field} />
                    </FormControl>
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
              name="slack"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Channels</FormLabel>
                  <label className="flex items-center gap-2">
                    <FormControl>
                      <Checkbox
                        checked={field.value}
                        onCheckedChange={(v) => field.onChange(v === true)}
                      />
                    </FormControl>
                    <span className="text-sm text-slate-700">Slack</span>
                  </label>
                </FormItem>
              )}
            />

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
              <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
                <AlertCircle className="h-4 w-4 text-red-500" />
                <span className="text-sm text-red-700">{errorMessage}</span>
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

export function AlertRulesSection() {
  const [createOpen, setCreateOpen] = useState(false)
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null)
  const [deleteId, setDeleteId] = useState<string | null>(null)

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const rulesQuery = useQuery({
    queryKey: ['alert-rules'],
    queryFn: () => api.get<AlertRule[]>('/alerts/rules'),
    retry: 1,
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

  const rules = rulesQuery.data ?? []
  const lenses = lensesQuery.data ?? []
  const monitors = monitorsQuery.data ?? []
  const collections = collectionsQuery.data ?? []
  const branches = diagnosticsQuery.data?.discovery.branch_quality ?? []

  function bodyFromValues(values: RuleFormValues): AlertRuleBody {
    return {
      name: values.name.trim(),
      rule_type: values.rule_type,
      rule_config: buildRuleConfig(values),
      channels: values.slack ? ['slack'] : [],
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
                      {rule.channels.map((ch) => (
                        <Badge key={ch} variant="outline">
                          <Hash className="mr-1 h-3 w-3" />
                          {ch}
                        </Badge>
                      ))}
                    </div>
                    <p className="mt-1.5 text-xs text-slate-400">
                      Created {formatDate(rule.created_at)}
                    </p>
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => toggleMutation.mutate(rule.id)}
                      disabled={toggleMutation.isPending}
                      title={rule.enabled ? 'Disable' : 'Enable'}
                    >
                      <Power className={`h-4 w-4 ${rule.enabled ? 'text-green-500' : 'text-slate-400'}`} />
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
                      <Trash2 className="h-4 w-4 text-red-400" />
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
        onOpenChange={setCreateOpen}
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
        branches={branches}
      />

      {/* Edit dialog — open when editingRule is non-null */}
      <RuleFormDialog
        open={editingRule !== null}
        onOpenChange={(open) => {
          if (!open) setEditingRule(null)
        }}
        title="Edit Alert Rule"
        description="Update the alert rule configuration."
        submitLabel="Save Changes"
        initialValues={editingRule ? ruleToFormValues(editingRule) : EMPTY_FORM_VALUES}
        isPending={updateMutation.isPending}
        isError={updateMutation.isError}
        errorMessage="Failed to update rule."
        onSubmit={(values) => {
          if (!editingRule) return
          updateMutation.mutate({ id: editingRule.id, body: bodyFromValues(values) })
        }}
        lenses={lenses}
        collections={collections}
        monitors={monitors}
        branches={branches}
      />

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
