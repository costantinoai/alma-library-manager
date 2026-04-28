import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { AlertCircle } from 'lucide-react'

import { Button } from '@/components/ui/button'
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
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'

// ── Schema ────────────────────────────────────────────────────────────────
// Name is optional but we always trim it on submit. At least one of the
// three identifiers must be present — enforced via `superRefine` with a
// form-level error on `path: []` so all three fields light up at once.

export interface AddAuthorPayload {
  name?: string
  scholar_id?: string
  openalex_id?: string
  orcid?: string
}

const addAuthorFormSchema = z
  .object({
    name: z.string().optional().default(''),
    scholarId: z.string().optional().default(''),
    openalexId: z.string().optional().default(''),
    orcid: z.string().optional().default(''),
  })
  .superRefine((data, ctx) => {
    const hasIdentifier =
      !!data.scholarId?.trim() || !!data.openalexId?.trim() || !!data.orcid?.trim()
    if (!hasIdentifier) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: [],
        message: 'Provide at least one identifier (Scholar ID, OpenAlex ID, or ORCID).',
      })
    }
  })

type AddAuthorFormValues = z.infer<typeof addAuthorFormSchema>

const EMPTY_VALUES: AddAuthorFormValues = {
  name: '',
  scholarId: '',
  openalexId: '',
  orcid: '',
}

interface AddAuthorDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onSubmit: (payload: AddAuthorPayload) => void
  isPending: boolean
  isError: boolean
}

export function AddAuthorDialog({
  open,
  onOpenChange,
  onSubmit,
  isPending,
  isError,
}: AddAuthorDialogProps) {
  const form = useForm<AddAuthorFormValues>({
    resolver: zodResolver(addAuthorFormSchema),
    defaultValues: EMPTY_VALUES,
    mode: 'onChange',
  })

  // Clear the form when the dialog closes so re-opening is a fresh slate.
  useEffect(() => {
    if (!open) {
      form.reset(EMPTY_VALUES)
    }
  }, [open, form])

  const handleSubmit = form.handleSubmit((values) => {
    onSubmit({
      name: values.name.trim() || undefined,
      scholar_id: values.scholarId.trim() || undefined,
      openalex_id: values.openalexId.trim() || undefined,
      orcid: values.orcid.trim() || undefined,
    })
  })

  // Root-level "at least one identifier" error surfaces below the fields.
  const rootError = form.formState.errors.root?.message

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add Author</DialogTitle>
          <DialogDescription>
            Add a monitored author using Scholar, OpenAlex, or ORCID. Name is optional but useful when remote lookup is incomplete.
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form id="add-author-form" onSubmit={handleSubmit} className="space-y-4 py-4">
            <FormField
              control={form.control}
              name="name"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Display Name (optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="e.g., Charlie Brown" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="scholarId"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Google Scholar ID</FormLabel>
                  <FormControl>
                    <Input placeholder="e.g., MG9cVagAAAAJ" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="openalexId"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>OpenAlex ID</FormLabel>
                  <FormControl>
                    <Input placeholder="e.g., A5084467223" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="orcid"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>ORCID</FormLabel>
                  <FormControl>
                    <Input placeholder="e.g., 0000-0002-1825-0097" {...field} />
                  </FormControl>
                  <FormDescription>
                    Provide at least one identifier. Newly added authors are followed automatically.
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* Form-level validation error (from superRefine path: []) */}
            {rootError && (
              <p className="text-sm font-medium text-red-500">{rootError}</p>
            )}

            {/* Mutation error (backend 4xx/5xx) */}
            {isError && (
              <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
                <AlertCircle className="h-4 w-4 text-red-500" />
                <span className="text-sm text-red-700">
                  Failed to add author. Please check the provided identifier and try again.
                </span>
              </div>
            )}
          </form>
        </Form>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button type="submit" form="add-author-form" loading={isPending} disabled={isPending}>
            Add Author
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
