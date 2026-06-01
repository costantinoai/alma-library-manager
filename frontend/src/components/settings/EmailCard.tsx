import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation } from '@tanstack/react-query'
import { Mail, Send } from 'lucide-react'

import { testPluginConnection, type Settings } from '@/api/client'
import { AsyncButton, SettingsCard } from '@/components/settings/primitives'
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
import { Switch } from '@/components/ui/switch'
import { useToast, errorToast } from '@/hooks/useToast'

const emailSchema = z.object({
  smtp_host: z.string(),
  smtp_port: z.number().int().min(1, 'Port 1–65535').max(65535, 'Port 1–65535'),
  smtp_username: z.string(),
  smtp_password: z.string(),
  smtp_from: z.string(),
  smtp_to: z.string(),
  smtp_use_tls: z.boolean(),
})

type EmailForm = z.infer<typeof emailSchema>

interface EmailCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
}

/**
 * EmailCard — SMTP digest channel config (sibling of ChannelsCard / Slack).
 * The password is stored in the secret store; GET returns it masked, and the
 * backend skips re-saving a masked echo, so leaving it untouched keeps the key.
 * To actually receive digests here, add "Email" to an alert's channels.
 */
export function EmailCard({ formData, onFormDataChange }: EmailCardProps) {
  const { toast } = useToast()

  const form = useForm<EmailForm>({
    resolver: zodResolver(emailSchema),
    defaultValues: {
      smtp_host: formData.smtp_host ?? '',
      smtp_port: formData.smtp_port ?? 587,
      smtp_username: formData.smtp_username ?? '',
      smtp_password: formData.smtp_password ?? '',
      smtp_from: formData.smtp_from ?? '',
      smtp_to: formData.smtp_to ?? '',
      smtp_use_tls: formData.smtp_use_tls ?? true,
    },
    mode: 'onBlur',
  })

  useEffect(() => {
    form.reset({
      smtp_host: formData.smtp_host ?? '',
      smtp_port: formData.smtp_port ?? 587,
      smtp_username: formData.smtp_username ?? '',
      smtp_password: formData.smtp_password ?? '',
      smtp_from: formData.smtp_from ?? '',
      smtp_to: formData.smtp_to ?? '',
      smtp_use_tls: formData.smtp_use_tls ?? true,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    formData.smtp_host,
    formData.smtp_port,
    formData.smtp_username,
    formData.smtp_password,
    formData.smtp_from,
    formData.smtp_to,
    formData.smtp_use_tls,
  ])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        smtp_host: values.smtp_host ?? '',
        smtp_port: values.smtp_port ?? 587,
        smtp_username: values.smtp_username ?? '',
        smtp_password: values.smtp_password ?? '',
        smtp_from: values.smtp_from ?? '',
        smtp_to: values.smtp_to ?? '',
        smtp_use_tls: values.smtp_use_tls ?? true,
      }))
    })
    return () => sub.unsubscribe()
  }, [form, onFormDataChange])

  const testEmailMutation = useMutation({
    mutationFn: () => testPluginConnection('email'),
    onSuccess: (result) => {
      if (result.ok) {
        toast({
          title: 'Test email sent',
          description: result.target ? `Delivered to ${result.target}.` : result.message,
        })
      } else {
        errorToast('Email test failed', result.error || result.message || 'Check your SMTP settings.')
      }
    },
    onError: (err) => {
      errorToast('Email test failed', err instanceof Error ? err.message : 'Check your SMTP settings.')
    },
  })

  return (
    <SettingsCard
      icon={Mail}
      title="Email digests"
      description="Send new-paper digests to your inbox over SMTP. Add “Email” to an alert's channels to use it."
    >
      <Form {...form}>
        <form className="space-y-4" onSubmit={(e) => e.preventDefault()}>
          <div className="grid gap-4 sm:grid-cols-[1fr_auto]">
            <FormField
              control={form.control}
              name="smtp_host"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>SMTP host</FormLabel>
                  <FormControl>
                    <Input placeholder="smtp.gmail.com" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="smtp_port"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Port</FormLabel>
                  <FormControl>
                    <Input
                      type="number"
                      min={1}
                      max={65535}
                      className="w-24"
                      value={field.value}
                      onChange={(e) => field.onChange(parseInt(e.target.value, 10) || 0)}
                      onBlur={field.onBlur}
                      name={field.name}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          </div>

          <FormField
            control={form.control}
            name="smtp_username"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Username</FormLabel>
                <FormControl>
                  <Input placeholder="you@gmail.com" autoComplete="off" {...field} />
                </FormControl>
                <FormDescription>Leave blank for an unauthenticated relay.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="smtp_password"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Password</FormLabel>
                <FormControl>
                  <Input type="password" placeholder="App password or SMTP key" autoComplete="off" {...field} />
                </FormControl>
                <FormDescription>
                  Stored encrypted in the secret store, never in plain settings. Leave the masked value to keep it.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="smtp_from"
            render={({ field }) => (
              <FormItem>
                <FormLabel>From address</FormLabel>
                <FormControl>
                  <Input placeholder="alma@yourdomain.com" {...field} />
                </FormControl>
                <FormDescription>Defaults to the username when blank.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="smtp_to"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Send digests to</FormLabel>
                <FormControl>
                  <Input placeholder="you@university.edu, colleague@lab.org" {...field} />
                </FormControl>
                <FormDescription>One or more recipients, separated by commas.</FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="smtp_use_tls"
            render={({ field }) => (
              <FormItem className="flex items-center justify-between gap-4">
                <div>
                  <FormLabel>Use STARTTLS</FormLabel>
                  <FormDescription>Recommended for port 587. Ignored on port 465 (implicit TLS).</FormDescription>
                </div>
                <FormControl>
                  <Switch checked={field.value} onCheckedChange={field.onChange} />
                </FormControl>
              </FormItem>
            )}
          />

          <AsyncButton
            type="button"
            variant="outline"
            icon={<Send className="h-4 w-4" />}
            pending={testEmailMutation.isPending}
            onClick={() => testEmailMutation.mutate()}
          >
            Send test email
          </AsyncButton>
        </form>
      </Form>
    </SettingsCard>
  )
}
