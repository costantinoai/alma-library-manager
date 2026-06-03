import { useState } from 'react'
import { Plus, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { JargonHint } from '@/components/shared'
import {
  createFeedMonitor,
  deleteFeedMonitor,
  getApiErrorMessage,
  refreshFeedInbox,
} from '@/api/client'
import { toast, errorToast } from '@/hooks/useToast'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

const EXAMPLES = [
  'universality AND representations',
  'face processing',
  '(manifold OR topology) AND representations NOT images',
]

interface LocalMonitor {
  id: string
  query: string
}

export function StepKeywords({ state, patch, next, back }: StepContext) {
  const [value, setValue] = useState('')
  const [monitors, setMonitors] = useState<LocalMonitor[]>(
    state.keywords.map((q) => ({ id: '', query: q })),
  )
  const [adding, setAdding] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const add = async (query: string) => {
    const q = query.trim()
    if (!q) return
    if (monitors.some((m) => m.query.toLowerCase() === q.toLowerCase())) {
      setValue('')
      return
    }
    setAdding(true)
    try {
      const m = await createFeedMonitor({ monitor_type: 'query', query: q, label: q })
      const nextMonitors = [...monitors, { id: m.id, query: q }]
      setMonitors(nextMonitors)
      patch({ keywords: nextMonitors.map((x) => x.query) })
      setValue('')
    } catch (err) {
      errorToast('Could not add that monitor', getApiErrorMessage(err))
    } finally {
      setAdding(false)
    }
  }

  const remove = async (mon: LocalMonitor) => {
    const nextMonitors = monitors.filter((m) => m.query !== mon.query)
    setMonitors(nextMonitors)
    patch({ keywords: nextMonitors.map((x) => x.query) })
    if (mon.id) {
      try {
        await deleteFeedMonitor(mon.id)
      } catch {
        /* non-fatal */
      }
    }
  }

  const goContinue = async () => {
    // The Feed refresh is fired here — once monitors exist — not earlier.
    if (monitors.length > 0) {
      setRefreshing(true)
      try {
        await refreshFeedInbox()
        toast({ title: 'Watching for new papers', description: 'Your Feed is refreshing in the background.' })
      } catch {
        /* non-fatal */
      } finally {
        setRefreshing(false)
      }
    }
    next()
  }

  return (
    <StepShell
      eyebrow="Watch the literature"
      title="What topics should I keep an eye on?"
      lead="Keyword monitors scan new publications for the phrases you care about and surface matches in your Feed. Add as many as you like — you can edit them later in Settings."
      footer={
        <StepNav
          onBack={back}
          onSkip={next}
          onContinue={goContinue}
          continueLabel={monitors.length > 0 ? 'Save & refresh feed' : 'Continue'}
          continueLoading={refreshing}
        />
      }
    >
      <div className="space-y-5">
        <ConceptCallout
          eyebrow="How do monitors work?"
          summary="Each monitor is a search ALMa re-runs for you; matches appear in the Feed."
        >
          <p>
            A monitor is a standing query over new papers' titles and abstracts. You can use plain
            phrases, or boolean expressions with{' '}
            <span className="inline-flex items-center gap-1">
              AND / OR / NOT and parentheses
              <JargonHint
                title="Boolean operators"
                description={
                  <span>
                    Combine terms with <code>AND</code>, <code>OR</code>, <code>NOT</code> and group with
                    parentheses. Quotes match an exact phrase. Example:{' '}
                    <code>(manifold OR topology) AND representations NOT images</code>.
                  </span>
                }
              />
            </span>
            . Monitors live in Settings → Feed Monitor Controls, where you can edit or remove them anytime.
          </p>
        </ConceptCallout>

        <div className="space-y-2">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Try an example</p>
          <div className="flex flex-wrap gap-2">
            {EXAMPLES.map((ex) => (
              <button
                key={ex}
                type="button"
                onClick={() => setValue(ex)}
                className="rounded-full border border-[var(--color-border)] bg-surface-2 px-3 py-1 text-xs text-slate-600 transition-colors hover:border-alma-folio hover:text-alma-folio"
              >
                {ex}
              </button>
            ))}
          </div>
        </div>

        <div className="flex gap-2">
          <Input
            placeholder="Type a keyword or boolean expression…"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') add(value)
            }}
          />
          <Button variant="accent" onClick={() => add(value)} disabled={!value.trim()} loading={adding}>
            <Plus className="h-4 w-4" /> Add
          </Button>
        </div>

        {monitors.length > 0 ? (
          <ul className="space-y-2">
            {monitors.map((m) => (
              <li
                key={m.query}
                className="flex items-center justify-between gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 px-3 py-2"
              >
                <code className="truncate text-sm text-alma-800">{m.query}</code>
                <button
                  type="button"
                  onClick={() => remove(m)}
                  className="shrink-0 rounded-sm p-1 text-slate-400 transition-colors hover:bg-surface-3 hover:text-critical-600"
                  aria-label={`Remove monitor ${m.query}`}
                >
                  <X className="h-4 w-4" />
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p className="text-sm text-slate-500">No monitors yet — add one above, or skip and set them up later.</p>
        )}
      </div>
    </StepShell>
  )
}
