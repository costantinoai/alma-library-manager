/**
 * UserProfileCard — who ALMa is working for.
 *
 * ALMa puts a person at the centre of a literature, so the person is the first
 * thing Settings talks about. Both facts here were previously write-once,
 * settable only during onboarding: the greeting name, and the author row you
 * claimed as yourself. A name you typed once and an ORCID you got wrong were
 * then unreachable forever — you had to reset onboarding to fix a typo.
 *
 * The two are deliberately separate:
 *   - **Name** is local decoration. It never leaves the machine.
 *   - **You** is an identity claim with real consequences: it follows your own
 *     author profile, backfills your papers, and marks that row as the owner so
 *     Feed, Authors and the maps can tell "your work" from everyone else's.
 *
 * Both reuse the canonical routes onboarding already owns — resolve → confirm →
 * ingest — rather than growing a second way to do the same thing. Un-claiming
 * clears the owner flag WITHOUT unfollowing: "I am not this person" and "I do
 * not want their papers" are different statements.
 */
import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, IdCard, UserRound } from 'lucide-react'

import {
  clearOwnerIdentity,
  getApiErrorMessage,
  getUserProfile,
  ingestOwner,
  resolveOwnerIdentity,
  setOnboardingProfile,
  type OwnerProfile,
} from '@/api/client'
import { AsyncButton, SettingsCard } from '@/components/settings/primitives'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { SubPanel } from '@/components/ui/sub-panel'
import { errorToast, useToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

export function UserProfileCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

  const profileQuery = useQuery({
    queryKey: ['user-profile'],
    queryFn: getUserProfile,
    staleTime: 30_000,
  })
  const profile = profileQuery.data

  const [name, setName] = useState('')
  // Seed the field once the server answers, but never clobber an edit in
  // progress — a refetch mid-typing would otherwise wipe what you just wrote.
  const [nameTouched, setNameTouched] = useState(false)
  useEffect(() => {
    if (!nameTouched && profile?.name != null) setName(profile.name)
  }, [profile?.name, nameTouched])

  const [identifier, setIdentifier] = useState('')
  const [candidate, setCandidate] = useState<OwnerProfile | null>(null)

  const saveName = useMutation({
    mutationFn: (value: string) => setOnboardingProfile(value),
    onSuccess: async () => {
      setNameTouched(false)
      await invalidateQueries(queryClient, ['user-profile'], ['home-brief'])
      toast({ title: 'Name saved', description: 'ALMa will greet you by it.' })
    },
    onError: (error) => errorToast('Could not save your name', getApiErrorMessage(error)),
  })

  // An ORCID and a bare OpenAlex author id look nothing alike, so the field
  // takes either and decides here rather than making you pick a type first.
  const resolve = useMutation({
    mutationFn: (raw: string) =>
      resolveOwnerIdentity(
        /^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/i.test(raw.trim().replace(/^https?:\/\/orcid\.org\//i, ''))
          ? { orcid: raw.trim() }
          : { openalex_id: raw.trim() },
      ),
    onSuccess: (found) => setCandidate(found),
    onError: (error) => errorToast('Could not find that profile', getApiErrorMessage(error)),
  })

  const claim = useMutation({
    mutationFn: (found: OwnerProfile) =>
      ingestOwner({ openalex_id: found.openalex_id, name: found.name ?? undefined }),
    onSuccess: async () => {
      setCandidate(null)
      setIdentifier('')
      await invalidateQueries(
        queryClient,
        ['user-profile'],
        ['authors'],
        ['library-followed-authors'],
      )
      toast({
        title: 'That’s you',
        description: 'Your papers are being backfilled in the background.',
      })
    },
    onError: (error) => errorToast('Could not claim that profile', getApiErrorMessage(error)),
  })

  const unclaim = useMutation({
    mutationFn: clearOwnerIdentity,
    onSuccess: async () => {
      await invalidateQueries(
        queryClient,
        ['user-profile'],
        ['authors'],
        ['library-followed-authors'],
      )
      toast({
        title: 'No longer marked as you',
        description: 'They stay followed — unfollow from Authors if you want that too.',
      })
    },
    onError: (error) => errorToast('Could not clear that', getApiErrorMessage(error)),
  })

  const owner = profile?.owner ?? null
  const nameDirty = name.trim() !== (profile?.name ?? '').trim()

  return (
    <SettingsCard
      icon={UserRound}
      title="You"
      description="The name ALMa greets you by, and the author profile it treats as your own work."
    >
      <div className="space-y-5">
        {/* ── Display name ─────────────────────────────────────────────── */}
        <div className="max-w-sm space-y-2">
          <Label htmlFor="settings-user-name">Your name</Label>
          <div className="flex gap-2">
            <Input
              id="settings-user-name"
              value={name}
              placeholder="e.g. Andrea"
              maxLength={120}
              onChange={(event) => {
                setNameTouched(true)
                setName(event.target.value)
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter' && nameDirty && name.trim()) {
                  saveName.mutate(name.trim())
                }
              }}
            />
            <AsyncButton
              size="sm"
              disabled={!nameDirty || !name.trim()}
              pending={saveName.isPending}
              onClick={() => saveName.mutate(name.trim())}
            >
              Save
            </AsyncButton>
          </div>
          <p className="text-xs text-slate-500">
            Local only — used for the greeting on Home. It is never sent anywhere.
          </p>
        </div>

        {/* ── Owner identity ───────────────────────────────────────────── */}
        <div className="space-y-2">
          <Label>Your author profile</Label>
          {owner ? (
            <SubPanel className="flex flex-wrap items-center justify-between gap-3">
              <div className="min-w-0">
                <p className="flex items-center gap-2 text-sm font-medium text-alma-800">
                  <IdCard className="h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
                  {owner.name ?? owner.author_id}
                  <StatusBadge tone="accent" size="sm">
                    This is you
                  </StatusBadge>
                </p>
                <p className="mt-1 flex flex-wrap gap-x-3 text-xs text-slate-500">
                  {owner.orcid && (
                    <a
                      className="inline-flex items-center gap-1 hover:text-alma-folio hover:underline"
                      href={`https://orcid.org/${owner.orcid}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      ORCID {owner.orcid}
                      <ExternalLink className="h-3 w-3" aria-hidden />
                    </a>
                  )}
                  {owner.openalex_id && (
                    <a
                      className="inline-flex items-center gap-1 hover:text-alma-folio hover:underline"
                      href={`https://openalex.org/${owner.openalex_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      OpenAlex {owner.openalex_id}
                      <ExternalLink className="h-3 w-3" aria-hidden />
                    </a>
                  )}
                </p>
              </div>
              <AsyncButton
                size="sm"
                variant="outline"
                pending={unclaim.isPending}
                onClick={() => unclaim.mutate()}
              >
                Not me
              </AsyncButton>
            </SubPanel>
          ) : (
            <p className="text-sm text-slate-500">
              Not set. Claiming your profile follows your own work, backfills your
              papers, and lets ALMa tell your output apart from everyone else&apos;s.
            </p>
          )}

          {/* Claim / replace. Shown even when set: correcting a wrong claim is
              exactly the case that used to need an onboarding reset. */}
          <div className="flex max-w-xl flex-wrap gap-2 pt-1">
            <Input
              value={identifier}
              placeholder="ORCID (0000-0002-1825-0097) or OpenAlex id (A5023888391)"
              onChange={(event) => {
                setIdentifier(event.target.value)
                setCandidate(null)
              }}
              onKeyDown={(event) => {
                if (event.key === 'Enter' && identifier.trim()) resolve.mutate(identifier)
              }}
            />
            <AsyncButton
              size="sm"
              variant="outline"
              disabled={!identifier.trim()}
              pending={resolve.isPending}
              onClick={() => resolve.mutate(identifier)}
            >
              Look up
            </AsyncButton>
          </div>

          {candidate && (
            <SubPanel className="mt-2 flex flex-wrap items-center justify-between gap-3">
              <div className="min-w-0">
                <p className="text-sm font-medium text-alma-800">
                  {candidate.name ?? candidate.openalex_id}
                </p>
                <p className="mt-0.5 text-xs text-slate-500">
                  {[
                    candidate.institution,
                    `${candidate.works_count.toLocaleString()} works`,
                    `${candidate.cited_by_count.toLocaleString()} citations`,
                  ]
                    .filter(Boolean)
                    .join(' · ')}
                </p>
              </div>
              <div className="flex gap-2">
                <Button size="sm" variant="ghost" onClick={() => setCandidate(null)}>
                  Cancel
                </Button>
                <AsyncButton
                  size="sm"
                  pending={claim.isPending}
                  onClick={() => claim.mutate(candidate)}
                >
                  {owner ? 'Use this instead' : 'That’s me'}
                </AsyncButton>
              </div>
            </SubPanel>
          )}
        </div>
      </div>
    </SettingsCard>
  )
}
