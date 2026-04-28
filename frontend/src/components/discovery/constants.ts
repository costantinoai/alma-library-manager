import {
  BookOpen,
  GitBranch,
  Link,
  Search,
  UserCheck,
  Users,
} from 'lucide-react'

// ── Pagination ──

export const PAGE_SIZE = 50

// ── Types ──

export type StatusFilter = 'all' | 'unseen' | 'liked' | 'dismissed'
export type SortField = 'score' | 'date' | 'title'

export interface DiscoveryStats {
  total: number
  /** Number of recommendations the user has taken an explicit action on
   *  (save / like / love / dismiss). Previously named `seen`, which
   *  implied "viewed"; we don't record impressions in this field. */
  actioned: number
  saved: number
  liked: number
  dismissed: number
}

export interface AuthorSuggestion {
  key: string
  name: string
  paper_count: number
  avg_score: number
  source_types: string[]
  sample_titles: string[]
}

// ── Source type configuration ──

export const FOLLOWED_SIGNAL_SOURCES = new Set<string>(['followed_author', 'openalex_topic'])

export const SOURCE_TYPE_CONFIG: Record<string, { label: string; icon: typeof Link; color: string; badgeClass: string }> = {
  openalex_related: {
    label: 'Related Works',
    icon: Link,
    color: '#3B82F6',
    badgeClass: 'bg-alma-100 text-alma-800 border-alma-200',
  },
  openalex_topic: {
    label: 'Topic Search',
    icon: Search,
    color: '#10B981',
    badgeClass: 'bg-green-100 text-green-800 border-green-200',
  },
  followed_author: {
    label: 'Followed Authors',
    icon: UserCheck,
    color: '#8B5CF6',
    badgeClass: 'bg-purple-100 text-purple-800 border-purple-200',
  },
  coauthor_network: {
    label: 'Co-author Network',
    icon: Users,
    color: '#F59E0B',
    badgeClass: 'bg-amber-100 text-amber-800 border-amber-200',
  },
  citation_chain: {
    label: 'Citation Chain',
    icon: GitBranch,
    color: '#EF4444',
    badgeClass: 'bg-red-100 text-red-800 border-red-200',
  },
  semantic_scholar: {
    label: 'Semantic Scholar',
    icon: BookOpen,
    color: '#06B6D4',
    badgeClass: 'bg-cyan-100 text-cyan-800 border-cyan-200',
  },
  preprint_lane: {
    label: 'Preprint Lane',
    icon: BookOpen,
    color: '#0891B2',
    badgeClass: 'bg-sky-100 text-sky-800 border-sky-200',
  },
  taste_topic: {
    label: 'Favorite Topic',
    icon: Search,
    color: '#0F766E',
    badgeClass: 'bg-teal-100 text-teal-800 border-teal-200',
  },
  taste_author: {
    label: 'Favorite Author',
    icon: UserCheck,
    color: '#B45309',
    badgeClass: 'bg-amber-100 text-amber-800 border-amber-200',
  },
  taste_venue: {
    label: 'Favorite Venue',
    icon: BookOpen,
    color: '#047857',
    badgeClass: 'bg-emerald-100 text-emerald-800 border-emerald-200',
  },
  recent_win: {
    label: 'Recent Win',
    icon: GitBranch,
    color: '#BE185D',
    badgeClass: 'bg-pink-100 text-pink-800 border-pink-200',
  },
}

export const STATUS_FILTERS: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'unseen', label: 'Unseen' },
  { value: 'liked', label: 'Liked' },
  { value: 'dismissed', label: 'Dismissed' },
]

// ── Author name parsing utilities ──

export function normalizeAuthorKey(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function looksLikeNoiseAuthorToken(token: string): boolean {
  const t = normalizeAuthorKey(token)
  return (
    !t ||
    t === 'et al' ||
    t === 'et al.' ||
    t === 'etal' ||
    t === 'anonymous' ||
    t === 'unknown'
  )
}

function parseAuthorNames(authors: string): string[] {
  const text = (authors ?? '').trim()
  if (!text) return []

  if (text.includes(';')) {
    return text.split(';').map((v) => v.trim()).filter(Boolean)
  }
  if (/\band\b/i.test(text)) {
    return text.split(/\band\b/i).map((v) => v.trim()).filter(Boolean)
  }

  const commaParts = text.split(',').map((v) => v.trim()).filter(Boolean)
  if (commaParts.length >= 4 && commaParts.length % 2 === 0) {
    const paired: string[] = []
    for (let i = 0; i < commaParts.length; i += 2) {
      paired.push(`${commaParts[i + 1]} ${commaParts[i]}`.trim())
    }
    return paired
  }
  if (commaParts.length >= 2 && commaParts.every((p) => p.includes(' '))) {
    return commaParts
  }
  return [text]
}

// ── Author suggestion aggregation ──

export function buildAuthorSuggestions(
  engineRecs: { recommended_authors?: string; score: number; source_type: string; recommended_title?: string }[],
  followedNameKeys: Set<string>,
): AuthorSuggestion[] {
  const byAuthor = new Map<
    string,
    {
      name: string
      paper_count: number
      score_sum: number
      source_types: Set<string>
      sample_titles: string[]
    }
  >()

  for (const rec of engineRecs) {
    const names = parseAuthorNames(rec.recommended_authors ?? '')
    for (const rawName of names) {
      const name = rawName.trim()
      const key = normalizeAuthorKey(name)
      if (!key || looksLikeNoiseAuthorToken(name) || followedNameKeys.has(key)) {
        continue
      }
      const current = byAuthor.get(key) ?? {
        name,
        paper_count: 0,
        score_sum: 0,
        source_types: new Set<string>(),
        sample_titles: [],
      }
      current.paper_count += 1
      current.score_sum += rec.score ?? 0
      current.source_types.add(rec.source_type)
      if (
        rec.recommended_title &&
        !current.sample_titles.includes(rec.recommended_title) &&
        current.sample_titles.length < 3
      ) {
        current.sample_titles.push(rec.recommended_title)
      }
      if (name.length > current.name.length) {
        current.name = name
      }
      byAuthor.set(key, current)
    }
  }

  return Array.from(byAuthor.entries())
    .map(([key, value]) => ({
      key,
      name: value.name,
      paper_count: value.paper_count,
      avg_score: value.paper_count > 0 ? value.score_sum / value.paper_count : 0,
      source_types: Array.from(value.source_types),
      sample_titles: value.sample_titles,
    }))
    .filter((item) => item.paper_count >= 2)
    .sort((a, b) => {
      if (b.paper_count !== a.paper_count) return b.paper_count - a.paper_count
      return b.avg_score - a.avg_score
    })
}
