import type { ComponentType } from 'react'
import {
  Newspaper,
  Users,
  Bell,
  Settings,
  Library,
  X,
  Sparkles,
  BarChart3,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { cn } from '@/lib/utils'
import { getBootstrap } from '@/api/client'
import type { BootstrapData } from '@/api/client'
import { BrandRule } from '@/components/ui/brand-rule'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'

export type Page =
  | 'feed'
  | 'discovery'
  | 'authors'
  | 'library'
  | 'insights'
  | 'alerts'
  | 'settings'

interface NavItem {
  id: Page
  label: string
  icon: ComponentType<{ className?: string }>
}

interface NavGroup {
  label: string
  items: NavItem[]
}

const navGroups: NavGroup[] = [
  {
    label: 'Explore',
    items: [
      { id: 'feed', label: 'Feed', icon: Newspaper },
      { id: 'discovery', label: 'Discovery', icon: Sparkles },
    ],
  },
  {
    label: 'Manage',
    items: [
      { id: 'authors', label: 'Authors', icon: Users },
      { id: 'library', label: 'Library', icon: Library },
      { id: 'alerts', label: 'Alerts', icon: Bell },
    ],
  },
  {
    label: 'Analyze',
    items: [{ id: 'insights', label: 'Insights', icon: BarChart3 }],
  },
  {
    label: 'Control',
    items: [{ id: 'settings', label: 'Settings', icon: Settings }],
  },
]

function getBadgeCount(pageId: Page, data: BootstrapData): number {
  switch (pageId) {
    case 'feed':
      return data.feed.unread
    case 'discovery':
      return data.discovery.pending_recommendations
    case 'alerts':
      return data.alerts.active
    default:
      return 0
  }
}

interface SidebarProps {
  currentPage: Page
  onNavigate: (page: Page) => void
  isOpen: boolean
  onClose: () => void
  /** Desktop-only: collapse to icon-rail width (~72px) when true. */
  collapsed: boolean
  /** Desktop-only: toggle between collapsed and expanded. */
  onToggleCollapsed: () => void
}

export function Sidebar({
  currentPage,
  onNavigate,
  isOpen,
  onClose,
  collapsed,
  onToggleCollapsed,
}: SidebarProps) {
  const { data: bootstrap } = useQuery({
    queryKey: ['bootstrap'],
    queryFn: getBootstrap,
    staleTime: 60_000,
    refetchInterval: 300_000,
  })

  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 lg:hidden"
          onClick={onClose}
        />
      )}

      <aside
        className={cn(
          // Cardstock cover — the sidebar reads as a small piece of
          // dyed carton paper: matte navy (alma-800 = #14233A), faint
          // fibre grain, gentle inner vignette so the corners settle.
          // Quiet and honest, not corporate-glossy. The Folio-blue accent on
          // active items is the only warm note; everything else stays
          // in the navy/cream pairing per branding/docs/accessibility.md.
          // Texture + shadow recipe lives in .alma-cardstock-cover
          // (index.css) — keep this class in sync when tweaking.
          'alma-cardstock-cover',
          'fixed inset-y-0 left-0 z-50 flex flex-col transition-[transform,width] duration-200 lg:translate-x-0',
          // Width is responsive to `collapsed` — but only on desktop.
          // On mobile the off-canvas drawer always uses the full width.
          collapsed ? 'w-[260px] lg:w-[72px]' : 'w-[260px]',
          isOpen ? 'translate-x-0' : '-translate-x-full',
        )}
      >
        {/*
         * Brand header — chrome "title plate" at the top of the sidebar
         * spine. Off-white (alma-chrome) bg makes it read as page-shell
         * trim, distinct from the navy cardstock cover below AND from
         * the warm canvas in the content area. When collapsed (desktop
         * icon-rail mode) the wordmark + subtitle hide and just the
         * brand mark survives, centered.
         */}
        <div
          className={cn(
            'relative flex flex-col items-center border-b border-[var(--color-border)] bg-alma-chrome shadow-paper-sm transition-[padding] duration-200',
            collapsed ? 'px-2 pb-2 pt-2 lg:px-1.5' : 'px-4 pb-2.5 pt-2.5',
          )}
        >
          <button
            onClick={onClose}
            className="absolute right-3 top-3 rounded-md p-1 text-alma-700 hover:bg-parchment-100 lg:hidden"
            aria-label="Close navigation"
          >
            <X className="h-5 w-5" />
          </button>
          <div className={cn('flex items-center', collapsed ? 'lg:gap-0' : 'gap-2.5')}>
            <img
              src="/brand/alma-mark-source.svg"
              alt=""
              aria-hidden
              className={cn(
                'shrink-0 transition-[height,width] duration-200',
                collapsed ? 'h-[68px] w-[68px] lg:h-[44px] lg:w-[44px]' : 'h-[68px] w-[68px]',
              )}
            />
            <span
              className={cn(
                'font-brand text-[34px] font-semibold leading-none tracking-[0.01em] text-alma-800',
                collapsed && 'lg:hidden',
              )}
            >
              ALMa
            </span>
          </div>
          {!collapsed && (
            <>
              <BrandRule center="dot" tone="gold" className="-mt-2 w-[160px]" />
              {/* Subtitle with the acronym letters (A · L · Ma) highlighted in
                  the title navy, the rest in Folio-blue — so the eye picks up
                  "ALMa" inside "Another Library Manager". */}
              <span className="mt-0.5 text-[9.5px] font-bold uppercase leading-none tracking-[0.16em] text-alma-folio">
                <span className="text-alma-800">A</span>nother{' '}
                <span className="text-alma-800">L</span>ibrary{' '}
                <span className="text-alma-800">Ma</span>nager
              </span>
            </>
          )}
        </div>

        {/* Navigation */}
        <nav className={cn('flex-1 overflow-y-auto py-4 transition-[padding] duration-200', collapsed ? 'px-2 lg:px-2' : 'px-3')}>
          <div className="space-y-5">
            {navGroups.map((group) => (
              <div key={group.label} className="space-y-1">
                <EyebrowLabel
                  tone="muted"
                  className={cn('px-3 !text-alma-400', collapsed && 'lg:hidden')}
                >
                  {group.label}
                </EyebrowLabel>
                {group.items.map((item) => {
                  const isActive = currentPage === item.id
                  const badge = bootstrap ? getBadgeCount(item.id, bootstrap) : 0
                  return (
                    <button
                      key={item.id}
                      onClick={() => {
                        onNavigate(item.id)
                        onClose()
                      }}
                      title={collapsed ? item.label : undefined}
                      aria-label={collapsed ? item.label : undefined}
                      className={cn(
                        // Base row sits on the cardstock cover. Hover
                        // lifts a faint white tint (cardstock catching
                        // light); active state uses a Folio-blue overlay so
                        // the cardstock fibre reads through the tint,
                        // and a 3px Folio-blue ribbon on the left echoes
                        // the bookmark in the ALMa mark itself.
                        'group relative flex w-full items-center rounded-sm py-2.5 text-sm font-medium transition-colors',
                        collapsed ? 'lg:justify-center lg:gap-0 lg:px-2 gap-3 px-3' : 'gap-3 px-3',
                        isActive
                          ? 'bg-[rgb(30_91_134_/_0.32)] text-alma-cream'
                          : 'text-alma-200 hover:bg-white/[0.04] hover:text-alma-cream',
                      )}
                    >
                      {isActive && (
                        <span
                          className="absolute left-0 top-1.5 bottom-1.5 w-[3px] rounded-r bg-alma-folio"
                          aria-hidden
                        />
                      )}
                      <item.icon className="h-5 w-5 shrink-0" />
                      <span className={cn(collapsed && 'lg:hidden')}>{item.label}</span>
                      {badge > 0 && !collapsed && (
                        <span className="ml-auto rounded-full bg-alma-folio px-1.5 py-0.5 text-[10px] font-medium text-alma-cream">
                          {badge}
                        </span>
                      )}
                      {badge > 0 && collapsed && (
                        // Collapsed: badge becomes a tiny dot top-right of
                        // the icon so the count cue isn't lost.
                        <span
                          className="absolute right-1.5 top-1.5 hidden h-1.5 w-1.5 rounded-full bg-alma-folio lg:block"
                          aria-label={`${badge} new papers`}
                        />
                      )}
                    </button>
                  )
                })}
              </div>
            ))}
          </div>
        </nav>

        {/* Footer — the cardstock cover ends with a thin inner-binding
            hairline (alma-700 on the navy). Carries the wordmark line
            when expanded and the desktop-only collapse toggle. */}
        <div
          className={cn(
            'border-t border-alma-700/70 transition-[padding] duration-200',
            collapsed ? 'px-2 py-3 lg:px-2' : 'p-4',
          )}
        >
          {!collapsed && (
            <>
              <div className="font-brand text-[11px] uppercase tracking-[0.18em] text-alma-300">
                <span className="text-alma-100">A</span>·<span className="text-alma-100">L</span>·<span className="text-alma-100">Ma</span>
              </div>
              <div className="mt-0.5 text-[10px] text-alma-400">Another Library Manager</div>
              <a
                href="https://costantinoai.github.io/alma-library-manager/"
                target="_blank"
                rel="noreferrer"
                className="mt-2 inline-flex items-center gap-1 text-[10px] text-alma-400 hover:text-alma-cream"
              >
                Documentation ↗
              </a>
            </>
          )}
          <button
            type="button"
            onClick={onToggleCollapsed}
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            className={cn(
              'hidden items-center justify-center gap-2 rounded-sm py-1.5 text-[11px] font-medium uppercase tracking-[0.16em] text-alma-300 transition-colors hover:bg-white/[0.04] hover:text-alma-cream lg:flex',
              collapsed ? 'lg:w-full' : 'mt-3 w-full',
            )}
          >
            {collapsed ? (
              <PanelLeftOpen className="h-4 w-4" />
            ) : (
              <>
                <PanelLeftClose className="h-4 w-4" />
                <span>Collapse</span>
              </>
            )}
          </button>
        </div>
      </aside>
    </>
  )
}
