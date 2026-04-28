import { useCallback, useEffect, useMemo, useState } from 'react'
import type { CSSProperties } from 'react'
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type ColumnOrderState,
  type RowData,
  type SortingState,
  type VisibilityState,
} from '@tanstack/react-table'

// Extend tanstack's ColumnMeta with a cell-overflow contract. Every td in
// the primitive is a clip-box; this hint chooses what the wrapper inside
// does with content that doesn't fit — ellipsize (default), wrap, or
// render as-is (for interactive controls / custom layouts).
declare module '@tanstack/react-table' {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<TData extends RowData, TValue> {
    cellOverflow?: 'ellipsis' | 'wrap' | 'none'
  }
}
import {
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from '@dnd-kit/core'
import {
  SortableContext,
  horizontalListSortingStrategy,
  arrayMove,
  useSortable,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { ArrowDown, ArrowUp, ArrowUpDown, GripVertical, SlidersHorizontal } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { cn } from '@/lib/utils'

/**
 * Shared data-table primitive.
 *
 * Built on `@tanstack/react-table` (headless engine) + `@dnd-kit/sortable`
 * (drag-reorder of header cells). Feed compact view, Library compact view,
 * and any future tabular surface all plug into this single component so the
 * look, keyboard behaviour, and column-management UX stay consistent.
 *
 * Features:
 * - Column visibility toggle (dropdown menu, persists to localStorage)
 * - Column drag-reorder (persists to localStorage)
 * - Column resize (drag handle on header divider, persists to localStorage)
 * - Sort (click header; respects per-column `enableSorting`)
 * - Row click (with a `stopPropagation` escape hatch for inner controls)
 *
 * ``storageKey`` is required for persistence and should be unique per
 * surface ("feed.compact", "library.all", …).
 */

interface DataTableProps<T> {
  data: T[]
  columns: ColumnDef<T, any>[]
  storageKey: string
  /** Called when a row body is clicked (not when an inner control is). */
  onRowClick?: (row: T) => void
  /** Pick the key for a row — defaults to ``row.id`` if present. */
  getRowId?: (row: T, index: number) => string
  /** Extra className on the row. */
  rowClassName?: (row: T) => string
  /** Default column sizing. */
  defaultColumnWidth?: number
  /** Optional empty-state content. */
  emptyState?: React.ReactNode
  /** Caption shown in the bottom-right of the table footer. */
  footerCaption?: React.ReactNode
  /** Controlled sort state. When provided, DataTable does NOT sort internally
      — it forwards header clicks to ``onSortingChange`` and assumes the
      caller has already sorted `data`. Use this for server-side sort. */
  sorting?: SortingState
  onSortingChange?: (updater: SortingState | ((prev: SortingState) => SortingState)) => void
  /** When true (with controlled sorting), DataTable skips its own sort step. */
  manualSorting?: boolean
  /**
   * Opt-in row selection. When `selectedIds` + `onSelectionChange` are both
   * provided, DataTable:
   *   - Prepends a non-sortable, non-hideable, non-resizable select column
   *     at index 0 with a Checkbox per row.
   *   - Applies a subtle alma tint (`bg-alma-50/40`) to every selected row.
   * Selection state is controlled — the caller owns the Set and receives
   * mutations via `onSelectionChange`. Row ids come from `getRowId` when
   * provided, otherwise from `row.original.id`, otherwise the row index.
   *
   * Callers typically maintain their own external bulk-action bar because
   * the bar often spans multiple view modes (cards + compact), so DataTable
   * does NOT render a bulk bar of its own — only the selection affordances
   * inside the table.
   */
  selectedIds?: ReadonlySet<string>
  onSelectionChange?: (next: Set<string>) => void
}

interface PersistedTableState {
  visibility?: VisibilityState
  order?: ColumnOrderState
  sizing?: Record<string, number>
  sorting?: SortingState
}

function loadPersisted(storageKey: string): PersistedTableState {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(`alma.datatable.${storageKey}`)
    if (!raw) return {}
    return JSON.parse(raw) as PersistedTableState
  } catch {
    return {}
  }
}

function savePersisted(storageKey: string, state: PersistedTableState): void {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(`alma.datatable.${storageKey}`, JSON.stringify(state))
  } catch {
    // Quota / privacy mode — ignore.
  }
}

export function DataTable<T>({
  data,
  columns,
  storageKey,
  onRowClick,
  getRowId,
  rowClassName,
  defaultColumnWidth = 160,
  emptyState,
  footerCaption,
  sorting: controlledSorting,
  onSortingChange: controlledSortingChange,
  manualSorting = false,
  selectedIds,
  onSelectionChange,
}: DataTableProps<T>) {
  const persisted = useMemo(() => loadPersisted(storageKey), [storageKey])
  const selectionEnabled = selectedIds !== undefined && onSelectionChange !== undefined

  // Derive a stable string id for a row. Precedence: caller-supplied
  // `getRowId`, then `row.original.id`, then the row index.
  const deriveRowId = useCallback((row: T, index: number): string => {
    if (getRowId) return getRowId(row, index)
    if (typeof row === 'object' && row !== null && 'id' in (row as object)) {
      return String((row as { id: unknown }).id)
    }
    return String(index)
  }, [getRowId])

  // When selection is enabled we prepend a pinned "select" column. Using a
  // sentinel id (``__select__``) keeps it clear of any user-supplied column
  // keys and the `enable*` flags make it invisible to the column-management
  // toolbar.
  const augmentedColumns = useMemo<ColumnDef<T, any>[]>(() => {
    if (!selectionEnabled) return columns
    const selectCol: ColumnDef<T, any> = {
      id: '__select__',
      header: () => <span className="sr-only">Select row</span>,
      enableSorting: false,
      enableHiding: false,
      enableResizing: false,
      size: 40,
      meta: { cellOverflow: 'none' },
      cell: ({ row }) => {
        const id = deriveRowId(row.original, row.index)
        const isSel = selectedIds!.has(id)
        return (
          <div onClick={(e) => e.stopPropagation()}>
            <Checkbox
              aria-label="Select row"
              checked={isSel}
              onCheckedChange={() => {
                const next = new Set(selectedIds!)
                if (isSel) next.delete(id)
                else next.add(id)
                onSelectionChange!(next)
              }}
            />
          </div>
        )
      },
    }
    return [selectCol, ...columns]
  }, [columns, selectionEnabled, selectedIds, onSelectionChange, deriveRowId])

  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(persisted.visibility ?? {})
  const [columnOrder, setColumnOrder] = useState<ColumnOrderState>(
    persisted.order ?? augmentedColumns.map((c) => String(c.id ?? (c as any).accessorKey ?? '')),
  )
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>(persisted.sizing ?? {})
  const [internalSorting, setInternalSorting] = useState<SortingState>(persisted.sorting ?? [])
  const sorting = controlledSorting ?? internalSorting
  const setSorting = controlledSortingChange ?? setInternalSorting

  // Keep column order in sync if the caller adds/removes columns.
  //
  // Merge contract: preserve the user's persisted order for columns that
  // still exist in `incoming`; insert brand-new columns at their
  // `incoming` array position (not at the end). The append-at-end
  // shortcut is surprising when a caller adds a first column — e.g. a
  // leading select checkbox — because existing users would see it at
  // the right edge instead of the left. Respecting the incoming index
  // makes "add a pinned-first column" just work without forcing a
  // storageKey bump that would wipe the user's visibility / resize /
  // sort state on the same table.
  useEffect(() => {
    setColumnOrder((prev) => {
      const incoming = augmentedColumns.map((c) => String(c.id ?? (c as any).accessorKey ?? ''))
      const prevSet = new Set(prev)
      const incomingSet = new Set(incoming)
      const result = prev.filter((id) => incomingSet.has(id))
      incoming.forEach((id, idx) => {
        if (!prevSet.has(id)) {
          const insertAt = Math.min(idx, result.length)
          result.splice(insertAt, 0, id)
        }
      })
      return result
    })
  }, [augmentedColumns])

  // Persist on any change.
  useEffect(() => {
    savePersisted(storageKey, {
      visibility: columnVisibility,
      order: columnOrder,
      sizing: columnSizing,
      sorting,
    })
  }, [storageKey, columnVisibility, columnOrder, columnSizing, sorting])

  const table = useReactTable({
    data,
    columns: augmentedColumns,
    state: {
      columnVisibility,
      columnOrder,
      columnSizing,
      sorting,
    },
    defaultColumn: { size: defaultColumnWidth, minSize: 60, maxSize: 800 },
    onColumnVisibilityChange: setColumnVisibility,
    onColumnOrderChange: setColumnOrder,
    onColumnSizingChange: setColumnSizing,
    onSortingChange: setSorting,
    manualSorting,
    // 2-state sort: first click = asc, second = desc, then cycles back
    // instead of clearing. Sort starts at ascending regardless of data type
    // so headers behave the same whether the column is text, number, or date.
    sortDescFirst: false,
    enableSortingRemoval: false,
    columnResizeMode: 'onChange',
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: manualSorting ? undefined : getSortedRowModel(),
    getRowId: getRowId ? (row, index) => getRowId(row, index) : undefined,
  })

  // Once the user has actively resized a column, switch to `table-layout:
  // fixed` so the widths are authoritative and cells can no longer reflow
  // against each other. Until then, `table-layout: auto` lets the browser
  // pick content-driven widths within the min/max bounds from `defaultColumn`.
  const hasUserSizedColumns = Object.keys(columnSizing).length > 0

  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 4 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 150, tolerance: 5 } }),
    useSensor(KeyboardSensor),
  )

  const onDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event
    if (!over || active.id === over.id) return
    setColumnOrder((prev) => {
      const oldIndex = prev.indexOf(String(active.id))
      const newIndex = prev.indexOf(String(over.id))
      if (oldIndex < 0 || newIndex < 0) return prev
      return arrayMove(prev, oldIndex, newIndex)
    })
  }, [])

  const visibleColumnIds = table.getVisibleLeafColumns().map((c) => c.id)
  const hasRows = table.getRowModel().rows.length > 0

  return (
    <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome">
      {/* ── Toolbar: column visibility menu only (other tools live on the
          table headers themselves for locality). ──────────────────────── */}
      <div className="flex items-center justify-end gap-2 border-b border-[var(--color-border)] px-2 py-1.5">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="xs" className="text-slate-500 hover:text-slate-800">
              <SlidersHorizontal className="h-3.5 w-3.5" />
              Columns
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuLabel className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Show columns
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            {table
              .getAllLeafColumns()
              .filter((col) => col.getCanHide())
              .map((col) => {
                const header = col.columnDef.header
                const label = typeof header === 'string' ? header : (col.columnDef.meta as any)?.label ?? col.id
                return (
                  <DropdownMenuCheckboxItem
                    key={col.id}
                    checked={col.getIsVisible()}
                    onCheckedChange={(value) => col.toggleVisibility(!!value)}
                    onSelect={(e) => e.preventDefault()}
                  >
                    {label}
                  </DropdownMenuCheckboxItem>
                )
              })}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="overflow-x-auto">
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
          <table
            className={cn(
              'w-full border-collapse text-sm',
              hasUserSizedColumns ? 'table-fixed' : 'table-auto',
            )}
            style={hasUserSizedColumns ? { width: table.getTotalSize() } : undefined}
          >
            <thead className="border-b border-[var(--color-border)] bg-parchment-50">
              {table.getHeaderGroups().map((headerGroup) => (
                <SortableContext
                  key={headerGroup.id}
                  items={visibleColumnIds}
                  strategy={horizontalListSortingStrategy}
                >
                  <tr>
                    {headerGroup.headers.map((header) => (
                      <DraggableHeader
                        key={header.id}
                        headerId={header.id}
                        size={header.getSize()}
                        fixedLayout={hasUserSizedColumns}
                      >
                        {(dragHandleProps) => (
                          <div
                            className="group flex h-full items-center justify-between gap-1"
                          >
                            <button
                              {...dragHandleProps.listeners}
                              {...dragHandleProps.attributes}
                              ref={dragHandleProps.setActivatorNodeRef}
                              type="button"
                              className="cursor-grab touch-none rounded p-0.5 text-slate-300 opacity-0 transition-opacity hover:text-slate-500 group-hover:opacity-100 active:cursor-grabbing"
                              aria-label="Drag to reorder column"
                              title="Drag to reorder"
                              tabIndex={-1}
                            >
                              <GripVertical className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              onClick={header.column.getToggleSortingHandler()}
                              disabled={!header.column.getCanSort()}
                              className={cn(
                                'flex min-w-0 flex-1 items-center gap-1 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500',
                                header.column.getCanSort() && 'cursor-pointer hover:text-slate-700',
                              )}
                            >
                              <span className="truncate">
                                {header.isPlaceholder
                                  ? null
                                  : flexRender(header.column.columnDef.header, header.getContext())}
                              </span>
                              {header.column.getCanSort() && (
                                <SortIndicator dir={header.column.getIsSorted()} />
                              )}
                            </button>
                            {header.column.getCanResize() && (
                              // Wider hit area (-right-1.5 w-3) so the column
                              // edge is genuinely grabbable, but the visible
                              // indicator is only the centred 2px pill. The
                              // indicator goes alma-tinted on hover and stays
                              // lit while resizing. Double-click resets to
                              // the column's default size.
                              <div
                                role="separator"
                                aria-orientation="vertical"
                                aria-label="Resize column"
                                title="Drag to resize · double-click to reset"
                                onMouseDown={header.getResizeHandler()}
                                onTouchStart={header.getResizeHandler()}
                                onDoubleClick={() => header.column.resetSize()}
                                onClick={(e) => e.stopPropagation()}
                                className="group/resize absolute -right-1.5 top-1/2 z-10 h-6 w-3 -translate-y-1/2 cursor-col-resize touch-none select-none"
                              >
                                <span
                                  className={cn(
                                    'absolute inset-y-1 left-1/2 w-0.5 -translate-x-1/2 rounded-full bg-slate-200 transition-colors',
                                    'group-hover/resize:bg-alma-500 group-hover/resize:w-1',
                                    header.column.getIsResizing() && 'bg-alma-600 w-1',
                                  )}
                                />
                              </div>
                            )}
                          </div>
                        )}
                      </DraggableHeader>
                    ))}
                  </tr>
                </SortableContext>
              ))}
            </thead>
            <tbody className="divide-y divide-slate-100">
              {hasRows ? (
                table.getRowModel().rows.map((row) => {
                  const extraClass = rowClassName ? rowClassName(row.original) : ''
                  const rowId = deriveRowId(row.original, row.index)
                  const isSelected = selectionEnabled && selectedIds!.has(rowId)
                  return (
                    <tr
                      key={row.id}
                      onClick={onRowClick ? () => onRowClick(row.original) : undefined}
                      className={cn(
                        'transition-colors hover:bg-parchment-50',
                        onRowClick && 'cursor-pointer',
                        isSelected && 'bg-alma-50/40',
                        extraClass,
                      )}
                    >
                      {row.getVisibleCells().map((cell) => {
                        const size = cell.column.getSize()
                        // In auto mode, cells size from content up to a cap;
                        // once the user drags, `size` is authoritative.
                        const style = hasUserSizedColumns
                          ? { width: size }
                          : { maxWidth: cell.column.columnDef.maxSize ?? 480 }
                        const overflow = cell.column.columnDef.meta?.cellOverflow ?? 'ellipsis'
                        const rendered = flexRender(cell.column.columnDef.cell, cell.getContext())
                        return (
                          <td
                            key={cell.id}
                            className="overflow-hidden px-3 py-2 align-middle text-slate-700"
                            style={style}
                          >
                            {overflow === 'none' ? (
                              rendered
                            ) : overflow === 'wrap' ? (
                              <div className="whitespace-normal break-words">{rendered}</div>
                            ) : (
                              <div className="truncate">{rendered}</div>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })
              ) : (
                <tr>
                  <td
                    colSpan={table.getVisibleLeafColumns().length || 1}
                    className="px-3 py-10 text-center text-sm text-slate-400"
                  >
                    {emptyState ?? 'No rows.'}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </DndContext>
      </div>

      {(footerCaption || hasRows) && (
        <div className="flex items-center justify-between border-t border-slate-100 bg-parchment-50 px-3 py-2 text-xs text-slate-500">
          <span className="tabular-nums">
            {table.getRowModel().rows.length} row{table.getRowModel().rows.length !== 1 ? 's' : ''}
          </span>
          {footerCaption && <span className="text-slate-400">{footerCaption}</span>}
        </div>
      )}
    </div>
  )
}

function SortIndicator({ dir }: { dir: false | 'asc' | 'desc' }) {
  if (dir === 'asc') return <ArrowUp className="h-3 w-3 text-alma-600" />
  if (dir === 'desc') return <ArrowDown className="h-3 w-3 text-alma-600" />
  return <ArrowUpDown className="h-3 w-3 text-slate-300 opacity-0 transition-opacity group-hover:opacity-100" />
}

/**
 * Wraps a `<th>` in a dnd-kit sortable context and exposes the drag
 * activator ref to the children (so only the grip handle — not the whole
 * cell — starts a drag; resize handles and sort clicks still work).
 */
function DraggableHeader({
  headerId,
  size,
  fixedLayout,
  children,
}: {
  headerId: string
  size: number
  fixedLayout: boolean
  children: (dragHandleProps: {
    attributes: ReturnType<typeof useSortable>['attributes']
    listeners: ReturnType<typeof useSortable>['listeners']
    setActivatorNodeRef: ReturnType<typeof useSortable>['setActivatorNodeRef']
  }) => React.ReactNode
}) {
  const { attributes, listeners, setNodeRef, setActivatorNodeRef, transform, transition, isDragging } = useSortable({ id: headerId })
  // Once the user has resized any column, we pin `table-layout: fixed` so
  // widths become authoritative. Before then, `size` acts as a hint — we set
  // it as `min-width` so short content doesn't collapse a column below its
  // default, but let long content stretch the column until the cell's
  // `max-width` kicks in.
  const style: CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.7 : 1,
    zIndex: isDragging ? 1 : undefined,
    ...(fixedLayout ? { width: size } : { minWidth: size }),
  }
  return (
    <th ref={setNodeRef} style={style} className="relative px-3 py-2 text-left">
      {children({ attributes, listeners, setActivatorNodeRef })}
    </th>
  )
}
