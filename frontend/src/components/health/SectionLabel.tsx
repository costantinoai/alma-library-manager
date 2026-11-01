/**
 * SectionLabel — the small uppercase band heading used across the Health page
 * (repair groups, diagnostics, system status). One definition so the eyebrow
 * styling never drifts between sections.
 */
export function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-caption font-bold uppercase tracking-[0.16em] text-slate-500">{children}</h2>
  )
}
