/**
 * Display relative freshness for the last pipeline run.
 */

function hoursSince(iso: string | null): string {
  if (!iso) return "unknown";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "unknown";
  const diffMs = Date.now() - t;
  const hours = Math.max(0, Math.floor(diffMs / 3600000));
  if (hours === 0) {
    const mins = Math.max(0, Math.floor(diffMs / 60000));
    return `${mins} minutes ago`;
  }
  if (hours === 1) return "1 hour ago";
  return `${hours} hours ago`;
}

type RefreshBadgeProps = {
  /** ISO timestamp of the last pipeline completion, if any. */
  lastRunIso: string | null;
};

export function RefreshBadge({ lastRunIso }: RefreshBadgeProps) {
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-black/30 px-3 py-1 text-xs text-slate-300">
      <span className="h-2 w-2 rounded-full bg-emerald-400/80" aria-hidden />
      <span>Last updated: {hoursSince(lastRunIso)}</span>
    </div>
  );
}
