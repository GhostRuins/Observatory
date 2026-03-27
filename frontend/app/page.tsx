import { ChartCard } from "@/components/charts/ChartCard";
import { RefreshBadge } from "@/components/RefreshBadge";
import { TopicFilter } from "@/components/TopicFilter";
import { fetchCharts, fetchHealth } from "@/lib/apiClient";

/** Always load chart list from the API — avoids stale ISR cache showing an empty overview. */
export const dynamic = "force-dynamic";

/**
 * Dashboard home: interactive grid of charts across every topic.
 */
export default async function HomePage() {
  const [charts, health] = await Promise.all([fetchCharts(), fetchHealth()]);

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
        <div>
          <h1 className="text-3xl font-semibold tracking-tight text-white">
            Observatory overview
          </h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Fresh public datasets cleaned once per day and charted automatically. Filter by topic
            to focus on climate, health, economics, politics, or general indicators.
          </p>
        </div>
        <RefreshBadge lastRunIso={health.last_pipeline_finished_at} />
      </div>

      <TopicFilter active="all" />

      <section className="grid gap-6 md:grid-cols-2">
        {charts.map((chart) => (
          <ChartCard key={chart.dataset_id} chart={chart} />
        ))}
      </section>

      {charts.length === 0 && (
        <div className="space-y-3 rounded-xl border border-dashed border-white/15 bg-black/20 p-6 text-sm text-slate-400">
          <p>No charts yet. The dashboard loads data from the FastAPI backend (not from the pipeline CLI directly).</p>
          <ul className="list-inside list-disc space-y-1 text-slate-500">
            <li>
              Start the API: from <code className="text-slate-300">backend/</code> run{" "}
              <code className="text-slate-300">python -m uvicorn main:app --reload --port 8000</code>
            </li>
            <li>
              Point the frontend at it: in <code className="text-slate-300">frontend/.env.local</code> set{" "}
              <code className="text-slate-300">NEXT_PUBLIC_API_URL=http://localhost:8000</code> (same host/port
              the browser/server can reach).
            </li>
            <li>
              Load data: <code className="text-slate-300">python -m pipeline.ingest --full</code> from{" "}
              <code className="text-slate-300">backend/</code> using the same <code className="text-slate-300">DATABASE_URL</code> as
              the API — without <code className="text-slate-300">--dry-run</code>.
            </li>
          </ul>
        </div>
      )}
    </div>
  );
}
