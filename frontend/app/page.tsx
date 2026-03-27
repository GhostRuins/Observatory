import { ChartCard } from "@/components/charts/ChartCard";
import { RefreshBadge } from "@/components/RefreshBadge";
import { TopicFilter } from "@/components/TopicFilter";
import { fetchCharts, fetchHealth } from "@/lib/apiClient";

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
        <div className="rounded-xl border border-dashed border-white/15 bg-black/20 p-6 text-sm text-slate-400">
          No charts yet. Run the backend daily pipeline (`python -m pipeline.ingest --full`) after
          configuring `DATABASE_URL` and seeding sources.
        </div>
      )}
    </div>
  );
}
