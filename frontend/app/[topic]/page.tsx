import { notFound } from "next/navigation";

import { ChartCard } from "@/components/charts/ChartCard";
import { RefreshBadge } from "@/components/RefreshBadge";
import { TopicFilter } from "@/components/TopicFilter";
import { fetchCharts, fetchHealth } from "@/lib/apiClient";
import { TOPICS, type TopicSlug } from "@/lib/topics";

type TopicPageProps = {
  params: { topic: string };
};

/**
 * Per-topic dashboard view filtered by slug.
 */
export default async function TopicPage({ params }: TopicPageProps) {
  const slug = params.topic as TopicSlug;
  const isKnown = TOPICS.some((t) => t.slug === slug);
  if (!isKnown) {
    notFound();
  }

  const [charts, health] = await Promise.all([fetchCharts(slug), fetchHealth()]);
  const label = TOPICS.find((t) => t.slug === slug)?.label ?? slug;

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
        <div>
          <h1 className="text-3xl font-semibold tracking-tight text-white">{label}</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Datasets tagged with the {label.toLowerCase()} topic. Each card links back to the
            original open source for transparency.
          </p>
        </div>
        <RefreshBadge lastRunIso={health.last_pipeline_finished_at} />
      </div>

      <TopicFilter active={slug} />

      <section className="grid gap-6 md:grid-cols-2">
        {charts.map((chart) => (
          <ChartCard key={chart.dataset_id} chart={chart} />
        ))}
      </section>

      {charts.length === 0 && (
        <div className="rounded-xl border border-dashed border-white/15 bg-black/20 p-6 text-sm text-slate-400">
          No charts for this topic yet. Confirm ingestion has completed for matching sources.
        </div>
      )}
    </div>
  );
}
