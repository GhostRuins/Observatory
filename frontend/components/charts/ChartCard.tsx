"use client";

import { topicColor } from "@/lib/topics";
import type { ChartView } from "@/lib/apiClient";

import { ChartRouter } from "./ChartRouter";

type ChartCardProps = {
  chart: ChartView;
};

/**
 * Card shell around a chart with metadata, provenance, and freshness context.
 */
function resolveChartType(cfg: Record<string, unknown>): string {
  const raw =
    (typeof cfg.type === "string" && cfg.type) ||
    (typeof cfg.chart_type === "string" && cfg.chart_type) ||
    (typeof cfg.chartType === "string" && cfg.chartType) ||
    "";
  const t = raw.trim().toLowerCase();
  if (t === "line" || t === "bar" || t === "area" || t === "scatter") return t;
  return "bar";
}

export function ChartCard({ chart }: ChartCardProps) {
  const cfg = chart.chart_config;
  const title =
    typeof cfg.title === "string" && cfg.title.length > 0
      ? cfg.title
      : chart.source_name;
  const chartType = resolveChartType(cfg);
  const accent = topicColor(chart.topic_slug);

  const sourceHref =
    typeof chart.source_url === "string" && chart.source_url.trim().length > 0
      ? chart.source_url.trim()
      : undefined;

  return (
    <article
      className="flex min-w-0 flex-col overflow-hidden rounded-2xl border border-white/10 bg-white/5 shadow-lg shadow-black/30"
      style={{ borderTopColor: accent, borderTopWidth: 3 }}
    >
      <div className="space-y-1 border-b border-white/10 px-4 py-3">
        <h2 className="text-base font-semibold text-white">{title}</h2>
        <p className="text-xs text-slate-400">
          Source:{" "}
          {sourceHref ? (
            <a
              href={sourceHref}
              target="_blank"
              rel="noreferrer"
              className="text-sky-300 underline-offset-2 hover:underline"
            >
              {chart.source_name}
            </a>
          ) : (
            <span className="text-slate-300">{chart.source_name}</span>
          )}
        </p>
        {chart.last_updated && (
          <p className="text-xs text-slate-500">
            Dataset refreshed: {new Date(chart.last_updated).toLocaleString()}
          </p>
        )}
      </div>
      <div className="px-2 pb-4 pt-3">
        <ChartRouter
          chartType={chartType}
          topicSlug={chart.topic_slug}
          data={chart.data_points}
        />
      </div>
    </article>
  );
}
