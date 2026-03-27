"use client";

import { topicColor } from "@/lib/topics";

import { AreaChart } from "./AreaChart";
import { BarChart } from "./BarChart";
import { LineChart } from "./LineChart";
import { ScatterChart } from "./ScatterChart";

type ChartRouterProps = {
  chartType: string;
  topicSlug: string;
  data: Array<Record<string, unknown>>;
};

/**
 * Maps backend chart_config.type values to concrete Recharts components.
 */
export function ChartRouter({ chartType, topicSlug, data }: ChartRouterProps) {
  const colour = topicColor(topicSlug);
  const normalised = data.map((d) => ({ ...d }));

  if (!normalised.length) {
    return (
      <div className="flex h-64 items-center justify-center rounded-lg border border-dashed border-white/15 text-sm text-slate-400">
        No plottable rows yet — run the daily pipeline after ingesting sources.
      </div>
    );
  }

  switch (chartType) {
    case "line":
      return <LineChart data={normalised} stroke={colour} />;
    case "bar":
      return <BarChart data={normalised} fill={colour} />;
    case "area":
      return <AreaChart data={normalised} stroke={colour} fill={colour} />;
    case "scatter":
      return <ScatterChart data={normalised} fill={colour} />;
    default:
      return <BarChart data={normalised} fill={colour} />;
  }
}
