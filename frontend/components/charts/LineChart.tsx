"use client";

import {
  CartesianGrid,
  Line,
  LineChart as RLineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatTooltipScalar } from "@/lib/chartData";

type LineChartProps = {
  /** Normalised point rows with at least x and y keys. */
  data: Array<Record<string, unknown>>;
  /** Optional stroke colour taken from the topic palette. */
  stroke?: string;
};

/**
 * Recharts line chart for time series or ordered numeric x axes.
 */
export function LineChart({ data, stroke = "#38bdf8" }: LineChartProps) {
  const safe = data.filter((d) => d.x !== undefined && d.y !== undefined);
  return (
    <div className="h-64 w-full min-w-0">
      <ResponsiveContainer width="100%" height="100%">
        <RLineChart data={safe}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="x" stroke="#94a3b8" tick={{ fontSize: 11 }} />
          <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} />
          <Tooltip
            formatter={(value: unknown) => formatTooltipScalar(value)}
            labelFormatter={(label: unknown) => formatTooltipScalar(label)}
            contentStyle={{ background: "#0f172a", border: "1px solid #334155" }}
            labelStyle={{ color: "#e2e8f0" }}
          />
          <Line type="monotone" dataKey="y" stroke={stroke} dot={false} strokeWidth={2} />
        </RLineChart>
      </ResponsiveContainer>
    </div>
  );
}
