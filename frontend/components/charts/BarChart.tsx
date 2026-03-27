"use client";

import {
  Bar,
  BarChart as RBarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type BarChartProps = {
  /** Normalised point rows with at least x (category) and y (value). */
  data: Array<Record<string, unknown>>;
  fill?: string;
};

/**
 * Recharts bar chart for categorical comparisons.
 */
export function BarChart({ data, fill = "#60a5fa" }: BarChartProps) {
  const safe = data.filter((d) => d.x !== undefined && d.y !== undefined);
  return (
    <div className="h-64 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RBarChart data={safe}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="x" stroke="#94a3b8" tick={{ fontSize: 10 }} />
          <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: "#0f172a", border: "1px solid #334155" }}
            labelStyle={{ color: "#e2e8f0" }}
          />
          <Bar dataKey="y" fill={fill} radius={[4, 4, 0, 0]} />
        </RBarChart>
      </ResponsiveContainer>
    </div>
  );
}
