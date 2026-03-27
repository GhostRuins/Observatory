"use client";

import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart as RScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

type ScatterChartProps = {
  /** Normalised point rows with numeric x and y keys. */
  data: Array<Record<string, unknown>>;
  fill?: string;
};

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/**
 * Recharts scatter chart for two continuous variables.
 */
export function ScatterChart({ data, fill = "#f472b6" }: ScatterChartProps) {
  const safe = data
    .map((d) => {
      const x = toNumber(d.x);
      const y = toNumber(d.y);
      if (x === null || y === null) return null;
      return { x, y };
    })
    .filter((d): d is { x: number; y: number } => d !== null);
  return (
    <div className="h-64 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RScatterChart>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis type="number" dataKey="x" name="x" stroke="#94a3b8" />
          <YAxis type="number" dataKey="y" name="y" stroke="#94a3b8" />
          <ZAxis range={[60, 60]} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            contentStyle={{ background: "#0f172a", border: "1px solid #334155" }}
            labelStyle={{ color: "#e2e8f0" }}
          />
          <Scatter name="values" data={safe} fill={fill} />
        </RScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
