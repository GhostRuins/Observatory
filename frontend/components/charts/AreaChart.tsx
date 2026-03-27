"use client";

import {
  Area,
  AreaChart as RAreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type AreaChartProps = {
  /** Normalised point rows with at least x and y keys. */
  data: Array<Record<string, unknown>>;
  stroke?: string;
  fill?: string;
};

/**
 * Recharts area chart emphasising magnitude of a single series.
 */
export function AreaChart({ data, stroke = "#34d399", fill = "#34d399" }: AreaChartProps) {
  const safe = data.filter((d) => d.x !== undefined && d.y !== undefined);
  return (
    <div className="h-64 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RAreaChart data={safe}>
          <defs>
            <linearGradient id="areaFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={fill} stopOpacity={0.8} />
              <stop offset="95%" stopColor={fill} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="x" stroke="#94a3b8" tick={{ fontSize: 11 }} />
          <YAxis stroke="#94a3b8" tick={{ fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: "#0f172a", border: "1px solid #334155" }}
            labelStyle={{ color: "#e2e8f0" }}
          />
          <Area
            type="monotone"
            dataKey="y"
            stroke={stroke}
            fillOpacity={1}
            fill="url(#areaFill)"
          />
        </RAreaChart>
      </ResponsiveContainer>
    </div>
  );
}
