/**
 * Centralised HTTP access to the FastAPI backend — use this instead of ad-hoc fetch() calls.
 */

const API_BASE =
  (typeof process !== "undefined" && process.env.NEXT_PUBLIC_API_URL) ||
  "http://localhost:8000";

export type ChartView = {
  dataset_id: number;
  source_id: number;
  source_name: string;
  source_url: string;
  topic_slug: string;
  chart_config: Record<string, unknown>;
  last_updated: string | null;
  data_points: Array<Record<string, unknown>>;
};

export type HealthResponse = {
  status: string;
  uptime_seconds: number;
  last_pipeline_finished_at: string | null;
};

/**
 * Fetch chart views, optionally filtered by topic slug.
 */
export async function fetchCharts(topic?: string): Promise<ChartView[]> {
  const params = new URLSearchParams();
  if (topic) params.set("topic", topic);
  const query = params.toString();
  const url = `${API_BASE}/charts${query ? `?${query}` : ""}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to load charts: ${res.status}`);
  }
  return (await res.json()) as ChartView[];
}

/**
 * Return API health including last pipeline completion time for the refresh badge.
 */
export async function fetchHealth(): Promise<HealthResponse> {
  const res = await fetch(`${API_BASE}/health`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to load health: ${res.status}`);
  }
  return (await res.json()) as HealthResponse;
}
