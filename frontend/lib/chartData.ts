/**
 * Normalise API chart points for Recharts: strip debug payloads and coerce x/y so
 * Tooltip / axes never receive plain objects (which React cannot render as children).
 */

export type SanitizedPoint = {
  x: string | number;
  y: number | string;
  series?: string;
};

function coerceAxisValue(v: unknown): string | number {
  if (v === null || v === undefined) return "";
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "string") return v;
  if (typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (o.date != null) return String(o.date);
    if (o.label != null) return String(o.label);
    if (o.name != null) return String(o.name);
    if (o.country != null) return String(o.country);
    return JSON.stringify(v);
  }
  return String(v);
}

function coerceYValue(v: unknown): number | string {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
    return v;
  }
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (typeof o.value === "number" && Number.isFinite(o.value)) return o.value;
    if (typeof o.value === "string") {
      const n = Number(o.value);
      if (Number.isFinite(n)) return n;
    }
    if (typeof o.Value === "string") {
      const n = Number(o.Value);
      if (Number.isFinite(n)) return n;
    }
    if (typeof o.Value === "number" && Number.isFinite(o.Value)) return o.Value;
  }
  if (v === null || v === undefined) return 0;
  return JSON.stringify(v);
}

/**
 * Drop `_raw` and coerce coordinates for Recharts consumers.
 */
export function sanitizeRechartsRows(
  data: Array<Record<string, unknown>>,
): SanitizedPoint[] {
  return data.map((row) => {
    const { _raw: _drop, ...rest } = row;
    const x = coerceAxisValue(rest.x);
    const y = coerceYValue(rest.y);
    const out: SanitizedPoint = { x, y };
    if (rest.series !== undefined) {
      out.series = String(coerceAxisValue(rest.series));
    }
    return out;
  });
}

/** Safe for Tooltip formatter / labelFormatter when values slip through. */
export function formatTooltipScalar(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v);
  }
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

/** Recharts Scatter requires numeric x/y; string dates or categories fail. */
function toNumberLike(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/**
 * True when at least one point has finite numeric x and y (Scatter-compatible).
 */
export function canRenderScatter(points: SanitizedPoint[]): boolean {
  return points.some((p) => toNumberLike(p.x) !== null && toNumberLike(p.y) !== null);
}
