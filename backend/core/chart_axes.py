"""Heuristics for choosing x/y columns so charts map to measures, not surrogate IDs."""

from __future__ import annotations

from typing import Any

# Prefer these names for the vertical axis (GDP, FRED, Open-Meteo, WHO, etc.).
_MEASURE_PRIORITY: tuple[str, ...] = (
    "value",
    "values",
    "observation",
    "observations",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "unrate",
    "gdp",
    "measure",
    "amount",
    "population",
    "life_expectancy",
    "numeric_value",
    "fact_value",
)

# Columns that are numeric but usually not the thing to plot on Y.
_DIMENSION_NAMES: frozenset[str] = frozenset(
    {
        "id",
        "index",
        "rank",
        "latitude",
        "longitude",
        "lat",
        "lon",
        "lng",
        "page",
        "per_page",
        "total",
        "precision",
        "decimal",
    }
)


def is_likely_dimension_column(name: str) -> bool:
    """Return True if the column name looks like an ID or geo coordinate, not a measure."""
    n = name.lower().strip()
    if n in _DIMENSION_NAMES:
        return True
    if n.endswith("_id") and n not in ("indicator_id",):
        return True
    if n in ("iso2code", "iso3code"):
        return True
    return False


def pick_y_numeric_name(candidates: list[str], dt_col: str | None = None) -> str | None:
    """
    Choose the best numeric column for a time series or bar Y axis.

    Skips surrogate keys (id, lat/lon) and prefers API-standard names like `value`.
    """
    if not candidates:
        return None
    pool = [c for c in candidates if dt_col is None or str(c) != str(dt_col)]
    if not pool:
        pool = list(candidates)
    filtered = [c for c in pool if not is_likely_dimension_column(str(c))]
    use = filtered if filtered else pool
    lower_map = {str(c).lower(): c for c in use}
    for want in _MEASURE_PRIORITY:
        if want in lower_map:
            return lower_map[want]
    return use[0]


def _numeric_keys_from_rows(
    rows: list[dict[str, Any]],
    x_key: str | None,
    max_rows: int = 40,
) -> list[str]:
    """
    Collect column names that look numeric in at least one row (not only the first).

    World Bank rows often have `value: null` on the first observation but valid scores
    later; using only row 0 wrongly leaves `decimal` (0) as the only numeric and hides series.
    """
    seen: set[str] = set()
    order: list[str] = []
    for row in rows[:max_rows]:
        if not isinstance(row, dict):
            continue
        for k, v in row.items():
            sk = str(k)
            if x_key is not None and sk == str(x_key):
                continue
            if v is None or v == "":
                continue
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                if sk not in seen:
                    seen.add(sk)
                    order.append(sk)
            elif isinstance(v, str) and v.strip():
                try:
                    float(v)
                    if sk not in seen:
                        seen.add(sk)
                        order.append(sk)
                except ValueError:
                    pass
    if "value" in order:
        order.remove("value")
        order.insert(0, "value")
    return order


def pick_y_key_from_sample_rows(
    rows: list[dict[str, Any]],
    x_key: str | None,
    current_y: str | None,
) -> str | None:
    """
    Infer a measure column from tabular rows when stored chart_config.y_key is wrong.

    Used by the charts API so existing DB rows render after fixing heuristics without re-ingest.
    """
    if not rows:
        return current_y
    row_keys = [str(k) for k in rows[0].keys()] if isinstance(rows[0], dict) else []
    numeric_keys = _numeric_keys_from_rows(rows, x_key)
    if not numeric_keys:
        return current_y
    chosen = pick_y_numeric_name(numeric_keys, dt_col=x_key)
    if chosen is None:
        return current_y
    if current_y and is_likely_dimension_column(str(current_y)):
        return chosen
    if current_y and current_y in row_keys:
        vals: list[float] = []
        for r in rows[: min(80, len(rows))]:
            raw = r.get(current_y)
            if raw is None or raw == "":
                continue
            try:
                vals.append(float(raw))
            except (TypeError, ValueError):
                pass
        if len(vals) >= 3:
            spread = max(vals) - min(vals)
            if spread > 1e-9:
                return current_y
    return chosen
