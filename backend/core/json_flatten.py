"""Flatten nested API rows (e.g. World Bank indicator API) for pandas and charts."""

from __future__ import annotations

from typing import Any


def looks_like_world_bank_observation(row: dict[str, Any]) -> bool:
    """True when the row matches World Bank v2 indicator observation shape."""
    return bool(
        isinstance(row.get("indicator"), dict)
        and "date" in row
        and ("country" in row or "countryiso3code" in row)
    )


def flatten_world_bank_style_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Expand nested `country` / `indicator` objects into scalar columns.

    The World Bank JSON API returns observations where `country` is an object; plotting
    `country` on the X axis would otherwise stringify to `[object Object]` or break Recharts.
    """
    out = dict(row)
    country = out.get("country")
    if isinstance(country, dict):
        out["country_name"] = country.get("value")
        out["country_id"] = country.get("id")
        del out["country"]
    indicator = out.get("indicator")
    if isinstance(indicator, dict):
        out["indicator_id"] = indicator.get("id")
        out["indicator_label"] = indicator.get("value")
        del out["indicator"]
    return out


def flatten_world_bank_style_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply flattening when the first row looks like a World Bank observation."""
    if not records or not isinstance(records[0], dict):
        return records
    if not looks_like_world_bank_observation(records[0]):
        return records
    return [flatten_world_bank_style_row(dict(r)) for r in records]


def maybe_aggregate_world_bank_by_date(
    rows: list[dict[str, Any]],
    date_key: str = "date",
    value_key: str = "value",
) -> list[dict[str, Any]]:
    """
    Average `value` per `date` when the API returns many countries per year (country/all).

    Without this, Recharts draws hundreds of bars on the same category (invisible overlap) or
    a degenerate line; the dashboard shows an empty-looking chart even when data exists.
    """
    if not rows or not isinstance(rows[0], dict):
        return rows
    sample = rows[0]
    if date_key not in sample or value_key not in sample:
        return rows
    if "country_name" not in sample and "countryiso3code" not in sample:
        return rows
    n = min(400, len(rows))
    date_strings = [str(r.get(date_key)) for r in rows[:n] if r.get(date_key) is not None]
    if len(date_strings) < 2:
        return rows
    unique = len(set(date_strings))
    if unique >= len(date_strings) * 0.85:
        return rows
    from collections import defaultdict

    buckets: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        d = r.get(date_key)
        v = r.get(value_key)
        if d is None or v is None:
            continue
        try:
            buckets[str(d)].append(float(v))
        except (TypeError, ValueError):
            continue
    if not buckets:
        return rows
    aggregated = [
        {date_key: period, value_key: sum(vals) / len(vals)}
        for period, vals in sorted(buckets.items(), key=lambda item: item[0])
    ]
    return aggregated
