"""Chart-oriented views over stored dataset configurations."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from core.chart_axes import pick_y_key_from_sample_rows
from core.config import get_settings
from core.json_flatten import flatten_world_bank_style_rows, maybe_aggregate_world_bank_by_date
from core.topics import ALL_TOPIC_SLUGS
from db.postgres import fetch_all, get_pool

router = APIRouter(prefix="/charts", tags=["charts"])


class ChartView(BaseModel):
    """Chart configuration with dataset context for frontend rendering."""

    dataset_id: int
    source_id: int
    source_name: str
    source_url: str
    topic_slug: str
    chart_config: dict[str, Any]
    last_updated: datetime | None
    data_points: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Rows projected from cleaned_data for the chosen x/y keys.",
    )


def _records_from_dataset(cleaned: Any, raw_snapshot: Any) -> list[dict[str, Any]]:
    """Prefer cleaned_data; fall back to raw_snapshot.records after ingest."""
    if isinstance(cleaned, str):
        try:
            cleaned = json.loads(cleaned)
        except json.JSONDecodeError:
            cleaned = None
    if isinstance(cleaned, list) and cleaned:
        recs = [dict(r) for r in cleaned if isinstance(r, dict)]
        return flatten_world_bank_style_rows(recs)
    if isinstance(raw_snapshot, str):
        try:
            raw_snapshot = json.loads(raw_snapshot)
        except json.JSONDecodeError:
            raw_snapshot = None
    if isinstance(raw_snapshot, dict):
        rec = raw_snapshot.get("records")
        if isinstance(rec, list) and rec:
            raw_recs = [dict(r) for r in rec if isinstance(r, dict)]
            return flatten_world_bank_style_rows(raw_recs)
    return []


def _fallback_chart_config(rows: list[dict[str, Any]], source_name: str) -> dict[str, Any]:
    """Minimal axes when chart_config was never written (e.g. ingest-only or failed clean step)."""
    if not rows:
        return {
            "type": "bar",
            "title": source_name,
            "x_key": None,
            "y_key": None,
        }
    keys = [str(k) for k in rows[0].keys()]
    if len(keys) >= 2:
        return {
            "type": "bar",
            "title": source_name,
            "x_key": keys[0],
            "y_key": keys[1],
        }
    if len(keys) == 1:
        return {
            "type": "bar",
            "title": source_name,
            "x_key": keys[0],
            "y_key": keys[0],
        }
    return {"type": "bar", "title": source_name, "x_key": None, "y_key": None}


ALLOWED_CHART_TYPES: frozenset[str] = frozenset({"line", "bar", "area", "scatter"})


def _normalize_chart_type(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Ensure chart_config.type matches what the frontend ChartRouter expects.

    LLM or legacy rows may use chart_type / chartType or unsupported values.
    """
    out = dict(cfg)
    t = out.get("type")
    if not isinstance(t, str) or not t.strip():
        alt = out.get("chart_type") or out.get("chartType")
        if isinstance(alt, str) and alt.strip():
            t = alt
    if isinstance(t, str):
        t = t.strip().lower()
    else:
        t = "bar"
    if t not in ALLOWED_CHART_TYPES:
        t = "bar"
    out["type"] = t
    return out


def _merge_chart_config(
    stored: Any,
    rows: list[dict[str, Any]],
    source_name: str,
) -> dict[str, Any]:
    """Use DB chart_config when present; otherwise infer axes from tabular rows."""
    cfg: dict[str, Any] = {}
    if stored is not None and isinstance(stored, dict):
        cfg = dict(stored)
    elif stored is not None and hasattr(stored, "keys"):
        cfg = dict(stored)
    needs_axes = not cfg or (
        (not cfg.get("x_key")) and (not cfg.get("y_key"))
    )
    if needs_axes and rows:
        fb = _fallback_chart_config(rows, source_name)
        cfg = {**fb, **cfg}
    elif needs_axes:
        cfg = _fallback_chart_config(rows, source_name)
    return _normalize_chart_type(cfg)


def _sort_points_by_x_for_time_series(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order points by x so lines and bars follow chronological order (FRED, World Bank years)."""

    def _sort_key(p: dict[str, Any]) -> tuple[int | float, str]:
        x = p.get("x")
        if x is None:
            return (0, "")
        s = str(x).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return (0, s)
        if len(s) == 4 and s.isdigit():
            return (0, s)
        return (1, s)

    try:
        return sorted(points, key=_sort_key)
    except (TypeError, ValueError):
        return points


def _build_data_points(
    cleaned: Any,
    chart_config: dict[str, Any],
    limit: int = 20000,
) -> list[dict[str, Any]]:
    """Project cleaned tabular rows into chart-friendly point dicts."""
    rows: list[dict[str, Any]] = []
    if isinstance(cleaned, str):
        try:
            cleaned = json.loads(cleaned)
        except json.JSONDecodeError:
            cleaned = []
    if not isinstance(cleaned, list):
        return rows
    x_key = chart_config.get("x_key")
    y_key = chart_config.get("y_key")
    color_key = chart_config.get("color_key")
    if cleaned and isinstance(cleaned[0], dict):
        sample_keys = [str(k) for k in cleaned[0].keys()]
        if not isinstance(x_key, str) or x_key not in cleaned[0]:
            if len(sample_keys) > 0:
                x_key = sample_keys[0]
        if (not isinstance(y_key, str) or y_key not in cleaned[0]) and len(sample_keys) > 1:
            y_key = sample_keys[1]
        y_key = pick_y_key_from_sample_rows(
            cleaned,
            x_key if isinstance(x_key, str) else None,
            y_key if isinstance(y_key, str) else None,
        )
    for raw in cleaned[:limit]:
        if not isinstance(raw, dict):
            continue
        point: dict[str, Any] = {}
        if isinstance(x_key, str) and x_key in raw:
            point["x"] = raw.get(x_key)
        if isinstance(y_key, str) and y_key in raw:
            point["y"] = raw.get(y_key)
        if isinstance(color_key, str) and color_key in raw:
            point["series"] = raw.get(color_key)
        if point:
            rows.append(point)
    return _sort_points_by_x_for_time_series(rows)


_DATASET_HAS_ROWS = """
    (
        (d.cleaned_data IS NOT NULL AND jsonb_typeof(d.cleaned_data) = 'array'
            AND jsonb_array_length(d.cleaned_data) > 0)
        OR (
            d.raw_snapshot IS NOT NULL
            AND d.raw_snapshot ? 'records'
            AND jsonb_typeof(d.raw_snapshot->'records') = 'array'
            AND jsonb_array_length(d.raw_snapshot->'records') > 0
        )
    )
"""


@router.get("", response_model=list[ChartView])
async def list_charts(topic: str | None = Query(default=None)) -> list[ChartView]:
    """
    Return chart views for datasets with tabular data.

    Includes rows after full pipeline (chart_config + cleaned_data) and after ingest-only
    (raw_snapshot.records) by synthesizing axis keys when chart_config is missing.

    Optional `topic` filters by topic slug (climate, health, economics, politics, general).
    """
    if topic is not None and topic not in ALL_TOPIC_SLUGS:
        return []

    settings = get_settings()
    pool = await get_pool(settings.database_url)
    topic_filter = "AND t.slug = $1" if topic is not None else ""
    params: tuple[Any, ...] = (topic,) if topic is not None else ()

    rows = await fetch_all(
        pool,
        f"""
            SELECT
                d.id AS dataset_id,
                d.source_id,
                s.name AS source_name,
                s.url AS source_url,
                t.slug AS topic_slug,
                d.chart_config,
                d.cleaned_data,
                d.raw_snapshot,
                d.last_cleaned_at,
                d.last_ingested_at
            FROM datasets AS d
            JOIN sources AS s ON s.id = d.source_id
            JOIN topics AS t ON t.id = s.topic_id
            WHERE {_DATASET_HAS_ROWS}
            {topic_filter}
            ORDER BY d.id ASC
            """,
        *params,
    )

    views: list[ChartView] = []
    for r in rows:
        source_name = str(r["source_name"])
        tabular = _records_from_dataset(r.get("cleaned_data"), r.get("raw_snapshot"))
        if not tabular:
            continue
        pre_rows = len(tabular)
        tabular = maybe_aggregate_world_bank_by_date(tabular)
        aggregated_wb = len(tabular) < pre_rows and len(tabular) > 0
        cfg = _merge_chart_config(r.get("chart_config"), tabular, source_name)
        if not cfg:
            continue
        if aggregated_wb:
            cfg = {
                **cfg,
                "type": "line",
                "x_key": "date",
                "y_key": "value",
                "title": cfg.get("title") or source_name,
            }
            cfg = _normalize_chart_type(cfg)
        y_resolved = pick_y_key_from_sample_rows(
            tabular,
            cfg.get("x_key") if isinstance(cfg.get("x_key"), str) else None,
            cfg.get("y_key") if isinstance(cfg.get("y_key"), str) else None,
        )
        if y_resolved and y_resolved != cfg.get("y_key"):
            cfg = {**cfg, "y_key": y_resolved}
        last_updated: datetime | None = r["last_cleaned_at"] or r["last_ingested_at"]
        points = _build_data_points(tabular, cfg)
        views.append(
            ChartView(
                dataset_id=int(r["dataset_id"]),
                source_id=int(r["source_id"]),
                source_name=source_name,
                source_url=str(r["source_url"]),
                topic_slug=str(r["topic_slug"]),
                chart_config=cfg,
                last_updated=last_updated,
                data_points=points,
            )
        )
    return views
