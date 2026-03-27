"""Chart-oriented views over stored dataset configurations."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from core.config import get_settings
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


def _build_data_points(
    cleaned: Any,
    chart_config: dict[str, Any],
    limit: int = 200,
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
            point["_raw"] = raw
            rows.append(point)
    return rows


@router.get("", response_model=list[ChartView])
async def list_charts(topic: str | None = Query(default=None)) -> list[ChartView]:
    """
    Return chart configurations for datasets that have been processed.

    Optional `topic` filters by topic slug (climate, health, economics, politics, general).
    """
    if topic is not None and topic not in ALL_TOPIC_SLUGS:
        return []

    settings = get_settings()
    pool = await get_pool(settings.database_url)
    if topic is None:
        rows = await fetch_all(
            pool,
            """
            SELECT
                d.id AS dataset_id,
                d.source_id,
                s.name AS source_name,
                s.url AS source_url,
                t.slug AS topic_slug,
                d.chart_config,
                d.cleaned_data,
                d.last_cleaned_at,
                d.last_ingested_at
            FROM datasets AS d
            JOIN sources AS s ON s.id = d.source_id
            JOIN topics AS t ON t.id = s.topic_id
            WHERE d.chart_config IS NOT NULL
            ORDER BY d.id ASC
            """,
        )
    else:
        rows = await fetch_all(
            pool,
            """
            SELECT
                d.id AS dataset_id,
                d.source_id,
                s.name AS source_name,
                s.url AS source_url,
                t.slug AS topic_slug,
                d.chart_config,
                d.cleaned_data,
                d.last_cleaned_at,
                d.last_ingested_at
            FROM datasets AS d
            JOIN sources AS s ON s.id = d.source_id
            JOIN topics AS t ON t.id = s.topic_id
            WHERE d.chart_config IS NOT NULL AND t.slug = $1
            ORDER BY d.id ASC
            """,
            topic,
        )

    views: list[ChartView] = []
    for r in rows:
        cfg = r["chart_config"]
        if cfg is not None and not isinstance(cfg, dict):
            cfg = dict(cfg) if hasattr(cfg, "keys") else {}
        if not cfg:
            continue
        last_updated: datetime | None = r["last_cleaned_at"] or r["last_ingested_at"]
        points = _build_data_points(r.get("cleaned_data"), cfg)
        views.append(
            ChartView(
                dataset_id=int(r["dataset_id"]),
                source_id=int(r["source_id"]),
                source_name=str(r["source_name"]),
                source_url=str(r["source_url"]),
                topic_slug=str(r["topic_slug"]),
                chart_config=cfg,
                last_updated=last_updated,
                data_points=points,
            )
        )
    return views
