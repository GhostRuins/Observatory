"""Dataset listing and detail routes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import get_settings
from db.postgres import fetch_all, fetch_one, get_pool

router = APIRouter(prefix="/datasets", tags=["datasets"])


class DatasetSummary(BaseModel):
    """High-level dataset metadata for list views."""

    id: int
    title: str | None
    source_id: int
    source_name: str
    topic_slug: str
    last_ingested_at: datetime | None
    last_cleaned_at: datetime | None


class DatasetDetail(BaseModel):
    """Full dataset payload including optional cleaned rows and chart config."""

    id: int
    title: str | None
    source_id: int
    source_name: str
    topic_slug: str
    source_url: str
    raw_snapshot: dict[str, Any] | None
    cleaned_data: list[dict[str, Any]] | None
    chart_config: dict[str, Any] | None
    last_ingested_at: datetime | None
    last_cleaned_at: datetime | None


@router.get("", response_model=list[DatasetSummary])
async def list_datasets() -> list[DatasetSummary]:
    """Return all datasets joined with their sources and topics."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    rows = await fetch_all(
        pool,
        """
        SELECT
            d.id,
            d.title,
            d.source_id,
            s.name AS source_name,
            t.slug AS topic_slug,
            d.last_ingested_at,
            d.last_cleaned_at
        FROM datasets AS d
        JOIN sources AS s ON s.id = d.source_id
        JOIN topics AS t ON t.id = s.topic_id
        ORDER BY d.id ASC
        """,
    )
    return [
        DatasetSummary(
            id=int(r["id"]),
            title=r["title"],
            source_id=int(r["source_id"]),
            source_name=str(r["source_name"]),
            topic_slug=str(r["topic_slug"]),
            last_ingested_at=r["last_ingested_at"],
            last_cleaned_at=r["last_cleaned_at"],
        )
        for r in rows
    ]


@router.get("/{dataset_id}", response_model=DatasetDetail)
async def get_dataset(dataset_id: int) -> DatasetDetail:
    """Return a single dataset by identifier."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    row = await fetch_one(
        pool,
        """
        SELECT
            d.id,
            d.title,
            d.source_id,
            s.name AS source_name,
            t.slug AS topic_slug,
            s.url AS source_url,
            d.raw_snapshot,
            d.cleaned_data,
            d.chart_config,
            d.last_ingested_at,
            d.last_cleaned_at
        FROM datasets AS d
        JOIN sources AS s ON s.id = d.source_id
        JOIN topics AS t ON t.id = s.topic_id
        WHERE d.id = $1
        """,
        dataset_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    cleaned = row["cleaned_data"]
    if isinstance(cleaned, str):
        cleaned = json.loads(cleaned)
    if cleaned is not None and not isinstance(cleaned, list):
        cleaned = None

    raw_snap = row["raw_snapshot"]
    if raw_snap is not None and not isinstance(raw_snap, dict):
        raw_snap = dict(raw_snap) if hasattr(raw_snap, "keys") else None

    chart_cfg = row["chart_config"]
    if chart_cfg is not None and not isinstance(chart_cfg, dict):
        chart_cfg = dict(chart_cfg) if hasattr(chart_cfg, "keys") else None

    return DatasetDetail(
        id=int(row["id"]),
        title=row["title"],
        source_id=int(row["source_id"]),
        source_name=str(row["source_name"]),
        topic_slug=str(row["topic_slug"]),
        source_url=str(row["source_url"]),
        raw_snapshot=raw_snap,
        cleaned_data=cleaned,
        chart_config=chart_cfg,
        last_ingested_at=row["last_ingested_at"],
        last_cleaned_at=row["last_cleaned_at"],
    )
