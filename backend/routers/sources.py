"""Public data source listing routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.config import get_settings
from db.postgres import fetch_all, fetch_one, get_pool

router = APIRouter(prefix="/sources", tags=["sources"])


class SourceSummary(BaseModel):
    """Source metadata for dashboards."""

    id: int
    name: str
    url: str
    topic_slug: str
    fetch_format: str
    refresh_interval_hours: int
    is_active: bool


class SourceDetail(SourceSummary):
    """Source detail including timestamps."""

    created_at: datetime
    updated_at: datetime


@router.get("", response_model=list[SourceSummary])
async def list_sources() -> list[SourceSummary]:
    """Return all configured data sources."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    rows = await fetch_all(
        pool,
        """
        SELECT
            s.id,
            s.name,
            s.url,
            t.slug AS topic_slug,
            s.fetch_format,
            s.refresh_interval_hours,
            s.is_active
        FROM sources AS s
        JOIN topics AS t ON t.id = s.topic_id
        ORDER BY s.id ASC
        """,
    )
    return [
        SourceSummary(
            id=int(r["id"]),
            name=str(r["name"]),
            url=str(r["url"]),
            topic_slug=str(r["topic_slug"]),
            fetch_format=str(r["fetch_format"]),
            refresh_interval_hours=int(r["refresh_interval_hours"]),
            is_active=bool(r["is_active"]),
        )
        for r in rows
    ]


@router.get("/{source_id}", response_model=SourceDetail)
async def get_source(source_id: int) -> SourceDetail:
    """Return a single source by identifier."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    row = await fetch_one(
        pool,
        """
        SELECT
            s.id,
            s.name,
            s.url,
            t.slug AS topic_slug,
            s.fetch_format,
            s.refresh_interval_hours,
            s.is_active,
            s.created_at,
            s.updated_at
        FROM sources AS s
        JOIN topics AS t ON t.id = s.topic_id
        WHERE s.id = $1
        """,
        source_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return SourceDetail(
        id=int(row["id"]),
        name=str(row["name"]),
        url=str(row["url"]),
        topic_slug=str(row["topic_slug"]),
        fetch_format=str(row["fetch_format"]),
        refresh_interval_hours=int(row["refresh_interval_hours"]),
        is_active=bool(row["is_active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
