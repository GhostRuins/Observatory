"""Health and operational status endpoints."""

from __future__ import annotations

import time
from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from core.config import get_settings
from db.postgres import fetch_one, get_pool

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Aggregated health payload for uptime and pipeline freshness."""

    status: str = Field(description="Overall API status string.")
    uptime_seconds: float = Field(description="Seconds since process start.")
    last_pipeline_finished_at: datetime | None = Field(
        default=None,
        description="Most recent finished daily pipeline run (UTC).",
    )


@router.get("/health", response_model=HealthResponse)
async def read_health(request: Request) -> HealthResponse:
    """Return process uptime and the last successful pipeline completion time."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    started = float(getattr(request.app.state, "started_at", time.time()))
    uptime = max(0.0, time.time() - started)
    row = await fetch_one(
        pool,
        """
        SELECT finished_at
        FROM pipeline_runs
        WHERE run_type = 'daily' AND success = TRUE AND finished_at IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
        """,
    )
    last_run: datetime | None = None
    if row and row.get("finished_at"):
        last_run = row["finished_at"]
    return HealthResponse(status="ok", uptime_seconds=uptime, last_pipeline_finished_at=last_run)
