"""LLM-assisted discovery of candidate public data sources (never auto-activated)."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import structlog

from core.config import get_settings
from db.postgres import ensure_schema_and_seeds, execute, fetch_all, get_pool
from pipeline.llm_client import MODEL_DISCOVERY, call_ollama_json
from pipeline.prompts import SYSTEM_DISCOVERY

logger = structlog.get_logger(__name__)


DEFAULT_CANDIDATES: list[dict[str, str]] = [
    {
        "topic": "climate",
        "url": "https://api.worldbank.org/v2/country/all/indicator/EN.ATM.CO2E.PC?format=json&per_page=50",
        "name": "World Bank CO2 per capita (candidate)",
    },
    {
        "topic": "health",
        "url": "https://ghoapi.azureedge.net/api/Indicator?$top=5",
        "name": "WHO GHO indicators sample (candidate)",
    },
    {
        "topic": "economics",
        "url": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&per_page=20",
        "name": "World Bank GDP per capita (candidate)",
    },
]


def _utc_now() -> datetime:
    """Return current UTC time as an aware datetime."""
    return datetime.now(timezone.utc)


async def discover_candidates(dry_run: bool = False) -> None:
    """
    Evaluate candidate URLs with deepseek-r1:32b and insert rows into pending_sources.

    Never activates sources automatically — human review is required.
    """
    settings = get_settings()
    await ensure_schema_and_seeds(settings.database_url)
    pool = await get_pool(settings.database_url)
    topic_rows = await fetch_all(pool, "SELECT id, slug FROM topics")
    topic_ids = {str(r["slug"]): int(r["id"]) for r in topic_rows}

    user_blob = json.dumps({"candidates": DEFAULT_CANDIDATES}, ensure_ascii=False)
    parsed = await call_ollama_json(MODEL_DISCOVERY, SYSTEM_DISCOVERY, user_blob)
    items: list[dict[str, Any]] = []
    if isinstance(parsed.get("items"), list):
        items = [x for x in parsed["items"] if isinstance(x, dict)]
    elif isinstance(parsed, list):
        items = [x for x in parsed if isinstance(x, dict)]
    else:
        for key in ("results", "candidates", "evaluations"):
            val = parsed.get(key)
            if isinstance(val, list):
                items = [x for x in val if isinstance(x, dict)]
                break

    if not items:
        for cand in DEFAULT_CANDIDATES:
            slug = cand["topic"]
            topic_id = topic_ids.get(slug)
            items.append(
                {
                    "url": cand["url"],
                    "name": cand["name"],
                    "relevance_score": 0.5,
                    "summary": "Fallback row — model returned no structured JSON.",
                    "concerns": "needs_manual_review",
                    "_topic_id": topic_id,
                }
            )

    for item in items:
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        name = item.get("name")
        score = item.get("relevance_score")
        summary = item.get("summary")
        concerns = item.get("concerns")
        topic_id = item.get("_topic_id")
        if topic_id is None:
            slug_guess = str(item.get("topic", "general")).lower()
            topic_id = topic_ids.get(slug_guess) or topic_ids.get("general")
        notes = summary
        if concerns:
            notes = f"{summary or ''} | concerns: {concerns}"
        if dry_run:
            logger.info("discover_dry_run", url=url, name=name, topic_id=topic_id)
            continue
        await execute(
            pool,
            """
            INSERT INTO pending_sources (
                topic_id, name, url, candidate_reason, score, evaluation_notes, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            topic_id,
            name,
            url,
            "weekly_discovery",
            score,
            notes,
            _utc_now(),
        )
        logger.info("pending_source_inserted", url=url)

    if not dry_run:
        now = _utc_now()
        await execute(
            pool,
            """
            INSERT INTO pipeline_runs (run_type, started_at, finished_at, success, message, updated_at)
            VALUES ('weekly_discover', $1, $2, TRUE, $3, $2)
            """,
            now,
            now,
            "discover_completed",
        )


async def _async_main() -> None:
    """CLI entrypoint for weekly discovery."""
    parser = argparse.ArgumentParser(description="Discover and evaluate candidate data sources.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without inserting pending_sources rows.",
    )
    args = parser.parse_args()
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
    )
    await discover_candidates(dry_run=args.dry_run)


def main() -> None:
    """Synchronous wrapper for asyncio CLI execution."""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
