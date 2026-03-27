"""Hardcoded public data sources — URLs are ingested at runtime from the database."""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from core.config import Settings, get_settings
from db.postgres import execute, fetch_all, get_pool

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SourceConfig:
    """Declarative configuration for a seeded public data source."""

    topic_slug: str
    name: str
    url: str
    fetch_format: str
    refresh_interval_hours: int = 24


SEEDED_SOURCES: tuple[SourceConfig, ...] = (
    SourceConfig(
        topic_slug="climate",
        name="Open-Meteo London historical temperature (2024)",
        url=(
            "https://archive-api.open-meteo.com/v1/archive?"
            "latitude=51.5&longitude=-0.1&start_date=2024-01-01&end_date=2024-12-31"
            "&daily=temperature_2m_mean&timezone=UTC"
        ),
        fetch_format="json",
    ),
    SourceConfig(
        topic_slug="climate",
        name="Our World in Data — CO2 and greenhouse gas emissions",
        url="https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv",
        fetch_format="csv",
    ),
    SourceConfig(
        topic_slug="health",
        name="WHO Global Health Observatory — life expectancy summary",
        url="https://ghoapi.azureedge.net/api/WHOSIS_000001",
        fetch_format="json",
    ),
    SourceConfig(
        topic_slug="health",
        name="Our World in Data — life expectancy (CSV)",
        url=(
            "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
            "Life%20expectancy%20%E2%80%93%20OWID%20based%20on%20UN%20and%20other%20sources/"
            "Life%20expectancy%20%E2%80%93%20OWID%20based%20on%20UN%20and%20other%20sources.csv"
        ),
        fetch_format="csv",
    ),
    SourceConfig(
        topic_slug="economics",
        name="World Bank — GDP (current US$)",
        url=(
            "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD"
            "?format=json&per_page=20000"
        ),
        fetch_format="json",
    ),
    SourceConfig(
        topic_slug="politics",
        name="Our World in Data — Democracy Index (EIU)",
        url=(
            "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
            "Democracy%20Index%20-%20EIU/Democracy%20Index%20-%20EIU.csv"
        ),
        fetch_format="csv",
    ),
    SourceConfig(
        topic_slug="politics",
        name="World Bank — government effectiveness estimate",
        url=(
            "https://api.worldbank.org/v2/country/USA/indicator/GE.EST"
            "?format=json&per_page=20000"
        ),
        fetch_format="json",
    ),
    SourceConfig(
        topic_slug="general",
        name="Our World in Data — Internet users (OWID)",
        url=(
            "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
            "Internet%20users%20%28OWID%29/Internet%20users%20%28OWID%29.csv"
        ),
        fetch_format="csv",
    ),
    SourceConfig(
        topic_slug="general",
        name="Wikipedia pageviews — English monthly aggregate (2023)",
        url=(
            "https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate/"
            "en.wikipedia/all-access/all-agents/monthly/2023010100/2024010100"
        ),
        fetch_format="json",
    ),
)


def _fred_source(settings: Settings) -> SourceConfig | None:
    """Return the FRED unemployment source when an API key is configured."""
    key = settings.fred_api_key
    if not key or not str(key).strip():
        logger.info("fred_source_skipped_missing_api_key")
        return None
    url = (
        "https://api.stlouisfed.org/fred/series/observations?"
        f"series_id=UNRATE&api_key={key}&file_type=json"
        "&sort_order=asc&limit=100000"
    )
    return SourceConfig(
        topic_slug="economics",
        name="FRED — US unemployment rate (UNRATE)",
        url=url,
        fetch_format="json",
    )


async def seed_sources() -> None:
    """
    Insert configured sources if missing (idempotent upsert by topic+name).

    Uses DATABASE_URL from settings and never stores secrets beyond the FRED URL when keyed.
    """
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    topic_rows = await fetch_all(pool, "SELECT id, slug FROM topics")
    topic_map = {str(r["slug"]): int(r["id"]) for r in topic_rows}

    extra = _fred_source(settings)
    all_sources: list[SourceConfig] = list(SEEDED_SOURCES)
    if extra is not None:
        all_sources.append(extra)

    for cfg in all_sources:
        topic_id = topic_map.get(cfg.topic_slug)
        if topic_id is None:
            logger.warning("unknown_topic_slug", slug=cfg.topic_slug)
            continue
        await execute(
            pool,
            """
            INSERT INTO sources (
                topic_id, name, url, fetch_format, refresh_interval_hours, is_active, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, TRUE, NOW())
            ON CONFLICT (topic_id, name) DO UPDATE SET
                url = EXCLUDED.url,
                fetch_format = EXCLUDED.fetch_format,
                refresh_interval_hours = EXCLUDED.refresh_interval_hours,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            """,
            topic_id,
            cfg.name,
            cfg.url,
            cfg.fetch_format,
            cfg.refresh_interval_hours,
        )
    logger.info("sources_seed_complete", count=len(all_sources))
