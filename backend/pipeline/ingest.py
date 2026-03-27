"""Fetch active sources over HTTP and persist raw snapshots plus health rows."""

from __future__ import annotations

import argparse
import asyncio
import json
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
from io import StringIO
from typing import Any

import httpx
import pandas as pd
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.config import get_settings
from core.json_flatten import flatten_world_bank_style_rows
from db.postgres import ensure_schema_and_seeds, execute, fetch_all, fetch_one, get_pool
from pipeline.chart_suggest import suggest_chart
from pipeline.clean import clean_dataset

logger = structlog.get_logger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time as an aware datetime."""
    return datetime.now(timezone.utc)


def _expand_parallel_arrays(obj: dict[str, Any]) -> list[dict[str, Any]] | None:
    """
    Expand Open-Meteo-style blocks: daily.time + daily.series as parallel arrays.

    Without this, the whole response becomes one row and charts show a single bar.
    """
    for group in ("daily", "hourly", "minutely_15"):
        block = obj.get(group)
        if not isinstance(block, dict):
            continue
        times = block.get("time")
        if not isinstance(times, list) or not times:
            continue
        n = len(times)
        series_keys = [
            k
            for k, v in block.items()
            if k != "time" and isinstance(v, list) and len(v) == n
        ]
        if not series_keys:
            continue
        out: list[dict[str, Any]] = []
        for i in range(n):
            row: dict[str, Any] = {"time": times[i], "date": times[i]}
            for k in series_keys:
                row[k] = block[k][i]
            out.append(row)
        return out
    return None


def _json_to_records(payload: Any) -> list[dict[str, Any]]:
    """Normalise arbitrary JSON into a list of flat dict rows where possible."""
    if payload is None:
        return []
    if isinstance(payload, list):
        if all(isinstance(x, dict) for x in payload):
            return [dict(x) for x in payload]
        # World Bank indicator API: [ {page metadata}, [ {observation}, ... ] ]
        if len(payload) >= 2 and isinstance(payload[1], list):
            second = payload[1]
            if second and all(isinstance(x, dict) for x in second):
                return [dict(x) for x in second]
            nested = _json_to_records(second)
            if nested:
                return nested
        return [{"value": x} for x in payload]
    if isinstance(payload, dict):
        expanded = _expand_parallel_arrays(payload)
        if expanded:
            return expanded
        for key in ("data", "results", "observations", "items", "rows", "records"):
            inner = payload.get(key)
            if isinstance(inner, list) and inner:
                return _json_to_records(inner)
        if "fact" in payload and isinstance(payload["fact"], list):
            return _json_to_records(payload["fact"])
        if "pages" in payload and isinstance(payload["pages"], list):
            rows: list[dict[str, Any]] = []
            for page in payload["pages"]:
                if isinstance(page, dict) and isinstance(page.get("indicators"), list):
                    for ind in page["indicators"]:
                        if isinstance(ind, dict):
                            rows.append(ind)
            if rows:
                return rows
        # SDMX / GHO-style: top-level "value" array of observation dicts
        val = payload.get("value")
        if isinstance(val, list) and val and all(isinstance(x, dict) for x in val):
            return [dict(x) for x in val]
        return [dict(payload)]
    return [{"value": payload}]


def _csv_text_to_records(text: str) -> list[dict[str, Any]]:
    """Parse CSV text into a list of dict records using pandas."""
    frame = pd.read_csv(StringIO(text))
    frame = frame.replace({pd.NA: None})
    records = frame.to_dict(orient="records")
    return [dict(r) for r in records]


def _xml_text_to_records(text: str) -> list[dict[str, Any]]:
    """Parse simple XML into a list of dict rows (child tag names become keys)."""
    root = ET.fromstring(text)
    rows: list[dict[str, Any]] = []
    for child in list(root):
        row: dict[str, Any] = {}
        for el in list(child):
            tag = el.tag.split("}")[-1]
            row[tag] = el.text
        if row:
            rows.append(row)
    if not rows and root is not None:
        row = {child.tag.split("}")[-1]: child.text for child in list(root)}
        if row:
            rows.append(row)
    return rows


def _normalise_body_to_records(body: str, fetch_format: str) -> list[dict[str, Any]]:
    """Convert raw response text into tabular records according to declared format."""
    fmt = fetch_format.lower().strip()
    if fmt == "json":
        payload = json.loads(body)
        records = _json_to_records(payload)
        return flatten_world_bank_style_rows(records)
    if fmt == "csv":
        return _csv_text_to_records(body)
    if fmt == "xml":
        return _xml_text_to_records(body)
    raise ValueError(f"unsupported_fetch_format:{fetch_format}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TransportError)),
    reraise=True,
)
async def _http_get_with_retries(url: str) -> str:
    """Perform an HTTP GET with retries for transient network failures."""
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def ingest_all(dry_run: bool = False) -> None:
    """
    Fetch every active source, persist raw snapshots to datasets, and log health rows.

    Continues after individual source failures; never aborts the full run for one error.
    """
    settings = get_settings()
    await ensure_schema_and_seeds(settings.database_url)
    pool = await get_pool(settings.database_url)
    rows = await fetch_all(
        pool,
        """
        SELECT s.id, s.name, s.url, s.fetch_format
        FROM sources AS s
        WHERE s.is_active = TRUE
        ORDER BY s.id ASC
        """,
    )
    for row in rows:
        source_id = int(row["id"])
        name = str(row["name"])
        url = str(row["url"])
        fetch_format = str(row["fetch_format"])
        try:
            body = await _http_get_with_retries(url)
            records = _normalise_body_to_records(body, fetch_format)
            snapshot: dict[str, Any] = {
                "format": fetch_format,
                "record_count": len(records),
                "records": records[:5000],
            }
            if dry_run:
                logger.info("ingest_dry_run_ok", source_id=source_id, name=name, records=len(records))
                continue
            await execute(
                pool,
                """
                INSERT INTO datasets (source_id, title, raw_snapshot, last_ingested_at, updated_at)
                VALUES ($1, $2, $3::jsonb, $4, $4)
                ON CONFLICT (source_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    raw_snapshot = EXCLUDED.raw_snapshot,
                    last_ingested_at = EXCLUDED.last_ingested_at,
                    updated_at = EXCLUDED.updated_at
                """,
                source_id,
                name,
                json.dumps(snapshot),
                _utc_now(),
            )
            await execute(
                pool,
                """
                INSERT INTO source_health (source_id, status, message, checked_at, updated_at)
                VALUES ($1, 'success', $2, $3, $3)
                """,
                source_id,
                f"ingested {len(records)} rows",
                _utc_now(),
            )
            logger.info("ingest_source_ok", source_id=source_id, name=name, records=len(records))
        except Exception as exc:
            msg = str(exc)
            logger.warning("ingest_source_failed", source_id=source_id, name=name, error=msg)
            if not dry_run:
                await execute(
                    pool,
                    """
                    INSERT INTO source_health (source_id, status, message, checked_at, updated_at)
                    VALUES ($1, 'failure', $2, $3, $3)
                    """,
                    source_id,
                    msg[:2000],
                    _utc_now(),
                )


async def run_clean_and_charts(dry_run: bool = False) -> None:
    """
    Load raw snapshots, run cleaning, then chart suggestion for each dataset.

    Writes cleaned_data and chart_config columns when not in dry-run mode.
    """
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    rows = await fetch_all(
        pool,
        """
        SELECT d.id, d.raw_snapshot, s.name
        FROM datasets AS d
        JOIN sources AS s ON s.id = d.source_id
        WHERE d.raw_snapshot IS NOT NULL
        ORDER BY d.id ASC
        """,
    )
    for row in rows:
        dataset_id = int(row["id"])
        source_name = str(row["name"])
        raw = row["raw_snapshot"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        records = []
        if isinstance(raw, dict):
            records = raw.get("records") or []
        if not isinstance(records, list):
            records = []
        try:
            cleaned, notes = await clean_dataset([dict(r) for r in records if isinstance(r, dict)])
            df = pd.DataFrame(cleaned)
            chart_cfg = await suggest_chart(df, source_name)
            chart_cfg["cleaning_notes"] = notes
            if dry_run:
                logger.info(
                    "clean_chart_dry_run",
                    dataset_id=dataset_id,
                    rows=len(cleaned),
                    chart_type=chart_cfg.get("type"),
                )
                continue
            await execute(
                pool,
                """
                UPDATE datasets
                SET cleaned_data = $2::jsonb,
                    chart_config = $3::jsonb,
                    last_cleaned_at = $4,
                    updated_at = $4
                WHERE id = $1
                """,
                dataset_id,
                json.dumps(cleaned),
                json.dumps(chart_cfg),
                _utc_now(),
            )
            logger.info("clean_chart_ok", dataset_id=dataset_id, source=source_name)
        except Exception as exc:
            logger.warning("clean_chart_failed", dataset_id=dataset_id, error=str(exc))


async def run_daily_pipeline(dry_run: bool = False) -> None:
    """Run ingest, cleaning, chart suggestion, and record a pipeline run row."""
    settings = get_settings()
    pool = await get_pool(settings.database_url)
    started = _utc_now()
    run_row: Any | None = None
    if not dry_run:
        run_row = await fetch_one(
            pool,
            """
            INSERT INTO pipeline_runs (run_type, started_at, finished_at, success, message, updated_at)
            VALUES ('daily', $1, NULL, FALSE, 'running', $1)
            RETURNING id
            """,
            started,
        )
    try:
        await ingest_all(dry_run=dry_run)
        await run_clean_and_charts(dry_run=dry_run)
        finished = _utc_now()
        if not dry_run and run_row is not None:
            rid = int(run_row["id"])
            await execute(
                pool,
                """
                UPDATE pipeline_runs
                SET finished_at = $2,
                    success = TRUE,
                    message = 'completed',
                    updated_at = $2
                WHERE id = $1
                """,
                rid,
                finished,
            )
    except Exception as exc:
        finished = _utc_now()
        if not dry_run and run_row is not None:
            rid = int(run_row["id"])
            await execute(
                pool,
                """
                UPDATE pipeline_runs
                SET finished_at = $2,
                    success = FALSE,
                    message = $3,
                    updated_at = $2
                WHERE id = $1
                """,
                rid,
                finished,
                str(exc)[:2000],
            )
        raise


async def _async_main() -> None:
    """CLI entrypoint for ingestion and optional full daily pipeline."""
    parser = argparse.ArgumentParser(description="Ingest public data sources into Postgres.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse but do not write to the database.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Also run cleaning and chart suggestion after ingest (daily pipeline).",
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
    if args.full:
        await run_daily_pipeline(dry_run=args.dry_run)
    else:
        await ingest_all(dry_run=args.dry_run)


def main() -> None:
    """Synchronous wrapper for asyncio CLI execution."""
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
