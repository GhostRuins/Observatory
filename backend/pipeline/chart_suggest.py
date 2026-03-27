"""Rule-first chart configuration with optional qwen JSON fallback."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import structlog

from pipeline.llm_client import MODEL_CHART, call_ollama_json
from pipeline.prompts import SYSTEM_CHART_JSON

logger = structlog.get_logger(__name__)


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _detect_datetime_column(df: pd.DataFrame) -> str | None:
    """Return the first column that parses mostly as datetimes, if any."""
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            return str(col)
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            ratio = float(parsed.notna().mean()) if len(series) else 0.0
            if ratio >= 0.6:
                return str(col)
    return None


def _numeric_columns(df: pd.DataFrame) -> list[str]:
    """Return column names that are numeric."""
    cols: list[str] = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(str(col))
    return cols


def _categorical_columns(df: pd.DataFrame, max_unique: int) -> list[str]:
    """Return object/string columns with at most max_unique distinct values."""
    out: list[str] = []
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            unique_count = int(df[col].nunique(dropna=True))
            if unique_count <= max_unique and unique_count >= 1:
                out.append(str(col))
    return out


def _region_column(df: pd.DataFrame) -> str | None:
    """Return the first column named country or region (case-insensitive), if any."""
    for col in df.columns:
        lowered = str(col).lower()
        if lowered in ("country", "region"):
            return str(col)
    return None


async def suggest_chart(df: pd.DataFrame, source_name: str) -> dict[str, Any]:
    """
    Choose a chart type and axes using deterministic rules, then qwen if unknown.

    Returns a chart config dict with keys: type, title, x_key, y_key, color_key, unit,
    source_name, last_updated.
    """
    last_updated = _now_iso()
    base: dict[str, Any] = {
        "type": "unknown",
        "title": source_name,
        "x_key": None,
        "y_key": None,
        "color_key": None,
        "unit": None,
        "source_name": source_name,
        "last_updated": last_updated,
    }

    if df is None or df.empty:
        return await _qwen_fallback(df, source_name, base)

    dt_col = _detect_datetime_column(df)
    nums = _numeric_columns(df)

    if dt_col is not None and nums:
        return {
            **base,
            "type": "line",
            "title": f"{source_name} over time",
            "x_key": dt_col,
            "y_key": nums[0],
        }

    region_col = _region_column(df)
    if region_col is not None and nums:
        return {
            **base,
            "type": "bar",
            "title": f"{source_name} by {region_col}",
            "x_key": region_col,
            "y_key": nums[0],
        }

    if len(nums) == 2:
        return {
            **base,
            "type": "scatter",
            "title": f"{source_name} ({nums[0]} vs {nums[1]})",
            "x_key": nums[0],
            "y_key": nums[1],
        }

    cats = _categorical_columns(df, 12)
    if len(nums) == 1 and cats:
        return {
            **base,
            "type": "bar",
            "title": f"{source_name} by {cats[0]}",
            "x_key": cats[0],
            "y_key": nums[0],
        }

    if len(nums) == 1:
        if dt_col is not None:
            return {
                **base,
                "type": "area",
                "title": f"{source_name} over time",
                "x_key": dt_col,
                "y_key": nums[0],
            }
        df_reset = df.reset_index()
        if "index" in df_reset.columns:
            return {
                **base,
                "type": "area",
                "title": source_name,
                "x_key": "index",
                "y_key": nums[0],
            }
        non_numeric = [str(c) for c in df.columns if str(c) not in nums]
        if non_numeric:
            return {
                **base,
                "type": "area",
                "title": source_name,
                "x_key": non_numeric[0],
                "y_key": nums[0],
            }
        return {
            **base,
            "type": "area",
            "title": source_name,
            "x_key": str(df.columns[0]),
            "y_key": nums[0],
        }

    return await _qwen_fallback(df, source_name, base)


async def _qwen_fallback(df: pd.DataFrame | None, source_name: str, base: dict[str, Any]) -> dict[str, Any]:
    """Ask qwen for a JSON chart config when rules could not decide."""
    try:
        dtypes = {} if df is None or df.empty else {str(c): str(t) for c, t in df.dtypes.items()}
        head_txt = "[]" if df is None or df.empty else df.head(3).to_json(orient="records")
        user_message = json.dumps(
            {"source_name": source_name, "dtypes": dtypes, "sample": head_txt},
            ensure_ascii=False,
        )
        parsed = await call_ollama_json(MODEL_CHART, SYSTEM_CHART_JSON, user_message)
        if not parsed:
            return {
                **base,
                "type": "bar",
                "title": source_name,
                "x_key": None,
                "y_key": None,
            }
        merged = {**base, **parsed}
        merged.setdefault("type", "bar")
        merged.setdefault("title", source_name)
        merged.setdefault("source_name", source_name)
        merged.setdefault("last_updated", _now_iso())
        return merged
    except Exception as exc:
        logger.warning("chart_qwen_fallback_failed", error=str(exc))
        return {
            **base,
            "type": "bar",
            "title": source_name,
            "x_key": None,
            "y_key": None,
        }
