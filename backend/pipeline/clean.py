"""Pandas-first data cleaning with optional llama3-assisted column renaming."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import structlog

from pipeline.llm_client import MODEL_CLEANING, call_ollama_json
from pipeline.prompts import SYSTEM_CLEANING_RENAME

logger = structlog.get_logger(__name__)


def _is_snake_case(name: str) -> bool:
    """Return True if the string looks like a clean snake_case identifier."""
    return bool(re.match(r"^[a-z][a-z0-9_]*$", name))


def _looks_ambiguous_column(name: str) -> bool:
    """Heuristic: non-ASCII, non-snake_case, or very short cryptic tokens."""
    if not name or not isinstance(name, str):
        return True
    try:
        name.encode("ascii")
    except UnicodeEncodeError:
        return True
    if len(name) <= 1:
        return True
    if _is_snake_case(name):
        return False
    if re.match(r"^col\d+$", name, re.IGNORECASE):
        return True
    if re.match(r"^[A-Z0-9_]{1,6}$", name) and not name.islower():
        return True
    return True


def _rules_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply deterministic pandas rules: names, types, nulls, duplicates."""
    if df.empty:
        return df
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out.columns = [re.sub(r"\s+", "_", c) for c in out.columns]
    out.columns = [re.sub(r"[^0-9a-zA-Z_]+", "_", c).strip("_").lower() for c in out.columns]
    for col in out.columns:
        series = out[col]
        if pd.api.types.is_object_dtype(series):
            coerced = pd.to_numeric(series, errors="coerce")
            if coerced.notna().sum() >= max(1, series.notna().sum() // 2):
                out[col] = coerced
    out = out.dropna(how="all")
    out = out.dropna(axis=1, how="all")
    out = out.drop_duplicates()
    return out


def _coerce_rename_payload(data: dict[str, Any]) -> dict[str, Any]:
    """
    Unwrap common LLM JSON shapes: {'mapping': {...}}, {'rename_map': {...}}, etc.

    Returns a flat dict of original_column -> new_snake_case_name.
    """
    if not data:
        return {}
    for key in ("mapping", "rename_map", "renames", "column_map", "columns"):
        inner = data.get(key)
        if isinstance(inner, dict) and inner:
            return dict(inner)
    return data


async def _llm_rename_mapping(columns: list[str], sample_rows: list[dict[str, Any]]) -> dict[str, str]:
    """Ask llama3 for a rename map; returns {} on failure."""
    user_payload = {"columns": columns, "sample": sample_rows[:15]}
    user_message = json.dumps(user_payload, ensure_ascii=False)
    raw = await call_ollama_json(MODEL_CLEANING, SYSTEM_CLEANING_RENAME, user_message)
    data = _coerce_rename_payload(raw)
    mapping: dict[str, str] = {}
    if not data:
        return mapping
    for old, new in data.items():
        if isinstance(old, str) and isinstance(new, str) and new.strip():
            mapping[old] = new.strip().lower().replace(" ", "_")
    return mapping


def _dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Serialise a DataFrame to JSON-friendly records (native types)."""
    safe = df.replace({pd.NA: None})
    records = safe.to_dict(orient="records")
    cleaned: list[dict[str, Any]] = []
    for row in records:
        out_row: dict[str, Any] = {}
        for key, value in row.items():
            if hasattr(value, "item"):
                try:
                    value = value.item()
                except Exception:
                    pass
            if isinstance(value, pd.Timestamp):
                value = value.isoformat()
            if isinstance(value, datetime):
                value = value.replace(tzinfo=timezone.utc).isoformat()
            out_row[str(key)] = value
        cleaned.append(out_row)
    return cleaned


async def clean_dataset(raw_data: list[dict]) -> tuple[list[dict], str]:
    """
    Clean tabular data: pandas rules first, then optional llama3 column renaming.

    Returns (cleaned_records, notes). On total failure, returns the original raw_data unchanged.
    """
    notes_parts: list[str] = []
    if not raw_data:
        return [], "empty_input"

    try:
        df = pd.DataFrame(raw_data)
    except Exception as exc:
        logger.warning("clean_dataset_dataframe_failed", error=str(exc))
        return raw_data, f"dataframe_construct_failed:{exc}"

    try:
        working = _rules_clean_dataframe(df)
        ambiguous = [c for c in working.columns if _looks_ambiguous_column(str(c))]
        if ambiguous:
            sample = _dataframe_to_records(working.head(20))
            mapping = await _llm_rename_mapping([str(c) for c in working.columns], sample)
            if mapping:
                working = working.rename(columns=mapping)
                working = _rules_clean_dataframe(working)
                notes_parts.append("llm_rename_applied")
            else:
                notes_parts.append("llm_rename_skipped_or_empty")
        else:
            notes_parts.append("rules_only")

        records = _dataframe_to_records(working)
        notes = ";".join(notes_parts) if notes_parts else "ok"
        return records, notes
    except Exception as exc:
        logger.warning("clean_dataset_failed_restoring_raw", error=str(exc))
        return raw_data, f"clean_failed:{exc}"
