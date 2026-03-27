"""Regression tests for chart rules, cleaning, JSON helpers, and JSON normalisation."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pandas as pd

from core.chart_axes import pick_y_numeric_name
from core.json_flatten import (
    flatten_world_bank_style_row,
    flatten_world_bank_style_rows,
    maybe_aggregate_world_bank_by_date,
)
from pipeline.clean import _coerce_rename_payload, clean_dataset
from pipeline.chart_suggest import suggest_chart
from pipeline.ingest import _expand_parallel_arrays, _json_to_records
from pipeline.llm_client import _first_json_substring, _parse_json_lenient


def run_coro(coro):
    """Run an async coroutine in tests without requiring pytest-asyncio."""
    return asyncio.run(coro)


class TestChartRules:
    """Rule order: datetime+numeric → line; country+numeric → bar; 2 numeric → scatter; 1 num + cat → bar; 1 num → area; else qwen (degrades without Ollama)."""

    def test_line_when_datetime_and_numeric(self) -> None:
        df = pd.DataFrame(
            {
                "observed_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "temp": [1.0, 2.0],
            }
        )
        cfg = run_coro(suggest_chart(df, "Weather"))
        assert cfg["type"] == "line"
        assert cfg["x_key"] == "observed_at"
        assert cfg["y_key"] == "temp"

    def test_bar_when_country_and_numeric(self) -> None:
        df = pd.DataFrame({"country": ["A", "B"], "value": [1.0, 2.0]})
        cfg = run_coro(suggest_chart(df, "GDP"))
        assert cfg["type"] == "bar"
        assert cfg["x_key"] == "country"

    def test_scatter_when_two_numeric_only(self) -> None:
        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        cfg = run_coro(suggest_chart(df, "Pair"))
        assert cfg["type"] == "scatter"

    def test_bar_when_one_numeric_and_small_category(self) -> None:
        df = pd.DataFrame({"cat": ["x", "y"], "val": [1.0, 2.0]})
        cfg = run_coro(suggest_chart(df, "Cats"))
        assert cfg["type"] == "bar"
        assert cfg["x_key"] == "cat"

    def test_area_when_single_numeric_column(self) -> None:
        df = pd.DataFrame({"only_metric": [0.1, 0.2, 0.3]})
        cfg = run_coro(suggest_chart(df, "Single"))
        assert cfg["type"] == "area"


class TestCleaning:
    """Pandas-first cleaning; optional LLM path returns {} when Ollama is unset."""

    def test_rules_normalize_and_idempotent(self) -> None:
        raw = [{"Country Name": "Z", "GDP": "100"}]
        first_records, _notes = run_coro(clean_dataset(raw))
        second_records, _ = run_coro(clean_dataset(first_records))
        assert first_records == second_records
        assert "country_name" in first_records[0]

    def test_coerce_rename_payload_nested(self) -> None:
        out = _coerce_rename_payload({"mapping": {"OLD": "new_col"}})
        assert out == {"OLD": "new_col"}


class TestIngestJson:
    """World Bank / Open-Meteo style JSON becomes tabular rows."""

    def test_world_bank_pages(self) -> None:
        payload: Any = [
            {"page": 1},
            [{"countryiso3code": "USA", "value": 1}],
        ]
        rows = _json_to_records(payload)
        assert len(rows) == 1
        assert rows[0]["countryiso3code"] == "USA"

    def test_open_meteo_daily(self) -> None:
        payload = {
            "daily": {
                "time": ["2024-01-01", "2024-01-02"],
                "temperature_2m_mean": [1.0, 2.0],
            }
        }
        expanded = _expand_parallel_arrays(payload)
        assert expanded is not None
        assert len(expanded) == 2
        assert expanded[0]["temperature_2m_mean"] == 1.0


class TestWorldBankFlatten:
    """World Bank API nests country/indicator objects — flatten for charts."""

    def test_flattens_nested_country_and_indicator(self) -> None:
        row = {
            "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
            "country": {"id": "US", "value": "United States"},
            "countryiso3code": "USA",
            "date": "2023",
            "value": 1.23e6,
        }
        flat = flatten_world_bank_style_row(row)
        assert "country" not in flat
        assert flat["country_name"] == "United States"
        assert flat["date"] == "2023"
        assert flat["value"] == 1.23e6

    def test_rows_helper(self) -> None:
        rows = flatten_world_bank_style_rows(
            [
                {
                    "indicator": {"id": "X", "value": "L"},
                    "country": {"id": "1", "value": "Z"},
                    "date": "2020",
                    "value": 1.0,
                }
            ]
        )
        assert rows[0]["country_name"] == "Z"

    def test_aggregates_duplicate_dates(self) -> None:
        rows = [
            {"date": "2020", "value": 1.0, "countryiso3code": "USA"},
            {"date": "2020", "value": 3.0, "countryiso3code": "GBR"},
            {"date": "2021", "value": -1.0, "countryiso3code": "USA"},
        ]
        out = maybe_aggregate_world_bank_by_date(rows)
        assert len(out) == 2
        assert out[0]["date"] == "2020"
        assert out[0]["value"] == 2.0
        assert out[1]["value"] == -1.0


class TestChartAxes:
    """Y-axis must prefer measures (value, temperature_…) over surrogate ids."""

    def test_prefers_value_over_id(self) -> None:
        assert pick_y_numeric_name(["id", "value"]) == "value"

    def test_prefers_temperature_over_lat_lon(self) -> None:
        assert (
            pick_y_numeric_name(["latitude", "longitude", "temperature_2m_mean"])
            == "temperature_2m_mean"
        )

    def test_y_key_uses_later_rows_when_first_value_null(self) -> None:
        """World Bank often has value null on first row; do not fall back to decimal=0."""
        from core.chart_axes import pick_y_key_from_sample_rows

        rows = [
            {"date": "2020", "value": None, "decimal": 0},
            {"date": "2021", "value": -1.25, "decimal": 0},
        ]
        assert pick_y_key_from_sample_rows(rows, "date", "decimal") == "value"


class TestLlmJsonHelpers:
    """Lenient JSON extraction used by call_ollama_json."""

    def test_json_substring_from_prose(self) -> None:
        text = 'Here is the result:\n{"a": 1}\nthanks'
        sub = _first_json_substring(text)
        assert sub is not None
        assert json.loads(sub)["a"] == 1

    def test_parse_json_lenient_embedded_object(self) -> None:
        parsed = _parse_json_lenient('prefix {"x": 1} suffix')
        assert parsed is not None
        assert parsed.get("x") == 1


def test_parse_json_lenient_brace_fallback() -> None:
    """Brace matcher extracts JSON embedded in prose."""
    raw = 'Analysis: {"ok": true, "items": [1]}'
    parsed = _parse_json_lenient(raw)
    assert parsed is not None
    assert parsed.get("ok") is True
