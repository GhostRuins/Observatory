"""Centralised Ollama HTTP client — all LLM traffic goes through this module."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
import structlog

from core.config import get_settings

logger = structlog.get_logger(__name__)

MODEL_CLEANING: str = "llama3"
MODEL_CHART: str = "qwen"
MODEL_DISCOVERY: str = "deepseek-r1:32b"


def _timeout_seconds(model: str) -> float:
    """Return the HTTP timeout in seconds for the given model name."""
    if model == MODEL_DISCOVERY:
        return 120.0
    return 60.0


def _ollama_base_url() -> str | None:
    """Return Ollama base URL from settings; empty means LLM calls are skipped."""
    settings = get_settings()
    raw = settings.ollama_host
    if raw is None or str(raw).strip() == "":
        return None
    return str(raw).rstrip("/")


def _strip_json_fence(text: str) -> str:
    """Remove common markdown code fences from model output."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


async def call_ollama(model: str, system_prompt: str, user_message: str) -> str:
    """
    Call Ollama chat API and return assistant text content.

    If OLLAMA_HOST is unset or empty in configuration, logs a warning and returns an empty string.
    """
    base = _ollama_base_url()
    if base is None:
        logger.warning("ollama_host_unset_skipping_llm", model=model)
        return ""

    url = f"{base}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "stream": False,
    }
    timeout = httpx.Timeout(_timeout_seconds(model))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
    message = data.get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    logger.warning("ollama_unexpected_message_shape", model=model, data_keys=list(data.keys()))
    return ""


async def call_ollama_json(model: str, system_prompt: str, user_message: str) -> dict[str, Any]:
    """
    Call Ollama and parse JSON from the response.

    On parse failure, logs a warning and returns an empty dict {}.
    """
    text = await call_ollama(model, system_prompt, user_message)
    if not text.strip():
        return {}
    cleaned = _strip_json_fence(text)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"items": parsed}
        return {}
    except json.JSONDecodeError as exc:
        logger.warning("ollama_json_parse_failed", model=model, error=str(exc), snippet=cleaned[:200])
        return {}
