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


def _extract_assistant_text(data: dict[str, Any]) -> str:
    """
    Extract assistant text from an Ollama /api/chat JSON body.

    Also supports OpenAI-compatible responses (e.g. Ollama served at /v1/chat/completions)
    and thinking models that may expose reasoning in alternate fields when content is empty.
    """
    if not isinstance(data, dict):
        return ""

    choices = data.get("choices")
    if isinstance(choices, list) and len(choices) > 0:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message")
            if isinstance(msg, dict):
                c = msg.get("content")
                if isinstance(c, str) and c.strip():
                    return c
                if isinstance(c, list):
                    return _content_parts_to_text(c)

    msg = data.get("message")
    if isinstance(msg, dict):
        c = msg.get("content")
        if isinstance(c, str) and c.strip():
            return c
        if isinstance(c, list):
            text = _content_parts_to_text(c)
            if text.strip():
                return text

        for key in ("reasoning_content", "reasoning"):
            val = msg.get(key)
            if isinstance(val, str) and val.strip():
                return val

        thinking = msg.get("thinking")
        if isinstance(thinking, str) and thinking.strip():
            return thinking

    return ""


def _content_parts_to_text(parts: list[Any]) -> str:
    """Join multimodal content parts into a single string when possible."""
    out: list[str] = []
    for part in parts:
        if isinstance(part, str):
            out.append(part)
        elif isinstance(part, dict):
            if part.get("type") == "text" and isinstance(part.get("text"), str):
                out.append(part["text"])
            elif isinstance(part.get("content"), str):
                out.append(part["content"])
    return "".join(out)


def _first_json_substring(text: str) -> str | None:
    """
    If the model wraps JSON in prose, return the first balanced {...} or [...] span.

    Returns None if no plausible JSON segment is found.
    """
    text = text.strip()
    for open_ch, close_ch in (("{", "}"), ("[", "]")):
        start = text.find(open_ch)
        if start < 0:
            continue
        depth = 0
        in_string = False
        escape = False
        for idx in range(start, len(text)):
            ch = text[idx]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
                continue
            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
    return None


async def call_ollama(model: str, system_prompt: str, user_message: str) -> str:
    """
    Call Ollama POST /api/chat and return assistant text content.

    If OLLAMA_HOST is unset or empty in configuration, logs a warning and returns an empty
    string. Network and HTTP failures are logged and return an empty string so callers can
    fall back to rule-based logic without crashing.
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
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        body = exc.response.text[:500] if exc.response is not None else ""
        logger.warning(
            "ollama_http_error",
            model=model,
            status_code=exc.response.status_code if exc.response else None,
            body_snippet=body,
        )
        return ""
    except httpx.RequestError as exc:
        logger.warning("ollama_request_failed", model=model, error=str(exc))
        return ""
    except ValueError as exc:
        logger.warning("ollama_invalid_json_response", model=model, error=str(exc))
        return ""

    if not isinstance(data, dict):
        logger.warning("ollama_response_not_object", model=model, got_type=type(data).__name__)
        return ""

    text = _extract_assistant_text(data)
    if text.strip():
        return text

    logger.warning(
        "ollama_empty_assistant_text",
        model=model,
        top_keys=list(data.keys()),
    )
    return ""


async def call_ollama_json(model: str, system_prompt: str, user_message: str) -> dict[str, Any]:
    """
    Call Ollama and parse JSON from the response.

    On parse failure, tries to extract a JSON object/array substring from the text.
    On total failure, logs a warning and returns an empty dict {}.
    """
    text = await call_ollama(model, system_prompt, user_message)
    if not text.strip():
        return {}
    cleaned = _strip_json_fence(text)
    parsed = _parse_json_lenient(cleaned)
    if parsed is not None:
        return parsed
    logger.warning("ollama_json_parse_failed", model=model, snippet=cleaned[:200])
    return {}


def _parse_json_lenient(cleaned: str) -> dict[str, Any] | None:
    """Parse JSON from model text, with substring and brace-matching fallbacks."""
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"items": parsed}
        return None
    except json.JSONDecodeError:
        pass

    subset = _first_json_substring(cleaned)
    if subset and subset != cleaned:
        try:
            parsed = json.loads(subset)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"items": parsed}
        except json.JSONDecodeError:
            pass

    return None
