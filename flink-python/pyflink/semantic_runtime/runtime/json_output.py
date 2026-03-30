"""Helpers for parsing JSON objects returned by LLM backends."""

from __future__ import annotations

import json
import re
from typing import Any, Dict


_JSON_CODE_FENCE_PATTERN = re.compile(
    r"```(?:json)?\s*(?P<body>[\s\S]*?)\s*```",
    re.IGNORECASE,
)


def _strip_json_code_fence(text: str) -> str:
    """Strip one markdown JSON code fence when present."""
    stripped = text.strip()
    match = _JSON_CODE_FENCE_PATTERN.fullmatch(stripped)
    if match is not None:
        return match.group("body").strip()
    return stripped


def _extract_first_json_object_text(text: str) -> str:
    """Extract a best-effort JSON object substring from free-form text."""
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end >= 0 and end > start:
        return text[start : end + 1].strip()
    return text


def parse_llm_json_object(text: Any, *, operator_name: str) -> Dict[str, Any]:
    """Parse one JSON object payload from an LLM text response.

    This parser is strict on payload type (must end as one JSON object) but
    tolerant to common markdown formatting wrappers such as ```json fences.
    """
    if not isinstance(text, str):
        raise ValueError(
            f"{operator_name} expected valid JSON output: response is not string"
        )

    normalized = _strip_json_code_fence(text)
    if not normalized:
        raise ValueError(
            f"{operator_name} expected valid JSON output: response text is empty"
        )

    last_error: Exception | None = None
    candidates = [normalized, _extract_first_json_object_text(normalized)]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except (TypeError, json.JSONDecodeError) as exc:
            last_error = exc
            continue
        if not isinstance(payload, dict):
            raise ValueError(f"{operator_name} expected one JSON object payload")
        return payload

    assert last_error is not None
    raise ValueError(f"{operator_name} expected valid JSON output: {last_error}") from last_error

