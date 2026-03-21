"""Common helpers for internal pushdown execution."""

from __future__ import annotations

import json
from typing import Any, Dict

from pyflink.semantic_runtime.runtime.event_model import (
    is_window_snapshot,
)


def parse_json_or_passthrough(value: Any, *, operator_name: str) -> Any:
    """Parse JSON strings or return native values unchanged."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"{operator_name} expected valid JSON input") from exc
    return value


def parse_window_snapshot(value: Any, *, operator_name: str) -> Dict[str, Any]:
    """Validate and return one WindowSnapshot-like dict."""
    record = parse_json_or_passthrough(value, operator_name=operator_name)
    if not isinstance(record, dict) or not is_window_snapshot(record):
        raise ValueError(f"{operator_name} requires WindowSnapshot input")
    return record


def parse_candidate_pool(value: Any, *, operator_name: str) -> Dict[str, Any]:
    """Validate and return one bounded candidate pool record."""
    record = parse_json_or_passthrough(value, operator_name=operator_name)
    if not isinstance(record, dict) or not isinstance(record.get("candidates"), list):
        raise ValueError(f"{operator_name} requires a bounded candidate pool with candidates list")
    return record
