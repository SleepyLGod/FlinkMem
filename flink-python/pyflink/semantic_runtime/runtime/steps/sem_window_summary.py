"""Internal semantic window-summary step.

This module provides the reusable summary-update execution layer for the
`summary` variant of `sem_window`.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_window_summary_update_prompt,
)


class SemWindowSummaryFunction(SemMapFunction):
    """Internal semantic summary-update helper for `sem_window`."""

    def __init__(self, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=build_sem_window_summary_update_prompt(),
            output_schema={"summary": str},
            llm_config=llm_config,
        )

    def attach_client(self, client: LLMClient) -> None:
        """Attach an existing client owned by a higher-level runtime."""
        self._client = client


def _call_sem_window_summary_sync(
    summary_fn: SemWindowSummaryFunction,
    value: Any,
) -> Dict[str, Any]:
    """Run one semantic summary-update call synchronously and parse the JSON payload."""
    loop = asyncio.new_event_loop()
    try:
        outputs = loop.run_until_complete(summary_fn.async_invoke(value))
    finally:
        loop.close()
    if len(outputs) != 1:
        raise RuntimeError("sem_window_summary expected exactly one output payload")
    parsed = json.loads(outputs[0])
    if not isinstance(parsed, dict):
        raise ValueError("sem_window_summary expected one JSON object payload")
    return parsed


def parse_sem_window_summary(payload: Dict[str, Any]) -> str:
    """Validate one semantic summary-update payload."""
    summary = payload.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise ValueError("sem_window_summary payload must include non-empty summary")
    return summary


def update_sem_window_summary_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    current_summary: str,
    current_event: Dict[str, Any],
) -> str:
    """Update one semantic window summary synchronously."""
    summary_fn = SemWindowSummaryFunction(llm_config)
    summary_fn.attach_client(client)
    payload = _call_sem_window_summary_sync(
        summary_fn,
        {
            "current_summary": current_summary,
            "current_event": current_event,
        },
    )
    return parse_sem_window_summary(payload)
