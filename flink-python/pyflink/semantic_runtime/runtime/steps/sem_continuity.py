"""Internal `sem_continuity` step.

This module provides the reusable semantic continuity execution layer used by
`sem_window` variants. The caller decides what window state summary to pass.

This module supports multiple continuity contracts:

1. pairwise: previous event + current event,
2. summary: current summary + current event,
3. all_history: active window history + current event.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_window_all_history_prompt,
    build_sem_window_pairwise_prompt,
    build_sem_window_summary_continuity_prompt,
)


class SemContinuityFunction(SemMapFunction):
    """Internal semantic continuity execution helper."""

    def __init__(self, prompt_template: str, llm_config: LLMClientConfig) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={
                "continue_window": bool,
                "confidence": float,
                "reason": str,
            },
            llm_config=llm_config,
        )

    def attach_client(self, client: LLMClient) -> None:
        """Attach an existing client owned by a higher-level runtime."""
        self._client = client


def _call_sem_continuity_sync(
    continuity_fn: SemContinuityFunction,
    value: Any,
) -> Dict[str, Any]:
    """Run one semantic continuity call synchronously and parse the JSON payload."""
    loop = asyncio.new_event_loop()
    try:
        outputs = loop.run_until_complete(continuity_fn.async_invoke(value))
    finally:
        loop.close()
    if len(outputs) != 1:
        raise RuntimeError("sem_continuity expected exactly one output payload")
    parsed = json.loads(outputs[0])
    if not isinstance(parsed, dict):
        raise ValueError("sem_continuity expected one JSON object payload")
    return parsed


def parse_sem_continuity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate one sem_continuity payload."""
    continue_window = payload.get("continue_window")
    confidence = payload.get("confidence")
    reason = payload.get("reason")
    if not isinstance(continue_window, bool):
        raise ValueError("sem_continuity payload must include bool continue_window")
    if not isinstance(confidence, (int, float)):
        raise ValueError("sem_continuity payload must include numeric confidence")
    if not isinstance(reason, str):
        raise ValueError("sem_continuity payload must include string reason")
    return {
        "continue_window": continue_window,
        "confidence": float(confidence),
        "reason": reason,
    }


def evaluate_pairwise_sem_continuity_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    previous_event: Dict[str, Any],
    current_event: Dict[str, Any],
) -> Dict[str, Any]:
    """Evaluate pairwise semantic continuity synchronously."""
    continuity_fn = SemContinuityFunction(
        build_sem_window_pairwise_prompt(),
        llm_config,
    )
    continuity_fn.attach_client(client)
    payload = _call_sem_continuity_sync(
        continuity_fn,
        {
            "previous_event": previous_event,
            "current_event": current_event,
        },
    )
    return parse_sem_continuity(payload)


def evaluate_summary_sem_continuity_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    current_summary: str,
    current_event: Dict[str, Any],
) -> Dict[str, Any]:
    """Evaluate summary-based semantic continuity synchronously."""
    continuity_fn = SemContinuityFunction(
        build_sem_window_summary_continuity_prompt(),
        llm_config,
    )
    continuity_fn.attach_client(client)
    payload = _call_sem_continuity_sync(
        continuity_fn,
        {
            "current_summary": current_summary,
            "current_event": current_event,
        },
    )
    return parse_sem_continuity(payload)


def evaluate_all_history_sem_continuity_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    active_window_events: list[Dict[str, Any]],
    current_event: Dict[str, Any],
) -> Dict[str, Any]:
    """Evaluate all-history semantic continuity synchronously."""
    continuity_fn = SemContinuityFunction(
        build_sem_window_all_history_prompt(),
        llm_config,
    )
    continuity_fn.attach_client(client)
    payload = _call_sem_continuity_sync(
        continuity_fn,
        {
            "active_window_events": active_window_events,
            "current_event": current_event,
        },
    )
    return parse_sem_continuity(payload)
