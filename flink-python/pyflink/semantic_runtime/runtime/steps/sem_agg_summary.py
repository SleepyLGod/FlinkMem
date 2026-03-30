"""Internal semantic summary-update step for ``sem_agg``."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import (
    LLMClient,
    LLMClientConfig,
    create_llm_client,
)
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_agg_summary_update_prompt,
)
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object

_THREAD_LOCAL = threading.local()


def _run_sync(coro):
    """Run one coroutine on a thread-local event loop."""
    loop = getattr(_THREAD_LOCAL, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _THREAD_LOCAL.loop = loop
    return loop.run_until_complete(coro)


async def _call_json(
    client: LLMClient,
    prompt: str,
    *,
    step_name: str,
) -> Dict[str, Any]:
    """Run one async LLM call and parse one JSON object."""
    text, _metrics = await client.call(prompt)
    return parse_llm_json_object(text, operator_name=step_name)


def parse_sem_agg_summary_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate one semantic summary-update payload."""
    summary = payload.get("summary")
    if not isinstance(summary, str):
        raise ValueError("sem_agg_summary payload must include string summary")
    return {"summary": summary}


async def evaluate_sem_agg_summary_update(
    *,
    client: LLMClient,
    mode: str,
    current_summary: str,
    added_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Evaluate one async semantic summary update."""
    prompt = build_sem_agg_summary_update_prompt(mode=mode).format(
        input=json.dumps(
            {
                "current_summary": current_summary,
                "added_events": added_events,
                "mode": mode,
            },
            ensure_ascii=False,
        ),
    )
    payload = await _call_json(client, prompt, step_name="sem_agg_summary")
    return parse_sem_agg_summary_payload(payload)


def evaluate_sem_agg_summary_update_sync(
    *,
    client: LLMClient,
    mode: str,
    current_summary: str,
    added_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Evaluate one synchronous semantic summary update."""
    return _run_sync(
        evaluate_sem_agg_summary_update(
            client=client,
            mode=mode,
            current_summary=current_summary,
            added_events=added_events,
        )
    )


def evaluate_sem_agg_summary_update_from_config_sync(
    *,
    llm_config: LLMClientConfig,
    mode: str,
    current_summary: str,
    added_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Create one client from config and evaluate one summary update."""
    client = create_llm_client(llm_config)
    try:
        return evaluate_sem_agg_summary_update_sync(
            client=client,
            mode=mode,
            current_summary=current_summary,
            added_events=added_events,
        )
    finally:
        client.close()
