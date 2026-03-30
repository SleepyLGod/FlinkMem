"""Internal semantic group-assignment step."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClient
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object
from pyflink.semantic_runtime.runtime.prompt_templates import build_sem_group_assign_prompt

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


def _call_json_sync(client: LLMClient, prompt: str, *, step_name: str) -> Dict[str, Any]:
    """Run one synchronous LLM call and parse one JSON object."""
    return _run_sync(_call_json(client, prompt, step_name=step_name))


def parse_sem_group_assignments(
    payload: Dict[str, Any],
    *,
    existing_group_ids: List[str],
) -> List[Dict[str, Any]]:
    """Validate one semantic group-assignment payload."""
    assignments = payload.get("assignments")
    if not isinstance(assignments, list):
        raise ValueError("sem_group_assign payload must include assignments list")
    known_group_ids = set(str(group_id) for group_id in existing_group_ids)
    normalized: List[Dict[str, Any]] = []
    for item in assignments:
        if not isinstance(item, dict):
            raise ValueError("sem_group_assign assignment items must be objects")
        event_seq_id = item.get("event_seq_id")
        decision = item.get("decision")
        group_id = str(item.get("group_id", "") or "")
        label = str(item.get("label", "") or "")
        confidence = item.get("confidence")
        reason = item.get("reason")
        if not isinstance(event_seq_id, int):
            raise ValueError("sem_group_assign assignment must include int event_seq_id")
        if decision not in {"existing", "new"}:
            raise ValueError("sem_group_assign decision must be 'existing' or 'new'")
        if not isinstance(confidence, (int, float)):
            raise ValueError("sem_group_assign assignment must include numeric confidence")
        if not isinstance(reason, str):
            raise ValueError("sem_group_assign assignment must include string reason")
        if decision == "existing":
            if group_id not in known_group_ids:
                raise ValueError(
                    f"sem_group_assign referenced unknown group_id={group_id!r}"
                )
        else:
            if group_id:
                raise ValueError("sem_group_assign new assignments must not predeclare group_id")
            if not label.strip():
                raise ValueError("sem_group_assign new assignments must include non-empty label")
        normalized.append(
            {
                "event_seq_id": event_seq_id,
                "decision": decision,
                "group_id": group_id,
                "label": label,
                "confidence": float(confidence),
                "reason": reason,
            }
        )
    return normalized


async def evaluate_sem_group_assignments(
    *,
    client: LLMClient,
    intent: str,
    existing_groups: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Evaluate one async semantic group-assignment batch."""
    prompt = build_sem_group_assign_prompt(intent).format(
        existing_groups=json.dumps(existing_groups, ensure_ascii=False),
        events=json.dumps(events, ensure_ascii=False),
    )
    payload = await _call_json(client, prompt, step_name="sem_group_assign")
    return parse_sem_group_assignments(
        payload,
        existing_group_ids=[str(item.get("group_id", "")) for item in existing_groups],
    )


async def evaluate_sem_group_assignment_chunks(
    *,
    client: LLMClient,
    intent: str,
    existing_groups: List[Dict[str, Any]],
    event_chunks: List[List[Dict[str, Any]]],
) -> List[List[Dict[str, Any]]]:
    """Evaluate multiple semantic group-assignment chunks concurrently."""
    tasks = [
        evaluate_sem_group_assignments(
            client=client,
            intent=intent,
            existing_groups=existing_groups,
            events=chunk,
        )
        for chunk in event_chunks
    ]
    return await asyncio.gather(*tasks)


def evaluate_sem_group_assignments_sync(
    *,
    client: LLMClient,
    intent: str,
    existing_groups: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Evaluate one synchronous semantic group-assignment batch."""
    prompt = build_sem_group_assign_prompt(intent).format(
        existing_groups=json.dumps(existing_groups, ensure_ascii=False),
        events=json.dumps(events, ensure_ascii=False),
    )
    payload = _call_json_sync(client, prompt, step_name="sem_group_assign")
    return parse_sem_group_assignments(
        payload,
        existing_group_ids=[str(item.get("group_id", "")) for item in existing_groups],
    )


def evaluate_sem_group_assignment_chunks_sync(
    *,
    client: LLMClient,
    intent: str,
    existing_groups: List[Dict[str, Any]],
    event_chunks: List[List[Dict[str, Any]]],
) -> List[List[Dict[str, Any]]]:
    """Evaluate multiple semantic group-assignment chunks concurrently."""
    return _run_sync(
        evaluate_sem_group_assignment_chunks(
            client=client,
            intent=intent,
            existing_groups=existing_groups,
            event_chunks=event_chunks,
        )
    )
