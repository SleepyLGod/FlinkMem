"""Internal semantic group-refinement step."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClient
from pyflink.semantic_runtime.runtime.json_output import parse_llm_json_object
from pyflink.semantic_runtime.runtime.prompt_templates import build_sem_group_refine_prompt

_THREAD_LOCAL = threading.local()


def _run_sync(coro):
    """Run one coroutine on a thread-local event loop."""
    loop = getattr(_THREAD_LOCAL, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _THREAD_LOCAL.loop = loop
    return loop.run_until_complete(coro)


async def _call_json(client: LLMClient, prompt: str) -> Dict[str, Any]:
    """Run one async LLM call and parse one JSON object."""
    text, _metrics = await client.call(prompt)
    return parse_llm_json_object(text, operator_name="sem_group_refine")


def _call_json_sync(client: LLMClient, prompt: str) -> Dict[str, Any]:
    """Run one synchronous LLM call and parse one JSON object."""
    return _run_sync(_call_json(client, prompt))


def parse_sem_group_refine_plan(
    payload: Dict[str, Any],
    *,
    existing_group_ids: List[str],
    group_examples: Dict[str, List[str]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Validate one semantic group-refinement payload."""
    known_group_ids = set(existing_group_ids)
    renames = payload.get("renames", [])
    merges = payload.get("merges", [])
    splits = payload.get("splits", [])
    if not isinstance(renames, list) or not isinstance(merges, list) or not isinstance(splits, list):
        raise ValueError("sem_group_refine payload must include list-valued renames/merges/splits")

    normalized_renames: List[Dict[str, Any]] = []
    renamed_ids = set()
    for item in renames:
        if not isinstance(item, dict):
            raise ValueError("sem_group_refine rename entries must be objects")
        group_id = str(item.get("group_id", "") or "")
        label = str(item.get("label", "") or "")
        if group_id not in known_group_ids:
            raise ValueError(f"sem_group_refine rename referenced unknown group_id={group_id!r}")
        if not label.strip():
            raise ValueError("sem_group_refine rename must include non-empty label")
        renamed_ids.add(group_id)
        normalized_renames.append({"group_id": group_id, "label": label})

    normalized_merges: List[Dict[str, Any]] = []
    merged_ids = set()
    for item in merges:
        if not isinstance(item, dict):
            raise ValueError("sem_group_refine merge entries must be objects")
        target_group_id = str(item.get("target_group_id", "") or "")
        source_group_ids = item.get("source_group_ids")
        label = str(item.get("label", "") or "")
        if not isinstance(source_group_ids, list) or len(source_group_ids) < 2:
            raise ValueError("sem_group_refine merge must include at least two source_group_ids")
        normalized_source_ids = [str(group_id) for group_id in source_group_ids]
        if target_group_id not in normalized_source_ids:
            raise ValueError("sem_group_refine merge target_group_id must be one of source_group_ids")
        if any(group_id not in known_group_ids for group_id in normalized_source_ids):
            raise ValueError("sem_group_refine merge referenced unknown group_id")
        overlap = merged_ids.intersection(normalized_source_ids)
        if overlap:
            raise ValueError(f"sem_group_refine merge reuses already merged groups: {sorted(overlap)!r}")
        merged_ids.update(normalized_source_ids)
        normalized_merges.append(
            {
                "target_group_id": target_group_id,
                "source_group_ids": normalized_source_ids,
                "label": label,
            }
        )

    normalized_splits: List[Dict[str, Any]] = []
    split_ids = set()
    for item in splits:
        if not isinstance(item, dict):
            raise ValueError("sem_group_refine split entries must be objects")
        group_id = str(item.get("group_id", "") or "")
        children = item.get("children")
        if group_id not in known_group_ids:
            raise ValueError(f"sem_group_refine split referenced unknown group_id={group_id!r}")
        if group_id in split_ids:
            raise ValueError(f"sem_group_refine split duplicated group_id={group_id!r}")
        if not isinstance(children, list) or len(children) < 2:
            raise ValueError("sem_group_refine split must include at least two children")
        source_examples = set(str(example) for example in group_examples.get(group_id, []))
        used_examples = set()
        normalized_children: List[Dict[str, Any]] = []
        for child in children:
            if not isinstance(child, dict):
                raise ValueError("sem_group_refine split children must be objects")
            label = str(child.get("label", "") or "")
            examples = child.get("examples")
            if not label.strip():
                raise ValueError("sem_group_refine split children must include non-empty label")
            if not isinstance(examples, list) or not examples:
                raise ValueError("sem_group_refine split children must include non-empty examples")
            normalized_examples = [str(example) for example in examples if str(example).strip()]
            if not normalized_examples:
                raise ValueError("sem_group_refine split children examples must be non-empty strings")
            if any(example not in source_examples for example in normalized_examples):
                raise ValueError("sem_group_refine split children examples must come from source group examples")
            overlap = used_examples.intersection(normalized_examples)
            if overlap:
                raise ValueError(f"sem_group_refine split children overlap on examples: {sorted(overlap)!r}")
            used_examples.update(normalized_examples)
            normalized_children.append({"label": label, "examples": normalized_examples})
        split_ids.add(group_id)
        normalized_splits.append({"group_id": group_id, "children": normalized_children})

    overlap_ids = (merged_ids & split_ids) | (renamed_ids & split_ids)
    if overlap_ids:
        raise ValueError(
            f"sem_group_refine cannot apply conflicting operations to the same groups: {sorted(overlap_ids)!r}"
        )

    return {
        "renames": normalized_renames,
        "merges": normalized_merges,
        "splits": normalized_splits,
    }


async def evaluate_sem_group_refine(
    *,
    client: LLMClient,
    intent: str,
    groups: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Evaluate one async semantic refinement pass."""
    prompt = build_sem_group_refine_prompt(intent).format(
        groups=json.dumps(groups, ensure_ascii=False),
    )
    payload = await _call_json(client, prompt)
    return parse_sem_group_refine_plan(
        payload,
        existing_group_ids=[str(item.get("group_id", "")) for item in groups],
        group_examples={
            str(item.get("group_id", "")): [str(example) for example in item.get("examples", [])]
            for item in groups
        },
    )


def evaluate_sem_group_refine_sync(
    *,
    client: LLMClient,
    intent: str,
    groups: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Evaluate one synchronous semantic refinement pass."""
    prompt = build_sem_group_refine_prompt(intent).format(
        groups=json.dumps(groups, ensure_ascii=False),
    )
    payload = _call_json_sync(client, prompt)
    return parse_sem_group_refine_plan(
        payload,
        existing_group_ids=[str(item.get("group_id", "")) for item in groups],
        group_examples={
            str(item.get("group_id", "")): [str(example) for example in item.get("examples", [])]
            for item in groups
        },
    )
