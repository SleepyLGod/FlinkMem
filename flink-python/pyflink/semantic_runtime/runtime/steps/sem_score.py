"""Internal `sem_score` step.

This module provides the reusable semantic scoring execution layer.
The caller chooses execution granularity above this layer:

1. one item,
2. one bounded item block.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_score_block_prompt,
    build_sem_score_prompt,
)


class SemScoreFunction(SemMapFunction):
    """Internal semantic score execution helper."""

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
        *,
        block_mode: bool = False,
    ) -> None:
        output_schema = (
            {"scores": list}
            if block_mode
            else {"score": float, "confidence": float, "reason": str}
        )
        super().__init__(
            prompt_template=prompt_template,
            output_schema=output_schema,
            llm_config=llm_config,
        )

    def attach_client(self, client: LLMClient) -> None:
        """Attach an existing client owned by a higher-level runtime."""
        self._client = client


async def _call_sem_score(score_fn: SemScoreFunction, value: Any) -> Dict[str, Any]:
    """Run one sem_score call and return the parsed JSON object."""
    outputs = await score_fn.async_invoke(value)
    if len(outputs) != 1:
        raise RuntimeError("sem_score expected exactly one output payload")
    parsed = json.loads(outputs[0])
    if not isinstance(parsed, dict):
        raise ValueError("sem_score expected one JSON object payload")
    return parsed


def _call_sem_score_sync(score_fn: SemScoreFunction, value: Any) -> Dict[str, Any]:
    """Run one sem_score call synchronously and return the parsed JSON object."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_call_sem_score(score_fn, value))
    finally:
        loop.close()


def parse_sem_score(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate one single-item sem_score payload."""
    score = payload.get("score")
    confidence = payload.get("confidence")
    reason = payload.get("reason")
    if not isinstance(score, (int, float)):
        raise ValueError("sem_score payload must include numeric score")
    if not isinstance(confidence, (int, float)):
        raise ValueError("sem_score payload must include numeric confidence")
    if not isinstance(reason, str):
        raise ValueError("sem_score payload must include string reason")
    return {
        "score": float(score),
        "confidence": float(confidence),
        "reason": reason,
    }


def parse_sem_score_block(
    payload: Dict[str, Any],
    *,
    expected_size: int,
) -> List[Dict[str, Any]]:
    """Validate one block sem_score payload."""
    scores = payload.get("scores")
    if not isinstance(scores, list):
        raise ValueError("sem_score block response must contain a scores list")

    results: List[Dict[str, Any]] = []
    seen_indices: set[int] = set()
    for item in scores:
        if not isinstance(item, dict):
            raise ValueError("sem_score block entry must be a dict")
        item_idx = item.get("item_idx")
        score = item.get("score")
        confidence = item.get("confidence")
        reason = item.get("reason")
        if not isinstance(item_idx, int) or not 0 <= item_idx < expected_size:
            raise ValueError("sem_score block item_idx is out of range")
        if item_idx in seen_indices:
            raise ValueError("sem_score block item_idx must be unique")
        seen_indices.add(item_idx)
        if not isinstance(score, (int, float)):
            raise ValueError("sem_score block entry must include numeric score")
        if not isinstance(confidence, (int, float)):
            raise ValueError("sem_score block entry must include numeric confidence")
        if not isinstance(reason, str):
            raise ValueError("sem_score block entry must include string reason")
        results.append(
            {
                "item_idx": item_idx,
                "score": float(score),
                "confidence": float(confidence),
                "reason": reason,
            }
        )
    return results


async def evaluate_sem_score(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    item: Dict[str, Any],
) -> Dict[str, Any]:
    """Evaluate one item through the reusable sem_score execution step."""
    score_fn = SemScoreFunction(
        build_sem_score_prompt(intent),
        llm_config,
        block_mode=False,
    )
    score_fn.attach_client(client)
    payload = await _call_sem_score(score_fn, item)
    return parse_sem_score(payload)


async def evaluate_sem_score_block(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    score_block: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Evaluate one bounded item block through the reusable sem_score execution step."""
    if not score_block:
        return []

    score_fn = SemScoreFunction(
        build_sem_score_block_prompt(intent),
        llm_config,
        block_mode=True,
    )
    score_fn.attach_client(client)
    payload = await _call_sem_score(score_fn, score_block)
    return parse_sem_score_block(payload, expected_size=len(score_block))


async def evaluate_sem_score_block_payload(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    score_block: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Evaluate one bounded item block and return the raw parsed payload."""
    if not score_block:
        return {"scores": []}

    score_fn = SemScoreFunction(
        build_sem_score_block_prompt(intent),
        llm_config,
        block_mode=True,
    )
    score_fn.attach_client(client)
    return await _call_sem_score(score_fn, score_block)


def evaluate_sem_score_block_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    score_block: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Evaluate one bounded item block synchronously."""
    if not score_block:
        return []

    score_fn = SemScoreFunction(
        build_sem_score_block_prompt(intent),
        llm_config,
        block_mode=True,
    )
    score_fn.attach_client(client)
    payload = _call_sem_score_sync(score_fn, score_block)
    return parse_sem_score_block(payload, expected_size=len(score_block))
