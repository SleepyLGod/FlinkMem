"""Internal `sem_match` step.

This module provides the reusable semantic pair-judgement execution layer.
The caller chooses granularity above this layer:

1. one pair,
2. one pair block,
3. later other pair-space block shapes.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import build_sem_match_block_prompt


class SemMatchFunction(SemMapFunction):
    """Internal semantic match execution helper."""

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
        *,
        block_mode: bool = False,
    ) -> None:
        output_schema = (
            {"matches": list}
            if block_mode
            else {"matched": bool, "match_score": float, "reason": str}
        )
        super().__init__(
            prompt_template=prompt_template,
            output_schema=output_schema,
            llm_config=llm_config,
        )

    def attach_client(self, client: LLMClient) -> None:
        """Attach an existing client owned by a higher-level runtime."""
        self._client = client


def _call_sem_match_sync(match_fn: SemMatchFunction, value: Any) -> Dict[str, Any]:
    """Run one sem_match call synchronously and return the parsed JSON object."""
    loop = asyncio.new_event_loop()
    try:
        outputs = loop.run_until_complete(match_fn.async_invoke(value))
    finally:
        loop.close()
    if len(outputs) != 1:
        raise RuntimeError("sem_match expected exactly one output payload")
    parsed = json.loads(outputs[0])
    if not isinstance(parsed, dict):
        raise ValueError("sem_match expected one JSON object payload")
    return parsed


def parse_sem_match_block(
    payload: Dict[str, Any],
    *,
    expected_size: int,
) -> List[Dict[str, Any]]:
    """Validate one pair-block sem_match payload."""
    matches = payload.get("matches")
    if not isinstance(matches, list):
        raise ValueError("sem_match block response must contain a matches list")

    results: List[Dict[str, Any]] = []
    for item in matches:
        if not isinstance(item, dict):
            raise ValueError("sem_match match entry must be a dict")
        pair_idx = item.get("pair_idx")
        matched = item.get("matched")
        score = item.get("match_score")
        reason = item.get("reason")
        if not isinstance(pair_idx, int) or not 0 <= pair_idx < expected_size:
            raise ValueError("sem_match match pair_idx is out of range")
        if not isinstance(matched, bool):
            raise ValueError("sem_match match entry must include bool matched")
        if not isinstance(score, (int, float)):
            raise ValueError("sem_match match entry must include numeric match_score")
        if not isinstance(reason, str):
            raise ValueError("sem_match match entry must include string reason")
        results.append(
            {
                "pair_idx": pair_idx,
                "matched": matched,
                "match_score": float(score),
                "reason": reason,
            }
        )
    return results


def evaluate_sem_match_block_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    pair_block: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Evaluate one pair block through the reusable sem_match execution step."""
    if not pair_block:
        return []

    match_fn = SemMatchFunction(
        build_sem_match_block_prompt(intent),
        llm_config,
        block_mode=True,
    )
    match_fn.attach_client(client)
    payload = _call_sem_match_sync(match_fn, pair_block)
    return parse_sem_match_block(payload, expected_size=len(pair_block))
