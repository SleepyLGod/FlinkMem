"""Internal `sem_rerank` step.

This module provides the reusable semantic reranking execution layer.
The caller decides bounded-pool geometry above this layer.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import build_sem_rerank_block_prompt


class SemRerankFunction(SemMapFunction):
    """Internal semantic rerank execution helper."""

    def __init__(
        self,
        prompt_template: str,
        llm_config: LLMClientConfig,
    ) -> None:
        super().__init__(
            prompt_template=prompt_template,
            output_schema={"ranked_item_ids": list},
            llm_config=llm_config,
        )

    def attach_client(self, client: LLMClient) -> None:
        """Attach an existing client owned by a higher-level runtime."""
        self._client = client


async def _call_sem_rerank(rerank_fn: SemRerankFunction, value: Any) -> Dict[str, Any]:
    """Run one sem_rerank call and return the parsed JSON object."""
    outputs = await rerank_fn.async_invoke(value)
    if len(outputs) != 1:
        raise RuntimeError("sem_rerank expected exactly one output payload")
    parsed = json.loads(outputs[0])
    if not isinstance(parsed, dict):
        raise ValueError("sem_rerank expected one JSON object payload")
    return parsed


def parse_sem_rerank_block(
    payload: Dict[str, Any],
    *,
    valid_item_ids: List[str],
) -> List[str]:
    """Validate one rerank block payload and return ranked ids."""
    ranked_ids = payload.get("ranked_item_ids")
    if not isinstance(ranked_ids, list):
        raise ValueError("sem_rerank block response must contain ranked_item_ids")
    remaining = set(valid_item_ids)
    ranked: List[str] = []
    for item_id in ranked_ids:
        if not isinstance(item_id, str):
            raise ValueError("sem_rerank ranked item ids must be strings")
        if item_id not in remaining:
            raise ValueError("sem_rerank returned unknown or duplicate item id")
        ranked.append(item_id)
        remaining.remove(item_id)
    ranked.extend(item_id for item_id in valid_item_ids if item_id in remaining)
    return ranked


async def evaluate_sem_rerank_block(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    method: str,
    rerank_block: List[Dict[str, Any]],
) -> List[str]:
    """Evaluate one bounded rerank block through the reusable sem_rerank step."""
    if not rerank_block:
        return []

    rerank_fn = SemRerankFunction(
        build_sem_rerank_block_prompt(intent, method=method),
        llm_config,
    )
    rerank_fn.attach_client(client)
    payload = await _call_sem_rerank(rerank_fn, rerank_block)
    valid_item_ids = [str(item["item_id"]) for item in rerank_block]
    return parse_sem_rerank_block(payload, valid_item_ids=valid_item_ids)


def evaluate_sem_rerank_block_sync(
    *,
    client: LLMClient,
    llm_config: LLMClientConfig,
    intent: str,
    method: str,
    rerank_block: List[Dict[str, Any]],
) -> List[str]:
    """Evaluate one bounded rerank block synchronously."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            evaluate_sem_rerank_block(
                client=client,
                llm_config=llm_config,
                intent=intent,
                method=method,
                rerank_block=rerank_block,
            )
        )
    finally:
        loop.close()
