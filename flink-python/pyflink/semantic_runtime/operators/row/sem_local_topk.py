# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
sem_local_topk — local (row-level) semantic top-k over a bounded candidate list.

The input record is expected to be a JSON string carrying a ``candidates``
list. The operator uses the internal ``sem_score`` step to score these
candidates and returns the top-k.

This is the **local** V0.1 variant (no keyed state).  The continuous,
stateful ``sem_topk`` lives in ``operators/stateful/sem_topk.py``.

Public callers should use ``build_sem_local_topk_operator(...)`` so ranking
intent stays separate from internal backend configuration.

"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, List, Optional

from pyflink.datastream.functions import AsyncFunction, RuntimeContext

from pyflink.semantic_runtime.llm_client import LLMClient, LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.metrics import OperatorMetrics
from pyflink.semantic_runtime.operators.row._common import validate_generic_sem_spec
from pyflink.semantic_runtime.runtime.steps import (
    evaluate_sem_score_block_payload,
    parse_sem_score_block,
)
from pyflink.semantic_runtime.sem_spec import SemSpec

if TYPE_CHECKING:
    from pyflink.semantic_runtime.runtime_config import RuntimeConfig

logger = logging.getLogger(__name__)


class _BaseSemLocalTopKAsyncFunction(AsyncFunction):
    """Shared async scoring logic for row-level semantic local top-k."""

    def __init__(
        self,
        score_intent: str,
        llm_config: LLMClientConfig,
        candidates_field: str = "candidates",
    ) -> None:
        self._score_intent = score_intent
        self._llm_config = llm_config
        self._candidates_field = candidates_field
        self._client: Optional[LLMClient] = None
        self._op_metrics: Optional[OperatorMetrics] = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._client = create_llm_client(self._llm_config)
        self._op_metrics = OperatorMetrics.from_runtime_context(runtime_context, "sem_local_topk")
        logger.info("SemLocalTopK async function opened (backend=%s)", self._llm_config.backend)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def timeout(self, value) -> List[str]:
        """Fail fast on Flink-level timeout."""
        if self._op_metrics:
            self._op_metrics.record_timeout()
        raise TimeoutError(f"sem_local_topk timed out for input: {value!r}")

    def _parse_input_record(self, value: Any) -> tuple[Any, List[Any]]:
        """Parse one bounded candidate input record."""
        try:
            record = json.loads(value) if isinstance(value, str) else value
            candidates = record[self._candidates_field]
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            logger.warning("sem_topk input parse failed: %s", exc)
            if self._op_metrics:
                self._op_metrics.record_invalid_output()
            raise ValueError(
                f"sem_local_topk input is missing '{self._candidates_field}': {exc}"
            ) from exc
        if not isinstance(candidates, list):
            raise ValueError("sem_local_topk candidates must be a list")
        return record, candidates

    async def _score_candidates(self, value: Any) -> tuple[List[dict], int]:
        """Call the semantic scoring backend and return one scored candidate list."""
        assert self._client is not None, "open() was not called"

        record, candidates = self._parse_input_record(value)
        try:
            payload = await evaluate_sem_score_block_payload(
                client=self._client,
                llm_config=self._llm_config,
                intent=self._score_intent,
                score_block=[
                    {
                        "item_idx": idx,
                        "input_record": record,
                        "candidate": candidate,
                    }
                    for idx, candidate in enumerate(candidates)
                ],
            )
        except Exception as exc:
            logger.warning("LLM call failed for sem_local_topk sem_score block: %s", exc)
            if self._op_metrics:
                self._op_metrics.record_error()
            raise RuntimeError(f"sem_local_topk LLM call failed: {exc}") from exc

        metrics = payload.get("_metrics")
        if self._op_metrics is not None and isinstance(metrics, dict):
            self._op_metrics.record_call(
                float(metrics.get("latency_ms", 0.0)),
                int(metrics.get("input_tokens", 0)),
                int(metrics.get("output_tokens", 0)),
                int(metrics.get("attempts", 1)),
            )

        try:
            score_rows = parse_sem_score_block(payload, expected_size=len(candidates))
        except ValueError as exc:
            if self._op_metrics:
                self._op_metrics.record_invalid_output()
            raise ValueError(f"sem_local_topk expected valid sem_score block output: {exc}") from exc

        score_map = {row["item_idx"]: row for row in score_rows}
        normalized: List[dict] = []
        for idx, candidate in enumerate(candidates):
            if idx not in score_map:
                if self._op_metrics:
                    self._op_metrics.record_invalid_output()
                raise ValueError("sem_local_topk sem_score block is missing candidate score")
            row = score_map[idx]
            normalized.append(
                {
                    "candidate": candidate,
                    "score": float(row["score"]),
                    "reason": str(row["reason"]),
                }
            )
        return normalized, len(candidates)


class SemLocalTopKScoringFunction(_BaseSemLocalTopKAsyncFunction):
    """Internal async scoring step for local top-k pushdown."""

    async def async_invoke(self, value) -> List[str]:
        scored_candidates, original_count = await self._score_candidates(value)
        return [
            json.dumps(
                {
                    "scored_candidates": scored_candidates,
                    "original_count": original_count,
                }
            )
        ]


class SemLocalTopKFunction(_BaseSemLocalTopKAsyncFunction):
    """Async local semantic top-k operator (V0.1, row-level, no keyed state).

    Parameters
    ----------
    prompt_template : str
        Semantic scoring intent used by the internal ``sem_score`` step.
    k : int
        Number of top results to return.
    llm_config : LLMClientConfig
        Picklable LLM backend configuration.
    candidates_field : str
        JSON key in the input record that holds the candidate list
        (default ``"candidates"``).
    """

    def __init__(
        self,
        prompt_template: str,
        k: int,
        llm_config: LLMClientConfig,
        candidates_field: str = "candidates",
    ) -> None:
        super().__init__(
            score_intent=prompt_template,
            llm_config=llm_config,
            candidates_field=candidates_field,
        )
        self._k = k

    # -- core ----------------------------------------------------------------

    async def async_invoke(self, value) -> List[str]:
        scored_candidates, original_count = await self._score_candidates(value)
        # Truncate to k
        top = scored_candidates[: self._k]

        result = {
            "_input": value,
            "top_k": top,
            "k": self._k,
            "original_count": original_count,
        }
        return [json.dumps(result)]


def build_sem_local_topk_operator(
    semantic: SemSpec,
    *,
    k: int,
    runtime_config: "RuntimeConfig",
    candidates_field: str = "candidates",
) -> SemLocalTopKFunction:
    """Build a public row-style semantic local top-k operator."""
    validate_generic_sem_spec(
        semantic,
        operator_name="sem_local_topk",
        allowed_output_modes={"score"},
    )
    if k <= 0:
        raise ValueError("sem_local_topk requires k > 0")
    from pyflink.semantic_runtime.public_api import sem_local_topk
    from pyflink.semantic_runtime.runtime.plans import lower_sem_local_topk_request

    plan = lower_sem_local_topk_request(
        sem_local_topk(intent=semantic.instruction, k=k),
        runtime_config,
        candidates_field=candidates_field,
    )
    return SemLocalTopKFunction(
        prompt_template=plan.intent,
        k=plan.k,
        llm_config=plan.llm_config,
        candidates_field=plan.candidates_field,
    )
