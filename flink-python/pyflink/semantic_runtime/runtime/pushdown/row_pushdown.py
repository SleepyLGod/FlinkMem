"""Row-level internal physical pushdown builders."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict

from pyflink.common import Time, Types
from pyflink.datastream import AsyncDataStream, DataStream
from pyflink.datastream.functions import FilterFunction, MapFunction

from pyflink.semantic_runtime.public_api import (
    SemFilterRequest,
    SemLocalTopKRequest,
    SemLookupJoinRequest,
    SemMapRequest,
)
from pyflink.semantic_runtime.runtime.plans import (
    lower_sem_filter_request,
    lower_sem_local_topk_request,
    lower_sem_lookup_join_request,
    lower_sem_map_request,
)
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_filter_prompt,
    build_sem_lookup_join_prompt,
    build_sem_map_prompt,
)
from pyflink.semantic_runtime.runtime.pushdown.common import parse_candidate_pool

if TYPE_CHECKING:
    from pyflink.semantic_runtime.runtime_config import RuntimeConfig


class JsonDecisionFilter(FilterFunction):
    """Filter only records whose semantic predicate evaluates to true."""

    def filter(self, value: str) -> bool:
        """Return the semantic decision embedded in one JSON result record."""
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("sem_filter pushdown expected valid JSON output") from exc
        if not isinstance(parsed, dict) or "decision" not in parsed:
            raise ValueError("sem_filter pushdown expected JSON object with decision field")
        return bool(parsed["decision"])


class SemMapProjector(MapFunction):
    """Project sem_map async output into final row output."""

    def __init__(self, *, output_mode: str) -> None:
        self._output_mode = output_mode

    def map(self, value: str) -> str:
        """Project one semantic map result record."""
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("sem_map pushdown expected valid JSON output") from exc

        if self._output_mode == "text":
            if not isinstance(parsed, dict) or "text" not in parsed:
                raise ValueError("sem_map text pushdown expected text envelope")
            return str(parsed["text"])
        return json.dumps(parsed)


class SemLocalTopKProjector(MapFunction):
    """Project scored candidate lists into top-k outputs."""

    def __init__(self, *, k: int, candidates_field: str) -> None:
        self._k = k
        self._candidates_field = candidates_field

    def map(self, value: str) -> str:
        """Sort scored candidates and return the top-k envelope."""
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("sem_local_topk pushdown expected valid JSON output") from exc

        if not isinstance(payload, dict) or "scored_candidates" not in payload:
            raise ValueError("sem_local_topk pushdown expected scored_candidates")
        scored_candidates = payload["scored_candidates"]
        if not isinstance(scored_candidates, list):
            raise ValueError("sem_local_topk pushdown expected scored_candidates list")

        normalized = []
        for item in scored_candidates:
            if not isinstance(item, dict):
                raise ValueError("sem_local_topk pushdown expected dict items")
            if "score" not in item or "candidate" not in item:
                raise ValueError("sem_local_topk pushdown expected candidate and score")
            normalized.append(
                {
                    "candidate": item["candidate"],
                    "score": float(item["score"]),
                    "reason": str(item.get("reason", "")),
                }
            )

        normalized.sort(key=lambda entry: entry["score"], reverse=True)
        top = normalized[: self._k]
        original_count = payload.get("_original_count")
        if original_count is None:
            original_count = len(normalized)

        return json.dumps(
            {
                "top_k": top,
                "k": self._k,
                "original_count": int(original_count),
            }
        )


def apply_sem_filter_pushdown(
    input_stream: DataStream,
    *,
    request: SemFilterRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply semantic predicate generation plus native Flink filter."""
    if timeout_ms <= 0:
        raise ValueError("sem_filter pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_filter pushdown requires async_capacity > 0")

    from pyflink.semantic_runtime.operators.row.sem_filter import SemFilterFunction

    plan = lower_sem_filter_request(request, runtime_config)
    predicate_fn = SemFilterFunction(
        prompt_template=build_sem_filter_prompt(plan.intent),
        llm_config=plan.llm_config,
    )
    predicate_records = AsyncDataStream.unordered_wait(
        input_stream,
        predicate_fn,
        Time.milliseconds(timeout_ms),
        async_capacity,
        Types.STRING(),
    )
    return predicate_records.filter(JsonDecisionFilter())


def apply_sem_map_pushdown(
    input_stream: DataStream,
    *,
    request: SemMapRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply semantic transformation plus native projection."""
    if timeout_ms <= 0:
        raise ValueError("sem_map pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_map pushdown requires async_capacity > 0")

    from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction

    plan = lower_sem_map_request(request, runtime_config)
    transform_fn = SemMapFunction(
        prompt_template=build_sem_map_prompt(
            plan.intent,
            output_schema=plan.output_schema,
            output_mode=plan.output_mode,
        ),
        output_schema=plan.output_schema,
        llm_config=plan.llm_config,
        return_mode=plan.output_mode,
    )
    transformed = AsyncDataStream.unordered_wait(
        input_stream,
        transform_fn,
        Time.milliseconds(timeout_ms),
        async_capacity,
        Types.STRING(),
    )
    return transformed.map(SemMapProjector(output_mode=request.output_mode), output_type=Types.STRING())


def apply_sem_local_topk_pushdown(
    input_stream: DataStream,
    *,
    request: SemLocalTopKRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
    candidates_field: str = "candidates",
) -> DataStream:
    """Apply semantic candidate scoring plus native local top-k projection."""
    if timeout_ms <= 0:
        raise ValueError("sem_local_topk pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_local_topk pushdown requires async_capacity > 0")

    from pyflink.semantic_runtime.operators.row.sem_local_topk import SemLocalTopKScoringFunction

    plan = lower_sem_local_topk_request(
        request,
        runtime_config,
        candidates_field=candidates_field,
    )
    scoring_fn = SemLocalTopKScoringFunction(
        score_intent=plan.intent,
        llm_config=plan.llm_config,
        candidates_field=plan.candidates_field,
    )
    scored = AsyncDataStream.unordered_wait(
        input_stream,
        scoring_fn,
        Time.milliseconds(timeout_ms),
        async_capacity,
        Types.STRING(),
    )
    return scored.map(
        SemLocalTopKProjector(k=plan.k, candidates_field=plan.candidates_field),
        output_type=Types.STRING(),
    )


def apply_sem_lookup_join_pushdown(
    input_stream: DataStream,
    *,
    request: SemLookupJoinRequest,
    runtime_config: "RuntimeConfig",
    timeout_ms: int = 30_000,
    async_capacity: int = 20,
) -> DataStream:
    """Apply retrieval-backed semantic lookup join through the row runtime."""
    if timeout_ms <= 0:
        raise ValueError("sem_lookup_join pushdown requires timeout_ms > 0")
    if async_capacity <= 0:
        raise ValueError("sem_lookup_join pushdown requires async_capacity > 0")

    from pyflink.semantic_runtime.operators.row.sem_lookup_join import SemLookupJoinFunction

    plan = lower_sem_lookup_join_request(request, runtime_config)
    join_fn = SemLookupJoinFunction(
        prompt_template=build_sem_lookup_join_prompt(plan.intent),
        llm_config=plan.llm_config,
        join_config=plan.join_config,
    )
    return AsyncDataStream.unordered_wait(
        input_stream,
        join_fn,
        Time.milliseconds(timeout_ms),
        async_capacity,
        Types.STRING(),
    )


__all__ = [
    "JsonDecisionFilter",
    "SemMapProjector",
    "SemLocalTopKProjector",
    "apply_sem_filter_pushdown",
    "apply_sem_map_pushdown",
    "apply_sem_local_topk_pushdown",
    "apply_sem_lookup_join_pushdown",
]
