"""Tests for internal per-operator plans."""

from __future__ import annotations

from pyflink.semantic_runtime.public_api import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_join,
    sem_lookup_join,
    sem_local_topk,
    sem_map,
    sem_topk,
    sem_window,
)
from pyflink.semantic_runtime.runtime.plans import (
    lower_sem_agg_request,
    lower_sem_filter_request,
    lower_sem_groupby_request,
    lower_sem_join_request,
    lower_sem_lookup_join_request,
    lower_sem_local_topk_request,
    lower_sem_map_request,
    lower_sem_topk_request,
    lower_sem_window_request,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig


def _runtime_config(operator_name: str, *, response: str, delay_s: float = 0.05) -> RuntimeConfig:
    return RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                operator_name: {
                    "kernel": {
                        "mock_delay_s": delay_s,
                        "mock_response": response,
                    }
                }
            },
        }
    )


def _stateful_runtime_config() -> RuntimeConfig:
    return RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_window": {"kernel": {}},
                "sem_topk": {"query_spec": {}, "kernel": {}},
                "sem_groupby": {"query_spec": {}, "kernel": {}},
                "sem_agg": {"query_spec": {}, "kernel": {}},
                "sem_join": {"query_spec": {}, "kernel": {}},
            }
        }
    )


def test_lower_sem_map_request() -> None:
    runtime_config = _runtime_config(
        "sem_map",
        response='{"sentiment":"positive","confidence":0.9}',
    )
    plan = lower_sem_map_request(
        sem_map(
            intent="Extract sentiment: {input}",
            output_schema={"sentiment": str, "confidence": float},
        ),
        runtime_config,
    )
    assert plan.intent == "Extract sentiment: {input}"
    assert plan.output_mode == "json"
    assert plan.output_schema == {"sentiment": str, "confidence": float}
    assert plan.semantic.backend == "hybrid"
    assert plan.llm_config.backend == "mock"


def test_lower_sem_filter_request() -> None:
    runtime_config = _runtime_config(
        "sem_filter",
        response='{"decision": true, "confidence": 0.9, "reason": "positive"}',
    )
    plan = lower_sem_filter_request(
        sem_filter(intent="Keep weather-related events"),
        runtime_config,
    )
    assert plan.intent == "Keep weather-related events"
    assert plan.semantic.output_mode == "bool"
    assert plan.semantic.backend == "hybrid"
    assert plan.llm_config.backend == "mock"


def test_lower_sem_local_topk_request() -> None:
    runtime_config = _runtime_config(
        "sem_local_topk",
        response='["C","A","B"]',
    )
    plan = lower_sem_local_topk_request(
        sem_local_topk(intent="Rank candidates", k=2),
        runtime_config,
        items_field="items",
    )
    assert plan.intent == "Rank candidates"
    assert plan.k == 2
    assert plan.semantic.output_mode == "score"
    assert plan.semantic.backend == "hybrid"
    assert plan.items_field == "items"


def test_lower_sem_lookup_join_request() -> None:
    runtime_config = _runtime_config(
        "sem_lookup_join",
        response='{"matched":"candidate_1","score":0.9}',
    )
    plan = lower_sem_lookup_join_request(
        sem_lookup_join(
            intent="Join with the most relevant memory",
            candidate_source=["candidate_1", "candidate_2"],
        ),
        runtime_config,
    )
    assert plan.intent == "Join with the most relevant memory"
    assert plan.left_block_size == 1
    assert plan.right_block_size is None
    assert plan.llm_config.backend == "mock"
    assert plan.join_config.mock_candidates == ["candidate_1", "candidate_2"]


def test_lower_sem_window_request() -> None:
    plan = lower_sem_window_request(sem_window(context=context("window")), _stateful_runtime_config())
    assert plan.context_kind == "window"


def test_lower_sem_topk_request_window() -> None:
    plan = lower_sem_topk_request(
        sem_topk(intent="Rank by relevance", k=3, context=context("window")),
        _stateful_runtime_config(),
    )
    assert plan.input_kind == "window_snapshot"
    assert plan.query_spec.trigger_policy.mode == "on_scope_close"
    assert plan.query_spec.scope_policy.window_kind is None


def test_lower_sem_groupby_request_session() -> None:
    plan = lower_sem_groupby_request(
        sem_groupby(intent="Group by topic", context=context("session")),
        _stateful_runtime_config(),
    )
    assert plan.input_kind == "event_stream"
    assert plan.query_spec.scope_policy.window_kind == "session"
    assert plan.query_spec.trigger_policy.mode == "on_event"


def test_lower_sem_agg_request_semantic_segment() -> None:
    plan = lower_sem_agg_request(
        sem_agg(
            intent="Summarize topic segment",
            mode="summarize",
            context=context("semantic_segment"),
        ),
        _stateful_runtime_config(),
    )
    assert plan.input_kind == "event_stream"
    assert plan.query_spec.agg_method == "summarize"
    assert plan.query_spec.trigger_policy.mode == "on_scope_close"
    assert plan.query_spec.scope_policy.window_kind == "semantic"


def test_lower_sem_join_request_stream() -> None:
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {
                        "backend": "llm",
                        "pairing_method": "candidate_pruned",
                    },
                    "kernel": {
                        "pair_block_size": 4,
                        "ttl_seconds": 90,
                    },
                }
            },
        }
    )
    right_input = object()
    plan = lower_sem_join_request(
        sem_join(
            intent="Match contradictory facts",
            context=context("stream"),
            right_input=right_input,
        ),
        runtime_config,
    )
    assert plan.intent == "Match contradictory facts"
    assert plan.context_kind == "stream"
    assert plan.right_input is right_input
    assert plan.query_spec.semantic.instruction == "Match contradictory facts"
    assert plan.kernel_config.pair_block_size == 4
