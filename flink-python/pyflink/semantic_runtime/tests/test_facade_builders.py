"""Tests for internal facade builders."""

from __future__ import annotations

from unittest.mock import sentinel

from pyflink.semantic_runtime.public_api import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_lookup_join,
    sem_local_topk,
    sem_map,
    sem_topk,
    sem_window,
)
from pyflink.semantic_runtime.runtime import facade_builders
from pyflink.semantic_runtime.runtime_config import RuntimeConfig


def _row_runtime_config(operator_name: str, *, response: str) -> RuntimeConfig:
    return RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                operator_name: {
                    "kernel": {
                        "mock_delay_s": 0.05,
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
            }
        }
    )


def test_build_sem_map_from_request() -> None:
    cfg = _row_runtime_config(
        "sem_map",
        response='{"sentiment":"positive","confidence":0.9}',
    )
    op = facade_builders.build_sem_map_from_request(
        sem_map(intent="Extract sentiment: {input}", output_schema={"sentiment": str}),
        cfg,
    )
    assert op.__class__.__name__ == "SemMapFunction"


def test_build_sem_filter_from_request() -> None:
    cfg = _row_runtime_config(
        "sem_filter",
        response='{"decision": true, "confidence": 0.9, "reason": "ok"}',
    )
    op = facade_builders.build_sem_filter_from_request(
        sem_filter(intent="Keep weather-related events"),
        cfg,
    )
    assert op.__class__.__name__ == "SemFilterFunction"


def test_build_sem_local_topk_from_request() -> None:
    cfg = _row_runtime_config("sem_local_topk", response='["B","A"]')
    op = facade_builders.build_sem_local_topk_from_request(
        sem_local_topk(intent="Rank candidates", k=2),
        cfg,
        candidates_field="items",
    )
    assert op.__class__.__name__ == "SemLocalTopKFunction"


def test_build_sem_lookup_join_from_request() -> None:
    cfg = _row_runtime_config(
        "sem_lookup_join",
        response='{"matched":"candidate_1","score":0.9}',
    )
    op = facade_builders.build_sem_lookup_join_from_request(
        sem_lookup_join(
            intent="Join with the most relevant memory",
            candidate_source=["candidate_1", "candidate_2"],
        ),
        cfg,
    )
    assert op.__class__.__name__ == "SemLookupJoinFunction"


def test_build_sem_window_from_request() -> None:
    op = facade_builders.build_sem_window_from_request(
        sem_window(context=context("window")),
        _stateful_runtime_config(),
    )
    assert op.__class__.__name__ == "SemWindowFunction"


def test_build_sem_groupby_from_request() -> None:
    op = facade_builders.build_sem_groupby_from_request(
        sem_groupby(intent="Group by topic", context=context("session")),
        _stateful_runtime_config(),
    )
    assert op.__class__.__name__ == "SemGroupbyFunction"


def test_build_sem_agg_from_request() -> None:
    op = facade_builders.build_sem_agg_from_request(
        sem_agg(intent="Summarize session", mode="summarize", context=context("window")),
        _stateful_runtime_config(),
    )
    assert op.__class__.__name__ == "WindowOwnedSemAggFunction"


def test_build_sem_topk_from_request_uses_internal_plan(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_sem_topk_pipeline(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.operators.stateful.sem_topk_pipeline.build_sem_topk_pipeline",
        fake_build_sem_topk_pipeline,
    )

    result = facade_builders.build_sem_topk_from_request(
        sentinel.input_ds,
        key_selector=sentinel.key_selector,
        request=sem_topk(intent="Rank by relevance", k=3, context=context("window")),
        runtime_config=_stateful_runtime_config(),
    )

    assert result is sentinel.stream
    assert captured["args"] == (sentinel.input_ds,)
    kwargs = captured["kwargs"]
    assert kwargs["key_selector"] is sentinel.key_selector
    assert kwargs["query_spec"].k == 3
    assert kwargs["query_spec"].trigger_policy.mode == "on_scope_close"
