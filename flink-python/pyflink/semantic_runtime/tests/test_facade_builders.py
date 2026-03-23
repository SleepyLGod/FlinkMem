"""Tests for internal facade builders."""

from __future__ import annotations

from unittest.mock import sentinel

import pytest

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
                "sem_join": {"query_spec": {}, "kernel": {}},
            }
        }
    )


class _FakeDataStream:
    def __init__(self):
        self.connected = None

    def key_by(self, selector):
        self.selector = selector
        return self

    def connect(self, other):
        connected = _FakeConnectedStreams(self, other)
        self.connected = connected
        return connected


class _FakeConnectedStreams:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.processed_with = None

    def process(self, func):
        self.processed_with = func
        return sentinel.join_stream


def test_apply_sem_map_from_request_uses_pushdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_map_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.map_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_map_pushdown",
        fake_apply_sem_map_pushdown,
    )

    result = facade_builders.apply_sem_map_from_request(
        sentinel.input_ds,
        request=sem_map(intent="Extract sentiment", output_schema={"sentiment": str}),
        runtime_config=_row_runtime_config("sem_map", response='{"sentiment":"positive"}'),
    )

    assert result is sentinel.map_stream
    assert captured["args"] == (sentinel.input_ds,)


def test_apply_sem_filter_from_request_uses_pushdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_filter_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.filter_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_filter_pushdown",
        fake_apply_sem_filter_pushdown,
    )

    result = facade_builders.apply_sem_filter_from_request(
        sentinel.input_ds,
        request=sem_filter(intent="Keep weather-related events"),
        runtime_config=_row_runtime_config(
            "sem_filter",
            response='{"decision": true, "confidence": 0.9, "reason": "ok"}',
        ),
    )

    assert result is sentinel.filter_stream
    assert captured["args"] == (sentinel.input_ds,)


def test_apply_sem_local_topk_from_request_uses_pushdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_local_topk_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.topk_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_local_topk_pushdown",
        fake_apply_sem_local_topk_pushdown,
    )

    result = facade_builders.apply_sem_local_topk_from_request(
        sentinel.input_ds,
        request=sem_local_topk(intent="Rank candidates", k=2),
        runtime_config=_row_runtime_config("sem_local_topk", response='["B","A"]'),
        candidates_field="items",
    )

    assert result is sentinel.topk_stream
    assert captured["args"] == (sentinel.input_ds,)
    assert captured["kwargs"]["candidates_field"] == "items"


def test_apply_sem_lookup_join_from_request_uses_pushdown(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_lookup_join_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.lookup_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_lookup_join_pushdown",
        fake_apply_sem_lookup_join_pushdown,
    )

    result = facade_builders.apply_sem_lookup_join_from_request(
        sentinel.input_ds,
        request=sem_lookup_join(
            intent="Join with the most relevant memory",
            candidate_source=["candidate_1", "candidate_2"],
        ),
        runtime_config=_row_runtime_config(
            "sem_lookup_join",
            response='{"matched": true, "match_score": 0.9, "selected_candidate": "candidate_1", "reason": "ok"}',
        ),
    )

    assert result is sentinel.lookup_stream
    assert captured["args"] == (sentinel.input_ds,)


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

    def fake_apply_sem_topk_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_topk_pushdown",
        fake_apply_sem_topk_pushdown,
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
    assert kwargs["request"].k == 3
    assert kwargs["runtime_config"].__class__.__name__ == "RuntimeConfig"


def test_apply_sem_groupby_from_request_uses_pushdown_for_window(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_groupby_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.grouped_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_groupby_pushdown",
        fake_apply_sem_groupby_pushdown,
    )

    result = facade_builders.apply_sem_groupby_from_request(
        sentinel.input_ds,
        request=sem_groupby(intent="Group by topic", context=context("window")),
        runtime_config=_stateful_runtime_config(),
    )

    assert result is sentinel.grouped_stream
    assert captured["args"] == (sentinel.input_ds,)


def test_apply_sem_agg_from_request_uses_pushdown_for_window_algebraic(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_apply_sem_agg_pushdown(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return sentinel.agg_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.facade_builders.apply_sem_agg_pushdown",
        fake_apply_sem_agg_pushdown,
    )

    result = facade_builders.apply_sem_agg_from_request(
        sentinel.input_ds,
        request=sem_agg(intent="Aggregate totals", mode="algebraic", context=context("window")),
        runtime_config=_stateful_runtime_config(),
    )

    assert result is sentinel.agg_stream
    assert captured["args"] == (sentinel.input_ds,)


def test_apply_sem_join_from_request_connects_two_streams() -> None:
    left_ds = _FakeDataStream()
    right_ds = _FakeDataStream()
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {"backend": "llm"},
                    "kernel": {
                        "mock_delay_s": 0.01,
                        "mock_response": '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 0.9, "reason": "ok"}]}',
                    },
                }
            },
        }
    )

    result = facade_builders.apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match if same issue",
            context=context("stream"),
            right_input=right_ds,
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row["key"],
        right_key_selector=lambda row: row["key"],
    )

    assert result is sentinel.join_stream
    assert left_ds.connected is not None
    assert left_ds.connected.right is right_ds
    assert left_ds.connected.processed_with.__class__.__name__ == "SemJoinFunction"


def test_apply_sem_join_from_request_uses_window_owned_runtime() -> None:
    left_ds = _FakeDataStream()
    right_ds = _FakeDataStream()
    runtime_config = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_join": {
                    "query_spec": {"backend": "llm"},
                    "kernel": {"mock_delay_s": 0.01, "mock_response": '{"matches": []}'},
                }
            },
        }
    )
    result = facade_builders.apply_sem_join_from_request(
        left_ds,
        request=sem_join(
            intent="Match if same issue",
            context=context("window"),
            right_input=right_ds,
        ),
        runtime_config=runtime_config,
        left_key_selector=lambda row: row["key"],
        right_key_selector=lambda row: row["key"],
    )
    assert result is sentinel.join_stream
    assert left_ds.connected.processed_with.__class__.__name__ == "WindowOwnedSemJoinFunction"
