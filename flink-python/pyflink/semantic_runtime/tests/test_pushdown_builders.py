"""Tests for internal physical pushdown builders."""

from __future__ import annotations

from unittest.mock import sentinel

import pytest

from pyflink.semantic_runtime.public_api import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_local_topk,
    sem_lookup_join,
    sem_map,
    sem_topk,
)
from pyflink.semantic_runtime.runtime import pushdown
from pyflink.semantic_runtime.runtime.pushdown import row_pushdown, stateful_pushdown
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


class _FakePredicateStream:
    def __init__(self) -> None:
        self.filter_func = None

    def filter(self, func):
        self.filter_func = func
        return sentinel.filtered_stream


class _FakeMappedStream:
    def __init__(self) -> None:
        self.map_func = None

    def map(self, func, output_type=None):
        self.map_func = func
        return sentinel.mapped_stream


class _FakeFlatMappedStream:
    def __init__(self) -> None:
        self.flat_map_func = None

    def flat_map(self, func, output_type=None):
        self.flat_map_func = func
        return sentinel.flat_mapped_stream


def test_apply_sem_filter_pushdown_builds_async_then_native_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}
    predicate_stream = _FakePredicateStream()

    def fake_unordered_wait(input_stream, async_fn, timeout, async_capacity, output_type):
        captured["input_stream"] = input_stream
        captured["async_fn"] = async_fn
        captured["timeout"] = timeout
        captured["async_capacity"] = async_capacity
        captured["output_type"] = output_type
        return predicate_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.pushdown.row_pushdown.AsyncDataStream.unordered_wait",
        fake_unordered_wait,
    )

    result = pushdown.apply_sem_filter_pushdown(
        sentinel.input_stream,
        request=sem_filter(intent="Keep weather-related events"),
        runtime_config=_row_runtime_config(
            "sem_filter",
            response='{"decision": true, "confidence": 0.9, "reason": "ok"}',
        ),
        timeout_ms=12_345,
        async_capacity=7,
    )

    assert result is sentinel.filtered_stream
    assert captured["input_stream"] is sentinel.input_stream
    assert captured["async_fn"].__class__.__name__ == "SemFilterFunction"
    assert captured["async_capacity"] == 7
    assert predicate_stream.filter_func.filter('{"decision": true}') is True
    assert predicate_stream.filter_func.filter('{"decision": false}') is False


def test_apply_sem_map_pushdown_builds_async_then_native_projection(monkeypatch) -> None:
    captured: dict[str, object] = {}
    projected_stream = _FakeMappedStream()

    def fake_unordered_wait(input_stream, async_fn, timeout, async_capacity, output_type):
        captured["input_stream"] = input_stream
        captured["async_fn"] = async_fn
        return projected_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.pushdown.row_pushdown.AsyncDataStream.unordered_wait",
        fake_unordered_wait,
    )

    result = pushdown.apply_sem_map_pushdown(
        sentinel.input_stream,
        request=sem_map(intent="Extract sentiment", output_schema={"sentiment": str}),
        runtime_config=_row_runtime_config(
            "sem_map",
            response='{"sentiment":"positive"}',
        ),
    )

    assert result is sentinel.mapped_stream
    assert captured["async_fn"].__class__.__name__ == "SemMapFunction"
    assert projected_stream.map_func.map('{"sentiment":"positive"}') == '{"sentiment": "positive"}'


def test_apply_sem_local_topk_pushdown_builds_async_then_native_topk(monkeypatch) -> None:
    captured: dict[str, object] = {}
    projected_stream = _FakeMappedStream()

    def fake_unordered_wait(input_stream, async_fn, timeout, async_capacity, output_type):
        captured["input_stream"] = input_stream
        captured["async_fn"] = async_fn
        return projected_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.pushdown.row_pushdown.AsyncDataStream.unordered_wait",
        fake_unordered_wait,
    )

    result = pushdown.apply_sem_local_topk_pushdown(
        sentinel.input_stream,
        request=sem_local_topk(intent="Rank candidates", k=2),
        runtime_config=_row_runtime_config(
            "sem_local_topk",
            response='{"scored_candidates":[{"candidate":"a","score":0.4},{"candidate":"b","score":0.9},{"candidate":"c","score":0.7}]}',
        ),
    )

    assert result is sentinel.mapped_stream
    assert captured["async_fn"].__class__.__name__ == "SemLocalTopKScoringFunction"
    projected = projected_stream.map_func.map(
        '{"scored_candidates":[{"candidate":"a","score":0.4},{"candidate":"b","score":0.9},{"candidate":"c","score":0.7}]}'
    )
    assert '"candidate": "b"' in projected
    assert '"candidate": "c"' in projected


def test_apply_sem_lookup_join_pushdown_builds_async_runtime(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_unordered_wait(input_stream, async_fn, timeout, async_capacity, output_type):
        captured["input_stream"] = input_stream
        captured["async_fn"] = async_fn
        return sentinel.joined_stream

    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.pushdown.row_pushdown.AsyncDataStream.unordered_wait",
        fake_unordered_wait,
    )

    cfg = RuntimeConfig.from_dict(
        {
            "llm": {"backend": "mock"},
            "operators": {
                "sem_lookup_join": {
                    "kernel": {
                        "mock_delay_s": 0.05,
                        "mock_response": '{"matched": true, "match_score": 0.9, "selected_candidate": {"candidate_id":"c1"}, "reason": "best"}',
                        "right_block_size": 2,
                    }
                }
            },
        }
    )

    result = pushdown.apply_sem_lookup_join_pushdown(
        sentinel.input_stream,
        request=sem_lookup_join(intent="Join with relevant memory", candidate_source=["c1", "c2"]),
        runtime_config=cfg,
    )

    assert result is sentinel.joined_stream
    assert captured["async_fn"].__class__.__name__ == "SemLookupJoinFunction"


def test_apply_sem_groupby_pushdown_builds_native_label_then_flat_map() -> None:
    snapshot = {
        "key": "k1",
        "window_id": "w1",
        "trigger_reason": "close",
        "events": [
            {"key": "k1", "payload": "project budget", "seq_id": 1},
            {"key": "k1", "payload": "project risk", "seq_id": 2},
            {"key": "k1", "payload": "travel flight", "seq_id": 3},
        ],
    }
    cfg = RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_groupby": {
                    "query_spec": {},
                    "kernel": {"assignment_method": "rule", "confidence_threshold": 0.5},
                }
            }
        }
    )

    envelope = stateful_pushdown.WindowOwnedLocalGroupbyLabeler(
        config=cfg.get_groupby_kernel_config(),
        query_spec=cfg.get_groupby_query_spec(),
    ).map(snapshot)

    rows = stateful_pushdown.GroupbyAssignmentsEmitter().flat_map(envelope)
    assert len(rows) == 3
    assert rows[0]["group_id"] == rows[1]["group_id"]
    assert rows[2]["group_id"] != rows[0]["group_id"]


def test_apply_sem_topk_pushdown_builds_native_projection_for_external_score() -> None:
    pool = {
        "key": "k1",
        "query": "budget",
        "query_seq_id": 9,
        "candidates": [
            {"candidate_id": "c1", "text": "Budget plan", "score": 0.7},
            {"candidate_id": "c2", "text": "Budget risk", "score": 0.95},
            {"candidate_id": "c3", "text": "Travel agenda", "score": 0.4},
        ],
    }
    cfg = RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_topk": {
                    "query_spec": {},
                    "kernel": {"scorer_backend": "external_score"},
                }
            }
        }
    )
    envelope = stateful_pushdown.TopKExternalScoreEnvelopeBuilder(score_field="score").map(pool)
    out = stateful_pushdown.StatefulTopKProjector(k=2).map(envelope)
    assert out["top_ids"] == ["c2", "c1"]
    assert len(out["topk"]) == 2


def test_apply_sem_agg_pushdown_aggregates_one_snapshot() -> None:
    def sum_reduce(acc, event):
        return {"key": acc.get("key", "k"), "total": acc.get("total", 0) + event.get("total", 0)}

    snapshot = {
        "key": "k",
        "window_id": "w1",
        "trigger_reason": "close",
        "events": [
            {"key": "k", "payload": "a", "total": 3, "seq_id": 1},
            {"key": "k", "payload": "b", "total": 5, "seq_id": 2},
        ],
    }
    cfg = RuntimeConfig.from_dict(
        {
            "operators": {
                "sem_agg": {
                    "query_spec": {},
                    "kernel": {"mode": "algebraic", "reduce_fn": sum_reduce},
                }
            }
        }
    )
    out = stateful_pushdown.WindowAlgebraicAggProjector(
        config=cfg.get_agg_kernel_config(),
    ).map(snapshot)
    assert out["mode"] == "algebraic_window"
    assert out["aggregate"]["total"] == 8


def test_apply_sem_filter_pushdown_rejects_invalid_runtime_numbers() -> None:
    with pytest.raises(ValueError, match="timeout_ms > 0"):
        pushdown.apply_sem_filter_pushdown(
            sentinel.input_stream,
            request=sem_filter(intent="Keep weather-related events"),
            runtime_config=_row_runtime_config(
                "sem_filter",
                response='{"decision": true, "confidence": 0.9, "reason": "ok"}',
            ),
            timeout_ms=0,
        )

    with pytest.raises(ValueError, match="async_capacity > 0"):
        pushdown.apply_sem_filter_pushdown(
            sentinel.input_stream,
            request=sem_filter(intent="Keep weather-related events"),
            runtime_config=_row_runtime_config(
                "sem_filter",
                response='{"decision": true, "confidence": 0.9, "reason": "ok"}',
            ),
            async_capacity=0,
        )


def test_json_decision_filter_crashes_on_invalid_json() -> None:
    filter_fn = row_pushdown.JsonDecisionFilter()

    with pytest.raises(ValueError, match="valid JSON"):
        filter_fn.filter("not-json")

    with pytest.raises(ValueError, match="decision field"):
        filter_fn.filter('{"confidence": 0.9}')


def test_sem_local_topk_projector_crashes_on_invalid_payload() -> None:
    projector = row_pushdown.SemLocalTopKProjector(k=2, candidates_field="candidates")

    with pytest.raises(ValueError, match="valid JSON"):
        projector.map("not-json")

    with pytest.raises(ValueError, match="scored_candidates"):
        projector.map('{"top_k":[]}')
