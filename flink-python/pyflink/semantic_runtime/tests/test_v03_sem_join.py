"""Tests for the V0.3 true two-input sem_join runtime."""

from __future__ import annotations

import pytest

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.stateful.sem_join import (
    SemJoinConfig,
    SemJoinFunction,
    WindowOwnedSemJoinFunction,
)
from pyflink.semantic_runtime.sem_spec import JoinQuerySpec


class _FakeListState:
    def __init__(self) -> None:
        self._items = []

    def get(self):
        return list(self._items)

    def add(self, item):
        self._items.append(item)

    def clear(self) -> None:
        self._items.clear()


class _FakeRuntimeContext:
    def get_list_state(self, _descriptor):
        return _FakeListState()

    def get_map_state(self, _descriptor):
        return _FakeMapState()


class _FakeMapState:
    def __init__(self) -> None:
        self._items = {}

    def put(self, key, value) -> None:
        self._items[key] = value

    def get(self, key):
        return self._items.get(key)

    def contains(self, key) -> bool:
        return key in self._items

    def remove(self, key) -> None:
        self._items.pop(key, None)

    def items(self):
        return list(self._items.items())


class _FakeTimerService:
    def __init__(self, current_time_ms: int = 1000) -> None:
        self._current_time_ms = current_time_ms
        self.registered = []

    def current_processing_time(self) -> int:
        return self._current_time_ms

    def register_processing_time_timer(self, timestamp: int) -> None:
        self.registered.append(timestamp)


class _FakeContext:
    def __init__(self, current_time_ms: int = 1000) -> None:
        self._timer_service = _FakeTimerService(current_time_ms)

    def timer_service(self):
        return self._timer_service


class _FakeOnTimerContext(_FakeContext):
    pass


def _llm_config(mock_response: str) -> LLMClientConfig:
    return LLMClientConfig(
        backend="mock",
        mock_delay_s=0.0,
        mock_response=mock_response,
    )


def test_sem_join_emits_match_when_second_side_arrives() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match contradictory facts",
        backend="llm",
    )
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config(
            '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 0.95, "reason": "contradiction"}]}'
        ),
        kernel_config=SemJoinConfig(ttl_seconds=60, pair_block_size=2),
    )
    fn.open(_FakeRuntimeContext())

    out1 = list(
        fn.process_element1(
            {"key": "alice", "fact": "Alice lives in Beijing"},
            _FakeContext(1000),
        )
    )
    assert out1 == []

    out2 = list(
        fn.process_element2(
            {"key": "alice", "fact": "Alice lives in Shanghai"},
            _FakeContext(1100),
        )
    )
    assert len(out2) == 1
    assert out2[0]["matched"] is True
    assert out2[0]["match_score"] == 0.95
    assert out2[0]["left"]["fact"] == "Alice lives in Beijing"
    assert out2[0]["right"]["fact"] == "Alice lives in Shanghai"


def test_sem_join_invalid_json_fails_fast() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="llm")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config("not-json"),
        kernel_config=SemJoinConfig(ttl_seconds=60, pair_block_size=1),
    )
    fn.open(_FakeRuntimeContext())
    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    with pytest.raises(ValueError, match="expected valid JSON"):
        list(fn.process_element2({"key": "k", "text": "right"}, _FakeContext(1100)))


def test_sem_join_unsupported_backend_fails_fast() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="embedding")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config(
            '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 1.0, "reason": "unused"}]}'
        ),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    with pytest.raises(NotImplementedError, match="supports only llm/hybrid"):
        fn.open(_FakeRuntimeContext())


def test_sem_join_timer_prunes_expired_records() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="llm")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())
    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    list(fn.process_element2({"key": "k", "text": "right"}, _FakeContext(1100)))
    list(fn.on_timer(2500, _FakeOnTimerContext(2500)))
    assert fn._load_buffer(fn._left_buffer) == []
    assert fn._load_buffer(fn._right_buffer) == []


def test_window_owned_sem_join_emits_match_for_aligned_window_id() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match contradictory facts",
        backend="llm",
    )
    fn = WindowOwnedSemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config(
            '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 0.91, "reason": "contradiction"}]}'
        ),
        kernel_config=SemJoinConfig(ttl_seconds=60, pair_block_size=2),
    )
    fn.open(_FakeRuntimeContext())

    out1 = list(
        fn.process_element1(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "Alice lives in Beijing", "seq_id": 1}],
                "trigger_reason": "close",
            },
            _FakeContext(1000),
        )
    )
    assert out1 == []

    out2 = list(
        fn.process_element2(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "Alice lives in Shanghai", "seq_id": 2}],
                "trigger_reason": "close",
            },
            _FakeContext(1100),
        )
    )
    assert len(out2) == 1
    assert out2[0]["window_id"] == "w1"
    assert out2[0]["matched"] is True


def test_sem_join_prefilter_can_skip_llm_when_no_pairs_survive() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="llm")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config(
            '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 1.0, "reason": "unused"}]}'
        ),
        kernel_config=SemJoinConfig(
            ttl_seconds=60,
            pair_block_size=1,
            prefilter_strategy="hashing_similarity",
            prefilter_min_score=0.9,
        ),
    )
    fn.open(_FakeRuntimeContext())
    list(fn.process_element1({"key": "k", "text": "budget update"}, _FakeContext(1000)))
    out = list(fn.process_element2({"key": "k", "text": "airport taxi"}, _FakeContext(1100)))
    assert out == []
