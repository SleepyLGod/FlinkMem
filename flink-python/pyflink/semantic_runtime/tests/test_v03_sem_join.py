"""Tests for the V0.3 true two-input sem_join runtime."""

from __future__ import annotations

import time

import pytest

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.operators.stateful.sem_join_kernel import (
    SemJoinConfig,
    SemJoinFunction,
)
from pyflink.semantic_runtime.sem_spec import JoinQuerySpec
from pyflink.semantic_runtime.sem_spec import TriggerPolicy


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
    def __init__(
        self,
        current_time_ms: int = 1000,
        current_watermark_ms: int = -1,
    ) -> None:
        self._current_time_ms = current_time_ms
        self._current_watermark_ms = current_watermark_ms
        self.registered_processing = []
        self.registered_event = []

    def current_processing_time(self) -> int:
        return self._current_time_ms

    def current_watermark(self) -> int:
        return self._current_watermark_ms

    def register_processing_time_timer(self, timestamp: int) -> None:
        self.registered_processing.append(timestamp)

    def register_event_time_timer(self, timestamp: int) -> None:
        self.registered_event.append(timestamp)


class _FakeContext:
    def __init__(
        self,
        current_time_ms: int = 1000,
        current_watermark_ms: int = -1,
    ) -> None:
        self._timer_service = _FakeTimerService(current_time_ms, current_watermark_ms)

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


def _drain_async(
    fn: SemJoinFunction,
    *,
    start_ms: int,
    steps: int = 50,
) -> list[dict]:
    outputs: list[dict] = []
    for idx in range(steps):
        timestamp = start_ms + idx
        time.sleep(0.005)
        outputs.extend(list(fn.on_timer(timestamp, _FakeOnTimerContext(timestamp))))
        if not fn._pending_futures:
            break
    return outputs


def _drain_async_event_time(
    fn: SemJoinFunction,
    *,
    start_ms: int,
    watermark_ms: int,
    steps: int = 50,
) -> list[dict]:
    outputs: list[dict] = []
    for idx in range(steps):
        timestamp = start_ms + idx
        time.sleep(0.005)
        outputs.extend(
            list(
                fn.on_timer(
                    timestamp,
                    _FakeOnTimerContext(
                        current_time_ms=timestamp,
                        current_watermark_ms=watermark_ms,
                    ),
                )
            )
        )
        if not fn._pending_futures:
            break
    return outputs


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
    assert out2 == []

    out3 = _drain_async(fn, start_ms=1200)
    assert len(out3) == 1
    assert out3[0]["matched"] is True
    assert out3[0]["match_score"] == 0.95
    assert out3[0]["left"]["fact"] == "Alice lives in Beijing"
    assert out3[0]["right"]["fact"] == "Alice lives in Shanghai"


def test_sem_join_invalid_json_fails_fast() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="llm")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config("not-json"),
        kernel_config=SemJoinConfig(ttl_seconds=60, pair_block_size=1),
    )
    fn.open(_FakeRuntimeContext())
    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    list(fn.process_element2({"key": "k", "text": "right"}, _FakeContext(1100)))
    with pytest.raises(ValueError, match="expected valid JSON"):
        _drain_async(fn, start_ms=1200)


def test_sem_join_embedding_backend_emits_match() -> None:
    query_spec = JoinQuerySpec.simple(instruction="Match same issue", backend="embedding")
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config(
            '{"matches": [{"pair_idx": 0, "matched": true, "match_score": 1.0, "reason": "unused"}]}'
        ),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())
    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    out = list(fn.process_element2({"key": "k", "text": "right"}, _FakeContext(1100)))
    assert len(out) == 1
    assert out[0]["matched"] is True


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
    _drain_async(fn, start_ms=1200)
    list(fn.on_timer(2500, _FakeOnTimerContext(2500)))
    assert fn._load_buffer(fn._left_buffer) == []
    assert fn._load_buffer(fn._right_buffer) == []


def test_sem_join_window_snapshot_ingests_delta_only() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match contradictory facts",
        backend="llm",
    )
    fn = SemJoinFunction(
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
                "close_time_ms": 1000,
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
                "close_time_ms": 1100,
            },
            _FakeContext(1100),
        )
    )
    assert out2 == []

    out3 = _drain_async(fn, start_ms=1200)
    assert len(out3) == 1
    assert out3[0]["matched"] is True
    assert out3[0]["left"] == "Alice lives in Beijing"
    assert out3[0]["right"] == "Alice lives in Shanghai"

    out4 = list(
        fn.process_element2(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "Alice lives in Shanghai", "seq_id": 2}],
                "trigger_reason": "close",
                "close_time_ms": 1300,
            },
            _FakeContext(1300),
        )
    )
    assert out4 == []
    out5 = _drain_async(fn, start_ms=1400)
    assert out5 == []


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


def test_sem_join_left_event_time_finalize_emits_unmatched() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="left",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 1.0
    query_spec.scope_policy.time_basis = "event"

    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    left_event = {"key": "k", "text": "left only", "event_time_ms": 1000}
    out1 = list(fn.process_element1(left_event, _FakeContext(1000, current_watermark_ms=1000)))
    assert out1 == []

    out2 = _drain_async_event_time(fn, start_ms=1100, watermark_ms=1500)
    assert out2 == []

    out3 = _drain_async_event_time(fn, start_ms=2100, watermark_ms=2500)
    assert len(out3) == 1
    assert out3[0]["matched"] is False
    assert out3[0]["left"]["text"] == "left only"
    assert out3[0]["right"] is None


def test_sem_join_right_event_time_finalize_emits_unmatched() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="right",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 1.0
    query_spec.scope_policy.time_basis = "event"

    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    right_event = {"key": "k", "text": "right only", "event_time_ms": 1000}
    out1 = list(fn.process_element2(right_event, _FakeContext(1000, current_watermark_ms=1000)))
    assert out1 == []

    out2 = _drain_async_event_time(fn, start_ms=2100, watermark_ms=2500)
    assert len(out2) == 1
    assert out2[0]["matched"] is False
    assert out2[0]["left"] is None
    assert out2[0]["right"]["text"] == "right only"


def test_sem_join_full_event_time_finalize_emits_both_sides() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="full",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 1.0
    query_spec.scope_policy.time_basis = "event"

    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    list(
        fn.process_element1(
            {"key": "k", "text": "left only", "event_time_ms": 1000},
            _FakeContext(1000, current_watermark_ms=1000),
        )
    )
    list(
        fn.process_element2(
            {"key": "k", "text": "right only", "event_time_ms": 1000},
            _FakeContext(1000, current_watermark_ms=1000),
        )
    )

    out = _drain_async_event_time(fn, start_ms=2100, watermark_ms=2500)
    assert len(out) == 2
    left_rows = [row for row in out if row["left"] is not None and row["right"] is None]
    right_rows = [row for row in out if row["left"] is None and row["right"] is not None]
    assert len(left_rows) == 1
    assert len(right_rows) == 1


def test_sem_join_semi_emits_single_left_row_across_multiple_matches() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="semi",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    out1 = list(fn.process_element2({"key": "k", "text": "right-1"}, _FakeContext(1100)))
    out2 = list(fn.process_element2({"key": "k", "text": "right-2"}, _FakeContext(1200)))

    merged = out1 + out2
    assert len(merged) == 1
    assert merged[0]["left"]["text"] == "left"
    assert merged[0]["right"] is None
    assert merged[0]["matched"] is True


def test_sem_join_anti_finalize_emits_only_unmatched_left() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="anti",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 1.0
    query_spec.scope_policy.time_basis = "event"
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    list(
        fn.process_element1(
            {"key": "k", "text": "left anti", "event_time_ms": 1000},
            _FakeContext(1000, current_watermark_ms=1000),
        )
    )
    out = _drain_async_event_time(fn, start_ms=2100, watermark_ms=2500)
    assert len(out) == 1
    assert out[0]["matched"] is False
    assert out[0]["left"]["text"] == "left anti"
    assert out[0]["right"] is None


def test_sem_join_mixed_scope_left_snapshot_right_row() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(
        fn.process_element1(
            {
                "key": "alice",
                "window_id": "w1",
                "events": [{"key": "alice", "payload": "left-snapshot", "seq_id": 1}],
                "trigger_reason": "close",
                "close_time_ms": 1000,
            },
            _FakeContext(1000),
        )
    )
    out = list(fn.process_element2({"key": "alice", "text": "right-row"}, _FakeContext(1100)))
    assert len(out) == 1
    assert out[0]["matched"] is True
    assert out[0]["left"] == "left-snapshot"
    assert out[0]["right"]["text"] == "right-row"


def test_sem_join_does_not_require_window_id_pairing() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(
        fn.process_element1(
            {
                "key": "k",
                "window_id": "left-window",
                "events": [{"key": "k", "payload": "left payload", "seq_id": 1}],
                "trigger_reason": "close",
                "close_time_ms": 1000,
            },
            _FakeContext(1000),
        )
    )
    out = list(
        fn.process_element2(
            {
                "key": "k",
                "window_id": "right-window-different-id",
                "events": [{"key": "k", "payload": "right payload", "seq_id": 1}],
                "trigger_reason": "close",
                "close_time_ms": 1100,
            },
            _FakeContext(1100),
        )
    )
    assert len(out) == 1
    assert out[0]["matched"] is True
    assert out[0]["left"] == "left payload"
    assert out[0]["right"] == "right payload"


def test_sem_join_event_time_mode_requires_event_timestamp() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 0.0
    query_spec.scope_policy.time_basis = "event"
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    with pytest.raises(
        ValueError,
        match="time_basis='event' requires event_time_ms/timestamp_ms/proc_time_ms",
    ):
        list(fn.process_element1({"key": "k", "text": "missing-ts"}, _FakeContext(1000)))


def test_sem_join_external_scope_dedupes_seq_id_across_windows() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(
        fn.process_element1(
            {
                "key": "k",
                "window_id": "w-left-1",
                "events": [{"key": "k", "payload": "left payload", "seq_id": 1}],
                "trigger_reason": "close",
                "close_time_ms": 1000,
            },
            _FakeContext(1000),
        )
    )
    list(
        fn.process_element1(
            {
                "key": "k",
                "window_id": "w-left-2",
                "events": [{"key": "k", "payload": "left payload", "seq_id": 1}],
                "trigger_reason": "close",
                "close_time_ms": 1100,
            },
            _FakeContext(1100),
        )
    )
    out = list(fn.process_element2({"key": "k", "text": "right row"}, _FakeContext(1200)))
    assert len(out) == 1
    assert out[0]["left"] == "left payload"
    assert out[0]["right"]["text"] == "right row"


def test_sem_join_row_event_dedupes_seq_id() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(fn.process_element1({"key": "k", "payload": "left-1", "seq_id": 7}, _FakeContext(1000)))
    list(fn.process_element1({"key": "k", "payload": "left-1-duplicate", "seq_id": 7}, _FakeContext(1100)))
    out = list(fn.process_element2({"key": "k", "payload": "right", "seq_id": 8}, _FakeContext(1200)))
    assert len(out) == 1
    assert out[0]["left"] == "left-1"


def test_sem_join_unsupported_trigger_fails_fast() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
    )
    query_spec.semantic.threshold = 0.0
    query_spec.trigger_policy = TriggerPolicy(mode="periodic", interval_ms=1000)
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    with pytest.raises(NotImplementedError, match="trigger_policy.mode"):
        fn.open(_FakeRuntimeContext())


def test_sem_join_on_scope_close_defers_inner_match_until_finalize() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
        ttl_seconds=1,
    )
    query_spec.semantic.threshold = 0.0
    query_spec.trigger_policy = TriggerPolicy(mode="on_scope_close")
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=1),
    )
    fn.open(_FakeRuntimeContext())

    out1 = list(
        fn.process_element1(
            {"key": "k", "text": "left", "event_time_ms": 1000},
            _FakeContext(1000, current_watermark_ms=1000),
        )
    )
    out2 = list(
        fn.process_element2(
            {"key": "k", "text": "right", "event_time_ms": 1000},
            _FakeContext(1000, current_watermark_ms=1000),
        )
    )
    assert out1 == []
    assert out2 == []

    out3 = _drain_async_event_time(fn, start_ms=1100, watermark_ms=1500)
    assert out3 == []

    out4 = _drain_async_event_time(fn, start_ms=2100, watermark_ms=2500)
    assert len(out4) == 1
    assert out4[0]["matched"] is True
    assert out4[0]["left"]["text"] == "left"
    assert out4[0]["right"]["text"] == "right"


def test_sem_join_blocking_pairing_restricts_candidate_pairs() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
        pairing_method="blocking",
    )
    query_spec.semantic.threshold = 0.0
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(fn.process_element1({"key": "k", "text": "alpha one"}, _FakeContext(1000)))
    out = list(fn.process_element2({"key": "k", "text": "beta two"}, _FakeContext(1100)))
    assert out == []

    out2 = list(fn.process_element2({"key": "k", "text": "alpha three"}, _FakeContext(1200)))
    assert len(out2) == 1
    assert out2[0]["matched"] is True
    assert out2[0]["left"]["text"] == "alpha one"
    assert out2[0]["right"]["text"] == "alpha three"


def test_sem_join_embedding_prefilter_pairing_requires_threshold() -> None:
    query_spec = JoinQuerySpec.simple(
        instruction="Match same issue",
        backend="embedding",
        join_type="inner",
        pairing_method="embedding_prefilter",
    )
    fn = SemJoinFunction(
        query_spec=query_spec,
        llm_config=_llm_config('{"matches": []}'),
        kernel_config=SemJoinConfig(ttl_seconds=60),
    )
    fn.open(_FakeRuntimeContext())

    list(fn.process_element1({"key": "k", "text": "left"}, _FakeContext(1000)))
    with pytest.raises(ValueError, match="embedding_prefilter pairing requires semantic.threshold"):
        list(fn.process_element2({"key": "k", "text": "right"}, _FakeContext(1100)))
