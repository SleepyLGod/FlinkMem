"""
Unit tests for V0.2 stateful foundation: event model, state descriptors,
semantic window boundary logic, and timer policy.

These tests exercise pure Python logic — no Flink runtime required.
"""

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os, pathlib, pyflink as _pf  # noqa: E401,E402
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

import time
import pytest

from pyflink.semantic_runtime.stateful.event_model import (
    SemanticEvent,
    WindowSnapshot,
    simple_key_selector,
    composite_key_selector,
)
from pyflink.semantic_runtime.stateful.state_descriptors import (
    StateSafetyConfig,
    OverflowPolicy,
    build_ttl_config,
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
    sem_groupby_profiles_descriptor,
    cts_retrieve_cache_descriptor,
)
from pyflink.semantic_runtime.stateful.timer_policy import (
    TimerCategory,
    TimerPolicy,
    encode_timer_key,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
    schedule_policy_timers,
)
from pyflink.semantic_runtime.stateful.semantic_window import (
    SemWindowConfig,
    SemWindowFunction,
    _new_window_meta,
)


# ============================================================================
# Event Model Tests
# ============================================================================

class TestSemanticEvent:
    def test_roundtrip(self):
        e = SemanticEvent(key="u1", payload="hello", seq_id=1, event_time_ms=1000)
        d = e.to_dict()
        e2 = SemanticEvent.from_dict(d)
        assert e2.key == "u1"
        assert e2.payload == "hello"
        assert e2.seq_id == 1
        assert e2.event_time_ms == 1000

    def test_effective_time_event(self):
        e = SemanticEvent(key="k", payload="x", seq_id=0, event_time_ms=5000)
        assert e.effective_time_ms == 5000

    def test_effective_time_proc(self):
        e = SemanticEvent(key="k", payload="x", seq_id=0)
        assert e.effective_time_ms == e.proc_time_ms

    def test_has_boundary(self):
        e = SemanticEvent(key="k", payload="x", seq_id=0,
                          boundary_flags={"topic_shift": True})
        assert e.has_boundary("topic_shift") is True
        assert e.has_boundary("intent_shift") is False

    def test_from_dict_ignores_extra_keys(self):
        d = {"key": "k", "payload": "p", "seq_id": 0, "extra_field": 42}
        e = SemanticEvent.from_dict(d)
        assert e.key == "k"


class TestWindowSnapshot:
    def test_roundtrip(self):
        ws = WindowSnapshot(key="k", events=[{"a": 1}], event_count=1,
                            open_time_ms=100, close_time_ms=200, trigger_reason="count")
        d = ws.to_dict()
        ws2 = WindowSnapshot.from_dict(d)
        assert ws2.key == "k"
        assert ws2.event_count == 1
        assert ws2.trigger_reason == "count"


class TestKeySelectors:
    def test_simple(self):
        assert simple_key_selector({"key": "abc"}) == "abc"
        assert simple_key_selector({}) == ""

    def test_composite(self):
        sel = composite_key_selector("user", "session")
        assert sel({"user": "u1", "session": "s1"}) == "u1|s1"
        assert sel({"user": "u1"}) == "u1|"


# ============================================================================
# State Descriptors Tests
# ============================================================================

class TestStateSafetyConfig:
    def test_defaults(self):
        cfg = StateSafetyConfig()
        assert cfg.ttl_seconds == 3600
        assert cfg.max_window_events == 500
        assert cfg.overflow_policy == OverflowPolicy.DROP_OLDEST

    def test_custom(self):
        cfg = StateSafetyConfig(ttl_seconds=60, overflow_policy=OverflowPolicy.DEGRADE_TAG)
        assert cfg.ttl_seconds == 60
        assert cfg.overflow_policy == OverflowPolicy.DEGRADE_TAG


class TestDescriptors:
    def test_event_buffer_descriptor(self):
        desc = sem_window_event_buffer_descriptor(600)
        assert desc.name == "sem_window_event_buffer"

    def test_meta_descriptor(self):
        desc = sem_window_meta_descriptor()
        assert desc.name == "sem_window_meta"

    def test_groupby_descriptor(self):
        desc = sem_groupby_profiles_descriptor()
        assert desc.name == "sem_groupby_profiles"

    def test_retrieve_cache_descriptor(self):
        desc = cts_retrieve_cache_descriptor(1800)
        assert desc.name == "cts_retrieve_cache"


# ============================================================================
# Timer Policy Tests
# ============================================================================

class TestTimerPolicy:
    def test_encode_timer_key(self):
        assert encode_timer_key(TimerCategory.FLUSH) == "_timer_flush"
        assert encode_timer_key(TimerCategory.EVICT) == "_timer_evict"

    def test_resolve_category_exact(self):
        meta = {"_timer_flush": 10000}
        assert resolve_timer_category(meta, 10000) == TimerCategory.FLUSH

    def test_resolve_category_within_tolerance(self):
        meta = {"_timer_flush": 10000}
        assert resolve_timer_category(meta, 10100) == TimerCategory.FLUSH

    def test_resolve_category_stale(self):
        meta = {"_timer_flush": 10000}
        assert resolve_timer_category(meta, 20000) is None

    def test_resolve_category_empty(self):
        assert resolve_timer_category({}, 10000) is None

    def test_clear_registration(self):
        meta = {"_timer_flush": 10000, "_timer_evict": 20000}
        clear_timer_registration(meta, TimerCategory.FLUSH)
        assert "_timer_flush" not in meta
        assert "_timer_evict" in meta

    def test_schedule_policy_timers(self):
        """Test that schedule_policy_timers registers all enabled categories."""
        class FakeTimerService:
            def __init__(self):
                self.proc_timers = []
                self.event_timers = []
            def register_processing_time_timer(self, ts):
                self.proc_timers.append(ts)
            def register_event_time_timer(self, ts):
                self.event_timers.append(ts)

        ts = FakeTimerService()
        meta = {}
        policy = TimerPolicy(flush_interval_ms=1000, recompute_interval_ms=500,
                             evict_interval_ms=2000, use_event_time=False)
        schedule_policy_timers(ts, meta, policy, base_time_ms=10000)

        assert len(ts.proc_timers) == 3
        assert 11000 in ts.proc_timers  # flush
        assert 10500 in ts.proc_timers  # recompute
        assert 12000 in ts.proc_timers  # evict
        assert meta["_timer_flush"] == 11000
        assert meta["_timer_recompute"] == 10500
        assert meta["_timer_evict"] == 12000

    def test_schedule_policy_timers_event_time(self):
        class FakeTimerService:
            def __init__(self):
                self.proc_timers = []
                self.event_timers = []
            def register_processing_time_timer(self, ts):
                self.proc_timers.append(ts)
            def register_event_time_timer(self, ts):
                self.event_timers.append(ts)

        ts = FakeTimerService()
        meta = {}
        policy = TimerPolicy(flush_interval_ms=1000, recompute_interval_ms=0,
                             evict_interval_ms=0, use_event_time=True)
        schedule_policy_timers(ts, meta, policy, base_time_ms=5000)

        assert len(ts.event_timers) == 1
        assert ts.event_timers[0] == 6000
        assert len(ts.proc_timers) == 0

    def test_resolve_multiple_categories(self):
        meta = {"_timer_flush": 10000, "_timer_evict": 20000}
        assert resolve_timer_category(meta, 10000) == TimerCategory.FLUSH
        assert resolve_timer_category(meta, 20000) == TimerCategory.EVICT


# ============================================================================
# SemWindow Logic Tests (no Flink runtime)
# ============================================================================

class TestSemWindowConfig:
    def test_defaults(self):
        cfg = SemWindowConfig()
        assert cfg.max_window_events == 50
        assert cfg.window_timeout_ms == 30_000
        assert cfg.boundary_flag == "topic_shift"

class TestNewWindowMeta:
    def test_structure(self):
        meta = _new_window_meta(1000)
        assert "window_id" in meta
        assert meta["open_time_ms"] == 1000
        assert meta["event_count"] == 0
        assert len(meta["window_id"]) == 12

class TestSemWindowBoundaryLogic:
    """Test _check_triggers without Flink runtime."""

    def _make_func(self, max_events=5):
        cfg = SemWindowConfig(max_window_events=max_events)
        return SemWindowFunction(cfg)

    def test_count_trigger(self):
        func = self._make_func(max_events=3)
        event = SemanticEvent(key="k", payload="x", seq_id=0)
        meta = {"event_count": 3}
        assert func._check_triggers(event, meta) == "count"

    def test_count_not_yet(self):
        func = self._make_func(max_events=3)
        event = SemanticEvent(key="k", payload="x", seq_id=0)
        meta = {"event_count": 2}
        assert func._check_triggers(event, meta) is None

    def test_semantic_boundary(self):
        func = self._make_func()
        event = SemanticEvent(key="k", payload="x", seq_id=0,
                              boundary_flags={"topic_shift": True})
        meta = {"event_count": 1}
        assert func._check_triggers(event, meta) == "semantic_boundary"

    def test_semantic_boundary_priority(self):
        """Semantic boundary takes priority over count."""
        func = self._make_func(max_events=1)
        event = SemanticEvent(key="k", payload="x", seq_id=0,
                              boundary_flags={"topic_shift": True})
        meta = {"event_count": 1}
        assert func._check_triggers(event, meta) == "semantic_boundary"


# ============================================================================
# Async Bridge Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.async_bridge import (
    AsyncWorkItem,
    AsyncResult,
    ASYNC_WORK_TAG,
)

class TestAsyncWorkItem:
    def test_roundtrip(self):
        item = AsyncWorkItem(key="u1", task_type="classify",
                             payload={"text": "hello"})
        d = item.to_dict()
        item2 = AsyncWorkItem.from_dict(d)
        assert item2.key == "u1"
        assert item2.task_type == "classify"
        assert item2.payload == {"text": "hello"}
        assert len(item2.request_id) == 12

    def test_from_dict_ignores_extra(self):
        d = {"key": "k", "task_type": "t", "payload": {}, "request_id": "abc",
             "extra": 42}
        item = AsyncWorkItem.from_dict(d)
        assert item.key == "k"

class TestAsyncResult:
    def test_roundtrip(self):
        r = AsyncResult(key="u1", task_type="classify",
                        result={"group_id": "g1"}, request_id="abc")
        d = r.to_dict()
        r2 = AsyncResult.from_dict(d)
        assert r2.key == "u1"
        assert r2.success is True
        assert r2.result == {"group_id": "g1"}

    def test_error_result(self):
        r = AsyncResult(key="u1", task_type="classify",
                        success=False, error="timeout")
        assert r.success is False
        assert r.error == "timeout"

class TestAsyncWorkTag:
    def test_tag_exists(self):
        assert ASYNC_WORK_TAG is not None
        assert ASYNC_WORK_TAG.tag_id == "async_work_items"


# ============================================================================
# SemGroupby Logic Tests (no Flink runtime — test pure internals)
# ============================================================================

from pyflink.semantic_runtime.stateful.sem_groupby_stateful import (
    SemGroupbyConfig,
    SemGroupbyFunction,
    _new_group_profile,
)

class TestGroupProfile:
    def test_new_profile(self):
        p = _new_group_profile("g1", "machine learning", 1000)
        assert p["group_id"] == "g1"
        assert p["label"] == "machine learning"
        assert p["event_count"] == 0
        assert p["created_ms"] == 1000
        assert p["last_update_ms"] == 1000

class TestSemGroupbyConfig:
    def test_defaults(self):
        cfg = SemGroupbyConfig()
        assert cfg.max_groups_per_key == 50
        assert cfg.confidence_threshold == 0.7
        assert cfg.new_group_creation_threshold == 0.3
        assert cfg.overflow_policy == OverflowPolicy.DROP_OLDEST

class TestSemGroupbyLocalAssign:
    """Test _local_assign without Flink state — using a mock MapState."""

    def _make_func_with_groups(self, groups):
        """Create a SemGroupbyFunction with injected mock MapState."""
        func = SemGroupbyFunction(SemGroupbyConfig())
        func._group_profiles = _FakeMapState(groups)
        return func

    def test_exact_match(self):
        func = self._make_func_with_groups({
            "g1": {"label": "machine learning algorithms", "event_count": 5},
            "g2": {"label": "web development frontend", "event_count": 3},
        })
        event = SemanticEvent(key="k", payload="machine learning algorithms rock", seq_id=0)
        gid, score = func._local_assign(event)
        assert gid == "g1"
        assert score > 0.5

    def test_no_match(self):
        func = self._make_func_with_groups({
            "g1": {"label": "machine learning", "event_count": 5},
        })
        event = SemanticEvent(key="k", payload="completely unrelated topic", seq_id=0)
        gid, score = func._local_assign(event)
        assert score == 0.0

    def test_empty_groups(self):
        func = self._make_func_with_groups({})
        event = SemanticEvent(key="k", payload="anything", seq_id=0)
        gid, score = func._local_assign(event)
        assert gid is None
        assert score == 0.0

    def test_best_of_multiple(self):
        func = self._make_func_with_groups({
            "g1": {"label": "python programming", "event_count": 5},
            "g2": {"label": "python web flask django", "event_count": 3},
        })
        event = SemanticEvent(key="k", payload="python web flask", seq_id=0)
        gid, score = func._local_assign(event)
        assert gid == "g2"


# ============================================================================
# CtsRetrieve Logic Tests (no Flink runtime)
# ============================================================================

from pyflink.semantic_runtime.stateful.cts_retrieve import (
    CtsRetrieveConfig,
    CtsRetrieveFunction,
)

class TestCtsRetrieveConfig:
    def test_defaults(self):
        cfg = CtsRetrieveConfig()
        assert cfg.max_candidates_per_request == 20
        assert cfg.max_cache_entries_per_key == 200
        assert cfg.ttl_seconds == 1800
        assert cfg.overflow_policy == OverflowPolicy.DROP_OLDEST

class TestCtsRetrieveLocalRetrieve:
    """Test _local_retrieve without Flink state."""

    def _make_func_with_cache(self, cache_entries):
        func = CtsRetrieveFunction(CtsRetrieveConfig())
        func._cache = _FakeMapState(cache_entries)
        return func

    def test_keyword_match(self):
        func = self._make_func_with_cache({
            "c1": {"content": "machine learning deep neural network", "source": "doc1"},
            "c2": {"content": "web development react frontend", "source": "doc2"},
            "c3": {"content": "machine learning pytorch training", "source": "doc3"},
        })
        event = SemanticEvent(key="k", payload="machine learning", seq_id=0)
        results = func._local_retrieve(event)
        assert len(results) >= 2
        # Top results should be ML-related
        assert results[0]["candidate_id"] in ("c1", "c3")

    def test_no_match(self):
        func = self._make_func_with_cache({
            "c1": {"content": "completely irrelevant xyz", "source": "doc1"},
        })
        event = SemanticEvent(key="k", payload="quantum computing", seq_id=0)
        results = func._local_retrieve(event)
        assert len(results) == 0

    def test_empty_cache(self):
        func = self._make_func_with_cache({})
        event = SemanticEvent(key="k", payload="anything", seq_id=0)
        results = func._local_retrieve(event)
        assert results == []

    def test_score_ordering(self):
        func = self._make_func_with_cache({
            "c1": {"content": "python", "source": "a"},
            "c2": {"content": "python programming language", "source": "b"},
        })
        event = SemanticEvent(key="k", payload="python", seq_id=0)
        results = func._local_retrieve(event)
        assert len(results) >= 1
        # c1 has perfect keyword match (1/1), c2 partial (1/3)
        assert results[0]["candidate_id"] == "c1"

class TestCtsRetrieveCacheEnforcement:
    def test_enforce_cache_limit(self):
        entries = {f"c{i}": {"content": f"text {i}", "_cached_at_ms": i * 100}
                   for i in range(10)}
        func = CtsRetrieveFunction(CtsRetrieveConfig(max_cache_entries_per_key=5))
        func._cache = _FakeMapState(entries)
        evicted = func._enforce_cache_limit()
        assert evicted == 5
        # Should keep the 5 newest (c5-c9)
        remaining_keys = list(func._cache.keys())
        assert len(remaining_keys) == 5

    def test_under_limit_no_eviction(self):
        entries = {"c1": {"content": "a"}, "c2": {"content": "b"}}
        func = CtsRetrieveFunction(CtsRetrieveConfig(max_cache_entries_per_key=10))
        func._cache = _FakeMapState(entries)
        evicted = func._enforce_cache_limit()
        assert evicted == 0


# ============================================================================
# Helpers — Fake MapState for testing without Flink runtime
# ============================================================================

class _FakeMapState:
    """Minimal MapState mock for unit tests."""
    def __init__(self, data: dict = None):
        self._data = dict(data) if data else {}

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key] = value

    def contains(self, key):
        return key in self._data

    def remove(self, key):
        self._data.pop(key, None)

    def keys(self):
        return list(self._data.keys())

    def values(self):
        return list(self._data.values())

    def items(self):
        return list(self._data.items())

    def is_empty(self):
        return len(self._data) == 0


# ============================================================================
# SemAgg Logic Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.sem_agg_stateful import (
    SemAggConfig,
    SemAggFunction,
)

class TestSemAggConfig:
    def test_defaults(self):
        cfg = SemAggConfig()
        assert cfg.mode == "algebraic"
        assert cfg.max_buffer_events == 100
        assert cfg.overflow_policy == OverflowPolicy.DROP_OLDEST

    def test_summarize_mode(self):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=20)
        assert cfg.mode == "summarize"
        assert cfg.max_buffer_events == 20


class TestSemAggAlgebraic:
    """Test algebraic aggregation path (no Flink runtime)."""

    def _make_func(self, reduce_fn=None):
        cfg = SemAggConfig(mode="algebraic", reduce_fn=reduce_fn)
        func = SemAggFunction(cfg)
        func._buffer = _FakeListState()
        func._agg_value = _FakeValueState()
        func._meta = _FakeValueState()
        return func

    def test_first_event_stores_directly(self):
        func = self._make_func()
        func._meta.update({"key": "k", "event_count": 1, "version": 0})
        result = list(func._algebraic_step(
            {"key": "k", "count": 1}, func._meta.value(), 1000
        ))
        assert len(result) == 1
        assert result[0]["mode"] == "algebraic"
        assert result[0]["version"] == 1

    def test_reduce_fn_applied(self):
        def sum_reduce(a, b):
            return {"key": a.get("key", "k"),
                    "total": a.get("total", 0) + b.get("total", 0)}

        func = self._make_func(reduce_fn=sum_reduce)
        func._agg_value.update({"key": "k", "total": 10})
        func._meta.update({"key": "k", "event_count": 2, "version": 1})
        result = list(func._algebraic_step(
            {"key": "k", "total": 5}, func._meta.value(), 2000
        ))
        assert result[0]["aggregate"]["total"] == 15
        assert result[0]["version"] == 2


class TestSemAggSummarize:
    """Test summarize path buffer logic (no Flink runtime)."""

    def _make_func(self, max_buf=3):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=max_buf)
        func = SemAggFunction(cfg)
        func._buffer = _FakeListState()
        func._agg_value = _FakeValueState()
        func._meta = _FakeValueState()
        return func

    def test_buffer_accumulates(self):
        func = self._make_func(max_buf=5)
        meta = {"key": "k", "event_count": 1, "version": 0,
                "pending_summarize": False, "_last_summarize_count": 0}
        func._meta.update(meta)
        results = list(func._summarize_step(
            {"key": "k", "payload": "hello"}, meta, 1000
        ))
        # Should not emit yet (1 < 5)
        assert len(results) == 0
        assert len(list(func._buffer.get())) == 1

    def test_buffer_triggers_summarize(self):
        func = self._make_func(max_buf=2)
        # Pre-fill buffer
        func._buffer.add({"key": "k", "payload": "a"})
        meta = {"key": "k", "event_count": 2, "version": 0,
                "pending_summarize": False, "_last_summarize_count": 0}
        func._meta.update(meta)
        results = list(func._summarize_step(
            {"key": "k", "payload": "b"}, meta, 2000
        ))
        # Should emit an async work item via side output
        assert len(results) == 1
        assert results[0][0] == ASYNC_WORK_TAG  # (tag, dict) tuple

    def test_handle_summarize_result(self):
        func = self._make_func()
        func._meta.update({"key": "k", "event_count": 5, "version": 1,
                           "pending_summarize": True})
        result_dict = {
            "task_type": "summarize", "key": "k", "success": True,
            "result": {"summary": "A summary of events"},
        }
        results = list(func._handle_summarize_result(result_dict, 3000))
        assert len(results) == 1
        assert results[0]["mode"] == "summarize"
        assert results[0]["aggregate"]["summary"] == "A summary of events"
        assert results[0]["version"] == 2


# ============================================================================
# SemTopK Logic Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
    SemTopKFunction,
)

class TestSemTopKConfig:
    def test_defaults(self):
        cfg = SemTopKConfig()
        assert cfg.k == 10
        assert cfg.max_candidates == 100
        assert cfg.emission_policy == "delta"


class TestSemTopKRecompute:
    """Test top-k recomputation logic (no Flink runtime)."""

    def _make_func(self, k=3, max_cand=10, policy="delta"):
        cfg = SemTopKConfig(k=k, max_candidates=max_cand, emission_policy=policy)
        func = SemTopKFunction(cfg)
        func._candidates = _FakeMapState()
        func._snapshot = _FakeValueState()
        func._meta = _FakeValueState()
        return func

    def test_basic_topk(self):
        func = self._make_func(k=2)
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9},
            "c2": {"candidate_id": "c2", "score": 0.5},
            "c3": {"candidate_id": "c3", "score": 0.7},
        })
        meta = {"key": "k", "update_count": 3}
        func._meta.update(meta)

        results = list(func._recompute_and_emit(meta, 1000))
        assert len(results) == 1
        assert results[0]["top_ids"] == ["c1", "c3"]
        assert results[0]["total_candidates"] == 3

    def test_delta_no_change(self):
        func = self._make_func(k=2, policy="delta")
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9},
            "c2": {"candidate_id": "c2", "score": 0.5},
        })
        # Pre-set snapshot with same order
        func._snapshot.update({"top_ids": ["c1", "c2"]})
        meta = {"key": "k", "update_count": 2}
        func._meta.update(meta)

        results = list(func._recompute_and_emit(meta, 2000))
        # Delta policy: no change → no emission
        assert len(results) == 0

    def test_snapshot_always_emits(self):
        func = self._make_func(k=2, policy="snapshot")
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9},
            "c2": {"candidate_id": "c2", "score": 0.5},
        })
        func._snapshot.update({"top_ids": ["c1", "c2"]})
        meta = {"key": "k", "update_count": 2}
        func._meta.update(meta)

        results = list(func._recompute_and_emit(meta, 2000))
        assert len(results) == 1
        assert results[0]["changed"] is False

    def test_enforce_candidate_limit(self):
        func = self._make_func(k=2, max_cand=3)
        entries = {f"c{i}": {"candidate_id": f"c{i}", "score": i * 0.1,
                              "_updated_ms": i * 100}
                   for i in range(5)}
        func._candidates = _FakeMapState(entries)
        evicted = func._enforce_candidate_limit()
        assert evicted == 2
        assert len(func._candidates.keys()) == 3


# ============================================================================
# State Safety Audit Tests
# ============================================================================

class TestStateSafetyAudit:
    """Verify state safety guardrails across operators."""

    def test_new_descriptors_exist(self):
        """Verify sem_agg and sem_topk descriptors are declared."""
        from pyflink.semantic_runtime.stateful.state_descriptors import (
            sem_agg_buffer_descriptor,
            sem_agg_value_descriptor,
            sem_agg_meta_descriptor,
            sem_topk_candidates_descriptor,
            sem_topk_snapshot_descriptor,
        )
        assert sem_agg_buffer_descriptor().name == "sem_agg_buffer"
        assert sem_agg_value_descriptor().name == "sem_agg_value"
        assert sem_agg_meta_descriptor().name == "sem_agg_meta"
        assert sem_topk_candidates_descriptor().name == "sem_topk_candidates"
        assert sem_topk_snapshot_descriptor().name == "sem_topk_snapshot"

    def test_groupby_overflow_drop_oldest_evicts(self):
        """sem_groupby: DROP_OLDEST should evict when at max groups."""
        cfg = SemGroupbyConfig(max_groups_per_key=2, overflow_policy=OverflowPolicy.DROP_OLDEST)
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": {"label": "old group", "event_count": 1, "last_update_ms": 100,
                    "created_ms": 100, "summary": "", "group_id": "g1"},
            "g2": {"label": "newer group", "event_count": 2, "last_update_ms": 200,
                    "created_ms": 200, "summary": "", "group_id": "g2"},
        })
        event = SemanticEvent(key="k", payload="brand new topic", seq_id=0)
        gid = func._maybe_create_group(event, 300)
        assert gid is not None
        # g1 (oldest) should have been evicted
        assert not func._group_profiles.contains("g1")
        assert func._group_profiles.contains("g2")

    def test_groupby_overflow_drop_newest_refuses(self):
        """sem_groupby: DROP_NEWEST should refuse to create group at limit."""
        cfg = SemGroupbyConfig(max_groups_per_key=2, overflow_policy=OverflowPolicy.DROP_NEWEST)
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": {"label": "a", "event_count": 1, "last_update_ms": 100},
            "g2": {"label": "b", "event_count": 1, "last_update_ms": 200},
        })
        event = SemanticEvent(key="k", payload="c", seq_id=0)
        gid = func._maybe_create_group(event, 300)
        assert gid is None

    def test_retrieve_overflow_degrade_tag(self):
        """cts_retrieve: DEGRADE_TAG should keep all entries."""
        cfg = CtsRetrieveConfig(max_cache_entries_per_key=2,
                                overflow_policy=OverflowPolicy.DEGRADE_TAG)
        func = CtsRetrieveFunction(cfg)
        func._cache = _FakeMapState({
            "c1": {"content": "a", "_cached_at_ms": 100},
            "c2": {"content": "b", "_cached_at_ms": 200},
            "c3": {"content": "c", "_cached_at_ms": 300},
        })
        evicted = func._enforce_cache_limit()
        assert evicted == 0  # DEGRADE_TAG keeps all
        assert len(func._cache.keys()) == 3


# ============================================================================
# Helpers — Fake ListState and ValueState for testing
# ============================================================================

class _FakeListState:
    """Minimal ListState mock for unit tests."""
    def __init__(self, data: list = None):
        self._data = list(data) if data else []

    def add(self, value):
        self._data.append(value)

    def get(self):
        return iter(self._data)

    def clear(self):
        self._data.clear()


class _FakeValueState:
    """Minimal ValueState mock for unit tests."""
    def __init__(self, initial=None):
        self._value = initial

    def value(self):
        return self._value

    def update(self, val):
        self._value = val

    def clear(self):
        self._value = None


# ============================================================================
# Continuous RAG Workflow Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.continuous_rag_workflow import (
    ContinuousRAGConfig,
    _StreamRouter,
    _AnswerSynthesiser,
    validate_rag_config,
    MEMORY_EVENT_TAG,
    QUERY_REQUEST_TAG,
)


class _FakeContext:
    """Minimal mock for KeyedProcessFunction.Context."""

    def __init__(self, key="test_key"):
        self._key = key

    def get_current_key(self):
        return self._key

    def timer_service(self):
        return None

    def output(self, tag, value):
        pass


class TestContinuousRAGConfig:
    def test_defaults(self):
        cfg = ContinuousRAGConfig()
        assert cfg.workflow_version == "v0.2.0"
        assert cfg.topk_config is None
        assert cfg.async_timeout_ms == 30_000
        assert isinstance(cfg.window_config, SemWindowConfig)
        assert isinstance(cfg.groupby_config, SemGroupbyConfig)
        assert isinstance(cfg.agg_config, SemAggConfig)
        assert isinstance(cfg.retrieve_config, CtsRetrieveConfig)

    def test_custom_configs(self):
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(max_window_events=10),
            topk_config=SemTopKConfig(k=5),
            workflow_version="v0.2.1",
        )
        assert cfg.window_config.max_window_events == 10
        assert cfg.topk_config.k == 5
        assert cfg.workflow_version == "v0.2.1"


class TestStreamRouter:
    """Test the stream router that splits events by stream_type."""

    def _route(self, event_dict):
        router = _StreamRouter()
        results = list(router.process_element(event_dict, _FakeContext()))
        return results

    def test_memory_event_routed(self):
        results = self._route({"key": "k1", "stream_type": "memory_event", "payload": "hello"})
        assert len(results) == 1
        tag, value = results[0]
        assert tag == MEMORY_EVENT_TAG
        assert value["payload"] == "hello"

    def test_query_request_routed(self):
        results = self._route({"key": "k1", "stream_type": "query_request", "query": "what?"})
        assert len(results) == 1
        tag, value = results[0]
        assert tag == QUERY_REQUEST_TAG
        assert value["query"] == "what?"

    def test_unknown_type_defaults_to_memory(self):
        results = self._route({"key": "k1", "stream_type": "unknown", "payload": "x"})
        assert len(results) == 1
        tag, _ = results[0]
        assert tag == MEMORY_EVENT_TAG

    def test_missing_type_defaults_to_memory(self):
        results = self._route({"key": "k1", "payload": "x"})
        assert len(results) == 1
        tag, _ = results[0]
        assert tag == MEMORY_EVENT_TAG

    def test_non_dict_dropped(self):
        router = _StreamRouter()
        results = list(router.process_element("not_a_dict", _FakeContext()))
        assert len(results) == 0


class TestAnswerSynthesiser:
    """Test the answer synthesiser that builds prompts with audit fields."""

    def _synthesise(self, value_dict, template=None):
        template = template or "Context:\n{context}\n\nQuery:\n{query}\n\nAnswer:"
        syn = _AnswerSynthesiser(
            prompt_template=template,
            workflow_version="v0.2.0",
            config_version="test_v1",
        )
        results = list(syn.process_element(value_dict, _FakeContext("k1")))
        return results

    def test_basic_synthesis(self):
        results = self._synthesise({
            "key": "k1",
            "query": "What is Flink?",
            "retrieved_context": [
                {"candidate_id": "c1", "payload": "Flink is a stream processor"},
                {"candidate_id": "c2", "payload": "Flink supports stateful ops"},
            ],
        })
        assert len(results) == 1
        out = results[0]
        assert out["stream_type"] == "answer_request"
        assert "Flink is a stream processor" in out["prompt"]
        assert "What is Flink?" in out["prompt"]
        assert out["retrieved_ids"] == ["c1", "c2"]
        assert out["workflow_version"] == "v0.2.0"
        assert out["config_version"] == "test_v1"

    def test_empty_context(self):
        results = self._synthesise({
            "key": "k1",
            "query": "test?",
            "retrieved_context": [],
        })
        assert len(results) == 1
        assert results[0]["retrieved_ids"] == []

    def test_topk_format(self):
        """Should also work with topk output format."""
        results = self._synthesise({
            "key": "k1",
            "payload": "What about X?",
            "topk": [
                {"candidate_id": "t1", "content": "X is interesting"},
            ],
            "version": 42,
            "total_candidates": 100,
            "changed": True,
        })
        assert len(results) == 1
        out = results[0]
        assert out["retrieved_ids"] == ["t1"]
        assert out["memory_version"] == 42
        assert out["total_candidates"] == 100
        assert out["retrieval_changed"] is True

    def test_non_dict_dropped(self):
        syn = _AnswerSynthesiser(prompt_template="{context}\n{query}")
        results = list(syn.process_element("not_a_dict", _FakeContext()))
        assert len(results) == 0


class TestValidateRAGConfig:
    """Test workflow config validation."""

    def test_valid_defaults(self):
        cfg = ContinuousRAGConfig()
        warnings = validate_rag_config(cfg)
        assert len(warnings) == 0

    def test_window_exceeds_agg_buffer(self):
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(max_window_events=200),
            agg_config=SemAggConfig(max_buffer_events=50),
        )
        warnings = validate_rag_config(cfg)
        assert any("window max_events" in w for w in warnings)

    def test_retrieve_exceeds_topk(self):
        cfg = ContinuousRAGConfig(
            retrieve_config=CtsRetrieveConfig(max_candidates_per_request=50),
            topk_config=SemTopKConfig(max_candidates=10),
        )
        warnings = validate_rag_config(cfg)
        assert any("retrieve max_candidates_per_request" in w for w in warnings)

    def test_ttl_spread_warning(self):
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(ttl_seconds=100),
            agg_config=SemAggConfig(ttl_seconds=100),
            groupby_config=SemGroupbyConfig(ttl_seconds=100),
            retrieve_config=CtsRetrieveConfig(ttl_seconds=10000),
        )
        warnings = validate_rag_config(cfg)
        assert any("TTL spread" in w for w in warnings)

    def test_summarize_no_flush_warning(self):
        cfg = ContinuousRAGConfig(
            agg_config=SemAggConfig(mode="summarize", flush_interval_ms=0),
        )
        warnings = validate_rag_config(cfg)
        assert any("flush_interval_ms" in w for w in warnings)


# ============================================================================
# Run
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

