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

import asyncio
import json
import time

import pytest

from pyflink.semantic_runtime.runtime.event_model import (
    SemanticEvent,
    WindowSnapshot,
    simple_key_selector,
    composite_key_selector,
)
from pyflink.semantic_runtime.runtime.state_descriptors import (
    StateSafetyConfig,
    OverflowPolicy,
    build_ttl_config,
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
    sem_groupby_profiles_descriptor,
    sem_search_cache_descriptor,
)
from pyflink.semantic_runtime.runtime.timer_policy import (
    TimerCategory,
    TimerPolicy,
    encode_timer_key,
    register_timer,
    resolve_timer_category,
    clear_timer_registration,
    schedule_policy_timers,
)
from pyflink.semantic_runtime.operators.stateful.sem_window import (
    SemWindowConfig,
    SemWindowFunction,
    _new_window_meta,
)
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG
from pyflink.semantic_runtime.semantic_spec import SemanticSpec, TriggerPolicy, TopKQuerySpec, TopKScopePolicy


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
        cfg = StateSafetyConfig(ttl_seconds=60, overflow_policy=OverflowPolicy.DROP_NEWEST)
        assert cfg.ttl_seconds == 60
        assert cfg.overflow_policy == OverflowPolicy.DROP_NEWEST


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
        desc = sem_search_cache_descriptor(1800)
        assert desc.name == "sem_search_cache"


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

from pyflink.semantic_runtime.runtime.async_bridge import (
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

from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
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
# SemSearch Logic Tests (no Flink runtime)
# ============================================================================

from pyflink.semantic_runtime.runtime.sem_search import (
    SemSearchConfig,
    SemSearchFunction,
)

class TestSemSearchConfig:
    def test_defaults(self):
        cfg = SemSearchConfig()
        assert cfg.max_candidates_per_request == 20
        assert cfg.max_cache_entries_per_key == 200
        assert cfg.ttl_seconds == 1800
        assert cfg.overflow_policy == OverflowPolicy.DROP_OLDEST

class TestSemSearchLocalRetrieve:
    """Test _local_retrieve without Flink state."""

    def _make_func_with_cache(self, cache_entries):
        func = SemSearchFunction(SemSearchConfig())
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

    def test_embedding_match(self):
        func = SemSearchFunction(
            SemSearchConfig(cache_match_fn_name="embedding", cache_embedding_dim=64)
        )
        func._cache = _FakeMapState({
            "c1": {"content": "sunny warm weather forecast", "source": "a"},
            "c2": {"content": "rain storm alert", "source": "b"},
        })
        event = SemanticEvent(key="k", payload="best sunny weather", seq_id=0)
        results = func._local_retrieve(event)
        assert results
        assert results[0]["candidate_id"] == "c1"

    def test_embedding_path_supports_text_field(self):
        func = SemSearchFunction(
            SemSearchConfig(cache_match_fn_name="embedding", cache_embedding_dim=64)
        )
        func._cache = _FakeMapState({
            "c1": {"text": "budget planning update", "source": "a"},
            "c2": {"text": "travel packing list", "source": "b"},
        })
        event = SemanticEvent(key="k", payload="budget update", seq_id=0)
        results = func._local_retrieve(event)
        assert results
        assert results[0]["candidate_id"] == "c1"

class TestSemSearchCacheEnforcement:
    def test_enforce_cache_limit(self):
        entries = {f"c{i}": {"content": f"text {i}", "_cached_at_ms": i * 100}
                   for i in range(10)}
        func = SemSearchFunction(SemSearchConfig(max_cache_entries_per_key=5))
        func._cache = _FakeMapState(entries)
        evicted = func._enforce_cache_limit()
        assert evicted == 5
        # Should keep the 5 newest (c5-c9)
        remaining_keys = list(func._cache.keys())
        assert len(remaining_keys) == 5

    def test_under_limit_no_eviction(self):
        entries = {"c1": {"content": "a"}, "c2": {"content": "b"}}
        func = SemSearchFunction(SemSearchConfig(max_cache_entries_per_key=10))
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


def _split_outputs(items):
    main = []
    side = []
    for item in items:
        if isinstance(item, tuple) and len(item) == 2:
            side.append(item[1])
        else:
            main.append(item)
    return main, side


class _FakeListState:
    def __init__(self, values=None):
        self._values = list(values) if values else []

    def add(self, value):
        self._values.append(value)

    def get(self):
        return list(self._values)

    def update(self, values):
        self._values = list(values)

    def clear(self):
        self._values = []


class _FakeValueState:
    def __init__(self, value=None):
        self._value = value

    def value(self):
        return self._value

    def update(self, value):
        self._value = value

    def clear(self):
        self._value = None


class _FakeContext:
    def __init__(self, key="k"):
        self._key = key
        self.outputs = []

    def get_current_key(self):
        return self._key

    def output(self, tag, value):
        self.outputs.append((tag, value))


# ============================================================================
# SemAgg Logic Tests
# ============================================================================

from pyflink.semantic_runtime.operators.stateful.sem_agg import (
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

from pyflink.semantic_runtime.operators.stateful.sem_topk import (
    SemTopKConfig,
    SemTopKFunction,
)
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec, TopKScopePolicy

class TestSemTopKConfig:
    def test_defaults(self):
        cfg = SemTopKConfig()
        assert cfg.max_candidates == 100
        assert cfg.emission_policy == "delta"
        assert cfg.score_field == "score"
        assert cfg.ttl_seconds == 3600

    def test_query_spec_defaults(self):
        qs = TopKQuerySpec()
        assert qs.k == 10
        assert qs.query_version == 1
        assert qs.ranking_method == "pointwise"
        assert qs.scope_policy.ttl_seconds is None

    def test_query_spec_simple(self):
        qs = TopKQuerySpec.simple("rank by relevance", k=5, backend="llm",
                                  ttl_seconds=600, max_candidates=50)
        assert qs.k == 5
        assert qs.semantic.backend == "llm"
        assert qs.scope_policy.ttl_seconds == 600
        assert qs.scope_policy.max_candidates == 50

    def test_query_spec_invalid_ranking_method(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid ranking_method"):
            TopKQuerySpec(ranking_method="bogus")

    def test_scope_policy_session_gap_validation(self):
        import pytest
        with pytest.raises(ValueError, match="session_gap_ms"):
            TopKScopePolicy(window_kind="session", session_gap_ms=0)

    def test_topk_query_spec_roundtrip_with_session_scope(self):
        spec = TopKQuerySpec(
            semantic=SemanticSpec.for_sem_topk("rank best weather days", scorer_backend="embedding"),
            k=4,
            query_id="topk1",
            query_version=3,
            ranking_method="pointwise",
            trigger_policy=TriggerPolicy(mode="periodic", interval_ms=2000),
            scope_policy=TopKScopePolicy(
                ttl_seconds=600,
                max_candidates=50,
                window_kind="session",
                session_gap_ms=15000,
                boundary_flag="topic_shift",
            ),
        )
        restored = TopKQuerySpec.from_dict(spec.to_dict())
        assert restored.query_id == "topk1"
        assert restored.trigger_policy.mode == "periodic"
        assert restored.scope_policy.window_kind == "session"
        assert restored.scope_policy.session_gap_ms == 15000
        assert restored.scope_policy.boundary_flag == "topic_shift"


class TestSemTopKRecompute:
    """Test top-k recomputation logic (no Flink runtime)."""

    def _make_func(self, k=3, max_cand=10, policy="delta"):
        cfg = SemTopKConfig(max_candidates=max_cand, emission_policy=policy)
        qs = TopKQuerySpec(k=k)
        func = SemTopKFunction(cfg, query_spec=qs)
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


class TestSemTopKPureStateMachine:
    """Test the pure state machine: upsert + versioning (no scorer routing)."""

    def _make_func(self, k=3, max_cand=10, **qs_kwargs):
        cfg = SemTopKConfig(max_candidates=max_cand, emission_policy="snapshot",
                            recompute_interval_ms=0)
        qs = TopKQuerySpec(k=k, **qs_kwargs)
        func = SemTopKFunction(cfg, query_spec=qs)
        func._candidates = _FakeMapState()
        func._snapshot = _FakeValueState()
        func._meta = _FakeValueState()
        func._metrics = None
        return func

    def test_upsert_attaches_versioning_metadata(self):
        func = self._make_func()
        item = {"candidate_id": "c1", "score": 0.9, "text": "hello"}
        func._upsert_candidate(item, 1000)
        stored = func._candidates.get("c1")
        assert stored is not None
        assert stored["score"] == 0.9
        assert stored["_updated_ms"] == 1000
        assert stored["_score_version"] == 1
        assert stored["_query_version"] == 1
        assert stored["_score_backend"] == "external_score"  # default from TopKQuerySpec

    def test_upsert_preserves_existing_versioning(self):
        func = self._make_func()
        item = {
            "candidate_id": "c1", "score": 0.9,
            "_score_version": 3, "_query_version": 2,
            "_score_backend": "llm",
        }
        func._upsert_candidate(item, 2000)
        stored = func._candidates.get("c1")
        assert stored["_score_version"] == 3
        assert stored["_query_version"] == 2
        assert stored["_score_backend"] == "llm"

    def test_upsert_skips_no_candidate_id(self):
        func = self._make_func()
        accepted = func._upsert_candidate({"score": 0.5}, 1000)
        assert accepted is False
        assert len(func._candidates.keys()) == 0

    def test_upsert_rejects_unscored_candidate(self):
        func = self._make_func()
        accepted = func._upsert_candidate({"candidate_id": "c1"}, 1000)
        assert accepted is False
        assert len(func._candidates.keys()) == 0

    def test_query_spec_k_drives_topk(self):
        """k comes from TopKQuerySpec, not SemTopKConfig."""
        cfg = SemTopKConfig(max_candidates=20)
        qs = TopKQuerySpec(k=2)
        func = SemTopKFunction(cfg, query_spec=qs)
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9},
            "c2": {"candidate_id": "c2", "score": 0.5},
            "c3": {"candidate_id": "c3", "score": 0.7},
        })
        func._snapshot = _FakeValueState()
        func._meta = _FakeValueState({"key": "k", "update_count": 3})
        results = list(func._recompute_and_emit(
            {"key": "k", "update_count": 3}, 1000))
        assert len(results) == 1
        assert results[0]["top_ids"] == ["c1", "c3"]

    def test_stale_query_version_excluded_from_frontier(self):
        """Candidates scored under an old query_version are excluded."""
        func = self._make_func(k=2, max_cand=20, query_version=2)
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9, "_query_version": 2},
            "c2": {"candidate_id": "c2", "score": 0.8, "_query_version": 1},  # stale
            "c3": {"candidate_id": "c3", "score": 0.7, "_query_version": 2},
        })
        func._meta = _FakeValueState({"key": "k", "update_count": 3})
        results = list(func._recompute_and_emit(
            {"key": "k", "update_count": 3}, 1000))
        assert len(results) == 1
        assert results[0]["top_ids"] == ["c1", "c3"]
        assert results[0]["stale_candidates"] == 1
        assert results[0]["total_candidates"] == 2  # only eligible

    def test_stale_candidates_remain_in_state(self):
        """Stale candidates are not deleted, just excluded from ranking."""
        func = self._make_func(k=2, max_cand=20, query_version=2)
        func._candidates = _FakeMapState({
            "c1": {"candidate_id": "c1", "score": 0.9, "_query_version": 1},
        })
        func._meta = _FakeValueState({"key": "k", "update_count": 1})
        list(func._recompute_and_emit({"key": "k", "update_count": 1}, 1000))
        # c1 is stale but should still be in state
        assert func._candidates.get("c1") is not None

    def test_scope_policy_overrides_kernel_max_candidates(self):
        """TopKScopePolicy.max_candidates overrides SemTopKConfig.max_candidates."""
        cfg = SemTopKConfig(max_candidates=100)
        qs = TopKQuerySpec(k=2, scope_policy=TopKScopePolicy(max_candidates=3))
        func = SemTopKFunction(cfg, query_spec=qs)
        assert func._resolved_max_candidates == 3

    def test_scope_policy_defaults_to_kernel_config(self):
        """When scope_policy.max_candidates is None, fall back to kernel config."""
        cfg = SemTopKConfig(max_candidates=50)
        qs = TopKQuerySpec(k=2, scope_policy=TopKScopePolicy())
        func = SemTopKFunction(cfg, query_spec=qs)
        assert func._resolved_max_candidates == 50

    def test_scope_policy_ttl_overrides_kernel(self):
        """TopKScopePolicy.ttl_seconds overrides SemTopKConfig.ttl_seconds."""
        cfg = SemTopKConfig(ttl_seconds=3600)
        qs = TopKQuerySpec(k=2, scope_policy=TopKScopePolicy(ttl_seconds=600))
        func = SemTopKFunction(cfg, query_spec=qs)
        assert func._resolved_ttl_seconds == 600

    def test_process_element_rejects_envelope(self):
        """Kernel no longer expands retrieval envelopes — flat dict only."""
        func = self._make_func()
        envelope = {"candidates": [
            {"candidate_id": "c1", "score": 0.9},
            {"candidate_id": "c2", "score": 0.5},
        ]}
        # process_element should treat envelope as a single candidate;
        # since it has no candidate_id, nothing is upserted.
        results = list(func.process_element(envelope, _FakeContext("k")))
        assert len(func._candidates.keys()) == 0
        assert results == []

    def test_process_element_rejects_unscored_candidate_without_emission(self):
        func = self._make_func()
        results = list(func.process_element(
            {"candidate_id": "c1", "query": "q", "query_seq_id": 7},
            _FakeContext("k"),
        ))
        assert len(func._candidates.keys()) == 0
        assert results == []

    def test_process_element_propagates_query_fields_to_snapshot(self):
        func = self._make_func(k=1)
        results = list(func.process_element(
            {
                "candidate_id": "c1",
                "score": 0.95,
                "query": "best weather days",
                "query_seq_id": 42,
                "source": "async_score",
                "error": "partial_score_timeout",
            },
            _FakeContext("k"),
        ))
        assert len(results) == 1
        out = results[0]
        assert out["query"] == "best weather days"
        assert out["query_seq_id"] == 42
        assert out["source"] == "async_score"
        assert out["error"] == "partial_score_timeout"

    def test_idle_flush_emits_only_when_flush_timer_fires(self):
        class _TimerService:
            def __init__(self):
                self.registered = []

            def register_processing_time_timer(self, ts):
                self.registered.append(ts)

            def register_event_time_timer(self, ts):
                self.registered.append(ts)

        class _Ctx:
            def __init__(self):
                self._ts = _TimerService()

            def get_current_key(self):
                return "k"

            def timer_service(self):
                return self._ts

        func = self._make_func(
            k=1,
            query_version=1,
            trigger_policy=TriggerPolicy(mode="idle_flush", idle_ms=50),
        )
        ctx = _Ctx()
        results = list(func.process_element(
            {"candidate_id": "c1", "score": 0.9, "event_time_ms": 1000},
            ctx,
        ))
        assert results == []
        meta = func._meta.value()
        fire_at = meta["_timer_flush"]
        timer_results = list(func.on_timer(fire_at, ctx))
        assert len(timer_results) == 1
        assert timer_results[0]["top_ids"] == ["c1"]

    def test_count_threshold_emits_every_n_accepted_candidates(self):
        func = self._make_func(
            k=1,
            trigger_policy=TriggerPolicy(mode="count_threshold", count_threshold=2),
        )
        ctx = _FakeContext("k")
        first = list(func.process_element(
            {"candidate_id": "c1", "score": 0.3, "event_time_ms": 1000},
            ctx,
        ))
        second = list(func.process_element(
            {"candidate_id": "c2", "score": 0.9, "event_time_ms": 1001},
            ctx,
        ))
        third = list(func.process_element(
            {"candidate_id": "c3", "score": 0.2, "event_time_ms": 1002},
            ctx,
        ))
        assert first == []
        assert len(second) == 1
        assert second[0]["top_ids"] == ["c2"]
        assert third == []

    def test_operator_owned_scope_close_session_emits_and_resets_on_timer(self):
        class _TimerService:
            def __init__(self):
                self.registered = []

            def register_processing_time_timer(self, ts):
                self.registered.append(ts)

            def register_event_time_timer(self, ts):
                self.registered.append(ts)

        class _Ctx:
            def __init__(self):
                self._ts = _TimerService()

            def get_current_key(self):
                return "k"

            def timer_service(self):
                return self._ts

        func = self._make_func(
            k=1,
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=TopKScopePolicy(window_kind="session", session_gap_ms=50),
        )
        ctx = _Ctx()
        first = list(func.process_element(
            {"candidate_id": "c1", "score": 0.4, "event_time_ms": 1000},
            ctx,
        ))
        second = list(func.process_element(
            {"candidate_id": "c2", "score": 0.9, "event_time_ms": 1010},
            ctx,
        ))
        assert first == []
        assert second == []
        meta = func._meta.value()
        fire_at = meta["_timer_flush"]
        timer_results = list(func.on_timer(fire_at, ctx))
        assert len(timer_results) == 1
        assert timer_results[0]["top_ids"] == ["c2"]
        assert timer_results[0]["scope_close_reason"] == "session_gap"
        assert len(func._candidates.keys()) == 0

    def test_operator_owned_scope_close_semantic_emits_after_boundary(self):
        func = self._make_func(
            k=1,
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=TopKScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
        )
        ctx = _FakeContext("k")
        first = list(func.process_element(
            {"candidate_id": "c1", "score": 0.4, "event_time_ms": 1000, "boundary_flags": {}},
            ctx,
        ))
        second = list(func.process_element(
            {
                "candidate_id": "c2",
                "score": 0.9,
                "event_time_ms": 1010,
                "boundary_flags": {"topic_shift": True},
            },
            ctx,
        ))
        assert first == []
        assert len(second) == 1
        assert second[0]["top_ids"] == ["c2"]
        assert second[0]["scope_close_reason"] == "semantic_boundary"
        assert len(func._candidates.keys()) == 0

    def test_session_scope_resets_before_new_event(self):
        func = self._make_func(
            k=1,
            scope_policy=TopKScopePolicy(window_kind="session", session_gap_ms=100),
        )
        list(func.process_element(
            {"candidate_id": "c1", "score": 0.9, "event_time_ms": 1000},
            _FakeContext("k"),
        ))
        results = list(func.process_element(
            {"candidate_id": "c2", "score": 0.8, "event_time_ms": 1201},
            _FakeContext("k"),
        ))
        assert len(results) == 2
        assert results[0]["scope_close_reason"] == "session_gap"
        assert results[-1]["top_ids"] == ["c2"]
        assert set(func._candidates.keys()) == {"c2"}
        meta = func._meta.value()
        assert meta["scope_epoch"] == 1
        assert meta["last_scope_reset_reason"] == "session_gap"

    def test_tumbling_scope_resets_on_bucket_rollover(self):
        func = self._make_func(
            k=1,
            scope_policy=TopKScopePolicy(window_kind="tumbling", window_size_ms=100),
        )
        list(func.process_element(
            {"candidate_id": "c1", "score": 0.9, "event_time_ms": 10},
            _FakeContext("k"),
        ))
        results = list(func.process_element(
            {"candidate_id": "c2", "score": 0.8, "event_time_ms": 120},
            _FakeContext("k"),
        ))
        assert len(results) == 2
        assert results[0]["scope_close_reason"] == "tumbling_rollover"
        assert results[-1]["top_ids"] == ["c2"]
        assert set(func._candidates.keys()) == {"c2"}

    def test_semantic_scope_resets_after_boundary_event(self):
        func = self._make_func(
            k=2,
            scope_policy=TopKScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
        )
        list(func.process_element(
            {"candidate_id": "c1", "score": 0.9, "event_time_ms": 10},
            _FakeContext("k"),
        ))
        results = list(func.process_element(
            {
                "candidate_id": "c2",
                "score": 0.8,
                "event_time_ms": 20,
                "boundary_flags": {"topic_shift": True},
            },
            _FakeContext("k"),
        ))
        assert len(results) == 1
        assert results[0]["top_ids"] == ["c1", "c2"]
        assert len(func._candidates.keys()) == 0
        meta = func._meta.value()
        assert meta["scope_epoch"] == 1
        assert meta["last_scope_reset_reason"] == "semantic_boundary"

    def test_sliding_scope_evicts_old_candidates(self):
        func = self._make_func(
            k=1,
            scope_policy=TopKScopePolicy(window_kind="sliding", window_size_ms=100),
        )
        list(func.process_element(
            {"candidate_id": "c1", "score": 0.9, "event_time_ms": 10},
            _FakeContext("k"),
        ))
        list(func.process_element(
            {"candidate_id": "c2", "score": 0.8, "event_time_ms": 50},
            _FakeContext("k"),
        ))
        results = list(func.process_element(
            {"candidate_id": "c3", "score": 0.7, "event_time_ms": 200},
            _FakeContext("k"),
        ))
        assert len(results) == 1
        assert results[0]["top_ids"] == ["c3"]
        assert set(func._candidates.keys()) == {"c3"}


# ============================================================================
# SemanticSpec & RuntimeConfig Tests
# ============================================================================
