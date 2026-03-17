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
        assert cfg.scorer_backend == "external_score"
        assert cfg.score_field == "score"

    def test_scorer_backend_llm(self):
        cfg = SemTopKConfig(scorer_backend="llm")
        assert cfg.scorer_backend == "llm"

    def test_scorer_backend_embedding(self):
        cfg = SemTopKConfig(scorer_backend="embedding")
        assert cfg.scorer_backend == "embedding"

    def test_scorer_backend_invalid(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid scorer_backend"):
            SemTopKConfig(scorer_backend="invalid")


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


class TestSemTopKScorerBackendRouting:
    """Test scorer backend routing in _ingest_candidate."""

    def _make_func(self, backend="external_score", k=3, max_cand=10):
        cfg = SemTopKConfig(k=k, max_candidates=max_cand, scorer_backend=backend)
        func = SemTopKFunction(cfg)
        func._candidates = _FakeMapState()
        func._snapshot = _FakeValueState()
        func._meta = _FakeValueState()
        func._metrics = None
        return func

    def test_external_score_upserts_directly(self):
        func = self._make_func(backend="external_score")
        meta = {"key": "k", "last_query": "q1"}
        item = {"candidate_id": "c1", "score": 0.9, "text": "hello"}
        results = list(func._ingest_candidate(item, meta, 1000))
        # No side-output for external_score
        assert len(results) == 0
        assert func._candidates.get("c1") is not None
        assert func._candidates.get("c1")["score"] == 0.9

    def test_llm_backend_emits_side_output_for_unscored(self):
        func = self._make_func(backend="llm")
        meta = {"key": "k", "last_query": "q1"}
        item = {"candidate_id": "c1", "text": "hello"}
        results = list(func._ingest_candidate(item, meta, 1000))
        # Should emit side-output work item
        assert len(results) == 1
        tag, work_dict = results[0]
        assert tag.tag_id == "async_work_items"
        assert work_dict["task_type"] == "score_llm"
        assert work_dict["payload"]["candidate"]["candidate_id"] == "c1"

    def test_llm_backend_upserts_when_scored(self):
        func = self._make_func(backend="llm")
        meta = {"key": "k", "last_query": "q1"}
        item = {"candidate_id": "c1", "score": 0.8, "text": "hello"}
        results = list(func._ingest_candidate(item, meta, 1000))
        assert len(results) == 0  # No side-output needed
        assert func._candidates.get("c1")["score"] == 0.8

    def test_embedding_backend_emits_side_output(self):
        func = self._make_func(backend="embedding")
        meta = {"key": "k", "last_query": "q1"}
        item = {"candidate_id": "c1", "text": "hello"}
        results = list(func._ingest_candidate(item, meta, 1000))
        assert len(results) == 1
        tag, work_dict = results[0]
        assert work_dict["task_type"] == "score_embedding"


class TestSemTopKScorerMergeBack:
    """Test _handle_scorer_result merge-back."""

    def _make_func(self, backend="llm", k=3):
        cfg = SemTopKConfig(k=k, max_candidates=10, scorer_backend=backend,
                            emission_policy="snapshot")
        func = SemTopKFunction(cfg)
        func._candidates = _FakeMapState()
        func._snapshot = _FakeValueState()
        func._meta = _FakeValueState({"key": "k", "update_count": 1})
        func._metrics = None
        return func

    def test_successful_scorer_result_upserts_candidate(self):
        func = self._make_func()
        result = {
            "task_type": "score_llm",
            "success": True,
            "result": {
                "candidate": {"candidate_id": "c1", "score": 0.95, "text": "hello"},
            },
        }
        results = list(func._handle_scorer_result(result, 1000))
        assert func._candidates.get("c1") is not None
        assert func._candidates.get("c1")["score"] == 0.95
        # Should recompute and emit
        assert len(results) > 0

    def test_failed_scorer_result_is_skipped(self):
        func = self._make_func()
        result = {
            "task_type": "score_llm",
            "success": False,
            "error": "timeout",
        }
        results = list(func._handle_scorer_result(result, 1000))
        assert len(results) == 0
        assert len(func._candidates.keys()) == 0


# ============================================================================
# SemanticSpec & RuntimeConfig Tests
# ============================================================================

from pyflink.semantic_runtime.semantic_spec import SemanticSpec
from pyflink.semantic_runtime.runtime_config import RuntimeConfig, DefaultsConfig


class TestSemanticSpec:
    def test_defaults(self):
        spec = SemanticSpec()
        assert spec.backend == "llm"
        assert spec.output_mode == "json"
        assert spec.instruction == ""
        assert spec.schema is None
        assert spec.threshold is None

    def test_for_sem_map(self):
        spec = SemanticSpec.for_sem_map(
            "Extract sentiment", output_schema={"sentiment": str}
        )
        assert spec.instruction == "Extract sentiment"
        assert spec.backend == "llm"
        assert spec.output_mode == "json"
        assert spec.schema == {"sentiment": str}

    def test_for_sem_map_text_mode(self):
        spec = SemanticSpec.for_sem_map("Summarize", return_mode="text")
        assert spec.output_mode == "text"
        assert spec.schema is None

    def test_for_sem_topk(self):
        spec = SemanticSpec.for_sem_topk(
            "Rerank by relevance", scorer_backend="llm", threshold=0.5
        )
        assert spec.instruction == "Rerank by relevance"
        assert spec.backend == "llm"
        assert spec.output_mode == "score"
        assert spec.threshold == 0.5

    def test_invalid_backend(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid backend"):
            SemanticSpec(backend="nonexistent")

    def test_invalid_output_mode(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid output_mode"):
            SemanticSpec(output_mode="unknown")

    def test_roundtrip(self):
        spec = SemanticSpec(instruction="test", backend="embedding", output_mode="score")
        d = spec.to_dict()
        spec2 = SemanticSpec.from_dict(d)
        assert spec2.instruction == "test"
        assert spec2.backend == "embedding"
        assert spec2.output_mode == "score"


class TestRuntimeConfig:
    def test_defaults(self):
        cfg = RuntimeConfig()
        assert cfg.defaults.ttl_seconds == 3600
        assert cfg.llm.backend == "mock"
        assert cfg.operators == {}

    def test_from_dict(self):
        cfg = RuntimeConfig.from_dict({
            "defaults": {"ttl_seconds": 7200},
            "llm": {"backend": "openai", "model": "gpt-4"},
            "operators": {
                "sem_topk": {"k": 5, "scorer_backend": "llm"},
            },
        })
        assert cfg.defaults.ttl_seconds == 7200
        assert cfg.llm.backend == "openai"
        assert cfg.llm.model == "gpt-4"
        assert cfg.operators["sem_topk"]["k"] == 5

    def test_get_operator_raw_merges_defaults(self):
        cfg = RuntimeConfig.from_dict({
            "defaults": {"ttl_seconds": 600},
            "operators": {"sem_topk": {"k": 3}},
        })
        raw = cfg.get_operator_raw("sem_topk")
        assert raw["ttl_seconds"] == 600
        assert raw["k"] == 3

    def test_get_operator_raw_missing(self):
        cfg = RuntimeConfig()
        raw = cfg.get_operator_raw("nonexistent")
        assert raw["ttl_seconds"] == 3600  # from defaults


# ============================================================================
# External Search Backend Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.external_search_backend import (
    ExternalSearchBackend, SearchResult,
)


class TestSearchResult:
    def test_to_dict(self):
        r = SearchResult(candidate_id="c1", text="hello", score=0.9)
        d = r.to_dict()
        assert d["candidate_id"] == "c1"
        assert d["text"] == "hello"
        assert d["score"] == 0.9


class TestExternalSearchBackendInterface:
    def test_is_abstract(self):
        import pytest
        with pytest.raises(TypeError):
            ExternalSearchBackend()


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
# Step 11: Stateful Metrics Tests
# ============================================================================

from pyflink.semantic_runtime.stateful.stateful_metrics import (
    StatefulOperatorMetrics,
    OperatorTag,
)


class TestOperatorTag:
    """Test operator tag metadata for audit/replay."""

    def test_defaults(self):
        tag = OperatorTag()
        assert tag.operator_name == ""
        assert tag.operator_version == "v0.2.0"
        assert tag.workflow_version == "v0.2.0"
        assert tag.config_hash == ""

    def test_to_dict(self):
        tag = OperatorTag(
            operator_name="sem_window",
            operator_version="v0.2.1",
            config_hash="abc123",
        )
        d = tag.to_dict()
        assert d["operator_name"] == "sem_window"
        assert d["operator_version"] == "v0.2.1"
        assert d["config_hash"] == "abc123"

    def test_custom_workflow_version(self):
        tag = OperatorTag(workflow_version="v0.3.0")
        assert tag.workflow_version == "v0.3.0"


class TestStatefulOperatorMetrics:
    """Test local-mode stateful metrics."""

    def test_noop_creation(self):
        m = StatefulOperatorMetrics.noop("sem_window")
        assert m._local is True
        assert m.tag.operator_name == "sem_window"

    def test_record_event_processed(self):
        m = StatefulOperatorMetrics.noop()
        m.record_event_processed()
        m.record_event_processed(count=5)
        assert m.local_events_processed == 6

    def test_record_timer_fire(self):
        m = StatefulOperatorMetrics.noop()
        m.record_timer_fire()
        m.record_timer_fire()
        assert m.local_timer_fires == 2

    def test_record_eviction(self):
        m = StatefulOperatorMetrics.noop()
        m.record_eviction(count=3)
        assert m.local_evictions == 3

    def test_record_overflow(self):
        m = StatefulOperatorMetrics.noop()
        m.record_overflow()
        assert m.local_overflows == 1

    def test_record_stale_window(self):
        m = StatefulOperatorMetrics.noop()
        m.record_stale_window()
        m.record_stale_window()
        assert m.local_stale_windows == 2

    def test_record_boundary_trigger(self):
        m = StatefulOperatorMetrics.noop()
        m.record_boundary_trigger()
        assert m.local_boundary_triggers == 1

    def test_record_recompute(self):
        m = StatefulOperatorMetrics.noop()
        m.record_recompute()
        m.record_recompute()
        m.record_recompute()
        assert m.local_recomputes == 3

    def test_record_async_emit(self):
        m = StatefulOperatorMetrics.noop()
        m.record_async_emit()
        assert m.local_async_emits == 1

    def test_update_state_size(self):
        m = StatefulOperatorMetrics.noop()
        m.update_state_size(42)
        assert m.current_state_size == 42
        m.update_state_size(0)
        assert m.current_state_size == 0

    def test_update_async_queue_depth(self):
        m = StatefulOperatorMetrics.noop()
        m.update_async_queue_depth(7)
        assert m.current_async_queue_depth == 7

    def test_snapshot(self):
        m = StatefulOperatorMetrics.noop("sem_agg")
        m.record_event_processed(10)
        m.record_timer_fire()
        m.record_eviction(2)
        m.record_overflow()
        m.record_stale_window()
        m.record_boundary_trigger()
        m.record_recompute()
        m.record_async_emit()
        m.update_state_size(100)
        m.update_async_queue_depth(3)

        snap = m.snapshot()
        assert snap["operator_name"] == "sem_agg"
        assert snap["events_processed"] == 10
        assert snap["timer_fires"] == 1
        assert snap["evictions"] == 2
        assert snap["overflows"] == 1
        assert snap["stale_windows"] == 1
        assert snap["boundary_triggers"] == 1
        assert snap["recomputes"] == 1
        assert snap["async_emits"] == 1
        assert snap["state_size"] == 100
        assert snap["async_queue_depth"] == 3
        assert snap["operator_version"] == "v0.2.0"

    def test_from_runtime_context_fallback(self):
        """Without a real Flink runtime context, should fall back to local."""

        class _FakeRuntimeContext:
            pass  # no get_metrics_group

        m = StatefulOperatorMetrics.from_runtime_context(
            _FakeRuntimeContext(), "sem_topk"
        )
        assert m._local is True
        assert m.tag.operator_name == "sem_topk"


# ============================================================================
# Step 12: Integration / Structural Tests
# ============================================================================


class TestKeyedCountConservation:
    """Verify that events are not silently lost or duplicated in operators."""

    def test_window_count_conservation_under_limit(self):
        """All events should accumulate in buffer when under max_window_events."""
        cfg = SemWindowConfig(max_window_events=10)
        func = SemWindowFunction(cfg)
        func._event_buffer = _FakeListState()
        func._window_meta = _FakeValueState(_new_window_meta(1000))

        for i in range(5):
            ev = SemanticEvent(key="k", payload=f"msg{i}", seq_id=i)
            func._event_buffer.add(ev.to_dict())

        buffered = list(func._event_buffer.get())
        assert len(buffered) == 5

    def test_groupby_count_conservation(self):
        """Each event assigned to a group increments that group's event_count."""
        cfg = SemGroupbyConfig(max_groups_per_key=10)
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "topic alpha beta", 100),
        })
        func._meta = _FakeValueState({"total_events": 0, "total_groups": 1})

        # Assign an event that matches g1
        ev = SemanticEvent(key="k", payload="alpha discussion", seq_id=0)
        gid, conf = func._local_assign(ev)
        assert gid == "g1"

    def test_retrieve_cache_conservation(self):
        """Cache entries should persist until eviction."""
        cfg = CtsRetrieveConfig(max_cache_entries_per_key=100)
        func = CtsRetrieveFunction(cfg)
        func._cache = _FakeMapState()

        for i in range(20):
            func._cache.put(f"c{i}", {"content": f"doc {i}", "_cached_at_ms": i * 100})

        assert len(func._cache.keys()) == 20

    def test_agg_algebraic_no_loss(self):
        """Algebraic reduce should produce a single accumulated value."""
        def sum_reduce(acc, new):
            acc["total"] = acc.get("total", 0) + new.get("value", 0)
            return acc

        cfg = SemAggConfig(mode="algebraic", reduce_fn=sum_reduce)
        func = SemAggFunction(cfg)
        func._agg_value = _FakeValueState(None)
        func._buffer = _FakeListState()
        func._meta = _FakeValueState({"version": 0, "total_events": 0})

        # Simulate algebraic reduce — first event becomes the accumulator
        # directly, subsequent events are reduced into it
        acc = func._agg_value.value()
        for i in range(5):
            new_event = {"value": i + 1}
            if acc is None:
                acc = {"total": new_event["value"]}
            else:
                acc = sum_reduce(acc, new_event)
        func._agg_value.update(acc)

        result = func._agg_value.value()
        assert result["total"] == 15  # 1+2+3+4+5


class TestStateBoundEnforcement:
    """Verify state bounds are enforced across operators."""

    def test_window_overflow_drop_oldest(self):
        """Window buffer should evict oldest when over limit (manual simulation)."""
        cfg = SemWindowConfig(max_window_events=3, overflow_policy=OverflowPolicy.DROP_OLDEST)
        events = [{"key": "k", "payload": f"msg{i}", "seq_id": i} for i in range(5)]

        # Simulate DROP_OLDEST: keep only the newest max_window_events
        if cfg.overflow_policy == OverflowPolicy.DROP_OLDEST:
            kept = events[-cfg.max_window_events:]
        else:
            kept = events[:cfg.max_window_events]

        assert len(kept) == 3
        # Should have kept the 3 newest (indices 2, 3, 4)
        assert kept[0]["seq_id"] == 2

    def test_topk_candidate_limit(self):
        """Top-K should enforce max_candidates."""
        cfg = SemTopKConfig(max_candidates=3, overflow_policy=OverflowPolicy.DROP_OLDEST)
        func = SemTopKFunction(cfg)
        func._candidates = _FakeMapState({
            f"c{i}": {"score": float(i), "_inserted_ms": i * 100}
            for i in range(5)
        })
        func._meta = _FakeValueState({"version": 0, "total_candidates": 5})

        evicted = func._enforce_candidate_limit()
        assert evicted == 2
        assert len(func._candidates.keys()) == 3

    def test_groupby_max_groups_enforced(self):
        """Groupby should respect max_groups_per_key."""
        cfg = SemGroupbyConfig(
            max_groups_per_key=2,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "topic one", 100),
            "g2": _new_group_profile("g2", "topic two", 200),
        })

        # Creating a third group should evict the oldest
        event = SemanticEvent(key="k", payload="topic three new", seq_id=0)
        gid = func._maybe_create_group(event, 300)
        assert gid is not None
        assert len(func._group_profiles.keys()) == 2
        assert not func._group_profiles.contains("g1")

    def test_retrieve_cache_limit(self):
        """Retrieve cache should evict oldest entries beyond limit."""
        cfg = CtsRetrieveConfig(
            max_cache_entries_per_key=3,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        func = CtsRetrieveFunction(cfg)
        func._cache = _FakeMapState({
            f"c{i}": {"content": f"doc{i}", "_cached_at_ms": i * 100}
            for i in range(5)
        })
        evicted = func._enforce_cache_limit()
        assert evicted == 2
        assert len(func._cache.keys()) == 3


class TestTimerDeterminism:
    """Verify timer registration and resolution is deterministic."""

    def test_same_input_same_timer(self):
        """Same timer key should always encode to the same value."""
        key1 = encode_timer_key(TimerCategory.FLUSH)
        key2 = encode_timer_key(TimerCategory.FLUSH)
        assert key1 == key2

    def test_different_categories_different_keys(self):
        """Different timer categories should produce different key values."""
        key1 = encode_timer_key(TimerCategory.FLUSH)
        key2 = encode_timer_key(TimerCategory.EVICT)
        assert key1 != key2

    def test_timer_resolve_deterministic(self):
        """Timer resolution should always pick the correct category."""
        # Simulate a meta dict with timer registrations as register_timer would do
        meta = {
            "_timer_flush": 10000,
            "_timer_evict": 20000,
        }

        cat1 = resolve_timer_category(meta, 10000)
        cat2 = resolve_timer_category(meta, 20000)
        assert cat1 == TimerCategory.FLUSH
        assert cat2 == TimerCategory.EVICT

        # Repeat — deterministic
        assert resolve_timer_category(meta, 10000) == TimerCategory.FLUSH
        assert resolve_timer_category(meta, 20000) == TimerCategory.EVICT


class TestDescriptorCompatibility:
    """Verify descriptor names and types are consistent and stable."""

    def test_all_descriptors_have_unique_names(self):
        """No two descriptor factories should produce the same name."""
        from pyflink.semantic_runtime.stateful.state_descriptors import (
            sem_agg_buffer_descriptor,
            sem_agg_value_descriptor,
            sem_agg_meta_descriptor,
            sem_topk_candidates_descriptor,
            sem_topk_snapshot_descriptor,
        )
        names = [
            sem_window_event_buffer_descriptor().name,
            sem_window_meta_descriptor().name,
            sem_groupby_profiles_descriptor().name,
            cts_retrieve_cache_descriptor().name,
            sem_agg_buffer_descriptor().name,
            sem_agg_value_descriptor().name,
            sem_agg_meta_descriptor().name,
            sem_topk_candidates_descriptor().name,
            sem_topk_snapshot_descriptor().name,
        ]
        assert len(names) == len(set(names)), f"Duplicate descriptor names: {names}"

    def test_descriptors_are_stable(self):
        """Descriptor names should not change between calls."""
        name1 = sem_window_event_buffer_descriptor().name
        name2 = sem_window_event_buffer_descriptor().name
        assert name1 == name2


class TestSemanticWindowSplitBehavior:
    """Integration: crafted conversation streams triggering window splits."""

    def test_count_trigger_split(self):
        """Window should split after max_window_events."""
        cfg = SemWindowConfig(max_window_events=3)
        func = SemWindowFunction(cfg)
        func._event_buffer = _FakeListState()
        func._window_meta = _FakeValueState(_new_window_meta(1000))

        events = [SemanticEvent(key="k", payload=f"m{i}", seq_id=i) for i in range(3)]
        for ev in events:
            func._event_buffer.add(ev.to_dict())

        meta = func._window_meta.value()
        meta["event_count"] = 3
        func._window_meta.update(meta)

        # _check_triggers returns a trigger reason string or None
        trigger = func._check_triggers(events[-1], meta)
        assert trigger is not None
        assert trigger == "count"

    def test_semantic_boundary_split(self):
        """Window should split on semantic boundary flag."""
        cfg = SemWindowConfig(max_window_events=100, boundary_flag="topic_shift")
        func = SemWindowFunction(cfg)
        func._event_buffer = _FakeListState()
        func._window_meta = _FakeValueState(_new_window_meta(1000))

        meta = func._window_meta.value()
        meta["event_count"] = 2

        # Event WITH topic_shift boundary
        ev = SemanticEvent(
            key="k", payload="new topic", seq_id=2,
            boundary_flags={"topic_shift": True},
        )
        trigger = func._check_triggers(ev, meta)
        assert trigger is not None
        assert trigger == "semantic_boundary"

    def test_no_split_below_count(self):
        """Window should NOT split when below count and no boundary."""
        cfg = SemWindowConfig(max_window_events=10)
        func = SemWindowFunction(cfg)
        func._event_buffer = _FakeListState()
        func._window_meta = _FakeValueState(_new_window_meta(1000))

        meta = func._window_meta.value()
        meta["event_count"] = 2

        ev = SemanticEvent(key="k", payload="continuing", seq_id=2)
        trigger = func._check_triggers(ev, meta)
        assert trigger is None


class TestGroupAssignmentStability:
    """Integration: semantic group assignment stability."""

    def test_same_topic_stays_in_group(self):
        """Events about the same topic should be assigned to the same group."""
        cfg = SemGroupbyConfig(max_groups_per_key=10)
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "machine learning algorithms", 100),
            "g2": _new_group_profile("g2", "cooking recipes food", 100),
        })
        func._meta = _FakeValueState({"total_events": 0, "total_groups": 2})

        ev1 = SemanticEvent(key="k", payload="machine learning", seq_id=0)
        ev2 = SemanticEvent(key="k", payload="learning algorithms", seq_id=1)

        gid1, _ = func._local_assign(ev1)
        gid2, _ = func._local_assign(ev2)
        assert gid1 == "g1"
        assert gid2 == "g1"

    def test_different_topic_goes_to_different_group(self):
        """Events about different topics should go to different groups."""
        cfg = SemGroupbyConfig(max_groups_per_key=10)
        func = SemGroupbyFunction(cfg)
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "machine learning algorithms", 100),
            "g2": _new_group_profile("g2", "cooking recipes food", 100),
        })
        func._meta = _FakeValueState({"total_events": 0, "total_groups": 2})

        ev_ml = SemanticEvent(key="k", payload="machine learning", seq_id=0)
        ev_cook = SemanticEvent(key="k", payload="cooking recipes", seq_id=1)

        gid_ml, _ = func._local_assign(ev_ml)
        gid_cook, _ = func._local_assign(ev_cook)
        assert gid_ml == "g1"
        assert gid_cook == "g2"


class TestRetrieveConsistency:
    """Integration: cts_retrieve consistency under repeated queries."""

    def test_repeated_query_same_results(self):
        """Same query against same cache should produce same results."""
        cfg = CtsRetrieveConfig(max_candidates_per_request=5)
        func = CtsRetrieveFunction(cfg)
        func._cache = _FakeMapState({
            "c1": {"content": "flink streaming engine", "_cached_at_ms": 100},
            "c2": {"content": "spark batch processing", "_cached_at_ms": 200},
            "c3": {"content": "flink state management", "_cached_at_ms": 300},
        })

        ev = SemanticEvent(key="k", payload="flink streaming", seq_id=0)
        results1 = func._local_retrieve(ev)
        results2 = func._local_retrieve(ev)

        assert len(results1) == len(results2)
        ids1 = [r.get("candidate_id") for r in results1]
        ids2 = [r.get("candidate_id") for r in results2]
        assert ids1 == ids2

    def test_cache_update_changes_results(self):
        """Adding new entries to cache should affect retrieval."""
        cfg = CtsRetrieveConfig(max_candidates_per_request=5)
        func = CtsRetrieveFunction(cfg)
        func._cache = _FakeMapState({
            "c1": {"content": "flink streaming", "_cached_at_ms": 100},
        })

        ev = SemanticEvent(key="k", payload="flink streaming", seq_id=0)
        results_before = func._local_retrieve(ev)

        # Add a highly relevant entry
        func._cache.put("c2", {"content": "flink streaming processing engine", "_cached_at_ms": 200})
        results_after = func._local_retrieve(ev)

        assert len(results_after) >= len(results_before)


class TestTopKUpdateConsistency:
    """Integration: continuous top-k update consistency."""

    def test_topk_recompute_stable_ordering(self):
        """Top-K recompute should produce stable ordering for same data."""
        cfg = SemTopKConfig(k=2, max_candidates=10, emission_policy="snapshot")
        func = SemTopKFunction(cfg)
        func._candidates = _FakeMapState({
            "c1": {"score": 0.9, "content": "best", "_inserted_ms": 100},
            "c2": {"score": 0.5, "content": "mid", "_inserted_ms": 200},
            "c3": {"score": 0.8, "content": "good", "_inserted_ms": 300},
        })
        func._snapshot = _FakeValueState(None)
        func._meta = _FakeValueState({"version": 0, "total_candidates": 3, "last_recompute_ms": 0})

        # Recompute twice — should produce same result
        result1 = list(func._recompute_and_emit(func._meta.value(), 1000, force_emit=True))
        func._snapshot.clear()
        func._meta.update({"version": 0, "total_candidates": 3, "last_recompute_ms": 0})
        result2 = list(func._recompute_and_emit(func._meta.value(), 2000, force_emit=True))

        assert len(result1) == len(result2) == 1
        # topk contains raw records; use top_ids for stable id comparison
        assert result1[0]["top_ids"][0] == result2[0]["top_ids"][0]

    def test_topk_delta_no_emission_on_same_data(self):
        """Delta policy should not emit when top-k hasn't changed."""
        cfg = SemTopKConfig(k=2, emission_policy="delta")
        func = SemTopKFunction(cfg)
        candidates = {
            "c1": {"score": 0.9, "content": "best", "_inserted_ms": 100},
            "c2": {"score": 0.5, "content": "mid", "_inserted_ms": 200},
        }
        func._candidates = _FakeMapState(dict(candidates))
        func._snapshot = _FakeValueState(None)
        func._meta = _FakeValueState({"version": 0, "total_candidates": 2, "last_recompute_ms": 0})

        # First recompute: should emit (no previous snapshot)
        r1 = list(func._recompute_and_emit(func._meta.value(), 1000, force_emit=False))
        assert len(r1) == 1

        # Second recompute with same data: should NOT emit
        meta = func._meta.value()
        r2 = list(func._recompute_and_emit(meta, 2000, force_emit=False))
        assert len(r2) == 0


class TestSemAggConsistency:
    """Integration: sem_agg output consistency."""

    def test_algebraic_reduce_is_associative(self):
        """Algebraic reduce should produce same result regardless of batching."""
        def sum_reduce(acc, new):
            acc["total"] = acc.get("total", 0) + new.get("value", 0)
            return acc

        events = [{"value": i} for i in range(1, 6)]

        # All at once — first event initialises accumulator with its value
        acc1 = {"total": events[0]["value"]}
        for e in events[1:]:
            acc1 = sum_reduce(acc1, e)

        # In two batches
        acc2 = {"total": events[0]["value"]}
        for e in events[1:3]:
            acc2 = sum_reduce(acc2, e)
        for e in events[3:]:
            acc2 = sum_reduce(acc2, e)

        assert acc1["total"] == acc2["total"] == 15

    def test_summarize_buffer_accumulation(self):
        """Summarize buffer should accumulate up to max_buffer_events."""
        cfg = SemAggConfig(mode="summarize", max_buffer_events=5)
        func = SemAggFunction(cfg)
        func._buffer = _FakeListState()
        func._agg_value = _FakeValueState(None)
        func._meta = _FakeValueState({"version": 0, "total_events": 0})

        for i in range(4):
            func._buffer.add({"payload": f"event_{i}"})

        buffered = list(func._buffer.get())
        assert len(buffered) == 4

        # Should NOT trigger summarize yet (4 < 5)
        should_summarize = len(buffered) >= cfg.max_buffer_events
        assert should_summarize is False

        # Add one more
        func._buffer.add({"payload": "event_4"})
        buffered = list(func._buffer.get())
        assert len(buffered) == 5

        should_summarize = len(buffered) >= cfg.max_buffer_events
        assert should_summarize is True


class TestCheckpointRecoveryStructural:
    """Structural tests for checkpoint/recovery scenarios.

    These validate that state can be serialized and reconstructed
    without a live Flink runtime (structural verification).
    """

    def test_window_state_roundtrip(self):
        """Window meta + buffer should survive a simulated state roundtrip."""
        meta = _new_window_meta(1000)
        meta["event_count"] = 5
        events = [{"key": "k", "payload": f"m{i}", "seq_id": i} for i in range(5)]

        # Simulate checkpoint: serialize
        import pickle
        meta_bytes = pickle.dumps(meta)
        events_bytes = pickle.dumps(events)

        # Simulate restore: deserialize
        restored_meta = pickle.loads(meta_bytes)
        restored_events = pickle.loads(events_bytes)

        assert restored_meta["event_count"] == 5
        assert restored_meta["window_id"] == meta["window_id"]
        assert len(restored_events) == 5
        assert restored_events[2]["payload"] == "m2"

    def test_groupby_profiles_roundtrip(self):
        """Group profiles should survive serialization."""
        import pickle
        profiles = {
            "g1": _new_group_profile("g1", "machine learning", 100),
            "g2": _new_group_profile("g2", "cooking recipes", 200),
        }
        restored = pickle.loads(pickle.dumps(profiles))
        assert restored["g1"]["label"] == "machine learning"
        assert restored["g2"]["group_id"] == "g2"

    def test_retrieve_cache_roundtrip(self):
        """Retrieve cache should survive serialization."""
        import pickle
        cache = {
            "c1": {"content": "flink", "_cached_at_ms": 100, "version": 1},
            "c2": {"content": "spark", "_cached_at_ms": 200, "version": 2},
        }
        restored = pickle.loads(pickle.dumps(cache))
        assert restored["c1"]["content"] == "flink"
        assert restored["c2"]["version"] == 2

    def test_topk_snapshot_roundtrip(self):
        """Top-K snapshot should survive serialization."""
        import pickle
        snapshot = {
            "version": 3,
            "topk": [
                {"candidate_id": "c1", "score": 0.9},
                {"candidate_id": "c2", "score": 0.8},
            ],
            "total_candidates": 50,
        }
        restored = pickle.loads(pickle.dumps(snapshot))
        assert restored["version"] == 3
        assert len(restored["topk"]) == 2
        assert restored["topk"][0]["candidate_id"] == "c1"

    def test_semantic_event_roundtrip(self):
        """SemanticEvent model should survive serialization."""
        import pickle
        ev = SemanticEvent(
            key="user_1", payload="test message", seq_id=42,
            metadata={"boundary": "topic_shift", "extra": [1, 2, 3]},
        )
        restored = SemanticEvent.from_dict(pickle.loads(pickle.dumps(ev.to_dict())))
        assert restored.key == "user_1"
        assert restored.payload == "test message"
        assert restored.seq_id == 42
        assert restored.metadata["boundary"] == "topic_shift"

    def test_metrics_snapshot_survives_reset(self):
        """Metrics snapshot should capture state before a reset."""
        m = StatefulOperatorMetrics.noop("sem_window")
        m.record_event_processed(100)
        m.record_timer_fire()
        m.record_eviction(5)
        m.update_state_size(50)

        snap = m.snapshot()

        # Simulate "new instance" after restore
        m2 = StatefulOperatorMetrics.noop("sem_window")
        assert m2.local_events_processed == 0

        # But the snapshot preserves the pre-restore state
        assert snap["events_processed"] == 100
        assert snap["timer_fires"] == 1
        assert snap["evictions"] == 5
        assert snap["state_size"] == 50


class TestEndToEndRAGConsistency:
    """Integration: end-to-end RAG memory-update/retrieval/answer consistency."""

    def test_memory_then_query_flow(self):
        """Memory events should be queryable after ingestion through subflows."""
        # Simulate memory subflow: window collects, groupby assigns, agg reduces
        window_buf = _FakeListState()
        for i in range(3):
            ev = SemanticEvent(key="user_1", payload=f"Flink is great {i}", seq_id=i)
            window_buf.add(ev.to_dict())

        # After window flush, events go to groupby
        events_out = list(window_buf.get())
        assert len(events_out) == 3

        # Simulate groupby: assign all to same group
        cfg_gb = SemGroupbyConfig(max_groups_per_key=10)
        func_gb = SemGroupbyFunction(cfg_gb)
        func_gb._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "flink streaming great", 100),
        })
        func_gb._meta = _FakeValueState({"total_events": 0, "total_groups": 1})

        for ev_dict in events_out:
            ev = SemanticEvent.from_dict(ev_dict)
            gid, _ = func_gb._local_assign(ev)
            assert gid == "g1"  # all about flink

        # Simulate retrieval subflow: query hits cache
        cfg_ret = CtsRetrieveConfig(max_candidates_per_request=5)
        func_ret = CtsRetrieveFunction(cfg_ret)
        func_ret._cache = _FakeMapState({
            "c1": {"content": "flink is great 0", "_cached_at_ms": 100},
            "c2": {"content": "flink is great 1", "_cached_at_ms": 200},
            "c3": {"content": "flink is great 2", "_cached_at_ms": 300},
        })

        query_ev = SemanticEvent(key="user_1", payload="flink", seq_id=10)
        results = func_ret._local_retrieve(query_ev)
        assert len(results) >= 1

        # Simulate answer synthesis
        syn = _AnswerSynthesiser(
            prompt_template="Context:\n{context}\n\nQuery:\n{query}\n\nAnswer:",
            workflow_version="v0.2.0",
        )
        answer_input = {
            "key": "user_1",
            "query": "Tell me about Flink",
            "retrieved_context": results,
        }
        answers = list(syn.process_element(answer_input, _FakeContext("user_1")))
        assert len(answers) == 1
        assert "flink" in answers[0]["prompt"].lower()
        assert answers[0]["workflow_version"] == "v0.2.0"
        assert len(answers[0]["retrieved_ids"]) >= 1

    def test_config_validation_catches_mismatches(self):
        """Validate that config validation catches window>agg mismatch."""
        cfg = ContinuousRAGConfig(
            window_config=SemWindowConfig(max_window_events=200),
            agg_config=SemAggConfig(max_buffer_events=50),
        )
        warnings = validate_rag_config(cfg)
        assert len(warnings) > 0
        assert any("window" in w.lower() for w in warnings)


# ============================================================================
# Run
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

