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
# Run
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

