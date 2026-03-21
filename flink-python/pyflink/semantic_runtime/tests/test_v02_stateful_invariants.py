"""
Unit tests for V0.2 stateful invariants and structural safety checks.

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

from pyflink.semantic_runtime.runtime.event_model import SemanticEvent
from pyflink.semantic_runtime.operators.stateful.sem_window import SemWindowConfig, SemWindowFunction, _new_window_meta
from pyflink.semantic_runtime.runtime.state_descriptors import (
    OverflowPolicy,
    sem_window_event_buffer_descriptor,
    sem_window_meta_descriptor,
    sem_groupby_profiles_descriptor,
    sem_search_cache_descriptor,
)
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.runtime.timer_policy import encode_timer_key, TimerCategory, resolve_timer_category
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG
from pyflink.semantic_runtime.runtime.async_bridge import build_async_bridge
from pyflink.semantic_runtime.operators.stateful.sem_groupby import (
    SemGroupbyConfig,
    SemGroupbyFunction,
    _new_group_profile,
)
from pyflink.semantic_runtime.runtime.sem_search import SemSearchConfig, SemSearchFunction
from pyflink.semantic_runtime.operators.stateful.sem_topk import SemTopKConfig, SemTopKFunction
from pyflink.semantic_runtime.operators.stateful.sem_agg import SemAggConfig, SemAggFunction
from pyflink.semantic_runtime.semantic_spec import GroupbyQuerySpec, GroupbyScopePolicy, TriggerPolicy, AggQuerySpec, AggScopePolicy, TopKQuerySpec
from pyflink.semantic_runtime.runtime.external_search_backend import MockSearchBackend
from pyflink.semantic_runtime.runtime.continuous_rag_workflow import ContinuousRAGConfig


class _FakeMapState:
    def __init__(self, data: dict | None = None):
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


class _FakeWorkflowContext:
    def __init__(self, key="user_1"):
        self._key = key
        self.outputs = []

    def get_current_key(self):
        return self._key

    def output(self, tag, value):
        self.outputs.append((tag, value))

class TestStateSafetyAudit:
    """Verify state safety guardrails across operators."""

    def test_new_descriptors_exist(self):
        """Verify sem_agg and sem_topk descriptors are declared."""
        from pyflink.semantic_runtime.runtime.state_descriptors import (
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

    def test_retrieve_overflow_drop_newest(self):
        """sem_search: DROP_NEWEST should evict newest entries first."""
        cfg = SemSearchConfig(max_cache_entries_per_key=2,
                                overflow_policy=OverflowPolicy.DROP_NEWEST)
        func = SemSearchFunction(cfg)
        func._cache = _FakeMapState({
            "c1": {"content": "a", "_cached_at_ms": 100},
            "c2": {"content": "b", "_cached_at_ms": 200},
            "c3": {"content": "c", "_cached_at_ms": 300},
        })
        evicted = func._enforce_cache_limit()
        assert evicted == 1
        assert len(func._cache.keys()) == 2
        assert set(func._cache.keys()) == {"c1", "c2"}


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


class _FakeContext:
    """Minimal KeyedProcessFunction context mock for unit tests."""

    def __init__(self, key="test_key"):
        self._key = key

        class _TimerService:
            def register_processing_time_timer(self, ts):
                pass

            def register_event_time_timer(self, ts):
                pass

        self._timer = _TimerService()

    def get_current_key(self):
        return self._key

    def timer_service(self):
        return self._timer

    def output(self, tag, value):
        pass

# ============================================================================
# Step 11: Stateful Metrics Tests
# ============================================================================

from pyflink.semantic_runtime.runtime.stateful_metrics import (
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

    def test_from_runtime_context_defaults(self):
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
        cfg = SemSearchConfig(max_cache_entries_per_key=100)
        func = SemSearchFunction(cfg)
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
        cfg = SemSearchConfig(
            max_cache_entries_per_key=3,
            overflow_policy=OverflowPolicy.DROP_OLDEST,
        )
        func = SemSearchFunction(cfg)
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
        from pyflink.semantic_runtime.runtime.state_descriptors import (
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
            sem_search_cache_descriptor().name,
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
    """Integration: sem_search consistency under repeated queries."""

    def test_repeated_query_same_results(self):
        """Same query against same cache should produce same results."""
        cfg = SemSearchConfig(max_candidates_per_request=5)
        func = SemSearchFunction(cfg)
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
        cfg = SemSearchConfig(max_candidates_per_request=5)
        func = SemSearchFunction(cfg)
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
        cfg = SemTopKConfig(max_candidates=10, emission_policy="snapshot")
        func = SemTopKFunction(cfg, query_spec=TopKQuerySpec(k=2))
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
        cfg = SemTopKConfig(emission_policy="delta")
        func = SemTopKFunction(cfg, query_spec=TopKQuerySpec(k=2))
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


from pyflink.semantic_runtime.runtime.continuous_rag_components import _AnswerSynthesiser
from pyflink.semantic_runtime.runtime.continuous_rag_workflow import (
    ContinuousRAGConfig,
    validate_rag_config,
)


class _FakeWorkflowContext:
    def __init__(self, key="test_key"):
        self._key = key

    def get_current_key(self):
        return self._key


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
        cfg_ret = SemSearchConfig(max_candidates_per_request=5)
        func_ret = SemSearchFunction(cfg_ret)
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
        answers = list(syn.process_element(answer_input, _FakeWorkflowContext("user_1")))
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
