"""
Unit tests for V0.2 semantic specs, lowering plans, and planner-facing stateful
operator contracts.

These tests exercise pure Python logic — no Flink runtime required.
"""

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os, pathlib, pyflink as _pf  # noqa: E401,E402
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

import json
import time
import asyncio

import pytest

from pyflink.semantic_runtime.runtime.event_model import SemEvent
from pyflink.semantic_runtime.runtime.timer_policy import TimerCategory, encode_timer_key
from pyflink.semantic_runtime.runtime.state_descriptors import OverflowPolicy
from pyflink.semantic_runtime.runtime.async_bridge import ASYNC_WORK_TAG
from pyflink.semantic_runtime.runtime.stateful_metrics import StatefulOperatorMetrics
from pyflink.semantic_runtime.sem_spec import (
    SemSpec,
    TriggerPolicy,
    GroupbyQuerySpec,
    GroupbyScopePolicy,
    AggQuerySpec,
    AggScopePolicy,
    JoinQuerySpec,
    JoinScopePolicy,
    TopKQuerySpec,
    TopKScopePolicy,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig, DefaultsConfig
from pyflink.semantic_runtime.operators.stateful.sem_groupby_kernel import (
    SemGroupbyConfig,
    SemGroupbyFunction,
    _new_group_profile,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_pipeline import (
    build_sem_groupby_operator,
    resolve_groupby_execution_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_bounded import WindowOwnedSemGroupbyFunction
from pyflink.semantic_runtime.operators.stateful.sem_agg_kernel import SemAggConfig, SemAggFunction
from pyflink.semantic_runtime.operators.stateful.sem_agg_pipeline import (
    build_sem_agg_operator,
    resolve_agg_execution_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_bounded import WindowOwnedSemAggFunction
from pyflink.semantic_runtime.runtime.plans import (
    resolve_topk_lowering_plan,
    resolve_groupby_lowering_plan,
    resolve_agg_lowering_plan,
    resolve_join_lowering_plan,
)


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
    class _TimerService:
        def register_processing_time_timer(self, ts):
            return None

        def register_event_time_timer(self, ts):
            return None

    def __init__(self, key="k"):
        self._key = key
        self.outputs = []
        self._timer = self._TimerService()

    def get_current_key(self):
        return self._key

    def output(self, tag, value):
        self.outputs.append((tag, value))

    def timer_service(self):
        return self._timer


class _FakeGroupbyLLMClient:
    def __init__(self, payload: dict | list[dict]):
        if isinstance(payload, list):
            self._payloads = list(payload)
        else:
            self._payloads = [payload]

    async def call(self, prompt: str):
        if not self._payloads:
            raise RuntimeError("fake groupby LLM client exhausted payloads")
        payload = self._payloads.pop(0)
        return json.dumps(payload), {}

    def close(self):
        return None


class _ConcurrentFakeGroupbyLLMClient:
    def __init__(self, payloads: list[dict], delay_s: float = 0.02):
        self._payloads = list(payloads)
        self._delay_s = delay_s
        self.max_in_flight = 0
        self._in_flight = 0

    async def call(self, prompt: str):
        if not self._payloads:
            raise RuntimeError("fake groupby LLM client exhausted payloads")
        payload = self._payloads.pop(0)
        self._in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self._in_flight)
        try:
            await asyncio.sleep(self._delay_s)
            return json.dumps(payload), {}
        finally:
            self._in_flight -= 1

    def close(self):
        return None


def _split_outputs(items):
    main = []
    side = []
    for item in items:
        if isinstance(item, tuple) and len(item) == 2:
            side.append(item[1])
        else:
            main.append(item)
    return main, side


from pyflink.semantic_runtime.sem_spec import (
    SemSpec,
    TriggerPolicy,
    GroupbyQuerySpec,
    GroupbyScopePolicy,
    AggQuerySpec,
    AggScopePolicy,
    JoinQuerySpec,
    JoinScopePolicy,
    TopKQuerySpec,
    TopKScopePolicy,
)
from pyflink.semantic_runtime.runtime_config import RuntimeConfig, DefaultsConfig
from pyflink.semantic_runtime.operators.stateful.sem_groupby_pipeline import (
    build_sem_groupby_operator,
    resolve_groupby_execution_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_groupby_bounded import (
    WindowOwnedSemGroupbyFunction,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_pipeline import (
    build_sem_agg_operator,
    resolve_agg_execution_plan,
)
from pyflink.semantic_runtime.operators.stateful.sem_agg_bounded import (
    WindowOwnedSemAggFunction,
)
from pyflink.semantic_runtime.runtime.plans import (
    resolve_topk_lowering_plan,
    resolve_groupby_lowering_plan,
    resolve_agg_lowering_plan,
    resolve_join_lowering_plan,
)


class TestSemSpec:
    def test_defaults(self):
        spec = SemSpec()
        assert spec.backend == "llm"
        assert spec.output_mode == "json"
        assert spec.instruction == ""
        assert spec.schema is None
        assert spec.threshold is None

    def test_for_sem_map(self):
        spec = SemSpec.for_sem_map(
            "Extract sentiment", output_schema={"sentiment": str}
        )
        assert spec.instruction == "Extract sentiment"
        assert spec.backend == "llm"
        assert spec.output_mode == "json"
        assert spec.schema == {"sentiment": str}

    def test_for_sem_map_text_mode(self):
        spec = SemSpec.for_sem_map("Summarize", return_mode="text")
        assert spec.backend == "llm"
        assert spec.output_mode == "text"
        assert spec.schema is None

    def test_for_sem_filter(self):
        spec = SemSpec.for_sem_filter("Keep weather-related events", threshold=0.8)
        assert spec.instruction == "Keep weather-related events"
        assert spec.backend == "llm"
        assert spec.output_mode == "bool"
        assert spec.threshold == 0.8

    def test_for_sem_topk(self):
        spec = SemSpec.for_sem_topk(
            "Rerank by relevance", threshold=0.5
        )
        assert spec.instruction == "Rerank by relevance"
        assert spec.backend == "llm"
        assert spec.output_mode == "score"
        assert spec.threshold == 0.5

    def test_invalid_backend(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid backend"):
            SemSpec(backend="nonexistent")

    def test_invalid_output_mode(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid output_mode"):
            SemSpec(output_mode="unknown")

    def test_roundtrip(self):
        spec = SemSpec(instruction="test", output_mode="score")
        d = spec.to_dict()
        spec2 = SemSpec.from_dict(d)
        assert spec2.instruction == "test"
        assert spec2.backend == "llm"
        assert spec2.output_mode == "score"


class TestTriggerPolicy:
    def test_defaults(self):
        policy = TriggerPolicy()
        assert policy.mode == "on_event"
        assert policy.emit_intermediate is True
        assert policy.emit_final_on_scope_close is True

    def test_invalid_mode(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid trigger mode"):
            TriggerPolicy(mode="bogus")

    def test_roundtrip(self):
        policy = TriggerPolicy(
            mode="periodic",
            interval_ms=1000,
            idle_ms=500,
            count_threshold=10,
            emit_intermediate=False,
            emit_final_on_scope_close=True,
        )
        restored = TriggerPolicy.from_dict(policy.to_dict())
        assert restored.mode == "periodic"
        assert restored.interval_ms == 1000
        assert restored.idle_ms == 500
        assert restored.count_threshold == 10
        assert restored.emit_intermediate is False

    def test_periodic_requires_interval(self):
        import pytest
        with pytest.raises(ValueError, match="interval_ms"):
            TriggerPolicy(mode="periodic")

    def test_idle_flush_requires_idle_ms(self):
        import pytest
        with pytest.raises(ValueError, match="idle_ms"):
            TriggerPolicy(mode="idle_flush")

    def test_count_threshold_requires_positive_threshold(self):
        import pytest
        with pytest.raises(ValueError, match="count_threshold"):
            TriggerPolicy(mode="count_threshold", count_threshold=0)


class TestGroupbyQuerySpec:
    def test_defaults(self):
        spec = GroupbyQuerySpec()
        assert spec.semantic.output_mode == "label"
        assert spec.semantic.backend == "llm"
        assert spec.query_version == 1
        assert spec.trigger_policy.mode == "on_event"

    def test_simple_builder(self):
        spec = GroupbyQuerySpec.simple(
            "Group by semantic topic",
            ttl_seconds=600,
            max_groups_per_key=20,
        )
        assert spec.semantic.backend == "llm"
        assert spec.scope_policy.ttl_seconds == 600
        assert spec.scope_policy.max_groups_per_key == 20

    def test_roundtrip(self):
        spec = GroupbyQuerySpec(
            semantic=SemSpec(
                instruction="Group similar memories",
                backend="llm",
                output_mode="label",
            ),
            query_id="g1",
            query_version=2,
            trigger_policy=TriggerPolicy(mode="periodic", interval_ms=2000),
            maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=5000),
            scope_policy=GroupbyScopePolicy(
                ttl_seconds=1200,
                max_groups_per_key=12,
                window_kind="session",
                session_gap_ms=15000,
                boundary_flag="topic_shift",
            ),
        )
        restored = GroupbyQuerySpec.from_dict(spec.to_dict())
        assert restored.query_id == "g1"
        assert restored.query_version == 2
        assert restored.trigger_policy.mode == "periodic"
        assert restored.maintenance_trigger_policy.mode == "periodic"
        assert restored.scope_policy.max_groups_per_key == 12
        assert restored.scope_policy.window_kind == "session"
        assert restored.scope_policy.session_gap_ms == 15000
        assert restored.scope_policy.boundary_flag == "topic_shift"

    def test_invalid_scope_policy_session_gap(self):
        import pytest
        with pytest.raises(ValueError, match="session_gap_ms"):
            GroupbyScopePolicy(window_kind="session", session_gap_ms=0)

    def test_runtime_resolution_overrides_config(self):
        cfg = SemGroupbyConfig(
            max_groups_per_key=50,
            confidence_threshold=0.95,
            ttl_seconds=3600,
            new_group_creation_threshold=0.1,
        )
        spec = GroupbyQuerySpec(
            scope_policy=GroupbyScopePolicy(ttl_seconds=600, max_groups_per_key=12),
        )
        func = SemGroupbyFunction(cfg, spec)
        assert func._resolved_ttl_seconds == 600
        assert func._resolved_max_groups_per_key == 12
        assert func._resolved_assign_threshold == 0.95
        assert func._resolved_new_group_threshold == 0.1

    def test_invalid_internal_assignment_batch_size(self):
        with pytest.raises(ValueError, match="assignment_batch_size"):
            SemGroupbyConfig(assignment_batch_size=0)

    def test_runtime_assign_threshold_used_by_process_path(self):
        cfg = SemGroupbyConfig(
            max_groups_per_key=10,
            variant="rule",
            confidence_threshold=0.4,
            new_group_creation_threshold=0.1,
        )
        spec = GroupbyQuerySpec()
        func = SemGroupbyFunction(cfg, spec)
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "alpha beta", 100),
        })
        func._meta = _FakeValueState({"total_assigned": 1, "key": "k"})

        results = list(func.process_element(
            SemEvent(key="k", payload="alpha", seq_id=1).to_dict(),
            _FakeContext("k"),
        ))
        assert len(results) == 1
        assert results[0]["group_id"] == "g1"
        assert results[0]["source"] == "local"

    def test_embedding_assignment_method_uses_encoder_in_operator_owned_path(self):
        class _StubEncoder:
            def similarity(self, left: str, right: str) -> float:
                return 0.9 if "beta" in right else 0.1

        func = SemGroupbyFunction(
            SemGroupbyConfig(max_groups_per_key=10, variant="embedding"),
            GroupbyQuerySpec(),
        )
        func._encoder = _StubEncoder()
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "alpha topic", 100),
            "g2": _new_group_profile("g2", "beta topic", 100),
        })
        gid, score = func._local_assign(SemEvent(key="k", payload="anything", seq_id=1))
        assert gid == "g2"
        assert score == 0.9

    def test_execution_plan_auto_resolves_operator_owned(self):
        plan = resolve_groupby_execution_plan(GroupbyQuerySpec())
        assert plan.scope_source == "internal_scope"
        assert plan.persistence_policy == "persistent_across_scopes"

    def test_execution_plan_auto_window_snapshot_resolves_window_owned(self):
        plan = resolve_groupby_execution_plan(
            GroupbyQuerySpec(),
            input_kind="window_snapshot",
        )
        assert plan.scope_source == "external_window"
        assert plan.persistence_policy == "persistent_across_scopes"

    def test_builder_window_owned_returns_bounded_runtime(self):
        spec = GroupbyQuerySpec()
        op = build_sem_groupby_operator(
            SemGroupbyConfig(),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, SemGroupbyFunction)

    def test_builder_window_snapshot_reset_per_scope_returns_bounded_runtime(self):
        spec = GroupbyQuerySpec()
        op = build_sem_groupby_operator(
            SemGroupbyConfig(persistence_policy="reset_per_scope"),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, WindowOwnedSemGroupbyFunction)

    def test_builder_operator_owned_rejects_non_on_event_trigger(self):
        import pytest
        with pytest.raises(NotImplementedError, match="only .*on_event"):
            spec = GroupbyQuerySpec(
                trigger_policy=TriggerPolicy(mode="periodic", interval_ms=1000),
            )
            build_sem_groupby_operator(
                SemGroupbyConfig(),
                spec,
            )

    def test_builder_operator_owned_rejects_unsupported_maintenance(self):
        import pytest
        with pytest.raises(NotImplementedError, match="maintenance_trigger_policy.mode='periodic' or 'on_scope_close'"):
            spec = GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="idle_flush", idle_ms=1000),
            )
            build_sem_groupby_operator(
                SemGroupbyConfig(),
                spec,
            )

    def test_builder_operator_owned_rejects_on_scope_close_for_sliding(self):
        import pytest
        with pytest.raises(NotImplementedError, match="close-capable scopes"):
            spec = GroupbyQuerySpec(
                scope_policy=GroupbyScopePolicy(window_kind="sliding", window_size_ms=1000, slide_ms=250),
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
            )
            build_sem_groupby_operator(
                SemGroupbyConfig(),
                spec,
            )

    def test_builder_operator_owned_accepts_on_scope_close_for_semantic(self):
        spec = GroupbyQuerySpec(
            scope_policy=GroupbyScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
            maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
        )
        op = build_sem_groupby_operator(
            SemGroupbyConfig(),
            spec,
        )
        assert isinstance(op, SemGroupbyFunction)

    def test_builder_event_stream_defaults_to_operator_owned(self):
        spec = GroupbyQuerySpec()
        op = build_sem_groupby_operator(
            SemGroupbyConfig(),
            spec,
            input_kind="event_stream",
        )
        assert isinstance(op, SemGroupbyFunction)

    def test_builder_window_snapshot_default_returns_continuous_runtime(self):
        spec = GroupbyQuerySpec(
            maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=1000),
        )
        op = build_sem_groupby_operator(
            SemGroupbyConfig(),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, SemGroupbyFunction)

    def test_builder_window_snapshot_reset_per_scope_accepts_on_scope_close_maintenance(self):
        spec = GroupbyQuerySpec(
            maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
        )
        op = build_sem_groupby_operator(
            SemGroupbyConfig(persistence_policy="reset_per_scope"),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, WindowOwnedSemGroupbyFunction)

    def test_window_owned_groups_within_snapshot(self):
        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                confidence_threshold=0.5,
            ),
            GroupbyQuerySpec(),
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "count",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "alpha project budget", "seq_id": 1, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "alpha project timeline", "seq_id": 2, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "travel hotel booking", "seq_id": 3, "metadata": {}, "boundary_flags": {}},
            ],
        }
        outs = list(func.process_element(snapshot, _FakeContext("k")))
        main, side = _split_outputs(outs)
        assert len(main) == 3
        assert len(side) == 0
        first_gid = main[0]["group_id"]
        assert main[0]["source"] == "new_group"
        assert main[1]["group_id"] == first_gid
        assert main[2]["group_id"] != first_gid

    def test_window_owned_embedding_assignment_uses_encoder(self):
        class _StubEncoder:
            def similarity(self, left: str, right: str) -> float:
                return 0.9 if "beta" in right else 0.1

        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(max_groups_per_key=10, variant="embedding"),
            GroupbyQuerySpec(),
        )
        func._encoder = _StubEncoder()
        gid, score = func._local_assign(
            {
                "g1": _new_group_profile("g1", "alpha topic", 100),
                "g2": _new_group_profile("g2", "beta topic", 100),
            },
            SemEvent(key="k", payload="anything", seq_id=1),
        )
        assert gid == "g2"
        assert score == 0.9

    def test_window_owned_does_not_reuse_groups_across_snapshots(self):
        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                confidence_threshold=0.5,
            ),
            GroupbyQuerySpec(),
        )
        snapshot1 = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "count",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "alpha project budget", "seq_id": 1, "metadata": {}, "boundary_flags": {}},
            ],
        }
        snapshot2 = {
            "key": "k",
            "window_id": "w2",
            "trigger_reason": "count",
            "close_time_ms": 2000,
            "events": [
                {"key": "k", "payload": "alpha project update", "seq_id": 2, "metadata": {}, "boundary_flags": {}},
            ],
        }
        out1 = list(func.process_element(snapshot1, _FakeContext("k")))
        out2 = list(func.process_element(snapshot2, _FakeContext("k")))
        main1, _ = _split_outputs(out1)
        main2, _ = _split_outputs(out2)
        assert main1[0]["source"] == "new_group"
        assert main2[0]["source"] == "new_group"

    def test_window_owned_on_scope_close_maintenance_merges_similar_groups(self):
        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                confidence_threshold=0.85,
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
            ),
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "close",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "alpha budget planning", "seq_id": 1, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "alpha budget roadmap", "seq_id": 2, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "travel hotel booking", "seq_id": 3, "metadata": {}, "boundary_flags": {}},
            ],
        }
        outs = list(func.process_element(snapshot, _FakeContext("k")))
        main, _side = _split_outputs(outs)
        group_ids = [row["group_id"] for row in main]
        assert len(set(group_ids)) == 2
        assert group_ids[0] == group_ids[1]
        assert group_ids[2] != group_ids[0]

    def test_operator_owned_llm_new_group_assigns_sync(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(max_groups_per_key=10, variant="llm_basic"),
            GroupbyQuerySpec(),
        )
        func._group_profiles = _FakeMapState({})
        func._meta = _FakeValueState({"total_assigned": 0, "key": "k"})
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _FakeGroupbyLLMClient(
            {
                "assignments": [
                    {
                        "event_seq_id": 1,
                        "decision": "new",
                        "group_id": "",
                        "label": "brand new topic",
                        "confidence": 0.9,
                        "reason": "new group",
                    }
                ]
            }
        )
        outs = list(
            func.process_element(
                SemEvent(key="k", payload="brand new topic", seq_id=1).to_dict(),
                _FakeContext("k"),
            )
        )
        main, side = _split_outputs(outs)
        assert len(side) == 0
        assert len(main) == 1
        assert main[0]["source"] == "llm_basic"
        assert len(list(func._group_profiles.keys())) == 1

    def test_window_owned_llm_new_group_assigns_sync(self):
        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(max_groups_per_key=10, variant="llm_basic", assignment_batch_size=3),
            GroupbyQuerySpec(),
            llm_config=object(),
        )
        func._client = _FakeGroupbyLLMClient(
            {
                "assignments": [
                    {
                        "event_seq_id": 1,
                        "decision": "new",
                        "group_id": "",
                        "label": "brand new topic",
                        "confidence": 0.8,
                        "reason": "new group",
                    }
                ]
            }
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "close",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "brand new topic", "seq_id": 1, "metadata": {}, "boundary_flags": {}},
            ],
        }
        outs = list(func.process_element(snapshot, _FakeContext("k")))
        main, side = _split_outputs(outs)
        assert len(side) == 0
        assert len(main) == 1
        assert main[0]["source"] == "llm_basic"

    def test_operator_owned_llm_assignment_batch_size_batches_events(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_basic",
                assignment_batch_size=2,
            ),
            GroupbyQuerySpec(),
        )
        func._group_profiles = _FakeMapState({})
        func._meta = _FakeValueState({"total_assigned": 0, "key": "k", "scope_epoch": 0})
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _FakeGroupbyLLMClient(
            {
                "assignments": [
                    {
                        "event_seq_id": 1,
                        "decision": "new",
                        "group_id": "",
                        "label": "project kickoff",
                        "confidence": 0.9,
                        "reason": "new group",
                    },
                    {
                        "event_seq_id": 2,
                        "decision": "new",
                        "group_id": "",
                        "label": "travel hotel",
                        "confidence": 0.9,
                        "reason": "new group",
                    },
                ]
            }
        )

        first = list(
            func.process_element(
                SemEvent(key="k", payload="project kickoff", seq_id=1).to_dict(),
                _FakeContext("k"),
            )
        )
        first_main, first_side = _split_outputs(first)
        assert first_main == []
        assert first_side == []

        second = list(
            func.process_element(
                SemEvent(key="k", payload="travel hotel", seq_id=2).to_dict(),
                _FakeContext("k"),
            )
        )
        second_main, second_side = _split_outputs(second)
        assert second_side == []
        assert len(second_main) == 2
        assert {row["event_seq_id"] for row in second_main} == {1, 2}

    def test_external_window_llm_assignment_dispatches_chunks_concurrently(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_basic",
                assignment_batch_size=2,
            ),
            GroupbyQuerySpec(
                scope_policy=GroupbyScopePolicy(window_kind="tumbling", window_size_ms=1000),
            ),
            scope_source="external_window",
        )
        func._group_profiles = _FakeMapState({})
        func._scope_progress = _FakeMapState({})
        func._meta = _FakeValueState({"total_assigned": 0, "key": "k", "scope_epoch": 0})
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _ConcurrentFakeGroupbyLLMClient(
            payloads=[
                {
                    "assignments": [
                        {
                            "event_seq_id": 1,
                            "decision": "new",
                            "group_id": "",
                            "label": "alpha one",
                            "confidence": 0.9,
                            "reason": "new group",
                        },
                        {
                            "event_seq_id": 2,
                            "decision": "new",
                            "group_id": "",
                            "label": "alpha two",
                            "confidence": 0.9,
                            "reason": "new group",
                        },
                    ]
                },
                {
                    "assignments": [
                        {
                            "event_seq_id": 3,
                            "decision": "new",
                            "group_id": "",
                            "label": "beta one",
                            "confidence": 0.9,
                            "reason": "new group",
                        },
                        {
                            "event_seq_id": 4,
                            "decision": "new",
                            "group_id": "",
                            "label": "beta two",
                            "confidence": 0.9,
                            "reason": "new group",
                        },
                    ]
                },
            ],
            delay_s=0.02,
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "fire",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "alpha one", "seq_id": 1, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "alpha two", "seq_id": 2, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "beta one", "seq_id": 3, "metadata": {}, "boundary_flags": {}},
                {"key": "k", "payload": "beta two", "seq_id": 4, "metadata": {}, "boundary_flags": {}},
            ],
        }

        outs = list(func.process_element(snapshot, _FakeContext("k")))
        main, side = _split_outputs(outs)
        assert side == []
        assert len(main) == 4
        assert func._client.max_in_flight >= 2

    def test_operator_owned_llm_scope_close_flushes_pending_chunk(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_basic",
                assignment_batch_size=5,
                persistence_policy="reset_per_scope",
            ),
            GroupbyQuerySpec(
                trigger_policy=TriggerPolicy(mode="on_event"),
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
                scope_policy=GroupbyScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g_existing": _new_group_profile("g_existing", "project planning", 100),
        })
        func._meta = _FakeValueState({"total_assigned": 0, "key": "k", "scope_epoch": 0})
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _FakeGroupbyLLMClient(
            {
                "assignments": [
                    {
                        "event_seq_id": 1,
                        "decision": "existing",
                        "group_id": "g_existing",
                        "label": "",
                        "confidence": 0.9,
                        "reason": "same group",
                    }
                ]
            }
        )

        outs = list(
            func.process_element(
                SemEvent(
                    key="k",
                    payload="project budget",
                    seq_id=1,
                    boundary_flags={"topic_shift": True},
                ).to_dict(),
                _FakeContext("k"),
            )
        )
        main, side = _split_outputs(outs)
        assert side == []
        assert len(main) == 1
        assert main[0]["group_id"] == "g_existing"
        assert len(list(func._group_profiles.keys())) == 0
        assert func._meta.value()["scope_epoch"] == 1

    def test_operator_owned_periodic_maintenance_updates_meta(self):
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

        func = SemGroupbyFunction(
            SemGroupbyConfig(max_groups_per_key=10, variant="rule"),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "alpha topic", 100),
        })
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _Ctx()
        list(func.process_element(
            SemEvent(key="k", payload="alpha budget update", seq_id=1).to_dict(),
            ctx,
        ))
        meta = func._meta.value()
        assert "_timer_recompute" in meta
        fire_at = meta["_timer_recompute"]
        func.on_timer(fire_at, ctx)
        updated = func._meta.value()
        assert updated["refine_count"] == 1
        assert updated["last_refine_ms"] == fire_at

    def test_operator_owned_periodic_maintenance_merges_similar_groups(self):
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

        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                confidence_threshold=0.85,
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g1": {
                **_new_group_profile("g1", "alpha budget planning", 100),
                "event_count": 3,
            },
            "g2": {
                **_new_group_profile("g2", "alpha budget roadmap", 200),
                "event_count": 1,
            },
            "g3": {
                **_new_group_profile("g3", "travel hotel booking", 300),
                "event_count": 2,
            },
        })
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _Ctx()
        list(func.process_element(
            SemEvent(key="k", payload="alpha budget planning", seq_id=1).to_dict(),
            ctx,
        ))
        fire_at = func._meta.value()["_timer_recompute"]
        func.on_timer(fire_at, ctx)

        keys = set(func._group_profiles.keys())
        assert len(keys) == 2
        assert "g3" in keys
        survivor_id = "g1" if "g1" in keys else "g2"
        survivor = func._group_profiles.get(survivor_id)
        assert survivor["event_count"] == 5
        assert len(survivor["_merged_from"]) == 1
        assert func._meta.value()["last_merge_count"] == 1

    def test_operator_owned_periodic_maintenance_does_not_merge_dissimilar_groups(self):
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

        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                confidence_threshold=0.85,
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g1": _new_group_profile("g1", "alpha budget planning", 100),
            "g2": _new_group_profile("g2", "travel hotel booking", 200),
        })
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _Ctx()
        list(func.process_element(
            SemEvent(key="k", payload="alpha budget planning", seq_id=1).to_dict(),
            ctx,
        ))
        fire_at = func._meta.value()["_timer_recompute"]
        func.on_timer(fire_at, ctx)

        assert set(func._group_profiles.keys()) == {"g1", "g2"}
        assert func._meta.value()["last_merge_count"] == 0

    def test_operator_owned_refresh_labels_during_maintenance(self):
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

        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_basic",
                refresh_labels_during_maintenance=True,
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g1": {
                **_new_group_profile("g1", "stale label", 100),
                "summary": "alpha budget roadmap milestone",
                "event_count": 3,
            },
        })
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _FakeGroupbyLLMClient(
            {
                "assignments": [
                    {
                        "event_seq_id": 1,
                        "decision": "existing",
                        "group_id": "g1",
                        "label": "",
                        "confidence": 0.95,
                        "reason": "same group",
                    }
                ]
            }
        )

        ctx = _Ctx()
        list(func.process_element(
            SemEvent(key="k", payload="alpha update", seq_id=1).to_dict(),
            ctx,
        ))
        fire_at = func._meta.value()["_timer_recompute"]
        func.on_timer(fire_at, ctx)

        profile = func._group_profiles.get("g1")
        assert profile["label"] != "stale label"
        assert "alpha" in profile["label"].lower()

    def test_operator_owned_llm_refine_periodic_applies_merge_and_rename(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_refine",
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({
            "g_existing": {
                **_new_group_profile("g_existing", "old alpha", 100),
                "summary": "alpha budget planning",
                "examples": ["alpha budget planning", "alpha roadmap"],
                "event_count": 2,
            },
            "g_merge": {
                **_new_group_profile("g_merge", "travel old", 200),
                "summary": "travel hotel booking",
                "examples": ["travel hotel booking", "travel itinerary"],
                "event_count": 2,
            },
            "g_new_1": {
                **_new_group_profile("g_new_1", "fresh travel", 300),
                "summary": "travel reimbursement",
                "examples": ["travel reimbursement"],
                "event_count": 1,
            },
        })
        func._meta = _FakeValueState({"key": "k", "scope_epoch": 0})
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")
        func._client = _FakeGroupbyLLMClient(
            {
                "renames": [{"group_id": "g_existing", "label": "alpha refined"}],
                "merges": [
                    {
                        "target_group_id": "g_merge",
                        "source_group_ids": ["g_merge", "g_new_1"],
                        "label": "travel merged",
                    }
                ],
                "splits": [],
            }
        )
        meta = func._meta.value()
        func._run_maintenance(meta, now_ms=1000)
        func._meta.update(meta)

        assert set(func._group_profiles.keys()) == {"g_existing", "g_merge"}
        assert func._group_profiles.get("g_existing")["label"] == "alpha refined"
        merged = func._group_profiles.get("g_merge")
        assert merged["label"] == "travel merged"
        assert merged["event_count"] == 3
        assert func._meta.value()["last_merge_count"] == 1
        assert func._meta.value()["last_rename_count"] == 1

    def test_window_owned_llm_refine_scope_close_applies_split(self):
        func = WindowOwnedSemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="llm_refine",
            ),
            GroupbyQuerySpec(
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
            ),
        )
        func._client = _FakeGroupbyLLMClient(
            {
                "renames": [],
                "merges": [],
                "splits": [
                    {
                        "group_id": "g_new_1",
                        "children": [
                            {
                                "label": "alpha budget",
                                "examples": ["alpha budget planning"],
                            },
                            {
                                "label": "alpha roadmap",
                                "examples": ["alpha roadmap next quarter"],
                            },
                        ],
                    }
                ],
            }
        )
        func._new_group_id = lambda: "g_new_2"
        groups = {
            "g_new_1": {
                **_new_group_profile("g_new_1", "mixed alpha", 100),
                "summary": "alpha budget planning\nalpha roadmap next quarter",
                "examples": ["alpha budget planning", "alpha roadmap next quarter"],
                "event_count": 2,
            },
        }
        func._apply_llm_refine(groups)
        assert set(groups.keys()) == {"g_new_1", "g_new_2"}
        assert groups["g_new_1"]["label"] == "alpha budget"
        assert groups["g_new_2"]["label"] == "alpha roadmap"

    def test_external_window_persistent_across_scopes_processes_only_new_scope_events(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
            ),
            GroupbyQuerySpec(),
        )
        func._group_profiles = _FakeMapState({})
        func._pending_events = _FakeListState()
        func._scope_progress = _FakeMapState({})
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _FakeContext("k")
        first_snapshot = {
            "window_id": "w1",
            "key": "k",
            "trigger_reason": "on_event",
            "events": [
                SemEvent(key="k", payload="alpha planning", seq_id=1).to_dict(),
                SemEvent(key="k", payload="alpha roadmap", seq_id=2).to_dict(),
            ],
        }
        second_snapshot = {
            "window_id": "w1",
            "key": "k",
            "trigger_reason": "on_event",
            "events": [
                SemEvent(key="k", payload="alpha planning", seq_id=1).to_dict(),
                SemEvent(key="k", payload="alpha roadmap", seq_id=2).to_dict(),
                SemEvent(key="k", payload="alpha budget", seq_id=3).to_dict(),
            ],
        }

        first_rows = list(func.process_element(first_snapshot, ctx))
        second_rows = list(func.process_element(second_snapshot, ctx))

        assert len(first_rows) == 2
        assert len(second_rows) == 1
        assert second_rows[0]["event_seq_id"] == 3
        progress = func._scope_progress.get("w1")
        assert progress["seen_event_seq_ids"] == [1, 2, 3]
        assert func._meta.value()["total_assigned"] == 3

    def test_operator_owned_semantic_on_scope_close_resets_group_state(self):
        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                persistence_policy="reset_per_scope",
            ),
            GroupbyQuerySpec(
                trigger_policy=TriggerPolicy(mode="on_event"),
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
                scope_policy=GroupbyScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
            ),
        )
        func._group_profiles = _FakeMapState({})
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _FakeContext("k")
        list(func.process_element(
            SemEvent(key="k", payload="alpha planning", seq_id=1).to_dict(),
            ctx,
        ))
        assert len(list(func._group_profiles.keys())) == 1

        list(func.process_element(
            SemEvent(
                key="k",
                payload="travel booking",
                seq_id=2,
                boundary_flags={"topic_shift": True},
            ).to_dict(),
            ctx,
        ))
        assert len(list(func._group_profiles.keys())) == 0
        meta = func._meta.value()
        assert meta["last_scope_reset_reason"] == "semantic_boundary"
        assert meta["scope_epoch"] == 1

    def test_operator_owned_session_on_scope_close_timer_resets_group_state(self):
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

        func = SemGroupbyFunction(
            SemGroupbyConfig(
                max_groups_per_key=10,
                variant="rule",
                persistence_policy="reset_per_scope",
            ),
            GroupbyQuerySpec(
                trigger_policy=TriggerPolicy(mode="on_event"),
                maintenance_trigger_policy=TriggerPolicy(mode="on_scope_close"),
                scope_policy=GroupbyScopePolicy(window_kind="session", session_gap_ms=50),
            ),
        )
        func._group_profiles = _FakeMapState({})
        func._meta = _FakeValueState(None)
        func._metrics = StatefulOperatorMetrics.noop("sem_groupby")

        ctx = _Ctx()
        list(func.process_element(
            SemEvent(key="k", payload="alpha planning", seq_id=1).to_dict(),
            ctx,
        ))
        meta = func._meta.value()
        fire_at = meta["_timer_flush"]
        assert len(list(func._group_profiles.keys())) == 1

        func.on_timer(fire_at, ctx)
        assert len(list(func._group_profiles.keys())) == 0
        updated = func._meta.value()
        assert updated["last_scope_reset_reason"] == "session_gap"
        assert updated["scope_epoch"] == 1


class TestSemLoweringPlans:
    def test_topk_pointwise_lowers_to_score_plus_topn(self):
        plan = resolve_topk_lowering_plan(TopKQuerySpec(ranking_method="pointwise"))
        assert plan.lowering_kind == "derived_attribute_then_classical"
        assert plan.classical_operator == "topn"
        assert plan.derived_attribute is not None
        assert plan.derived_attribute.attribute_kind == "score"
        assert plan.derived_attribute.output_field == "score"

    def test_topk_window_snapshot_default_stays_native(self):
        plan = resolve_topk_lowering_plan(
            TopKQuerySpec(ranking_method="pointwise"),
            input_kind="window_snapshot",
        )
        assert plan.lowering_kind == "native_runtime"
        assert plan.derived_attribute is None

    def test_topk_window_reset_per_scope_lowers_to_score_plus_topn(self):
        plan = resolve_topk_lowering_plan(
            TopKQuerySpec(ranking_method="pointwise"),
            input_kind="window_snapshot",
            persistence_policy="reset_per_scope",
        )
        assert plan.lowering_kind == "derived_attribute_then_classical"
        assert plan.classical_operator == "topn"
        assert plan.derived_attribute is not None

    def test_topk_contextual_stays_native(self):
        plan = resolve_topk_lowering_plan(TopKQuerySpec(ranking_method="pairwise"))
        assert plan.lowering_kind == "native_runtime"
        assert plan.derived_attribute is None

    def test_groupby_window_snapshot_default_stays_native(self):
        plan = resolve_groupby_lowering_plan(
            GroupbyQuerySpec(),
            input_kind="window_snapshot",
        )
        assert plan.lowering_kind == "native_runtime"
        assert plan.derived_attribute is None

    def test_groupby_window_reset_per_scope_lowers_to_label_plus_groupby(self):
        plan = resolve_groupby_lowering_plan(
            GroupbyQuerySpec(),
            input_kind="window_snapshot",
            persistence_policy="reset_per_scope",
        )
        assert plan.lowering_kind == "derived_attribute_then_classical"
        assert plan.classical_operator == "groupby"
        assert plan.derived_attribute is not None
        assert plan.derived_attribute.attribute_kind == "label"
        assert plan.derived_attribute.output_field == "group_id"

    def test_groupby_operator_owned_stays_native(self):
        plan = resolve_groupby_lowering_plan(
            GroupbyQuerySpec(),
            input_kind="event_stream",
        )
        assert plan.lowering_kind == "native_runtime"
        assert plan.derived_attribute is None

    def test_agg_currently_stays_native(self):
        plan = resolve_agg_lowering_plan(AggQuerySpec(agg_method="summarize"))
        assert plan.lowering_kind == "native_runtime"
        assert plan.classical_operator is None

    def test_join_lowers_to_match_plus_join(self):
        plan = resolve_join_lowering_plan(JoinQuerySpec())
        assert plan.lowering_kind == "derived_attribute_then_classical"
        assert plan.classical_operator == "join/filter"
        assert plan.derived_attribute is not None
        assert plan.derived_attribute.attribute_kind == "match"


class TestAggQuerySpec:
    def test_defaults(self):
        spec = AggQuerySpec()
        assert spec.semantic.output_mode == "summary"
        assert spec.agg_method == "algebraic"
        assert spec.trigger_policy.mode == "on_event"

    def test_simple_builder(self):
        spec = AggQuerySpec.simple(
            "Summarize memory state",
            agg_method="summarize",
            ttl_seconds=600,
            max_buffer_events=50,
            flush_interval_ms=10000,
        )
        assert spec.semantic.backend == "llm"
        assert spec.agg_method == "summarize"
        assert spec.scope_policy.max_buffer_events == 50

    def test_invalid_agg_method(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid agg_method"):
            AggQuerySpec(agg_method="bogus")

    def test_roundtrip(self):
        spec = AggQuerySpec(
            semantic=SemSpec(
                instruction="Compress memory",
                backend="llm",
                output_mode="summary",
            ),
            query_id="agg1",
            query_version=3,
            agg_method="compressive",
            trigger_policy=TriggerPolicy(mode="count_threshold", count_threshold=16),
            scope_policy=AggScopePolicy(
                ttl_seconds=1800,
                max_buffer_events=16,
                flush_interval_ms=5000,
            ),
        )
        restored = AggQuerySpec.from_dict(spec.to_dict())
        assert restored.query_id == "agg1"
        assert restored.query_version == 3
        assert restored.agg_method == "compressive"
        assert restored.trigger_policy.mode == "count_threshold"
        assert restored.scope_policy.flush_interval_ms == 5000

    def test_roundtrip_with_close_capable_scope(self):
        spec = AggQuerySpec(
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=AggScopePolicy(
                ttl_seconds=300,
                window_kind="session",
                session_gap_ms=1000,
            ),
        )
        restored = AggQuerySpec.from_dict(spec.to_dict())
        assert restored.trigger_policy.mode == "on_scope_close"
        assert restored.scope_policy.window_kind == "session"
        assert restored.scope_policy.session_gap_ms == 1000

    def test_execution_plan_auto_resolves_operator_owned(self):
        plan = resolve_agg_execution_plan(AggQuerySpec())
        assert plan.scope_source == "internal_scope"
        assert plan.persistence_policy == "persistent_across_scopes"

    def test_execution_plan_auto_window_snapshot_resolves_window_owned(self):
        plan = resolve_agg_execution_plan(
            AggQuerySpec(),
            input_kind="window_snapshot",
        )
        assert plan.scope_source == "external_window"
        assert plan.persistence_policy == "persistent_across_scopes"

    def test_builder_window_snapshot_defaults_to_persistent_runtime(self):
        spec = AggQuerySpec()
        op = build_sem_agg_operator(
            SemAggConfig(),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, SemAggFunction)
        assert op._scope_source == "external_window"

    def test_builder_window_reset_per_scope_returns_bounded_runtime(self):
        spec = AggQuerySpec()
        op = build_sem_agg_operator(
            SemAggConfig(persistence_policy="reset_per_scope"),
            spec,
            input_kind="window_snapshot",
        )
        assert isinstance(op, WindowOwnedSemAggFunction)

    def test_builder_event_stream_defaults_to_operator_owned(self):
        spec = AggQuerySpec()
        op = build_sem_agg_operator(
            SemAggConfig(),
            spec,
            input_kind="event_stream",
        )
        assert isinstance(op, SemAggFunction)

    def test_builder_operator_owned_rejects_on_scope_close(self):
        import pytest
        with pytest.raises(NotImplementedError, match="on_scope_close"):
            build_sem_agg_operator(
                SemAggConfig(),
                AggQuerySpec(
                    trigger_policy=TriggerPolicy(mode="on_scope_close"),
                    scope_policy=AggScopePolicy(window_kind="sliding", window_size_ms=1000, slide_ms=250),
                ),
                input_kind="event_stream",
            )

    def test_builder_operator_owned_accepts_close_capable_scope(self):
        op = build_sem_agg_operator(
            SemAggConfig(),
            AggQuerySpec(
                trigger_policy=TriggerPolicy(mode="on_scope_close"),
                scope_policy=AggScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
            ),
            input_kind="event_stream",
        )
        assert isinstance(op, SemAggFunction)

    def test_window_owned_algebraic_aggregates_snapshot(self):
        def sum_reduce(a, b):
            return {"key": a.get("key", "k"), "total": a.get("total", 0) + b.get("total", 0)}

        func = WindowOwnedSemAggFunction(
            SemAggConfig(mode="algebraic", reduce_fn=sum_reduce),
            AggQuerySpec(agg_method="algebraic"),
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "close",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "a", "total": 3, "seq_id": 1},
                {"key": "k", "payload": "b", "total": 5, "seq_id": 2},
            ],
        }
        outs = list(func.process_element(snapshot, _FakeContext("k")))
        assert len(outs) == 1
        assert outs[0]["mode"] == "algebraic_window"
        assert outs[0]["aggregate"]["total"] == 8

    def test_external_window_persistent_algebraic_replaces_scope_contribution(self):
        def sum_reduce(a, b):
            return {"key": a.get("key", "k"), "total": a.get("total", 0) + b.get("total", 0)}

        func = SemAggFunction(
            SemAggConfig(mode="algebraic", reduce_fn=sum_reduce),
            AggQuerySpec(agg_method="algebraic"),
            scope_source="external_window",
        )
        func._buffer = _FakeListState()
        func._agg_value = _FakeValueState(None)
        func._meta = _FakeValueState(None)
        func._scope_contributions = _FakeMapState()

        first = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "close",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "a", "total": 3, "seq_id": 1},
                {"key": "k", "payload": "b", "total": 5, "seq_id": 2},
            ],
        }
        first_outs = list(func.process_element(first, _FakeContext("k")))
        assert len(first_outs) == 1
        assert first_outs[0]["mode"] == "algebraic_scope_fire"
        assert first_outs[0]["aggregate"]["total"] == 8

        second = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "early_fire",
            "close_time_ms": 1200,
            "events": [
                {"key": "k", "payload": "a", "total": 3, "seq_id": 1},
                {"key": "k", "payload": "b", "total": 5, "seq_id": 2},
                {"key": "k", "payload": "c", "total": 7, "seq_id": 3},
            ],
        }
        second_outs = list(func.process_element(second, _FakeContext("k")))
        assert len(second_outs) == 1
        assert second_outs[0]["aggregate"]["total"] == 15
        assert func._meta.value()["event_count"] == 3

    def test_window_owned_summarize_emits_async_work(self):
        func = WindowOwnedSemAggFunction(
            SemAggConfig(mode="summarize", max_buffer_events=10),
            AggQuerySpec(agg_method="summarize"),
        )
        snapshot = {
            "key": "k",
            "window_id": "w1",
            "trigger_reason": "close",
            "close_time_ms": 1000,
            "events": [
                {"key": "k", "payload": "alpha", "seq_id": 1},
                {"key": "k", "payload": "beta", "seq_id": 2},
            ],
        }
        outs = list(func.process_element(snapshot, _FakeContext("k")))
        main, side = _split_outputs(outs)
        assert len(main) == 0
        assert len(side) == 1
        assert side[0]["task_type"] == "summarize"

    def test_runtime_resolution_overrides_config(self):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=50, flush_interval_ms=5000, ttl_seconds=3600)
        spec = AggQuerySpec(
            agg_method="compressive",
            scope_policy=AggScopePolicy(ttl_seconds=600, max_buffer_events=12, flush_interval_ms=1000),
        )
        func = SemAggFunction(cfg, spec)
        assert func._resolved_mode == "compressive"
        assert func._resolved_ttl_seconds == 600
        assert func._resolved_max_buffer_events == 12
        assert func._resolved_flush_interval_ms == 1000

    def test_builder_without_query_spec_preserves_config_mode(self):
        op = build_sem_agg_operator(
            SemAggConfig(mode="summarize"),
            None,
            input_kind="event_stream",
        )
        assert isinstance(op, SemAggFunction)
        assert op._resolved_mode == "summarize"

    def test_builder_window_persistent_summarize_rejects_unsupported_path(self):
        op = build_sem_agg_operator(
            SemAggConfig(mode="summarize"),
            AggQuerySpec(agg_method="summarize"),
            input_kind="window_snapshot",
        )
        assert isinstance(op, SemAggFunction)


class TestSemAggTriggerRuntime:
    class _CaptureContext:
        def __init__(self, key="k"):
            self._key = key
            self.timers = []

            class _TimerService:
                def __init__(self, sink):
                    self._sink = sink

                def register_processing_time_timer(self, ts):
                    self._sink.append(("proc", ts))

                def register_event_time_timer(self, ts):
                    self._sink.append(("event", ts))

            self._timer = _TimerService(self.timers)

        def get_current_key(self):
            return self._key

        def timer_service(self):
            return self._timer

        def output(self, tag, value):
            pass

    def _make_func(self, cfg, spec):
        func = SemAggFunction(
            cfg,
            spec,
            llm_config=LLMClientConfig(
                backend="mock",
                mock_delay_s=0.0,
                mock_response='{"summary":"summary"}',
            ),
        )
        func._buffer = _FakeListState()
        func._agg_value = _FakeValueState(None)
        func._meta = _FakeValueState(None)
        func._scope_contributions = _FakeMapState()
        func._scope_progress = _FakeMapState()
        return func

    def test_algebraic_periodic_emits_only_on_timer(self):
        def sum_reduce(a, b):
            return {"key": a.get("key", "k"), "total": a.get("total", 0) + b.get("total", 0)}

        cfg = SemAggConfig(mode="algebraic", reduce_fn=sum_reduce)
        spec = AggQuerySpec(
            agg_method="algebraic",
            trigger_policy=TriggerPolicy(mode="periodic", interval_ms=50),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs = list(func.process_element({"key": "k", "payload": "alpha", "seq_id": 1, "total": 3}, ctx))
        assert outs == []
        fire_at = func._meta.value()[encode_timer_key(TimerCategory.FLUSH)]
        timer_outs = list(func.on_timer(fire_at, ctx))
        assert len(timer_outs) == 1
        assert timer_outs[0]["mode"] == "algebraic_periodic"
        assert timer_outs[0]["aggregate"]["total"] == 3

    def test_algebraic_idle_flush_emits_on_timer(self):
        cfg = SemAggConfig(mode="algebraic")
        spec = AggQuerySpec(
            agg_method="algebraic",
            trigger_policy=TriggerPolicy(mode="idle_flush", idle_ms=25),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs = list(func.process_element({"key": "k", "payload": "alpha", "seq_id": 1}, ctx))
        assert outs == []
        fire_at = func._meta.value()[encode_timer_key(TimerCategory.FLUSH)]
        timer_outs = list(func.on_timer(fire_at, ctx))
        assert len(timer_outs) == 1
        assert timer_outs[0]["mode"] == "algebraic_idle_flush"

    def test_summarize_count_threshold_uses_trigger_threshold(self):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=10)
        spec = AggQuerySpec(
            agg_method="summarize",
            trigger_policy=TriggerPolicy(mode="count_threshold", count_threshold=2),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs1 = list(func.process_element({"key": "k", "payload": "a", "seq_id": 1}, ctx))
        assert outs1 == []

        outs2 = list(func.process_element({"key": "k", "payload": "b", "seq_id": 2}, ctx))
        assert outs2 == []
        time.sleep(0.01)
        recompute_at = func._meta.value()[encode_timer_key(TimerCategory.RECOMPUTE)]
        timer_outs = list(func.on_timer(recompute_at, ctx))
        assert len(timer_outs) == 1
        assert timer_outs[0]["mode"] == "summarize_async"

    def test_summarize_result_preserves_post_request_events(self):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=10)
        spec = AggQuerySpec(
            agg_method="summarize",
            trigger_policy=TriggerPolicy(mode="on_event"),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        first = list(func.process_element({"key": "k", "payload": "a", "seq_id": 1}, ctx))
        assert first == []

        second = list(func.process_element({"key": "k", "payload": "b", "seq_id": 2}, ctx))
        assert second == []
        assert len(list(func._buffer.get())) == 1

        time.sleep(0.01)
        recompute_at = func._meta.value()[encode_timer_key(TimerCategory.RECOMPUTE)]
        merged = list(func.on_timer(recompute_at, ctx))
        assert len(merged) == 1
        assert merged[0]["mode"] == "summarize_async"
        assert len(list(func._buffer.get())) == 0
        assert func._meta.value()["pending_summarize"] is True

    def test_algebraic_semantic_on_scope_close_emits_final_and_resets(self):
        cfg = SemAggConfig(mode="algebraic")
        spec = AggQuerySpec(
            agg_method="algebraic",
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=AggScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs = list(func.process_element({
            "key": "k",
            "payload": "alpha",
            "seq_id": 1,
            "value": 1,
            "boundary_flags": {"topic_shift": True},
        }, ctx))
        assert len(outs) == 1
        assert outs[0]["mode"] == "algebraic_semantic_boundary"
        assert func._meta.value()["scope_epoch"] == 1
        assert func._agg_value.value() is None

    def test_summarize_semantic_on_scope_close_emits_work_and_final_result(self):
        cfg = SemAggConfig(mode="summarize", max_buffer_events=10)
        spec = AggQuerySpec(
            agg_method="summarize",
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=AggScopePolicy(window_kind="semantic", boundary_flag="topic_shift"),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs = list(func.process_element({
            "key": "k",
            "payload": "alpha",
            "seq_id": 1,
            "boundary_flags": {"topic_shift": True},
        }, ctx))
        assert outs == []
        assert func._meta.value()["scope_epoch"] == 1
        assert len(list(func._buffer.get())) == 0

        time.sleep(0.01)
        recompute_at = func._meta.value()[encode_timer_key(TimerCategory.RECOMPUTE)]
        merged = list(func.on_timer(recompute_at, ctx))
        assert len(merged) == 1
        assert merged[0]["mode"] == "summarize_scope_close_async"
        assert merged[0]["scope_epoch"] == 0

    def test_algebraic_session_on_scope_close_timer_emits_and_resets(self):
        cfg = SemAggConfig(mode="algebraic")
        spec = AggQuerySpec(
            agg_method="algebraic",
            trigger_policy=TriggerPolicy(mode="on_scope_close"),
            scope_policy=AggScopePolicy(window_kind="session", session_gap_ms=50),
        )
        func = self._make_func(cfg, spec)
        ctx = self._CaptureContext("k")

        outs = list(func.process_element({
            "key": "k",
            "payload": "alpha",
            "seq_id": 1,
        }, ctx))
        assert outs == []
        fire_at = func._meta.value()[encode_timer_key(TimerCategory.FLUSH)]
        timer_outs = list(func.on_timer(fire_at, ctx))
        assert len(timer_outs) == 1
        assert timer_outs[0]["mode"] == "algebraic_session_gap"
        assert func._meta.value()["scope_epoch"] == 1


class TestJoinQuerySpec:
    def test_defaults(self):
        spec = JoinQuerySpec()
        assert spec.semantic.output_mode == "bool"
        assert spec.join_type == "inner"
        assert spec.pairing_method == "candidate_pruned"
        assert spec.trigger_policy.mode == "on_event"

    def test_simple_builder(self):
        spec = JoinQuerySpec.simple(
            "Join request with candidate documents",
            backend="embedding",
            pairing_method="embedding_prefilter",
            ttl_seconds=900,
            max_left_buffer=10,
            max_right_buffer=200,
        )
        assert spec.semantic.backend == "embedding"
        assert spec.join_type == "inner"
        assert spec.pairing_method == "embedding_prefilter"
        assert spec.scope_policy.max_right_buffer == 200

    def test_invalid_join_type(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid join_type"):
            JoinQuerySpec(join_type="cross")

    def test_invalid_pairing_method(self):
        import pytest
        with pytest.raises(ValueError, match="Invalid pairing_method"):
            JoinQuerySpec(pairing_method="bogus")

    def test_roundtrip(self):
        spec = JoinQuerySpec(
            semantic=SemSpec(
                instruction="Judge semantic join eligibility",
                output_mode="bool",
            ),
            query_id="join1",
            query_version=4,
            join_type="left",
            pairing_method="blocking",
            trigger_policy=TriggerPolicy(mode="idle_flush", idle_ms=800),
            scope_policy=JoinScopePolicy(
                ttl_seconds=600,
                max_left_buffer=5,
                max_right_buffer=25,
                window_kind="sliding",
                window_size_ms=10000,
                slide_ms=2000,
            ),
        )
        restored = JoinQuerySpec.from_dict(spec.to_dict())
        assert restored.query_id == "join1"
        assert restored.query_version == 4
        assert restored.join_type == "left"
        assert restored.pairing_method == "blocking"
        assert restored.trigger_policy.mode == "idle_flush"
        assert restored.scope_policy.window_kind == "sliding"


class TestGenericSemanticBackendBoundaries:
    def test_topk_query_spec_rejects_public_backend(self):
        import pytest

        with pytest.raises(ValueError, match="requires semantic.backend='llm'"):
            TopKQuerySpec(
                semantic=SemSpec(
                    instruction="rank by relevance",
                    backend="embedding",
                    output_mode="score",
                )
            )

    def test_groupby_query_spec_rejects_public_backend(self):
        import pytest

        with pytest.raises(ValueError, match="requires semantic.backend='llm'"):
            GroupbyQuerySpec(
                semantic=SemSpec(
                    instruction="group by topic",
                    backend="embedding",
                    output_mode="label",
                )
            )

    def test_agg_query_spec_rejects_public_backend(self):
        import pytest

        with pytest.raises(ValueError, match="requires semantic.backend='llm'"):
            AggQuerySpec(
                semantic=SemSpec(
                    instruction="aggregate semantic state",
                    backend="rule",
                    output_mode="summary",
                )
            )


# ============================================================================
# External Search Backend Tests
# ============================================================================

from pyflink.semantic_runtime.runtime.external_search_backend import (
    ExternalSearchBackend, SearchResult, MockSearchBackend, SearchBackendAsyncFn,
    FaissSearchBackend,
)
from pyflink.semantic_runtime.operators.row.sem_lookup_join import (
    CandidateRetrieverFromSearchBackend,
    SemLookupJoinFunction,
    SemLookupJoinConfig,
)
from pyflink.semantic_runtime.llm_client import LLMClientConfig
