"""
Focused tests for the V0.2+ sem_topk pipeline builder support layer.

These tests stay in pure Python:
- candidate classification helpers
- pointwise LLM / embedding scorer workers
- in-memory pipeline semantics around the pure SemTopKFunction kernel
"""

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os, pathlib, pyflink as _pf  # noqa: E401,E402
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

import asyncio
from typing import Any, Dict, List

from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime_config import EmbeddingBackendConfig
from pyflink.semantic_runtime.semantic_spec import TopKQuerySpec
from pyflink.semantic_runtime.stateful.event_model import retrieve_to_topk_items
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
    SemTopKFunction,
    SemTopKScopeSnapshotFunction,
)
from pyflink.semantic_runtime.stateful.sem_topk_pipeline import (
    _BoundedPoolExternalTopKSnapshotWorker,
    _BoundedPoolLLMTopKSnapshotWorker,
    _BoundedPoolEmbeddingTopKSnapshotWorker,
    _EmbeddingScorerWorker,
    _PointwiseLLMScorerWorker,
    TopKContextualPlan,
    build_contextual_snapshot_query_spec,
    derive_topk_contextual_plan,
    _mark_missing_external_score,
    is_topk_candidate_record,
    is_topk_candidate_pool,
    topk_candidate_has_score,
    topk_candidate_needs_scoring,
)


class _FakeMapState:
    def __init__(self, initial=None):
        self._d = dict(initial or {})

    def get(self, key):
        return self._d.get(key)

    def put(self, key, value):
        self._d[key] = value

    def remove(self, key):
        self._d.pop(key, None)

    def keys(self):
        return list(self._d.keys())


class _FakeValueState:
    def __init__(self, initial=None):
        self._value = initial

    def value(self):
        return self._value

    def update(self, value):
        self._value = value


class _FakeContext:
    def __init__(self, key="test_key"):
        self._key = key
        self._timer_service = _FakeTimerService()

    def get_current_key(self):
        return self._key

    def timer_service(self):
        return self._timer_service


class _FakeTimerService:
    def __init__(self):
        self.registered = []

    def register_processing_time_timer(self, timestamp):
        self.registered.append(timestamp)

    def register_event_time_timer(self, timestamp):
        self.registered.append(timestamp)


class _FakeOnTimerContext(_FakeContext):
    pass


def _make_topk(query_spec: TopKQuerySpec) -> SemTopKFunction:
    func = SemTopKFunction(
        SemTopKConfig(max_candidates=16, emission_policy="snapshot", recompute_interval_ms=0),
        query_spec=query_spec,
    )
    func._candidates = _FakeMapState()
    func._snapshot = _FakeValueState(None)
    func._meta = _FakeValueState(None)
    func._metrics = None
    return func


def _make_scope_snapshot_emitter(query_spec: TopKQuerySpec) -> SemTopKScopeSnapshotFunction:
    func = SemTopKScopeSnapshotFunction(
        SemTopKConfig(max_candidates=16, emission_policy="snapshot", recompute_interval_ms=0),
        query_spec=query_spec,
    )
    func._candidates = _FakeMapState()
    func._meta = _FakeValueState()
    func._metrics = None
    return func


def _invoke_async(fn, payload):
    return asyncio.run(fn.async_invoke(payload))


def _run_pipeline_in_memory(
    rows: List[Dict[str, Any]],
    query_spec: TopKQuerySpec,
    *,
    llm_config: LLMClientConfig | None = None,
    embedding_config: EmbeddingBackendConfig | None = None,
) -> List[Dict[str, Any]]:
    topk = _make_topk(query_spec)
    snapshot_emitter = _make_scope_snapshot_emitter(query_spec)
    ctx = _FakeContext("user_1")
    outputs: List[Dict[str, Any]] = []
    score_field = topk._config.score_field
    execution_path = query_spec.execution_path
    trigger_mode = query_spec.trigger_policy.mode
    supported_scope_close_kinds = {"session", "tumbling", "semantic"}
    contextual_plan = None
    contextual_snapshot_spec = query_spec
    contextual_trigger_mode = trigger_mode

    llm_worker = None
    embedding_worker = None
    snapshot_pool_worker = None
    if query_spec.semantic.backend == "llm":
        llm_worker = _PointwiseLLMScorerWorker(
            query_spec,
            llm_config or LLMClientConfig(backend="mock"),
            score_field,
        )
    elif query_spec.semantic.backend == "embedding":
        embedding_worker = _EmbeddingScorerWorker(
            query_spec,
            embedding_config or EmbeddingBackendConfig(backend="mock"),
            score_field,
        )
    if query_spec.ranking_method in {"pairwise", "listwise"}:
        contextual_plan = derive_topk_contextual_plan(query_spec, topk._config)
        contextual_snapshot_spec = build_contextual_snapshot_query_spec(
            query_spec, topk._config, contextual_plan
        )
        contextual_trigger_mode = contextual_snapshot_spec.trigger_policy.mode
        snapshot_emitter = _make_scope_snapshot_emitter(contextual_snapshot_spec)
        if query_spec.semantic.backend == "llm":
            snapshot_pool_worker = _BoundedPoolLLMTopKSnapshotWorker(
                query_spec,
                llm_config or LLMClientConfig(backend="mock"),
                score_field,
            )
        elif query_spec.semantic.backend == "embedding":
            snapshot_pool_worker = _BoundedPoolEmbeddingTopKSnapshotWorker(
                query_spec,
                embedding_config or EmbeddingBackendConfig(backend="mock"),
                score_field,
            )
        elif query_spec.semantic.backend == "external_score":
            snapshot_pool_worker = _BoundedPoolExternalTopKSnapshotWorker(
                query_spec,
                score_field,
            )
        else:
            raise AssertionError(f"unexpected backend: {query_spec.semantic.backend}")
    elif execution_path in {"window_owned", "auto"} and trigger_mode == "on_scope_close":
        if query_spec.semantic.backend == "llm":
            snapshot_pool_worker = _BoundedPoolLLMTopKSnapshotWorker(
                query_spec,
                llm_config or LLMClientConfig(backend="mock"),
                score_field,
            )
        elif query_spec.semantic.backend == "embedding":
            snapshot_pool_worker = _BoundedPoolEmbeddingTopKSnapshotWorker(
                query_spec,
                embedding_config or EmbeddingBackendConfig(backend="mock"),
                score_field,
            )
        elif query_spec.semantic.backend == "external_score":
            snapshot_pool_worker = _BoundedPoolExternalTopKSnapshotWorker(
                query_spec,
                score_field,
            )
        else:
            raise AssertionError(f"unexpected backend: {query_spec.semantic.backend}")

    if trigger_mode not in {"on_event", "on_scope_close", "periodic", "idle_flush", "count_threshold"}:
        raise ValueError("sem_topk currently supports only trigger_policy.mode in {'on_event', 'on_scope_close', 'periodic', 'idle_flush', 'count_threshold'}")

    if execution_path == "operator_owned" and trigger_mode not in {"on_event", "periodic", "idle_flush", "count_threshold", "on_scope_close"}:
        raise ValueError(
            "sem_topk execution_path='operator_owned' currently supports only trigger_policy.mode in {'on_event', 'periodic', 'idle_flush', 'count_threshold', 'on_scope_close'}"
        )
    if (
        execution_path == "operator_owned"
        and trigger_mode == "on_scope_close"
        and query_spec.scope_policy.window_kind not in supported_scope_close_kinds
        and query_spec.ranking_method == "pointwise"
    ):
        raise ValueError(
            "sem_topk execution_path='operator_owned' with trigger_policy.mode='on_scope_close' "
            "requires scope_policy.window_kind in {'session', 'tumbling', 'semantic'}"
        )
    if execution_path == "window_owned" and trigger_mode in {"periodic", "idle_flush", "count_threshold"}:
        raise ValueError(f"sem_topk execution_path='window_owned' does not support trigger_policy.mode={trigger_mode!r} yet")
    if execution_path == "auto" and trigger_mode in {"periodic", "idle_flush", "count_threshold"}:
        execution_path = "operator_owned"

    if execution_path == "window_owned" and trigger_mode == "on_scope_close":
        for row in rows:
            if is_topk_candidate_pool(row):
                if row.get("candidates"):
                    outputs.extend(_invoke_async(snapshot_pool_worker, row))
                else:
                    outputs.append(row)
            elif is_topk_candidate_record(row):
                outputs.append(
                    {
                        "key": row.get("key", ""),
                        "query": row.get("query", ""),
                        "query_seq_id": int(row.get("query_seq_id", 0)),
                        "candidates": [],
                        "candidate_count": 0,
                        "source": "topk_scope_close",
                        "degraded": True,
                        "error": "topk_scope_close_requires_bounded_pool",
                        "timestamp_ms": 0,
                    }
                )
            else:
                outputs.append(row)
        return outputs

    if execution_path == "auto" and trigger_mode == "on_scope_close":
        residual_rows: List[Dict[str, Any]] = []
        for row in rows:
            if is_topk_candidate_pool(row):
                if row.get("candidates"):
                    outputs.extend(_invoke_async(snapshot_pool_worker, row))
                else:
                    outputs.append(row)
            else:
                residual_rows.append(row)
        rows = residual_rows
        if query_spec.ranking_method == "pointwise":
            execution_path = "operator_owned"

    expanded_rows: List[Dict[str, Any]] = []
    if query_spec.ranking_method == "pointwise" and execution_path in {"auto", "window_owned"}:
        for row in rows:
            if is_topk_candidate_pool(row):
                candidates = retrieve_to_topk_items(row)
                if candidates:
                    expanded_rows.extend(candidates)
                else:
                    expanded_rows.append(row)
            else:
                expanded_rows.append(row)
    else:
        expanded_rows = list(rows)

    for row in expanded_rows:
        if query_spec.ranking_method in {"pairwise", "listwise"}:
            if execution_path == "operator_owned":
                if not is_topk_candidate_record(row):
                    outputs.append(row)
                    continue
                emitted_pools = list(snapshot_emitter.process_element(row, ctx))
                for pool in emitted_pools:
                    reranked_rows = _invoke_async(snapshot_pool_worker, pool)
                    outputs.extend(reranked_rows)
                continue

            if execution_path == "auto":
                if is_topk_candidate_pool(row):
                    reranked_rows = _invoke_async(snapshot_pool_worker, row)
                    outputs.extend(reranked_rows)
                    continue
                if is_topk_candidate_record(row):
                    emitted_pools = list(snapshot_emitter.process_element(row, ctx))
                    for pool in emitted_pools:
                        outputs.extend(_invoke_async(snapshot_pool_worker, pool))
                    continue
                outputs.append(row)
                continue

            if is_topk_candidate_record(row):
                outputs.append(
                    {
                        "key": row.get("key", ""),
                        "query": row.get("query", ""),
                        "query_seq_id": int(row.get("query_seq_id", 0)),
                        "candidates": [],
                        "candidate_count": 0,
                        "source": f"topk_{query_spec.ranking_method}",
                        "degraded": True,
                        "error": "topk_contextual_requires_bounded_pool",
                        "timestamp_ms": 0,
                    }
                )
                continue
            if not is_topk_candidate_pool(row):
                outputs.append(row)
                continue
            reranked_rows = _invoke_async(snapshot_pool_worker, row)
            outputs.extend(reranked_rows)
            continue

        if execution_path == "operator_owned" and is_topk_candidate_pool(row):
            outputs.append(
                {
                    "key": row.get("key", ""),
                    "query": row.get("query", ""),
                    "query_seq_id": int(row.get("query_seq_id", 0)),
                    "candidates": [],
                    "candidate_count": 0,
                    "source": "topk_pointwise",
                    "degraded": True,
                    "error": "topk_operator_owned_requires_flat_candidates",
                    "timestamp_ms": 0,
                }
            )
            continue

        if not is_topk_candidate_record(row):
            outputs.append(row)
            continue

        backend = query_spec.semantic.backend
        if backend == "external_score":
            if topk_candidate_has_score(row, score_field):
                outputs.extend(list(topk.process_element(row, ctx)))
            else:
                outputs.append(_mark_missing_external_score(row, score_field))
            continue

        if not topk_candidate_needs_scoring(row, query_spec, score_field):
            outputs.extend(list(topk.process_element(row, ctx)))
            continue

        if backend == "llm":
            scored_rows = _invoke_async(llm_worker, row)
        elif backend == "embedding":
            scored_rows = _invoke_async(embedding_worker, row)
        else:
            raise AssertionError(f"unexpected backend: {backend}")

        for scored in scored_rows:
            if is_topk_candidate_record(scored) and topk_candidate_has_score(scored, score_field):
                outputs.extend(list(topk.process_element(scored, ctx)))
            else:
                outputs.append(scored)

    if (
        query_spec.ranking_method in {"pairwise", "listwise"}
        and execution_path in {"operator_owned", "auto"}
        and contextual_trigger_mode == "periodic"
    ):
        scope_meta = snapshot_emitter._meta.value()
        assert scope_meta is not None
        fire_at = scope_meta.get("_timer_recompute")
        assert fire_at is not None
        emitted_pools = list(snapshot_emitter.on_timer(fire_at, _FakeOnTimerContext("user_1")))
        for pool in emitted_pools:
            outputs.extend(_invoke_async(snapshot_pool_worker, pool))
    elif trigger_mode == "periodic":
        meta = topk._meta.value()
        if query_spec.ranking_method in {"pairwise", "listwise"} and execution_path in {"operator_owned", "auto"}:
            raise AssertionError("contextual periodic rerank should use contextual_trigger_mode branch")
        else:
            assert meta is not None
            fire_at = meta.get("_timer_recompute")
            assert fire_at is not None
            outputs.extend(list(topk.on_timer(fire_at, _FakeOnTimerContext("user_1"))))
    elif (
        query_spec.ranking_method in {"pairwise", "listwise"}
        and execution_path in {"operator_owned", "auto"}
        and contextual_trigger_mode == "idle_flush"
    ):
        meta = snapshot_emitter._meta.value()
        assert meta is not None
        fire_at = meta.get("_timer_flush")
        assert fire_at is not None
        emitted_pools = list(snapshot_emitter.on_timer(fire_at, _FakeOnTimerContext("user_1")))
        for pool in emitted_pools:
            outputs.extend(_invoke_async(snapshot_pool_worker, pool))
    elif trigger_mode == "idle_flush":
        if query_spec.ranking_method in {"pairwise", "listwise"} and execution_path in {"operator_owned", "auto"}:
            raise AssertionError("contextual idle_flush rerank should use contextual_trigger_mode branch")
        else:
            meta = topk._meta.value()
            assert meta is not None
            fire_at = meta.get("_timer_flush")
            assert fire_at is not None
            outputs.extend(list(topk.on_timer(fire_at, _FakeOnTimerContext("user_1"))))
    elif (
        query_spec.ranking_method in {"pairwise", "listwise"}
        and execution_path in {"operator_owned", "auto"}
        and contextual_trigger_mode == "count_threshold"
    ):
        pass
    elif trigger_mode == "count_threshold":
        pass
    elif (
        query_spec.ranking_method in {"pairwise", "listwise"}
        and execution_path in {"operator_owned", "auto"}
        and contextual_trigger_mode == "on_scope_close"
    ):
        meta = snapshot_emitter._meta.value()
        assert meta is not None
        fire_at = meta.get("_timer_flush")
        if fire_at is not None:
            emitted_pools = list(snapshot_emitter.on_timer(fire_at, _FakeOnTimerContext("user_1")))
            for pool in emitted_pools:
                outputs.extend(_invoke_async(snapshot_pool_worker, pool))
    elif trigger_mode == "on_scope_close" and execution_path == "operator_owned":
        if query_spec.ranking_method in {"pairwise", "listwise"}:
            raise AssertionError("contextual scope-close rerank should use contextual_trigger_mode branch")
        else:
            meta = topk._meta.value()
            assert meta is not None
            fire_at = meta.get("_timer_flush")
            if fire_at is not None:
                outputs.extend(list(topk.on_timer(fire_at, _FakeOnTimerContext("user_1"))))

    return outputs


class TestTopKPipelineHelpers:
    def test_candidate_detection(self):
        assert is_topk_candidate_record({"candidate_id": "c1"}) is True
        assert is_topk_candidate_record({"id": "c1"}) is False
        assert is_topk_candidate_record({"candidate_id": ""}) is False

    def test_external_score_needs_only_missing_score(self):
        spec = TopKQuerySpec.simple("rank weather days", backend="external_score")
        ready = {"candidate_id": "c1", "score": 0.8}
        missing = {"candidate_id": "c2"}
        assert topk_candidate_needs_scoring(ready, spec) is False
        assert topk_candidate_needs_scoring(missing, spec) is True

    def test_llm_needs_rescore_on_stale_query_version(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm")
        row = {
            "candidate_id": "c1",
            "score": 0.9,
            "_query_version": 0,
            "_score_backend": "llm",
        }
        assert topk_candidate_needs_scoring(row, spec) is True

    def test_llm_ready_when_backend_and_version_match(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm")
        row = {
            "candidate_id": "c1",
            "score": 0.9,
            "_query_version": spec.query_version,
            "_score_backend": "llm",
        }
        assert topk_candidate_needs_scoring(row, spec) is False

    def test_contextual_plan_pairwise_uses_tournament_of_pairs(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm")
        spec.ranking_method = "pairwise"
        plan = derive_topk_contextual_plan(spec, SemTopKConfig())
        assert plan == TopKContextualPlan(
            context_chunk_size=2,
            merge_strategy="tournament",
            close_surrogate="none",
            epoch_ms=None,
        )

    def test_contextual_plan_listwise_defaults_to_global_rank(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm")
        spec.ranking_method = "listwise"
        plan = derive_topk_contextual_plan(spec, SemTopKConfig())
        assert plan.context_chunk_size is None
        assert plan.merge_strategy == "global_rank"

    def test_contextual_sliding_scope_close_uses_epoch_surrogate(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm", execution_path="operator_owned")
        spec.ranking_method = "pairwise"
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "sliding"
        spec.scope_policy.window_size_ms = 60000
        plan = derive_topk_contextual_plan(spec, SemTopKConfig(recompute_interval_ms=5000))
        assert plan.close_surrogate == "epoch_close"
        eff = build_contextual_snapshot_query_spec(spec, SemTopKConfig(recompute_interval_ms=5000), plan)
        assert eff.trigger_policy.mode == "periodic"
        assert eff.trigger_policy.interval_ms == 60000

    def test_contextual_ttl_scope_close_uses_periodic_surrogate(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm", execution_path="operator_owned")
        spec.ranking_method = "listwise"
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.ttl_seconds = 30
        plan = derive_topk_contextual_plan(spec, SemTopKConfig(recompute_interval_ms=4000))
        assert plan.close_surrogate == "periodic_snapshot"
        eff = build_contextual_snapshot_query_spec(spec, SemTopKConfig(recompute_interval_ms=4000), plan)
        assert eff.trigger_policy.mode == "periodic"
        assert eff.trigger_policy.interval_ms == 4000


class TestPointwiseScorers:
    def test_pointwise_llm_mock_scores_candidate(self):
        spec = TopKQuerySpec.simple("best weather days", backend="llm")
        worker = _PointwiseLLMScorerWorker(spec, LLMClientConfig(backend="mock"))
        row = {
            "key": "user_1",
            "candidate_id": "c1",
            "query": "best sunny weather days",
            "text": "sunny warm dry weather with blue sky",
        }
        out = _invoke_async(worker, row)[0]
        assert out["candidate_id"] == "c1"
        assert 0.0 <= out["score"] <= 1.0
        assert out["_query_version"] == spec.query_version
        assert out["_score_backend"] == "llm"
        assert out["source"] == "topk_llm_pointwise"

    def test_embedding_mock_scores_candidate(self):
        spec = TopKQuerySpec.simple("best weather days", backend="embedding")
        worker = _EmbeddingScorerWorker(spec, EmbeddingBackendConfig(backend="mock"))
        row = {
            "key": "user_1",
            "candidate_id": "c1",
            "query": "best sunny weather days",
            "text": "rainy cold weather",
        }
        out = _invoke_async(worker, row)[0]
        assert out["candidate_id"] == "c1"
        assert 0.0 <= out["score"] <= 1.0
        assert out["_score_backend"] == "embedding"
        assert out["source"] == "topk_embedding_pointwise"

    def test_embedding_local_hashing_scores_candidate(self):
        spec = TopKQuerySpec.simple("best weather days", backend="embedding")
        worker = _EmbeddingScorerWorker(
            spec,
            EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        row = {
            "key": "user_1",
            "candidate_id": "c1",
            "query": "best sunny weather days",
            "text": "sunny warm weather forecast",
        }
        out = _invoke_async(worker, row)[0]
        assert out["candidate_id"] == "c1"
        assert 0.0 <= out["score"] <= 1.0
        assert out["_score_backend"] == "embedding"

    def test_embedding_unsupported_backend_degrades(self):
        spec = TopKQuerySpec.simple("best weather days", backend="embedding")
        worker = _EmbeddingScorerWorker(spec, EmbeddingBackendConfig(backend="remote_api"))
        row = {
            "key": "user_1",
            "candidate_id": "c1",
            "query": "best sunny weather days",
            "text": "sunny weather",
        }
        out = _invoke_async(worker, row)[0]
        assert out["degraded"] is True
        assert "not_implemented" in out["error"]


class TestTopKPipelineInMemory:
    def test_window_owned_scope_close_pointwise_emits_final_once(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="window_owned",
        )
        spec.trigger_policy.mode = "on_scope_close"
        rows = [
            {
                "key": "user_1",
                "query": "best sunny weather days",
                "query_seq_id": 4,
                "candidate_count": 2,
                "candidates": [
                    {"candidate_id": "d1", "text": "storm rain"},
                    {"candidate_id": "d2", "text": "sunny warm weather"},
                ],
            }
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]
        assert snapshots[0]["emission_policy"] == "scope_close_final"

    def test_operator_owned_scope_close_rejected(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "sliding"
        spec.scope_policy.window_size_ms = 1_000
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            }
        ]
        try:
            _run_pipeline_in_memory(rows, spec, embedding_config=EmbeddingBackendConfig(backend="mock"))
        except ValueError as exc:
            assert "operator_owned" in str(exc)
        else:
            raise AssertionError("expected operator_owned scope_close to be rejected")

    def test_operator_owned_scope_close_session_emits_final_on_idle_gap(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "session"
        spec.scope_policy.session_gap_ms = 50
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
                "event_time_ms": 1000,
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
                "event_time_ms": 1010,
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]
        assert snapshots[0]["scope_close_reason"] == "session_gap"

    def test_operator_owned_scope_close_semantic_emits_on_boundary(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "semantic"
        spec.scope_policy.boundary_flag = "topic_shift"
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
                "event_time_ms": 1000,
                "boundary_flags": {},
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
                "event_time_ms": 1010,
                "boundary_flags": {"topic_shift": True},
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]
        assert snapshots[0]["scope_close_reason"] == "semantic_boundary"

    def test_operator_owned_periodic_pointwise_emits_on_timer(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "periodic"
        spec.trigger_policy.interval_ms = 50
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_operator_owned_idle_flush_pointwise_emits_on_flush_timer(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "idle_flush"
        spec.trigger_policy.idle_ms = 50
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_operator_owned_count_threshold_pointwise_emits_every_n(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.trigger_policy.mode = "count_threshold"
        spec.trigger_policy.count_threshold = 2
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
            {
                "key": "user_1",
                "candidate_id": "d3",
                "query": "best sunny weather days",
                "text": "fog and drizzle",
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_auto_periodic_contextual_emits_on_timer(self):
        spec = TopKQuerySpec.simple("best sunny weather days", k=1, backend="llm")
        spec.ranking_method = "pairwise"
        spec.trigger_policy.mode = "periodic"
        spec.trigger_policy.interval_ms = 50
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_auto_scope_close_pointwise_splits_pool_and_flat(self):
        spec = TopKQuerySpec.simple("best sunny weather days", k=1, backend="embedding")
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "session"
        spec.scope_policy.session_gap_ms = 50
        rows = [
            {
                "key": "user_1",
                "query": "best sunny weather days",
                "query_seq_id": 3,
                "candidate_count": 2,
                "candidates": [
                    {"candidate_id": "p1", "text": "storm rain"},
                    {"candidate_id": "p2", "text": "sunny warm weather"},
                ],
            },
            {
                "key": "user_1",
                "candidate_id": "f1",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
                "event_time_ms": 1000,
            },
            {
                "key": "user_1",
                "candidate_id": "f2",
                "query": "best sunny weather days",
                "text": "storm rain",
                "event_time_ms": 1010,
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 2
        top_id_sets = {tuple(row["top_ids"]) for row in snapshots}
        assert ("p2",) in top_id_sets
        assert ("f1",) in top_id_sets

    def test_pointwise_auto_expands_bounded_pool(self):
        spec = TopKQuerySpec.simple("best weather days", k=1, backend="embedding")
        rows = [
            {
                "key": "user_1",
                "query": "best sunny weather days",
                "query_seq_id": 3,
                "candidate_count": 2,
                "candidates": [
                    {"candidate_id": "d1", "text": "storm rain"},
                    {"candidate_id": "d2", "text": "sunny warm weather"},
                ],
            }
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert snapshots[-1]["top_ids"] == ["d2"]

    def test_operator_owned_pairwise_on_event_emits_snapshot(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="llm",
            execution_path="operator_owned",
        )
        spec.ranking_method = "pairwise"
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            }
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 2
        assert snapshots[-1]["top_ids"] == ["d2"]

    def test_operator_owned_pairwise_sliding_scope_close_uses_epoch_surrogate(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="llm",
            execution_path="operator_owned",
        )
        spec.ranking_method = "pairwise"
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.window_kind = "sliding"
        spec.scope_policy.window_size_ms = 50
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
                "event_time_ms": 1000,
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
                "event_time_ms": 1010,
            },
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_operator_owned_listwise_ttl_scope_close_uses_periodic_surrogate(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="embedding",
            execution_path="operator_owned",
        )
        spec.ranking_method = "listwise"
        spec.trigger_policy.mode = "on_scope_close"
        spec.scope_policy.ttl_seconds = 30
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "storm rain",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert len(snapshots) == 1
        assert snapshots[0]["top_ids"] == ["d2"]

    def test_window_owned_contextual_flat_candidate_degrades(self):
        spec = TopKQuerySpec.simple(
            "best sunny weather days",
            k=1,
            backend="llm",
            execution_path="window_owned",
        )
        spec.ranking_method = "listwise"
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            }
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        assert len(out) == 1
        assert out[0]["degraded"] is True
        assert out[0]["error"] == "topk_contextual_requires_bounded_pool"

    def test_external_score_pipeline_emits_topk(self):
        spec = TopKQuerySpec.simple("best weather days", k=2, backend="external_score")
        rows = [
            {"key": "user_1", "candidate_id": "d1", "score": 0.3, "query": "best weather"},
            {"key": "user_1", "candidate_id": "d2", "score": 0.9, "query": "best weather"},
            {"key": "user_1", "candidate_id": "d3", "score": 0.7, "query": "best weather"},
        ]
        out = _run_pipeline_in_memory(rows, spec)
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert snapshots[-1]["top_ids"] == ["d2", "d3"]

    def test_llm_pointwise_pipeline_scores_then_emits_topk(self):
        spec = TopKQuerySpec.simple("best weather days", k=2, backend="llm")
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "sunny warm weather all day",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "rain and storms",
            },
            {
                "key": "user_1",
                "candidate_id": "d3",
                "query": "best sunny weather days",
                "text": "sunny clear dry sky",
            },
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert set(snapshots[-1]["top_ids"]) == {"d1", "d3"}

    def test_embedding_pipeline_scores_then_emits_topk(self):
        spec = TopKQuerySpec.simple("best weather days", k=1, backend="embedding")
        rows = [
            {
                "key": "user_1",
                "candidate_id": "d1",
                "query": "best sunny weather days",
                "text": "rain and storms",
            },
            {
                "key": "user_1",
                "candidate_id": "d2",
                "query": "best sunny weather days",
                "text": "sunny warm weather",
            },
        ]
        out = _run_pipeline_in_memory(rows, spec, embedding_config=EmbeddingBackendConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert snapshots[-1]["top_ids"] == ["d2"]

    def test_passthrough_records_survive(self):
        spec = TopKQuerySpec.simple("best weather days", k=1, backend="external_score")
        rows = [
            {
                "key": "user_1",
                "query": "best weather",
                "query_seq_id": 7,
                "candidates": [],
                "candidate_count": 0,
                "degraded": True,
                "error": "retrieve_timeout",
            }
        ]
        out = _run_pipeline_in_memory(rows, spec)
        assert len(out) == 1
        assert out[0]["degraded"] is True
        assert out[0]["error"] == "retrieve_timeout"

    def test_llm_pairwise_bounded_pool_emits_topk(self):
        spec = TopKQuerySpec.simple("best sunny weather days", k=2, backend="llm")
        spec.ranking_method = "pairwise"
        rows = [
            {
                "key": "user_1",
                "query": "best sunny weather days",
                "query_seq_id": 7,
                "candidate_count": 3,
                "candidates": [
                    {"candidate_id": "d1", "text": "sunny warm weather all day"},
                    {"candidate_id": "d2", "text": "rain and storms"},
                    {"candidate_id": "d3", "text": "sunny clear dry sky"},
                ],
            }
        ]
        out = _run_pipeline_in_memory(rows, spec, llm_config=LLMClientConfig(backend="mock"))
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert set(snapshots[-1]["top_ids"]) == {"d1", "d3"}

    def test_embedding_listwise_bounded_pool_emits_topk(self):
        spec = TopKQuerySpec.simple("best sunny weather days", k=1, backend="embedding")
        spec.ranking_method = "listwise"
        rows = [
            {
                "key": "user_1",
                "query": "best sunny weather days",
                "query_seq_id": 9,
                "candidate_count": 2,
                "candidates": [
                    {"candidate_id": "d1", "text": "cold rain and storms"},
                    {"candidate_id": "d2", "text": "sunny warm weather forecast"},
                ],
            }
        ]
        out = _run_pipeline_in_memory(
            rows,
            spec,
            embedding_config=EmbeddingBackendConfig(backend="local_hashing", dimensions=64),
        )
        snapshots = [row for row in out if "top_ids" in row]
        assert snapshots
        assert snapshots[-1]["top_ids"] == ["d2"]
