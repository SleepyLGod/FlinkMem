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
from pyflink.semantic_runtime.stateful.sem_topk_continuous import (
    SemTopKConfig,
    SemTopKFunction,
)
from pyflink.semantic_runtime.stateful.sem_topk_pipeline import (
    _EmbeddingScorerWorker,
    _PointwiseLLMScorerWorker,
    _mark_missing_external_score,
    is_topk_candidate_record,
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

    def get_current_key(self):
        return self._key

    def timer_service(self):
        return None


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
    ctx = _FakeContext("user_1")
    outputs: List[Dict[str, Any]] = []
    score_field = topk._config.score_field

    llm_worker = None
    embedding_worker = None
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

    for row in rows:
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

