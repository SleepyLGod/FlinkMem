"""
Unit tests for external search backends and row-style semantic lookup join.

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

import pytest

from pyflink.semantic_runtime.runtime.external_search_backend import (
    ExternalSearchBackend,
    SearchResult,
    MockSearchBackend,
    SearchBackendAsyncFn,
    FaissSearchBackend,
)
from pyflink.semantic_runtime.operators.row.sem_lookup_join import (
    CandidateRetrieverFromSearchBackend,
    SemLookupJoinFunction,
    SemLookupJoinConfig,
)
from pyflink.semantic_runtime.llm_client import LLMClientConfig
from pyflink.semantic_runtime.runtime.async_bridge import AsyncWorkItem


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

    def test_mock_search_backend_query_rules(self):
        backend = MockSearchBackend(
            query_rules={
                "budget": [
                    {"candidate_id": "c1", "text": "Budget plan", "score": 0.9},
                    {"candidate_id": "c2", "text": "Budget risk", "score": 0.7},
                ]
            }
        )
        results = asyncio.run(backend.search("budget update", top_k=2))
        assert [r.candidate_id for r in results] == ["c1", "c2"]

    def test_mock_search_backend_lexical_corpus(self):
        backend = MockSearchBackend(
            corpus=[
                {"candidate_id": "c1", "text": "Sunny warm weather", "score": 0.2},
                {"candidate_id": "c2", "text": "Rain and storms", "score": 0.2},
            ]
        )
        results = asyncio.run(backend.search("best sunny weather", top_k=1))
        assert len(results) == 1
        assert results[0].candidate_id == "c1"

    def test_search_backend_async_fn(self):
        backend = MockSearchBackend(
            query_rules={
                "budget": [
                    {"candidate_id": "c1", "text": "Budget plan", "score": 0.9},
                ]
            }
        )
        worker = SearchBackendAsyncFn(backend)
        payload = AsyncWorkItem(
            key="user_1",
            task_type="retrieve",
            payload={"query": "budget update", "event_seq_id": 7, "max_candidates": 3},
        ).to_dict()
        out = asyncio.run(worker.async_invoke(payload))[0]
        assert out["success"] is True
        result = out["result"]
        assert result["query"] == "budget update"
        assert result["event_seq_id"] == 7
        assert result["candidates"][0]["candidate_id"] == "c1"
        assert result["candidates"][0]["content"] == "Budget plan"

    def test_candidate_retriever_from_search_backend(self):
        backend = MockSearchBackend(
            query_rules={
                "travel": [
                    {"candidate_id": "c1", "text": "Flight and hotel", "score": 0.93},
                ]
            }
        )
        retriever = CandidateRetrieverFromSearchBackend(backend)
        retriever.open()
        try:
            results = asyncio.run(retriever.retrieve("travel plan", 3))
        finally:
            retriever.close()
        assert len(results) == 1
        assert results[0]["candidate_id"] == "c1"
        assert results[0]["content"] == "Flight and hotel"
        assert results[0]["score"] == 0.93

    def test_faiss_backend_optional(self):
        pytest.importorskip("faiss")
        backend = FaissSearchBackend(
            corpus=[
                {"candidate_id": "c1", "text": "Sunny warm weather"},
                {"candidate_id": "c2", "text": "Rain and storm"},
            ],
            dim=64,
        )
        backend.open()
        try:
            results = asyncio.run(backend.search("best sunny weather", top_k=1))
        finally:
            backend.close()
        assert results
        assert results[0].candidate_id == "c1"

    def test_sem_lookup_join_uses_search_backend(self):
        backend = MockSearchBackend(
            query_rules={
                "budget": [
                    {"candidate_id": "c1", "text": "Budget plan", "score": 0.9},
                ]
            }
        )
        fn = SemLookupJoinFunction(
            "Match {input} with: {candidates}",
            LLMClientConfig(
                backend="mock",
                mock_response='{"matched":"c1","score":0.9}',
            ),
            SemLookupJoinConfig(
                max_candidates_per_record=3,
                search_backend=backend,
            ),
        )

        class _FakeRuntimeContext:
            pass

        fn.open(_FakeRuntimeContext())
        try:
            out = asyncio.run(fn.async_invoke("budget update"))[0]
        finally:
            fn.close()

        assert backend.opened is False
        parsed = json.loads(out)
        assert parsed["candidate_count"] == 1
        assert parsed["join_result"]["matched"] == "c1"


# ============================================================================
# State Safety Audit Tests
# ============================================================================
