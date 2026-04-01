"""Tests for Mem0 external runtime adapters."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

import numpy as np

from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_config import (
    Mem0BackendConfig,
    Mem0EmbedderBackendConfig,
    Mem0GraphStoreConfig,
    Mem0LLMBackendConfig,
    Mem0RuntimeConfig,
    Mem0VectorStoreConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_runtime import (
    Mem0FaissFactBackend,
    Mem0GraphEmbeddingEntitySearcher,
    Mem0GraphEmbeddingRelationSearcher,
    Neo4jMem0GraphStore,
    build_mem0_external_bundle,
    create_mem0_external_clients,
)


class _FakeFaissIndexFlatIP:
    def __init__(self, dim: int) -> None:
        self._dim = int(dim)
        self._matrix = np.zeros((0, self._dim), dtype=np.float32)

    def add(self, matrix: np.ndarray) -> None:
        if matrix.ndim != 2:
            raise ValueError("matrix must be 2d")
        if int(matrix.shape[1]) != self._dim:
            raise ValueError("matrix dim mismatch")
        self._matrix = np.asarray(matrix, dtype=np.float32)

    def search(self, query_matrix: np.ndarray, top_k: int) -> tuple[np.ndarray, np.ndarray]:
        if query_matrix.shape[0] != 1:
            raise ValueError("query_matrix must have shape[0] == 1")
        if self._matrix.shape[0] == 0:
            scores = np.full((1, int(top_k)), -1.0, dtype=np.float32)
            positions = np.full((1, int(top_k)), -1, dtype=np.int64)
            return scores, positions
        query_vector = query_matrix[0]
        raw_scores = self._matrix @ query_vector
        order = np.argsort(-raw_scores)
        limit = min(int(top_k), int(order.shape[0]))
        scores = np.full((1, int(top_k)), -1.0, dtype=np.float32)
        positions = np.full((1, int(top_k)), -1, dtype=np.int64)
        for rank in range(limit):
            pos = int(order[rank])
            scores[0, rank] = float(raw_scores[pos])
            positions[0, rank] = pos
        return scores, positions


class _FakeFaissModule:
    IndexFlatIP = _FakeFaissIndexFlatIP


def _embedding_fn(text: str) -> Sequence[float]:
    normalized = str(text).strip().lower()
    if not normalized:
        raise ValueError("text must be non-empty")
    values = np.asarray(
        [
            float(len(normalized)),
            float(sum(1 for ch in normalized if ch in {"a", "e", "i", "o", "u"})),
            float(sum(1 for ch in normalized if ch.isalpha())),
        ],
        dtype=np.float32,
    )
    norm = float(np.linalg.norm(values))
    if norm <= 0.0:
        raise ValueError("embedding norm must be > 0")
    return list(values / norm)


@dataclass
class _FakeNeo4jState:
    entities: Dict[str, Dict[str, Any]]
    relations: Dict[str, Dict[str, Any]]


class _FakeNeo4jTx:
    def __init__(self, state: _FakeNeo4jState) -> None:
        self._state = state

    def run(self, query: str, **params: Any) -> Sequence[Dict[str, Any]]:
        if "CREATE (e:Mem0Entity" in query:
            entity_id = str(params["entity_id"])
            self._state.entities[entity_id] = {
                "entity_id": entity_id,
                "group_id": str(params["group_id"]),
                "entity_name": str(params["entity_name"]),
                "entity_type": str(params["entity_type"]),
            }
            return [{"entity_id": entity_id}]

        if "MATCH (e:Mem0Entity {uuid: $entity_id, group_id: $group_id})" in query:
            entity_id = str(params["entity_id"])
            if entity_id not in self._state.entities:
                return []
            row = self._state.entities[entity_id]
            if row["group_id"] != str(params["group_id"]):
                return []
            row["entity_name"] = str(params["entity_name"])
            row["entity_type"] = str(params["entity_type"])
            return [{"entity_id": entity_id}]

        if "CREATE (src)-[r:MEM0_RELATION" in query:
            src_id = str(params["source_entity_id"])
            dst_id = str(params["destination_entity_id"])
            group_id = str(params["group_id"])
            if src_id not in self._state.entities or dst_id not in self._state.entities:
                return []
            if self._state.entities[src_id]["group_id"] != group_id:
                return []
            if self._state.entities[dst_id]["group_id"] != group_id:
                return []
            relation_id = str(params["relation_id"])
            self._state.relations[relation_id] = {
                "relation_id": relation_id,
                "group_id": group_id,
                "source_entity_id": src_id,
                "destination_entity_id": dst_id,
                "relationship": str(params["relationship"]),
            }
            return [{"relation_id": relation_id}]

        if "SET r.relationship = $relationship" in query:
            relation_id = str(params["relation_id"])
            if relation_id not in self._state.relations:
                return []
            row = self._state.relations[relation_id]
            if row["group_id"] != str(params["group_id"]):
                return []
            row["relationship"] = str(params["relationship"])
            return [{"relation_id": relation_id}]

        if "DELETE r" in query:
            relation_id = str(params["relation_id"])
            if relation_id not in self._state.relations:
                return []
            row = self._state.relations[relation_id]
            if row["group_id"] != str(params["group_id"]):
                return []
            del self._state.relations[relation_id]
            return [{"relation_id": relation_id}]

        if "MATCH (e:Mem0Entity {group_id: $group_id})" in query:
            group_id = str(params["group_id"])
            return [
                {
                    "entity_id": entity_id,
                    "entity_name": row["entity_name"],
                    "entity_type": row["entity_type"],
                }
                for entity_id, row in self._state.entities.items()
                if row["group_id"] == group_id
            ]

        if "MATCH ()-[r:MEM0_RELATION {group_id: $group_id}]->()" in query:
            group_id = str(params["group_id"])
            source_entity_id = params.get("source_entity_id")
            destination_entity_id = params.get("destination_entity_id")
            output: List[Dict[str, Any]] = []
            for relation_id, row in self._state.relations.items():
                if row["group_id"] != group_id:
                    continue
                if source_entity_id is not None and row["source_entity_id"] != str(source_entity_id):
                    continue
                if destination_entity_id is not None and row["destination_entity_id"] != str(
                    destination_entity_id
                ):
                    continue
                output.append(
                    {
                        "relation_id": relation_id,
                        "source_entity_id": row["source_entity_id"],
                        "destination_entity_id": row["destination_entity_id"],
                        "relationship": row["relationship"],
                    }
                )
            return output

        raise RuntimeError(f"unsupported query: {query}")


class _FakeNeo4jSession:
    def __init__(self, state: _FakeNeo4jState) -> None:
        self._state = state

    def __enter__(self) -> "_FakeNeo4jSession":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        _ = (exc_type, exc, tb)
        return False

    def execute_read(self, fn: Any) -> Any:
        tx = _FakeNeo4jTx(self._state)
        return fn(tx)

    def execute_write(self, fn: Any) -> Any:
        tx = _FakeNeo4jTx(self._state)
        return fn(tx)


class _FakeNeo4jDriver:
    def __init__(self) -> None:
        self.state = _FakeNeo4jState(entities={}, relations={})
        self.closed = False

    def session(self, database: str) -> _FakeNeo4jSession:
        _ = database
        return _FakeNeo4jSession(self.state)

    def close(self) -> None:
        self.closed = True


def _backend_config(*, graph_enabled: bool) -> Mem0BackendConfig:
    return Mem0BackendConfig(
        llm=Mem0LLMBackendConfig(provider="openai", model="gpt-4.1-mini"),
        embedder=Mem0EmbedderBackendConfig(provider="openai", model="text-embedding-3-small"),
        vector_store=Mem0VectorStoreConfig(provider="faiss", path="/tmp/mem0-faiss-test"),
        graph_store=Mem0GraphStoreConfig(
            enabled=graph_enabled,
            provider="neo4j",
            url="bolt://localhost:7687",
            username="neo4j",
            password="secret",
            database="neo4j",
        ),
        runtime=Mem0RuntimeConfig(version="v1.1"),
    )


def test_mem0_faiss_fact_backend_crud_and_search(monkeypatch) -> None:
    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_runtime._ensure_faiss_module",
        lambda: _FakeFaissModule(),
    )
    backend = Mem0FaissFactBackend(embedding_fn=_embedding_fn)

    memory_id = asyncio.run(backend.add_fact(group_id="g1", content="Alice likes chess"))
    results_before = asyncio.run(
        backend.search(
            group_id="g1",
            query="Alice",
            top_k=3,
            memory_types=["fact"],
        )
    )
    assert len(results_before) == 1
    assert results_before[0].memory_id == memory_id

    asyncio.run(
        backend.update_fact(
            group_id="g1",
            memory_id=memory_id,
            content="Alice likes go",
        )
    )
    results_after = asyncio.run(
        backend.search(
            group_id="g1",
            query="go",
            top_k=1,
            memory_types=["fact"],
        )
    )
    assert len(results_after) == 1
    assert "go" in results_after[0].content

    asyncio.run(backend.delete_fact(group_id="g1", memory_id=memory_id))
    results_final = asyncio.run(
        backend.search(
            group_id="g1",
            query="Alice",
            top_k=1,
            memory_types=["fact"],
        )
    )
    assert len(results_final) == 0


def test_mem0_faiss_fact_backend_applies_embedding_input_cap(monkeypatch) -> None:
    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_runtime._ensure_faiss_module",
        lambda: _FakeFaissModule(),
    )
    captured_inputs: List[str] = []

    def _capturing_embedding_fn(text: str) -> Sequence[float]:
        captured_inputs.append(text)
        return _embedding_fn(text)

    backend = Mem0FaissFactBackend(
        embedding_fn=_capturing_embedding_fn,
        max_embedding_input_chars=16,
    )

    asyncio.run(
        backend.add_fact(
            group_id="g1",
            content="abcdefghijklmnopqrstuvwxyz",
        )
    )
    assert captured_inputs[0] == "abcdefghijklmnop"


def test_mem0_graph_store_and_embedding_searchers(monkeypatch) -> None:
    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_runtime._ensure_faiss_module",
        lambda: _FakeFaissModule(),
    )
    driver = _FakeNeo4jDriver()
    graph_store = Neo4jMem0GraphStore(driver=driver, database="neo4j")

    alice_id = asyncio.run(
        graph_store.upsert_entity(
            group_id="g1",
            entity_name="Alice",
            entity_type="person",
            existing_entity_id=None,
        )
    )
    bob_id = asyncio.run(
        graph_store.upsert_entity(
            group_id="g1",
            entity_name="Bob",
            entity_type="person",
            existing_entity_id=None,
        )
    )
    relation_id = asyncio.run(
        graph_store.add_relation(
            group_id="g1",
            source_entity_id=alice_id,
            destination_entity_id=bob_id,
            relationship="works with",
        )
    )
    asyncio.run(
        graph_store.update_relation(
            group_id="g1",
            relation_id=relation_id,
            relationship="collaborates with",
        )
    )

    entity_searcher = Mem0GraphEmbeddingEntitySearcher(
        graph_store=graph_store,
        embedding_fn=_embedding_fn,
    )
    relation_searcher = Mem0GraphEmbeddingRelationSearcher(
        graph_store=graph_store,
        embedding_fn=_embedding_fn,
    )

    entity_candidates = asyncio.run(
        entity_searcher.search(
            group_id="g1",
            query="Alice",
            top_k=1,
        )
    )
    relation_candidates = asyncio.run(
        relation_searcher.search(
            group_id="g1",
            query="collaborates",
            top_k=1,
            source_entity_id=alice_id,
            destination_entity_id=bob_id,
        )
    )
    assert len(entity_candidates) == 1
    assert entity_candidates[0].entity_id == alice_id
    assert len(relation_candidates) == 1
    assert relation_candidates[0].relation_id == relation_id
    assert relation_candidates[0].relationship == "collaborates with"

    asyncio.run(
        graph_store.delete_relation(
            group_id="g1",
            relation_id=relation_id,
        )
    )
    relation_candidates_after_delete = asyncio.run(
        relation_searcher.search(
            group_id="g1",
            query="collaborates",
            top_k=1,
            source_entity_id=alice_id,
            destination_entity_id=bob_id,
        )
    )
    assert len(relation_candidates_after_delete) == 0


def test_build_mem0_external_bundle_requires_neo4j_when_graph_enabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_runtime._ensure_faiss_module",
        lambda: _FakeFaissModule(),
    )
    config = _backend_config(graph_enabled=True)
    try:
        build_mem0_external_bundle(
            backend_config=config,
            embedding_fn=_embedding_fn,
            neo4j_driver=None,
        )
    except ValueError as exc:
        assert "neo4j_driver must not be None" in str(exc)
        return
    raise AssertionError("graph-enabled Mem0 bundle should require neo4j_driver")


def test_create_mem0_external_clients_graph_disabled_returns_empty() -> None:
    config = _backend_config(graph_enabled=False)
    clients = create_mem0_external_clients(backend_config=config)
    assert clients.neo4j_driver is None
