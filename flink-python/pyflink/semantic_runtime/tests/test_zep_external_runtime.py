"""Tests for Zep external runtime adapters."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import asyncio
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from typing import Any, Dict, List, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.external_config import (
    ZepBackendConfig,
    ZepEmbedderBackendConfig,
    ZepLLMBackendConfig,
    ZepNeo4jConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.external_runtime import (
    ZepExternalClients,
    build_zep_external_bundle,
)


class _FakeTx:
    def __init__(self, *, query_log: List[str]) -> None:
        self._query_log = query_log

    def run(self, query: str, **params: Any) -> Sequence[Dict[str, Any]]:
        self._query_log.append(str(query))
        if "CREATE FULLTEXT INDEX" in query:
            return []
        if "MATCH (e:Episodic" in query and "ORDER BY created_at_ms DESC" in query:
            return [
                {
                    "episode_id": "ep-old-1",
                    "content": "historical episode",
                    "created_at_ms": 100,
                }
            ]
        if "CREATE (e:Episodic" in query:
            return [{"episode_id": params["episode_id"]}]
        if "CALL db.index.fulltext.queryNodes" in query:
            return [
                {
                    "entity_id": "e-existing",
                    "entity_name": "Alice",
                    "summary": "Alice summary",
                    "score": 0.9,
                }
            ]
        if "MATCH (n:Entity {uuid: $existing_entity_id" in query:
            return [{"entity_id": params["existing_entity_id"]}]
        if "CREATE (n:Entity" in query:
            return [{"entity_id": params["entity_id"]}]
        if "CALL db.index.fulltext.queryRelationships" in query:
            return [
                {
                    "edge_id": "edge-old-1",
                    "source_entity_id": params["source_entity_id"],
                    "destination_entity_id": params["destination_entity_id"],
                    "relation": "knows",
                    "fact": "Alice knows Bob",
                    "score": 0.8,
                }
            ]
        if "MATCH ()-[r:RELATES_TO {uuid: $invalidates_edge_id}]->()" in query:
            return []
        if "CREATE (src)-[r:RELATES_TO" in query:
            return [{"edge_id": params["edge_id"]}]
        raise RuntimeError(f"unexpected query: {query}")


class _FakeSession:
    def __init__(self, *, query_log: List[str]) -> None:
        self._query_log = query_log

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute_read(self, fn):  # type: ignore[no-untyped-def]
        return fn(_FakeTx(query_log=self._query_log))

    def execute_write(self, fn):  # type: ignore[no-untyped-def]
        return fn(_FakeTx(query_log=self._query_log))


class _FakeDriver:
    def __init__(self) -> None:
        self.closed = False
        self.query_log: List[str] = []

    def session(self, *, database: str) -> _FakeSession:
        _ = database
        return _FakeSession(query_log=self.query_log)

    def close(self) -> None:
        self.closed = True


def _backend_config() -> ZepBackendConfig:
    return ZepBackendConfig(
        llm=ZepLLMBackendConfig(provider="openai", model="gpt-4.1-mini"),
        embedder=ZepEmbedderBackendConfig(provider="openai", model="text-embedding-3-small"),
        graph=ZepNeo4jConfig(
            uri="bolt://localhost:7687",
            username="neo4j",
            password="secret",
            database="neo4j",
        ),
    )


def test_zep_external_bundle_graph_store_methods() -> None:
    driver = _FakeDriver()
    bundle = build_zep_external_bundle(
        backend_config=_backend_config(),
        neo4j_driver=driver,
    )
    assert any(
        "CREATE FULLTEXT INDEX `node_name_and_summary` IF NOT EXISTS" in query
        for query in driver.query_log
    )
    assert any(
        "CREATE FULLTEXT INDEX `edge_name_and_fact` IF NOT EXISTS" in query
        for query in driver.query_log
    )

    recent = asyncio.run(bundle.graph_store.get_recent_episodes(group_id="g1", limit=3))
    assert len(recent) == 1
    assert recent[0].episode_id == "ep-old-1"

    episode_id = asyncio.run(
        bundle.graph_store.upsert_episode(
            group_id="g1",
            content="new episode",
            valid_at_ms=123,
        )
    )
    assert episode_id

    entity_candidates = asyncio.run(
        bundle.graph_store.search_entity_candidates(
            group_id="g1",
            query="Alice",
            top_k=2,
        )
    )
    assert len(entity_candidates) == 1
    assert entity_candidates[0].entity_id == "e-existing"

    existing_entity_id = asyncio.run(
        bundle.graph_store.upsert_entity(
            group_id="g1",
            entity_name="Alice",
            type_id="person",
            summary="updated summary",
            existing_entity_id="e-existing",
        )
    )
    assert existing_entity_id == "e-existing"

    new_entity_id = asyncio.run(
        bundle.graph_store.upsert_entity(
            group_id="g1",
            entity_name="Bob",
            type_id="person",
            summary="Bob summary",
            existing_entity_id=None,
        )
    )
    assert new_entity_id

    edge_candidates = asyncio.run(
        bundle.graph_store.search_edge_candidates(
            group_id="g1",
            source_entity_id="e-existing",
            destination_entity_id="e-existing",
            query_fact="Alice knows Bob",
            top_k=3,
        )
    )
    assert len(edge_candidates) == 1
    assert edge_candidates[0].edge_id == "edge-old-1"

    edge_id = asyncio.run(
        bundle.graph_store.upsert_edge(
            group_id="g1",
            source_entity_id="e-existing",
            destination_entity_id="e-existing",
            relation="knows",
            fact="Alice knows Bob",
            invalidates_edge_id="edge-old-1",
        )
    )
    assert edge_id


def test_zep_external_clients_close_calls_driver_close() -> None:
    driver = _FakeDriver()
    clients = ZepExternalClients(neo4j_driver=driver)
    clients.close()
    assert driver.closed is True
