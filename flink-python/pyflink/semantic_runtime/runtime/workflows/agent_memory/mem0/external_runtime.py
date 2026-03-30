"""External backend adapters for Mem0 Basic/Graph workflows."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from uuid import uuid4

import numpy as np

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphEntityCandidate,
    Mem0GraphRelationCandidate,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_config import (
    Mem0BackendConfig,
)


DEFAULT_MEM0_GRAPH_DATABASE = "neo4j"
DEFAULT_MEM0_MEMORY_TYPE = "fact"
FAISS_TOPK_SEARCH_OVERSAMPLE = 4


def _utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _normalize_text(value: str, *, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _embedding_to_vector(
    embedding: Sequence[float],
    *,
    field_name: str,
) -> np.ndarray:
    if not embedding:
        raise ValueError(f"{field_name} must be non-empty")
    vector = np.asarray(list(embedding), dtype=np.float32)
    if vector.ndim != 1:
        raise ValueError(f"{field_name} must be one-dimensional")
    norm = float(np.linalg.norm(vector))
    if norm <= 0.0:
        raise ValueError(f"{field_name} norm must be > 0")
    return vector / norm


def _ensure_faiss_module() -> Any:
    try:
        import faiss  # type: ignore
    except ImportError as exc:
        raise ImportError("faiss package is required for Mem0 FAISS backends") from exc
    return faiss


class Mem0FaissFactBackend:
    """FAISS-backed Mem0 Basic fact store and searcher."""

    def __init__(
        self,
        *,
        embedding_fn: Callable[[str], Sequence[float]],
    ) -> None:
        if embedding_fn is None:
            raise ValueError("embedding_fn must not be None")
        self._embedding_fn = embedding_fn
        self._rows: Dict[str, Dict[str, Any]] = {}
        self._index_by_group: Dict[str, Tuple[Any, List[str]]] = {}
        self._faiss = _ensure_faiss_module()

    async def add_fact(self, *, group_id: str, content: str) -> str:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_content = _normalize_text(content, field_name="content")
        vector = await self._embed_text(normalized_content)
        memory_id = str(uuid4())
        self._rows[memory_id] = {
            "group_id": normalized_group_id,
            "content": normalized_content,
            "vector": vector,
            "timestamp_ms": _utc_now_ms(),
        }
        self._rebuild_group_index(group_id=normalized_group_id)
        return memory_id

    async def update_fact(self, *, group_id: str, memory_id: str, content: str) -> None:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_memory_id = _normalize_text(memory_id, field_name="memory_id")
        normalized_content = _normalize_text(content, field_name="content")
        if normalized_memory_id not in self._rows:
            raise KeyError(f"unknown memory_id={normalized_memory_id}")
        row = self._rows[normalized_memory_id]
        if row["group_id"] != normalized_group_id:
            raise ValueError("group_id mismatch during update_fact")
        row["content"] = normalized_content
        row["vector"] = await self._embed_text(normalized_content)
        row["timestamp_ms"] = _utc_now_ms()
        self._rebuild_group_index(group_id=normalized_group_id)

    async def delete_fact(self, *, group_id: str, memory_id: str) -> None:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_memory_id = _normalize_text(memory_id, field_name="memory_id")
        if normalized_memory_id not in self._rows:
            raise KeyError(f"unknown memory_id={normalized_memory_id}")
        row = self._rows[normalized_memory_id]
        if row["group_id"] != normalized_group_id:
            raise ValueError("group_id mismatch during delete_fact")
        del self._rows[normalized_memory_id]
        self._rebuild_group_index(group_id=normalized_group_id)

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_query = _normalize_text(query, field_name="query")
        if int(top_k) <= 0:
            raise ValueError("top_k must be > 0")
        if memory_types is not None and DEFAULT_MEM0_MEMORY_TYPE not in set(memory_types):
            return []
        if normalized_group_id not in self._index_by_group:
            return []
        index, memory_ids = self._index_by_group[normalized_group_id]
        if not memory_ids:
            return []
        query_vector = await self._embed_text(normalized_query)
        query_matrix = np.asarray([query_vector], dtype=np.float32)
        search_limit = min(
            len(memory_ids),
            max(int(top_k), int(top_k) * FAISS_TOPK_SEARCH_OVERSAMPLE),
        )
        scores, positions = index.search(query_matrix, search_limit)
        output: List[RetrievedMemory] = []
        for rank in range(search_limit):
            position = int(positions[0][rank])
            if position < 0:
                continue
            memory_id = memory_ids[position]
            row = self._rows[memory_id]
            output.append(
                RetrievedMemory(
                    memory_id=memory_id,
                    memory_type=DEFAULT_MEM0_MEMORY_TYPE,
                    content=str(row["content"]),
                    score=float(scores[0][rank]),
                    source="faiss_embedding",
                    timestamp_ms=int(row["timestamp_ms"]),
                    metadata={"group_id": normalized_group_id},
                )
            )
            if len(output) >= int(top_k):
                break
        return output

    async def _embed_text(self, text: str) -> np.ndarray:
        embedding = await asyncio.to_thread(self._embedding_fn, text)
        return _embedding_to_vector(embedding, field_name="embedding")

    def _rebuild_group_index(self, *, group_id: str) -> None:
        memory_ids = [
            memory_id
            for memory_id, row in self._rows.items()
            if str(row["group_id"]) == group_id
        ]
        if not memory_ids:
            self._index_by_group[group_id] = (
                self._faiss.IndexFlatIP(1),
                [],
            )
            return
        matrix = np.asarray(
            [self._rows[memory_id]["vector"] for memory_id in memory_ids],
            dtype=np.float32,
        )
        if matrix.ndim != 2:
            raise RuntimeError("faiss matrix must be two-dimensional")
        index = self._faiss.IndexFlatIP(int(matrix.shape[1]))
        index.add(matrix)
        self._index_by_group[group_id] = (index, memory_ids)


class Neo4jMem0GraphStore:
    """Neo4j-backed Mem0 Graph store."""

    def __init__(
        self,
        *,
        driver: Any,
        database: str,
    ) -> None:
        self._driver = driver
        self._database = database

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        entity_type: str,
        existing_entity_id: Optional[str],
    ) -> str:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_entity_name = _normalize_text(entity_name, field_name="entity_name")
        normalized_entity_type = _normalize_text(entity_type, field_name="entity_type")
        return await asyncio.to_thread(
            self._upsert_entity_sync,
            normalized_group_id,
            normalized_entity_name,
            normalized_entity_type,
            existing_entity_id,
        )

    async def add_relation(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relationship: str,
    ) -> str:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_source = _normalize_text(
            source_entity_id, field_name="source_entity_id"
        )
        normalized_destination = _normalize_text(
            destination_entity_id, field_name="destination_entity_id"
        )
        normalized_relationship = _normalize_text(
            relationship, field_name="relationship"
        )
        relation_id = str(uuid4())
        return await asyncio.to_thread(
            self._add_relation_sync,
            normalized_group_id,
            normalized_source,
            normalized_destination,
            normalized_relationship,
            relation_id,
        )

    async def update_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
        relationship: str,
    ) -> None:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_relation_id = _normalize_text(relation_id, field_name="relation_id")
        normalized_relationship = _normalize_text(
            relationship, field_name="relationship"
        )
        await asyncio.to_thread(
            self._update_relation_sync,
            normalized_group_id,
            normalized_relation_id,
            normalized_relationship,
        )

    async def delete_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
    ) -> None:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_relation_id = _normalize_text(relation_id, field_name="relation_id")
        await asyncio.to_thread(
            self._delete_relation_sync,
            normalized_group_id,
            normalized_relation_id,
        )

    async def list_entities(
        self,
        *,
        group_id: str,
    ) -> Sequence[Mapping[str, Any]]:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        return await asyncio.to_thread(self._list_entities_sync, normalized_group_id)

    async def list_relations(
        self,
        *,
        group_id: str,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mapping[str, Any]]:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_source = (
            _normalize_text(source_entity_id, field_name="source_entity_id")
            if source_entity_id is not None
            else None
        )
        normalized_destination = (
            _normalize_text(destination_entity_id, field_name="destination_entity_id")
            if destination_entity_id is not None
            else None
        )
        return await asyncio.to_thread(
            self._list_relations_sync,
            normalized_group_id,
            normalized_source,
            normalized_destination,
        )

    def _upsert_entity_sync(
        self,
        group_id: str,
        entity_name: str,
        entity_type: str,
        existing_entity_id: Optional[str],
    ) -> str:
        if existing_entity_id is not None:
            query = """
                MATCH (e:Mem0Entity {uuid: $entity_id, group_id: $group_id})
                SET e.entity_name = $entity_name,
                    e.entity_type = $entity_type,
                    e.updated_at_ms = timestamp()
                RETURN e.uuid AS entity_id
            """
            params = {
                "entity_id": existing_entity_id,
                "group_id": group_id,
                "entity_name": entity_name,
                "entity_type": entity_type,
            }
        else:
            entity_id = str(uuid4())
            query = """
                CREATE (e:Mem0Entity {
                    uuid: $entity_id,
                    group_id: $group_id,
                    entity_name: $entity_name,
                    entity_type: $entity_type,
                    created_at_ms: timestamp(),
                    updated_at_ms: timestamp()
                })
                RETURN e.uuid AS entity_id
            """
            params = {
                "entity_id": entity_id,
                "group_id": group_id,
                "entity_name": entity_name,
                "entity_type": entity_type,
            }
        rows = self._execute_write(query, **params)
        if not rows:
            raise RuntimeError("upsert_entity returned no row")
        return _normalize_text(rows[0]["entity_id"], field_name="entity_id")

    def _add_relation_sync(
        self,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relationship: str,
        relation_id: str,
    ) -> str:
        query = """
            MATCH (src:Mem0Entity {uuid: $source_entity_id, group_id: $group_id})
            MATCH (dst:Mem0Entity {uuid: $destination_entity_id, group_id: $group_id})
            CREATE (src)-[r:MEM0_RELATION {
                uuid: $relation_id,
                group_id: $group_id,
                source_entity_id: $source_entity_id,
                destination_entity_id: $destination_entity_id,
                relationship: $relationship,
                created_at_ms: timestamp(),
                updated_at_ms: timestamp()
            }]->(dst)
            RETURN r.uuid AS relation_id
        """
        rows = self._execute_write(
            query,
            relation_id=relation_id,
            group_id=group_id,
            source_entity_id=source_entity_id,
            destination_entity_id=destination_entity_id,
            relationship=relationship,
        )
        if not rows:
            raise RuntimeError("add_relation returned no row")
        return _normalize_text(rows[0]["relation_id"], field_name="relation_id")

    def _update_relation_sync(
        self,
        group_id: str,
        relation_id: str,
        relationship: str,
    ) -> None:
        query = """
            MATCH ()-[r:MEM0_RELATION {uuid: $relation_id, group_id: $group_id}]->()
            SET r.relationship = $relationship,
                r.updated_at_ms = timestamp()
            RETURN r.uuid AS relation_id
        """
        rows = self._execute_write(
            query,
            relation_id=relation_id,
            group_id=group_id,
            relationship=relationship,
        )
        if not rows:
            raise KeyError(f"unknown relation_id={relation_id}")

    def _delete_relation_sync(self, group_id: str, relation_id: str) -> None:
        query = """
            MATCH ()-[r:MEM0_RELATION {uuid: $relation_id, group_id: $group_id}]->()
            WITH r, r.uuid AS relation_id
            DELETE r
            RETURN relation_id
        """
        rows = self._execute_write(
            query,
            relation_id=relation_id,
            group_id=group_id,
        )
        if not rows:
            raise KeyError(f"unknown relation_id={relation_id}")

    def _list_entities_sync(self, group_id: str) -> Sequence[Mapping[str, Any]]:
        query = """
            MATCH (e:Mem0Entity {group_id: $group_id})
            RETURN
                e.uuid AS entity_id,
                e.entity_name AS entity_name,
                e.entity_type AS entity_type
        """
        return self._execute_read(query, group_id=group_id)

    def _list_relations_sync(
        self,
        group_id: str,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mapping[str, Any]]:
        query = """
            MATCH ()-[r:MEM0_RELATION {group_id: $group_id}]->()
            WHERE ($source_entity_id IS NULL OR r.source_entity_id = $source_entity_id)
              AND ($destination_entity_id IS NULL OR r.destination_entity_id = $destination_entity_id)
            RETURN
                r.uuid AS relation_id,
                r.source_entity_id AS source_entity_id,
                r.destination_entity_id AS destination_entity_id,
                r.relationship AS relationship
        """
        return self._execute_read(
            query,
            group_id=group_id,
            source_entity_id=source_entity_id,
            destination_entity_id=destination_entity_id,
        )

    def _execute_read(self, query: str, **params: Any) -> Sequence[Mapping[str, Any]]:
        with self._driver.session(database=self._database) as session:
            rows = session.execute_read(
                lambda tx: [dict(record) for record in tx.run(query, **params)]
            )
        return list(rows)

    def _execute_write(self, query: str, **params: Any) -> Sequence[Mapping[str, Any]]:
        with self._driver.session(database=self._database) as session:
            rows = session.execute_write(
                lambda tx: [dict(record) for record in tx.run(query, **params)]
            )
        return list(rows)


class Mem0GraphEmbeddingEntitySearcher:
    """Embedding-based entity recall over Neo4j graph rows."""

    def __init__(
        self,
        *,
        graph_store: Neo4jMem0GraphStore,
        embedding_fn: Callable[[str], Sequence[float]],
    ) -> None:
        self._graph_store = graph_store
        self._embedding_fn = embedding_fn
        self._faiss = _ensure_faiss_module()

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[Mem0GraphEntityCandidate]:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_query = _normalize_text(query, field_name="query")
        if int(top_k) <= 0:
            raise ValueError("top_k must be > 0")
        rows = list(await self._graph_store.list_entities(group_id=normalized_group_id))
        if not rows:
            return []
        texts = [
            f"{_normalize_text(item['entity_name'], field_name='entity_name')} "
            f"{_normalize_text(item['entity_type'], field_name='entity_type')}"
            for item in rows
        ]
        vectors = await self._embed_texts(texts)
        query_vector = await asyncio.to_thread(self._embedding_fn, normalized_query)
        query_vec = _embedding_to_vector(query_vector, field_name="query_embedding")
        scores, positions = self._search_vectors(
            vectors=vectors,
            query_vector=query_vec,
            top_k=int(top_k),
        )
        result: List[Mem0GraphEntityCandidate] = []
        for rank, position in enumerate(positions):
            if position < 0:
                continue
            row = rows[position]
            result.append(
                Mem0GraphEntityCandidate(
                    entity_id=_normalize_text(row["entity_id"], field_name="entity_id"),
                    entity_name=_normalize_text(
                        row["entity_name"], field_name="entity_name"
                    ),
                    entity_type=_normalize_text(
                        row["entity_type"], field_name="entity_type"
                    ),
                    score=float(scores[rank]),
                    source="faiss_embedding",
                )
            )
        return result

    async def _embed_texts(self, texts: Sequence[str]) -> np.ndarray:
        vectors: List[np.ndarray] = []
        for text in texts:
            embedding = await asyncio.to_thread(self._embedding_fn, text)
            vectors.append(_embedding_to_vector(embedding, field_name="embedding"))
        return np.asarray(vectors, dtype=np.float32)

    def _search_vectors(
        self,
        *,
        vectors: np.ndarray,
        query_vector: np.ndarray,
        top_k: int,
    ) -> Tuple[List[float], List[int]]:
        index = self._faiss.IndexFlatIP(int(vectors.shape[1]))
        index.add(vectors)
        query_matrix = np.asarray([query_vector], dtype=np.float32)
        search_limit = min(
            int(vectors.shape[0]),
            max(top_k, top_k * FAISS_TOPK_SEARCH_OVERSAMPLE),
        )
        score_matrix, position_matrix = index.search(query_matrix, search_limit)
        scores: List[float] = []
        positions: List[int] = []
        for rank in range(search_limit):
            position = int(position_matrix[0][rank])
            if position < 0:
                continue
            scores.append(float(score_matrix[0][rank]))
            positions.append(position)
            if len(positions) >= top_k:
                break
        return scores, positions


class Mem0GraphEmbeddingRelationSearcher:
    """Embedding-based relation recall over Neo4j graph rows."""

    def __init__(
        self,
        *,
        graph_store: Neo4jMem0GraphStore,
        embedding_fn: Callable[[str], Sequence[float]],
    ) -> None:
        self._graph_store = graph_store
        self._embedding_fn = embedding_fn
        self._faiss = _ensure_faiss_module()

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mem0GraphRelationCandidate]:
        normalized_group_id = _normalize_text(group_id, field_name="group_id")
        normalized_query = _normalize_text(query, field_name="query")
        if int(top_k) <= 0:
            raise ValueError("top_k must be > 0")
        rows = list(
            await self._graph_store.list_relations(
                group_id=normalized_group_id,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
            )
        )
        if not rows:
            return []
        texts = [
            _normalize_text(item["relationship"], field_name="relationship")
            for item in rows
        ]
        vectors = await self._embed_texts(texts)
        query_vector = await asyncio.to_thread(self._embedding_fn, normalized_query)
        query_vec = _embedding_to_vector(query_vector, field_name="query_embedding")
        scores, positions = self._search_vectors(
            vectors=vectors,
            query_vector=query_vec,
            top_k=int(top_k),
        )
        result: List[Mem0GraphRelationCandidate] = []
        for rank, position in enumerate(positions):
            if position < 0:
                continue
            row = rows[position]
            result.append(
                Mem0GraphRelationCandidate(
                    relation_id=_normalize_text(
                        row["relation_id"], field_name="relation_id"
                    ),
                    source_entity_id=_normalize_text(
                        row["source_entity_id"], field_name="source_entity_id"
                    ),
                    destination_entity_id=_normalize_text(
                        row["destination_entity_id"],
                        field_name="destination_entity_id",
                    ),
                    relationship=_normalize_text(
                        row["relationship"], field_name="relationship"
                    ),
                    score=float(scores[rank]),
                    source="faiss_embedding",
                )
            )
        return result

    async def _embed_texts(self, texts: Sequence[str]) -> np.ndarray:
        vectors: List[np.ndarray] = []
        for text in texts:
            embedding = await asyncio.to_thread(self._embedding_fn, text)
            vectors.append(_embedding_to_vector(embedding, field_name="embedding"))
        return np.asarray(vectors, dtype=np.float32)

    def _search_vectors(
        self,
        *,
        vectors: np.ndarray,
        query_vector: np.ndarray,
        top_k: int,
    ) -> Tuple[List[float], List[int]]:
        index = self._faiss.IndexFlatIP(int(vectors.shape[1]))
        index.add(vectors)
        query_matrix = np.asarray([query_vector], dtype=np.float32)
        search_limit = min(
            int(vectors.shape[0]),
            max(top_k, top_k * FAISS_TOPK_SEARCH_OVERSAMPLE),
        )
        score_matrix, position_matrix = index.search(query_matrix, search_limit)
        scores: List[float] = []
        positions: List[int] = []
        for rank in range(search_limit):
            position = int(position_matrix[0][rank])
            if position < 0:
                continue
            scores.append(float(score_matrix[0][rank]))
            positions.append(position)
            if len(positions) >= top_k:
                break
        return scores, positions


@dataclass(frozen=True)
class Mem0ExternalBundle:
    """Concrete Mem0 dependencies ready for workflow injection."""

    fact_store: Mem0FaissFactBackend
    fact_searcher: Mem0FaissFactBackend
    graph_store: Optional[Neo4jMem0GraphStore]
    graph_entity_searcher: Optional[Mem0GraphEmbeddingEntitySearcher]
    graph_relation_searcher: Optional[Mem0GraphEmbeddingRelationSearcher]


@dataclass
class Mem0ExternalClients:
    """Container for SDK clients used by Mem0 external adapters."""

    neo4j_driver: Optional[Any]

    def close(self) -> None:
        """Close all initialized clients."""
        if self.neo4j_driver is not None:
            close_method = getattr(self.neo4j_driver, "close", None)
            if callable(close_method):
                close_method()


def create_mem0_external_clients(
    *,
    backend_config: Mem0BackendConfig,
) -> Mem0ExternalClients:
    """Create concrete SDK clients from Mem0 external config."""
    neo4j_driver: Optional[Any] = None
    if backend_config.graph_store.enabled and backend_config.graph_store.provider in {
        "neo4j",
        "memgraph",
    }:
        try:
            from neo4j import GraphDatabase  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "neo4j package is required for mem0 graph_store neo4j/memgraph providers"
            ) from exc
        if backend_config.graph_store.url is None:
            raise ValueError("graph_store.url must be non-empty when graph_store is enabled")
        if backend_config.graph_store.username is None:
            raise ValueError(
                "graph_store.username must be non-empty when graph_store is enabled"
            )
        if backend_config.graph_store.password is None:
            raise ValueError(
                "graph_store.password must be non-empty when graph_store is enabled"
            )
        neo4j_driver = GraphDatabase.driver(
            backend_config.graph_store.url,
            auth=(
                backend_config.graph_store.username,
                backend_config.graph_store.password,
            ),
        )
    return Mem0ExternalClients(neo4j_driver=neo4j_driver)


def build_mem0_external_bundle(
    *,
    backend_config: Mem0BackendConfig,
    embedding_fn: Callable[[str], Sequence[float]],
    neo4j_driver: Optional[Any],
) -> Mem0ExternalBundle:
    """Build Mem0 runtime bundle from concrete clients and embedding function."""
    if embedding_fn is None:
        raise ValueError("embedding_fn must not be None")
    fact_backend = Mem0FaissFactBackend(embedding_fn=embedding_fn)

    graph_store: Optional[Neo4jMem0GraphStore] = None
    graph_entity_searcher: Optional[Mem0GraphEmbeddingEntitySearcher] = None
    graph_relation_searcher: Optional[Mem0GraphEmbeddingRelationSearcher] = None
    if backend_config.graph_store.enabled:
        if neo4j_driver is None:
            raise ValueError("neo4j_driver must not be None when graph_store is enabled")
        database = (
            backend_config.graph_store.database
            if backend_config.graph_store.database is not None
            else DEFAULT_MEM0_GRAPH_DATABASE
        )
        graph_store = Neo4jMem0GraphStore(
            driver=neo4j_driver,
            database=database,
        )
        graph_entity_searcher = Mem0GraphEmbeddingEntitySearcher(
            graph_store=graph_store,
            embedding_fn=embedding_fn,
        )
        graph_relation_searcher = Mem0GraphEmbeddingRelationSearcher(
            graph_store=graph_store,
            embedding_fn=embedding_fn,
        )

    return Mem0ExternalBundle(
        fact_store=fact_backend,
        fact_searcher=fact_backend,
        graph_store=graph_store,
        graph_entity_searcher=graph_entity_searcher,
        graph_relation_searcher=graph_relation_searcher,
    )
