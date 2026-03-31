"""External backend adapters for Zep/Graphiti workflow."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any, Sequence
from uuid import uuid4

from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepEdgeCandidate,
    ZepEntityCandidate,
    ZepEpisodeCandidate,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.external_config import (
    ZepBackendConfig,
)


def _as_int(value: Any, *, field_name: str) -> int:
    if not isinstance(value, int):
        raise TypeError(f"{field_name} must be int")
    return int(value)


def _as_float(value: Any, *, field_name: str) -> float:
    if not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric")
    return float(value)


def _as_str(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be str")
    if not value:
        raise ValueError(f"{field_name} must be non-empty")
    return value


class Neo4jZepGraphStore:
    """Neo4j-backed graph store adapter for Zep add-episode workflow."""

    def __init__(
        self,
        *,
        driver: Any,
        database: str,
        entity_name_index: str,
        edge_fact_index: str,
    ) -> None:
        self._driver = driver
        self._database = database
        self._entity_name_index = self._normalize_index_name(
            entity_name_index,
            field_name="entity_name_index",
        )
        self._edge_fact_index = self._normalize_index_name(
            edge_fact_index,
            field_name="edge_fact_index",
        )
        self._ensure_fulltext_indexes()

    def _normalize_index_name(self, value: str, *, field_name: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError(f"{field_name} must be non-empty")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", normalized):
            raise ValueError(
                f"{field_name} must match identifier pattern [A-Za-z_][A-Za-z0-9_]*"
            )
        return normalized

    def _ensure_fulltext_indexes(self) -> None:
        create_entity_index = (
            f"CREATE FULLTEXT INDEX `{self._entity_name_index}` IF NOT EXISTS "
            "FOR (n:Entity) ON EACH [n.name, n.summary]"
        )
        create_edge_index = (
            f"CREATE FULLTEXT INDEX `{self._edge_fact_index}` IF NOT EXISTS "
            "FOR ()-[r:RELATES_TO]-() ON EACH [r.fact, r.relation]"
        )
        with self._driver.session(database=self._database) as session:
            session.execute_write(
                lambda tx: list(tx.run(create_entity_index))
            )
            session.execute_write(
                lambda tx: list(tx.run(create_edge_index))
            )

    async def get_recent_episodes(
        self,
        *,
        group_id: str,
        limit: int,
    ) -> Sequence[ZepEpisodeCandidate]:
        """Return recent episodic nodes sorted by time descending."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if int(limit) <= 0:
            raise ValueError("limit must be > 0")
        return await asyncio.to_thread(
            self._get_recent_episodes_sync,
            group_id,
            int(limit),
        )

    async def upsert_episode(
        self,
        *,
        group_id: str,
        content: str,
        valid_at_ms: int,
    ) -> str:
        """Persist one episode node and return its id."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not str(content).strip():
            raise ValueError("content must be non-empty")
        if int(valid_at_ms) < 0:
            raise ValueError("valid_at_ms must be >= 0")
        episode_id = str(uuid4())
        return await asyncio.to_thread(
            self._upsert_episode_sync,
            group_id,
            content,
            int(valid_at_ms),
            episode_id,
        )

    async def search_entity_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[ZepEntityCandidate]:
        """Search candidate entity nodes via Neo4j fulltext index."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not str(query).strip():
            raise ValueError("query must be non-empty")
        if int(top_k) <= 0:
            raise ValueError("top_k must be > 0")
        return await asyncio.to_thread(
            self._search_entity_candidates_sync,
            group_id,
            query,
            int(top_k),
        )

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        type_id: str,
        summary: str,
        existing_entity_id: str | None,
    ) -> str:
        """Upsert one entity node and return resolved entity id."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not str(entity_name).strip():
            raise ValueError("entity_name must be non-empty")
        if not str(type_id).strip():
            raise ValueError("type_id must be non-empty")
        if not str(summary).strip():
            raise ValueError("summary must be non-empty")
        return await asyncio.to_thread(
            self._upsert_entity_sync,
            group_id,
            entity_name,
            type_id,
            summary,
            existing_entity_id,
        )

    async def search_edge_candidates(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        query_fact: str,
        top_k: int,
    ) -> Sequence[ZepEdgeCandidate]:
        """Search candidate edges via Neo4j fulltext index."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not source_entity_id:
            raise ValueError("source_entity_id must be non-empty")
        if not destination_entity_id:
            raise ValueError("destination_entity_id must be non-empty")
        if not str(query_fact).strip():
            raise ValueError("query_fact must be non-empty")
        if int(top_k) <= 0:
            raise ValueError("top_k must be > 0")
        return await asyncio.to_thread(
            self._search_edge_candidates_sync,
            group_id,
            source_entity_id,
            destination_entity_id,
            query_fact,
            int(top_k),
        )

    async def upsert_edge(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relation: str,
        fact: str,
        invalidates_edge_id: str | None,
    ) -> str:
        """Insert one edge and optionally mark one old edge invalidated."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not source_entity_id:
            raise ValueError("source_entity_id must be non-empty")
        if not destination_entity_id:
            raise ValueError("destination_entity_id must be non-empty")
        if not str(relation).strip():
            raise ValueError("relation must be non-empty")
        if not str(fact).strip():
            raise ValueError("fact must be non-empty")
        edge_id = str(uuid4())
        return await asyncio.to_thread(
            self._upsert_edge_sync,
            group_id,
            source_entity_id,
            destination_entity_id,
            relation,
            fact,
            invalidates_edge_id,
            edge_id,
        )

    def _get_recent_episodes_sync(self, group_id: str, limit: int) -> Sequence[ZepEpisodeCandidate]:
        query = """
            MATCH (e:Episodic {group_id: $group_id})
            RETURN
                e.uuid AS episode_id,
                e.content AS content,
                coalesce(e.valid_at_ms, e.created_at_ms, 0) AS created_at_ms
            ORDER BY created_at_ms DESC
            LIMIT $limit
        """
        with self._driver.session(database=self._database) as session:
            rows = session.execute_read(
                lambda tx: [
                    dict(record)
                    for record in tx.run(query, group_id=group_id, limit=limit)
                ]
            )
        return [
            ZepEpisodeCandidate(
                episode_id=_as_str(item["episode_id"], field_name="episode_id"),
                content=_as_str(item["content"], field_name="content"),
                created_at_ms=_as_int(item["created_at_ms"], field_name="created_at_ms"),
            )
            for item in rows
        ]

    def _upsert_episode_sync(
        self,
        group_id: str,
        content: str,
        valid_at_ms: int,
        episode_id: str,
    ) -> str:
        query = """
            CREATE (e:Episodic {
                uuid: $episode_id,
                group_id: $group_id,
                content: $content,
                valid_at_ms: $valid_at_ms,
                created_at_ms: timestamp()
            })
            RETURN e.uuid AS episode_id
        """
        with self._driver.session(database=self._database) as session:
            rows = session.execute_write(
                lambda tx: [
                    dict(record)
                    for record in tx.run(
                        query,
                        episode_id=episode_id,
                        group_id=group_id,
                        content=content,
                        valid_at_ms=valid_at_ms,
                    )
                ]
            )
        if not rows:
            raise RuntimeError("upsert_episode returned no row")
        return _as_str(rows[0]["episode_id"], field_name="episode_id")

    def _search_entity_candidates_sync(
        self,
        group_id: str,
        query_text: str,
        top_k: int,
    ) -> Sequence[ZepEntityCandidate]:
        query = """
            CALL db.index.fulltext.queryNodes($index_name, $query_text, {limit: $top_k})
            YIELD node, score
            WHERE node.group_id = $group_id
            RETURN
                node.uuid AS entity_id,
                node.name AS entity_name,
                node.summary AS summary,
                score AS score
            ORDER BY score DESC
            LIMIT $top_k
        """
        with self._driver.session(database=self._database) as session:
            rows = session.execute_read(
                lambda tx: [
                    dict(record)
                    for record in tx.run(
                        query,
                        index_name=self._entity_name_index,
                        query_text=query_text,
                        top_k=top_k,
                        group_id=group_id,
                    )
                ]
            )
        return [
            ZepEntityCandidate(
                entity_id=_as_str(item["entity_id"], field_name="entity_id"),
                entity_name=_as_str(item["entity_name"], field_name="entity_name"),
                summary=_as_str(item["summary"], field_name="summary"),
                score=_as_float(item["score"], field_name="score"),
                source="neo4j_fulltext",
            )
            for item in rows
        ]

    def _upsert_entity_sync(
        self,
        group_id: str,
        entity_name: str,
        type_id: str,
        summary: str,
        existing_entity_id: str | None,
    ) -> str:
        if existing_entity_id is not None:
            query = """
                MATCH (n:Entity {uuid: $existing_entity_id, group_id: $group_id})
                SET n.name = $entity_name,
                    n.type_id = $type_id,
                    n.summary = $summary,
                    n.updated_at_ms = timestamp()
                RETURN n.uuid AS entity_id
            """
            params = {
                "existing_entity_id": existing_entity_id,
                "group_id": group_id,
                "entity_name": entity_name,
                "type_id": type_id,
                "summary": summary,
            }
        else:
            new_entity_id = str(uuid4())
            query = """
                CREATE (n:Entity {
                    uuid: $entity_id,
                    group_id: $group_id,
                    name: $entity_name,
                    type_id: $type_id,
                    summary: $summary,
                    created_at_ms: timestamp(),
                    updated_at_ms: timestamp()
                })
                RETURN n.uuid AS entity_id
            """
            params = {
                "entity_id": new_entity_id,
                "group_id": group_id,
                "entity_name": entity_name,
                "type_id": type_id,
                "summary": summary,
            }

        with self._driver.session(database=self._database) as session:
            rows = session.execute_write(
                lambda tx: [dict(record) for record in tx.run(query, **params)]
            )
        if not rows:
            raise RuntimeError("upsert_entity returned no row")
        return _as_str(rows[0]["entity_id"], field_name="entity_id")

    def _search_edge_candidates_sync(
        self,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        query_fact: str,
        top_k: int,
    ) -> Sequence[ZepEdgeCandidate]:
        query = """
            CALL db.index.fulltext.queryRelationships($index_name, $query_text, {limit: $top_k})
            YIELD relationship, score
            WITH relationship AS r, score
            MATCH (src:Entity {uuid: $source_entity_id, group_id: $group_id})-[r]->(dst:Entity {uuid: $destination_entity_id, group_id: $group_id})
            RETURN
                r.uuid AS edge_id,
                src.uuid AS source_entity_id,
                dst.uuid AS destination_entity_id,
                r.relation AS relation,
                r.fact AS fact,
                score AS score
            ORDER BY score DESC
            LIMIT $top_k
        """
        with self._driver.session(database=self._database) as session:
            rows = session.execute_read(
                lambda tx: [
                    dict(record)
                    for record in tx.run(
                        query,
                        index_name=self._edge_fact_index,
                        query_text=query_fact,
                        top_k=top_k,
                        group_id=group_id,
                        source_entity_id=source_entity_id,
                        destination_entity_id=destination_entity_id,
                    )
                ]
            )
        return [
            ZepEdgeCandidate(
                edge_id=_as_str(item["edge_id"], field_name="edge_id"),
                source_entity_id=_as_str(
                    item["source_entity_id"],
                    field_name="source_entity_id",
                ),
                destination_entity_id=_as_str(
                    item["destination_entity_id"],
                    field_name="destination_entity_id",
                ),
                relation=_as_str(item["relation"], field_name="relation"),
                fact=_as_str(item["fact"], field_name="fact"),
                score=_as_float(item["score"], field_name="score"),
                source="neo4j_fulltext",
            )
            for item in rows
        ]

    def _upsert_edge_sync(
        self,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relation: str,
        fact: str,
        invalidates_edge_id: str | None,
        edge_id: str,
    ) -> str:
        if invalidates_edge_id is not None:
            invalidate_query = """
                MATCH ()-[r:RELATES_TO {uuid: $invalidates_edge_id}]->()
                SET r.invalidated_at_ms = timestamp()
            """
            with self._driver.session(database=self._database) as session:
                session.execute_write(
                    lambda tx: list(
                        tx.run(
                            invalidate_query,
                            invalidates_edge_id=invalidates_edge_id,
                        )
                    )
                )

        insert_query = """
            MATCH (src:Entity {uuid: $source_entity_id, group_id: $group_id})
            MATCH (dst:Entity {uuid: $destination_entity_id, group_id: $group_id})
            CREATE (src)-[r:RELATES_TO {
                uuid: $edge_id,
                group_id: $group_id,
                relation: $relation,
                fact: $fact,
                created_at_ms: timestamp()
            }]->(dst)
            RETURN r.uuid AS edge_id
        """
        with self._driver.session(database=self._database) as session:
            rows = session.execute_write(
                lambda tx: [
                    dict(record)
                    for record in tx.run(
                        insert_query,
                        edge_id=edge_id,
                        group_id=group_id,
                        source_entity_id=source_entity_id,
                        destination_entity_id=destination_entity_id,
                        relation=relation,
                        fact=fact,
                    )
                ]
            )
        if not rows:
            raise RuntimeError("upsert_edge returned no row")
        return _as_str(rows[0]["edge_id"], field_name="edge_id")


@dataclass(frozen=True)
class ZepExternalBundle:
    """Concrete external adapters required by Zep workflow."""

    graph_store: Neo4jZepGraphStore


@dataclass(frozen=True)
class ZepExternalClients:
    """Live SDK clients for Zep external backends."""

    neo4j_driver: Any

    def close(self) -> None:
        """Close initialized clients."""
        close_method = getattr(self.neo4j_driver, "close", None)
        if callable(close_method):
            close_method()


def create_zep_external_clients(*, backend_config: ZepBackendConfig) -> ZepExternalClients:
    """Create concrete Neo4j SDK client from config."""
    try:
        from neo4j import GraphDatabase  # type: ignore
    except ImportError as exc:
        raise ImportError("neo4j package is required to create Zep external clients") from exc

    driver = GraphDatabase.driver(
        backend_config.graph.uri,
        auth=(backend_config.graph.username, backend_config.graph.password),
    )
    return ZepExternalClients(neo4j_driver=driver)


def build_zep_external_bundle(
    *,
    backend_config: ZepBackendConfig,
    neo4j_driver: Any,
) -> ZepExternalBundle:
    """Build Zep external bundle from initialized clients and config."""
    if neo4j_driver is None:
        raise ValueError("neo4j_driver must not be None")
    graph_store = Neo4jZepGraphStore(
        driver=neo4j_driver,
        database=backend_config.graph.database,
        entity_name_index=backend_config.graph.entity_name_index,
        edge_fact_index=backend_config.graph.edge_fact_index,
    )
    return ZepExternalBundle(graph_store=graph_store)
