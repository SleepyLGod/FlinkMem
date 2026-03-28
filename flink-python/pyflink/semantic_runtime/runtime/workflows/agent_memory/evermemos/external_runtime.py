"""External backend adapters for EverMemOS workflow.

This module provides concrete adapters that implement agent-memory workflow
protocols using MongoDB as source-of-truth plus Elasticsearch/Milvus indexes.
All backend calls are executed via ``asyncio.to_thread`` to keep workflow
interfaces async without requiring async database SDKs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    ConversationMessage,
    DecompositionArtifacts,
    EventLogArtifact,
    ForesightArtifact,
    MemCellRecord,
    RetrievedMemory,
    TopicClusterState,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.external_config import (
    EverMemOSBackendConfig,
)


DEFAULT_ES_SCORE_FLOOR = 0.0
DEFAULT_MILVUS_SCORE_FLOOR = 0.0
DEFAULT_VECTOR_COLLECTION_MEMORY_TYPE: Dict[str, str] = {
    "episode": "episodic_memories",
    "foresight": "foresight_records",
    "event_log": "event_log_records",
}
DEFAULT_KEYWORD_INDEX_MEMORY_TYPE: Dict[str, str] = {
    "episode": "episodic_memories",
    "foresight": "foresight_records",
    "event_log": "event_log_records",
}
DEFAULT_MILVUS_OUTPUT_FIELDS: Tuple[str, ...] = (
    "memory_id",
    "group_id",
    "content",
    "timestamp_ms",
    "memory_type",
)


def _normalize_int(value: Any, *, field_name: str) -> int:
    if not isinstance(value, int):
        raise TypeError(f"{field_name} must be int")
    return int(value)


def _normalize_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None:
        return None
    return _normalize_int(value, field_name=field_name)


def _normalize_optional_str(value: Any, *, field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be str")
    return value


def _message_to_doc(message: ConversationMessage) -> Dict[str, Any]:
    return {
        "_id": message.message_id,
        "message_id": message.message_id,
        "group_id": message.group_id,
        "sender_id": message.sender_id,
        "sender_name": message.sender_name,
        "role": message.role,
        "content": message.content,
        "timestamp_ms": int(message.timestamp_ms),
        "metadata": dict(message.metadata),
    }


def _doc_to_message(doc: Mapping[str, Any]) -> ConversationMessage:
    return ConversationMessage(
        message_id=str(doc.get("message_id") or doc.get("_id") or ""),
        group_id=str(doc.get("group_id") or ""),
        sender_id=str(doc.get("sender_id") or ""),
        sender_name=_normalize_optional_str(doc.get("sender_name"), field_name="sender_name"),
        role=str(doc.get("role") or "user"),
        content=str(doc.get("content") or ""),
        timestamp_ms=_normalize_int(doc.get("timestamp_ms"), field_name="timestamp_ms"),
        metadata=dict(doc.get("metadata") or {}),
    )


def _memcell_to_doc(memcell: MemCellRecord) -> Dict[str, Any]:
    return {
        "_id": memcell.memcell_id,
        "memcell_id": memcell.memcell_id,
        "group_id": memcell.group_id,
        "timestamp_ms": int(memcell.timestamp_ms),
        "original_messages": [_message_to_doc(item) for item in memcell.original_messages],
        "participants": list(memcell.participants),
        "scene": memcell.scene,
        "summary": memcell.summary,
        "subject": memcell.subject,
        "episode": memcell.episode,
        "topic_id": memcell.topic_id,
    }


def _doc_to_memcell(doc: Mapping[str, Any]) -> MemCellRecord:
    original_messages_raw = doc.get("original_messages")
    if not isinstance(original_messages_raw, list):
        raise TypeError("memcell.original_messages must be list")
    participants_raw = doc.get("participants")
    if not isinstance(participants_raw, list):
        raise TypeError("memcell.participants must be list")
    return MemCellRecord(
        memcell_id=str(doc.get("memcell_id") or doc.get("_id") or ""),
        group_id=str(doc.get("group_id") or ""),
        timestamp_ms=_normalize_int(doc.get("timestamp_ms"), field_name="memcell.timestamp_ms"),
        original_messages=[_doc_to_message(item) for item in original_messages_raw],
        participants=[str(item) for item in participants_raw],
        scene=str(doc.get("scene") or ""),
        summary=_normalize_optional_str(doc.get("summary"), field_name="memcell.summary"),
        subject=_normalize_optional_str(doc.get("subject"), field_name="memcell.subject"),
        episode=_normalize_optional_str(doc.get("episode"), field_name="memcell.episode"),
        topic_id=_normalize_optional_str(doc.get("topic_id"), field_name="memcell.topic_id"),
    )


def _topic_state_to_doc(group_id: str, state: TopicClusterState) -> Dict[str, Any]:
    return {
        "_id": group_id,
        "group_id": group_id,
        "event_ids": list(state.event_ids),
        "eventid_to_topic": dict(state.eventid_to_topic),
        "topic_centroids": {topic_id: list(values) for topic_id, values in state.topic_centroids.items()},
        "topic_representatives": dict(state.topic_representatives),
        "topic_counts": {topic_id: int(count) for topic_id, count in state.topic_counts.items()},
        "topic_last_ts": {topic_id: int(ts) for topic_id, ts in state.topic_last_ts.items()},
        "next_topic_idx": int(state.next_topic_idx),
    }


def _doc_to_topic_state(doc: Mapping[str, Any]) -> TopicClusterState:
    event_ids_raw = doc.get("event_ids", [])
    if not isinstance(event_ids_raw, list):
        raise TypeError("cluster_state.event_ids must be list")
    eventid_to_topic_raw = doc.get("eventid_to_topic", {})
    if not isinstance(eventid_to_topic_raw, dict):
        raise TypeError("cluster_state.eventid_to_topic must be dict")
    topic_centroids_raw = doc.get("topic_centroids", {})
    if not isinstance(topic_centroids_raw, dict):
        raise TypeError("cluster_state.topic_centroids must be dict")
    topic_representatives_raw = doc.get("topic_representatives", {})
    if not isinstance(topic_representatives_raw, dict):
        raise TypeError("cluster_state.topic_representatives must be dict")
    topic_counts_raw = doc.get("topic_counts", {})
    if not isinstance(topic_counts_raw, dict):
        raise TypeError("cluster_state.topic_counts must be dict")
    topic_last_ts_raw = doc.get("topic_last_ts", {})
    if not isinstance(topic_last_ts_raw, dict):
        raise TypeError("cluster_state.topic_last_ts must be dict")
    return TopicClusterState(
        event_ids=[str(item) for item in event_ids_raw],
        eventid_to_topic={str(key): str(value) for key, value in eventid_to_topic_raw.items()},
        topic_centroids={
            str(key): [float(item) for item in values]
            for key, values in topic_centroids_raw.items()
        },
        topic_representatives={str(key): str(value) for key, value in topic_representatives_raw.items()},
        topic_counts={str(key): int(value) for key, value in topic_counts_raw.items()},
        topic_last_ts={str(key): int(value) for key, value in topic_last_ts_raw.items()},
        next_topic_idx=int(doc.get("next_topic_idx", 0)),
    )


class MongoConversationStatusStore:
    """MongoDB-backed implementation of conversation status persistence."""

    def __init__(self, *, collection: Any) -> None:
        self._collection = collection

    async def get_last_memcell_time_ms(self, group_id: str) -> Optional[int]:
        """Return last sealed memcell timestamp for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        doc = await asyncio.to_thread(self._collection.find_one, {"_id": group_id})
        if doc is None:
            return None
        value = doc.get("last_memcell_time_ms")
        if value is None:
            return None
        return _normalize_int(value, field_name="last_memcell_time_ms")

    async def update_last_memcell_time_ms(self, group_id: str, timestamp_ms: int) -> None:
        """Persist last sealed memcell timestamp for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        _normalize_int(timestamp_ms, field_name="timestamp_ms")
        await asyncio.to_thread(
            self._collection.update_one,
            {"_id": group_id},
            {"$set": {"group_id": group_id, "last_memcell_time_ms": int(timestamp_ms)}},
            True,
        )


class MongoConversationBufferStore:
    """MongoDB-backed implementation of pre-boundary message buffer."""

    def __init__(self, *, collection: Any) -> None:
        self._collection = collection

    async def load_messages_since(
        self,
        group_id: str,
        start_time_ms: Optional[int],
    ) -> Sequence[ConversationMessage]:
        """Load buffered messages since one timestamp (inclusive)."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        query: Dict[str, Any] = {"group_id": group_id}
        if start_time_ms is not None:
            query["timestamp_ms"] = {"$gte": int(start_time_ms)}
        cursor = await asyncio.to_thread(self._collection.find, query)
        docs = await asyncio.to_thread(list, cursor)
        docs.sort(key=lambda item: (int(item["timestamp_ms"]), str(item.get("_id", ""))))
        return [_doc_to_message(item) for item in docs]

    async def append_messages(
        self,
        group_id: str,
        messages: Sequence[ConversationMessage],
    ) -> None:
        """Append one message batch into the buffer collection."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        docs = []
        for message in messages:
            if message.group_id != group_id:
                raise ValueError("message.group_id mismatch in append_messages")
            docs.append(_message_to_doc(message))
        if not docs:
            return
        await asyncio.to_thread(self._collection.insert_many, docs)

    async def clear_consumed_messages(
        self,
        group_id: str,
        consumed_message_ids: Sequence[str],
    ) -> None:
        """Delete consumed buffered messages after one sealed boundary."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        ids = [str(item) for item in consumed_message_ids if str(item)]
        if not ids:
            return
        await asyncio.to_thread(
            self._collection.delete_many,
            {"group_id": group_id, "_id": {"$in": ids}},
        )


class MongoMemCellStore:
    """MongoDB-backed implementation of MemCell persistence."""

    def __init__(self, *, collection: Any) -> None:
        self._collection = collection

    async def create_memcell(self, memcell: MemCellRecord) -> str:
        """Insert one MemCell and return persisted id."""
        doc = _memcell_to_doc(memcell)
        result = await asyncio.to_thread(self._collection.insert_one, doc)
        inserted = getattr(result, "inserted_id", None)
        if inserted is None:
            return memcell.memcell_id
        return str(inserted)

    async def update_memcell_fields(
        self,
        memcell_id: str,
        *,
        summary: Optional[str],
        subject: Optional[str],
        episode: Optional[str],
        topic_id: Optional[str],
    ) -> None:
        """Update semantic fields for one existing MemCell."""
        if not memcell_id:
            raise ValueError("memcell_id must be non-empty")
        update_doc = {
            "summary": summary,
            "subject": subject,
            "episode": episode,
            "topic_id": topic_id,
        }
        result = await asyncio.to_thread(
            self._collection.update_one,
            {"_id": memcell_id},
            {"$set": update_doc},
            False,
        )
        matched = int(getattr(result, "matched_count", 0))
        if matched <= 0:
            raise KeyError(f"unknown memcell_id={memcell_id}")

    async def list_memcells_by_topic(
        self,
        group_id: str,
        topic_id: str,
    ) -> Sequence[MemCellRecord]:
        """Load MemCells by topic, ordered by timestamp ascending."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not topic_id:
            raise ValueError("topic_id must be non-empty")
        cursor = await asyncio.to_thread(
            self._collection.find,
            {"group_id": group_id, "topic_id": topic_id},
        )
        docs = await asyncio.to_thread(list, cursor)
        docs.sort(key=lambda item: (int(item["timestamp_ms"]), str(item.get("_id", ""))))
        return [_doc_to_memcell(item) for item in docs]

    async def get_memcell_by_id(self, memcell_id: str) -> MemCellRecord:
        """Load one MemCell by id."""
        if not memcell_id:
            raise ValueError("memcell_id must be non-empty")
        doc = await asyncio.to_thread(self._collection.find_one, {"_id": memcell_id})
        if doc is None:
            raise KeyError(f"unknown memcell_id={memcell_id}")
        return _doc_to_memcell(doc)


class MongoTopicStateStore:
    """MongoDB-backed implementation of topic state persistence."""

    def __init__(self, *, collection: Any) -> None:
        self._collection = collection

    async def load_state(self, group_id: str) -> TopicClusterState:
        """Load topic state for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        doc = await asyncio.to_thread(self._collection.find_one, {"_id": group_id})
        if doc is None:
            return TopicClusterState()
        return _doc_to_topic_state(doc)

    async def save_state(self, group_id: str, state: TopicClusterState) -> None:
        """Persist topic state for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        doc = _topic_state_to_doc(group_id, state)
        await asyncio.to_thread(
            self._collection.update_one,
            {"_id": group_id},
            {"$set": doc},
            True,
        )


class MongoProfileStore:
    """MongoDB-backed implementation of profile persistence."""

    def __init__(self, *, collection: Any) -> None:
        self._collection = collection

    async def load_profiles(self, group_id: str) -> Mapping[str, Any]:
        """Load profile mapping for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        doc = await asyncio.to_thread(self._collection.find_one, {"_id": group_id})
        if doc is None:
            return {}
        profiles = doc.get("profiles", {})
        if not isinstance(profiles, dict):
            raise TypeError("profiles must be dict")
        return dict(profiles)

    async def save_profiles(self, group_id: str, profiles: Mapping[str, Any]) -> None:
        """Persist profile mapping for one group."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        await asyncio.to_thread(
            self._collection.update_one,
            {"_id": group_id},
            {"$set": {"group_id": group_id, "profiles": dict(profiles)}},
            True,
        )


class EverMemOSArtifactStore:
    """Mongo + optional ES/Milvus implementation for artifact persistence and sync."""

    def __init__(
        self,
        *,
        memcell_store: MongoMemCellStore,
        episodic_collection: Any,
        foresight_collection: Any,
        event_log_collection: Any,
        elasticsearch_client: Optional[Any],
        milvus_client: Optional[Any],
        backend_config: EverMemOSBackendConfig,
        embedding_fn: Callable[[str], Sequence[float]],
    ) -> None:
        self._memcell_store = memcell_store
        self._episodic_collection = episodic_collection
        self._foresight_collection = foresight_collection
        self._event_log_collection = event_log_collection
        self._elasticsearch_client = elasticsearch_client
        self._milvus_client = milvus_client
        self._backend_config = backend_config
        self._embedding_fn = embedding_fn

    async def persist_decomposition(
        self,
        *,
        group_id: str,
        memcell_id: str,
        scene: str,
        artifacts: DecompositionArtifacts,
    ) -> None:
        """Persist decomposition artifacts into Mongo collections."""
        _ = scene
        memcell = await self._memcell_store.get_memcell_by_id(memcell_id)
        episode_doc = {
            "_id": memcell_id,
            "memory_id": memcell_id,
            "memory_type": "episode",
            "group_id": group_id,
            "memcell_id": memcell_id,
            "content": artifacts.episode,
            "subject": artifacts.subject,
            "summary": memcell.summary,
            "timestamp_ms": int(memcell.timestamp_ms),
            "participants": list(memcell.participants),
            "scene": memcell.scene,
        }
        await asyncio.to_thread(
            self._episodic_collection.update_one,
            {"_id": memcell_id},
            {"$set": episode_doc},
            True,
        )
        for index, item in enumerate(artifacts.foresights):
            document_id = f"{memcell_id}:foresight:{index:03d}"
            doc = self._foresight_doc(
                document_id=document_id,
                group_id=group_id,
                memcell_id=memcell_id,
                timestamp_ms=int(memcell.timestamp_ms),
                value=item,
            )
            await asyncio.to_thread(
                self._foresight_collection.update_one,
                {"_id": document_id},
                {"$set": doc},
                True,
            )
        for index, item in enumerate(artifacts.event_logs):
            document_id = f"{memcell_id}:event_log:{index:03d}"
            doc = self._event_log_doc(
                document_id=document_id,
                group_id=group_id,
                memcell_id=memcell_id,
                timestamp_ms=int(memcell.timestamp_ms),
                value=item,
            )
            await asyncio.to_thread(
                self._event_log_collection.update_one,
                {"_id": document_id},
                {"$set": doc},
                True,
            )

    async def sync_indexes(self, *, group_id: str, memcell_id: str) -> None:
        """Sync one memcell's artifacts from Mongo to Elasticsearch and Milvus."""
        episode = await asyncio.to_thread(self._episodic_collection.find_one, {"_id": memcell_id})
        if episode is None:
            raise KeyError(f"missing episode artifact for memcell_id={memcell_id}")
        foresight_cursor = await asyncio.to_thread(
            self._foresight_collection.find,
            {"group_id": group_id, "memcell_id": memcell_id},
        )
        foresights = await asyncio.to_thread(list, foresight_cursor)
        event_log_cursor = await asyncio.to_thread(
            self._event_log_collection.find,
            {"group_id": group_id, "memcell_id": memcell_id},
        )
        event_logs = await asyncio.to_thread(list, event_log_cursor)
        docs = [episode] + foresights + event_logs
        if self._backend_config.elasticsearch.enabled:
            if self._elasticsearch_client is None:
                raise RuntimeError("elasticsearch is enabled but no client was provided")
            await self._sync_to_elasticsearch(docs)
        if self._backend_config.milvus.enabled:
            if self._milvus_client is None:
                raise RuntimeError("milvus is enabled but no client was provided")
            await self._sync_to_milvus(docs)

    def _foresight_doc(
        self,
        *,
        document_id: str,
        group_id: str,
        memcell_id: str,
        timestamp_ms: int,
        value: ForesightArtifact,
    ) -> Dict[str, Any]:
        content = value.content
        return {
            "_id": document_id,
            "memory_id": document_id,
            "memory_type": "foresight",
            "group_id": group_id,
            "memcell_id": memcell_id,
            "content": str(content),
            "timestamp_ms": int(timestamp_ms),
            "evidence": value.evidence,
            "start_time": value.start_time,
            "end_time": value.end_time,
            "duration_days": value.duration_days,
        }

    def _event_log_doc(
        self,
        *,
        document_id: str,
        group_id: str,
        memcell_id: str,
        timestamp_ms: int,
        value: EventLogArtifact,
    ) -> Dict[str, Any]:
        return {
            "_id": document_id,
            "memory_id": document_id,
            "memory_type": "event_log",
            "group_id": group_id,
            "memcell_id": memcell_id,
            "content": value.atomic_fact,
            "timestamp_ms": value.timestamp_ms if value.timestamp_ms is not None else timestamp_ms,
        }

    async def _sync_to_elasticsearch(self, docs: Sequence[Mapping[str, Any]]) -> None:
        index_map = {
            "episode": self._backend_config.elasticsearch.indexes.episodic_memories,
            "foresight": self._backend_config.elasticsearch.indexes.foresight_records,
            "event_log": self._backend_config.elasticsearch.indexes.event_log_records,
        }
        for doc in docs:
            memory_type = str(doc.get("memory_type") or "")
            index_name = index_map.get(memory_type)
            if index_name is None:
                raise ValueError(f"unknown memory_type for elasticsearch sync: {memory_type!r}")
            payload = dict(doc)
            payload.pop("_id", None)
            await asyncio.to_thread(
                self._elasticsearch_client.index,
                index=index_name,
                id=str(doc["memory_id"]),
                document=payload,
                refresh=True,
            )

    async def _sync_to_milvus(self, docs: Sequence[Mapping[str, Any]]) -> None:
        collection_map = {
            "episode": self._backend_config.milvus.collections.episodic_memories,
            "foresight": self._backend_config.milvus.collections.foresight_records,
            "event_log": self._backend_config.milvus.collections.event_log_records,
        }
        for doc in docs:
            memory_type = str(doc.get("memory_type") or "")
            collection_name = collection_map.get(memory_type)
            if collection_name is None:
                raise ValueError(f"unknown memory_type for milvus sync: {memory_type!r}")
            embedding = [float(item) for item in self._embedding_fn(str(doc["content"]))]
            if len(embedding) != self._backend_config.milvus.embedding_dim:
                raise ValueError(
                    "embedding dimension mismatch for milvus sync: "
                    f"expected {self._backend_config.milvus.embedding_dim}, got {len(embedding)}"
                )
            row = {
                "memory_id": str(doc["memory_id"]),
                "group_id": str(doc["group_id"]),
                "memcell_id": str(doc["memcell_id"]),
                "memory_type": memory_type,
                "content": str(doc["content"]),
                "timestamp_ms": int(doc["timestamp_ms"]),
                "embedding": embedding,
            }
            await asyncio.to_thread(
                self._milvus_client.upsert,
                collection_name=collection_name,
                data=[row],
            )


class EverMemOSElasticsearchSearcher:
    """Keyword search adapter over Elasticsearch artifact indexes."""

    def __init__(
        self,
        *,
        client: Any,
        backend_config: EverMemOSBackendConfig,
        score_floor: float = DEFAULT_ES_SCORE_FLOOR,
        memory_type_index_map: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._client = client
        self._config = backend_config
        self._score_floor = float(score_floor)
        self._memory_type_index_map = dict(memory_type_index_map or DEFAULT_KEYWORD_INDEX_MEMORY_TYPE)

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        """Run BM25-like keyword search in Elasticsearch."""
        if not self._config.elasticsearch.enabled:
            raise RuntimeError("elasticsearch searcher cannot run when elasticsearch is disabled")
        index_names = self._resolve_indexes(memory_types)
        result = await asyncio.to_thread(
            self._client.search,
            index=index_names,
            size=int(top_k),
            query={
                "bool": {
                    "must": [{"match": {"content": query}}],
                    "filter": [{"term": {"group_id": group_id}}],
                }
            },
        )
        hits = result.get("hits", {}).get("hits", [])
        rows: List[RetrievedMemory] = []
        for hit in hits:
            source = hit.get("_source", {})
            score = float(hit.get("_score", 0.0))
            if score < self._score_floor:
                continue
            rows.append(
                RetrievedMemory(
                    memory_id=str(source.get("memory_id") or hit.get("_id") or ""),
                    memory_type=str(source.get("memory_type") or ""),
                    content=str(source.get("content") or ""),
                    score=score,
                    source="keyword",
                    timestamp_ms=_normalize_optional_int(source.get("timestamp_ms"), field_name="timestamp_ms"),
                    metadata={"index": hit.get("_index")},
                )
            )
        return rows

    def _resolve_indexes(self, memory_types: Optional[Sequence[str]]) -> List[str]:
        if memory_types is None:
            return list(dict.fromkeys(self._memory_type_index_map.values()))
        indexes: List[str] = []
        for memory_type in memory_types:
            index_name = self._memory_type_index_map.get(str(memory_type))
            if index_name is None:
                raise ValueError(f"unsupported memory_type for keyword search: {memory_type!r}")
            indexes.append(index_name)
        return list(dict.fromkeys(indexes))


class EverMemOSMilvusSearcher:
    """Vector search adapter over Milvus artifact collections."""

    def __init__(
        self,
        *,
        client: Any,
        backend_config: EverMemOSBackendConfig,
        embedding_fn: Callable[[str], Sequence[float]],
        output_fields: Sequence[str] = DEFAULT_MILVUS_OUTPUT_FIELDS,
        score_floor: float = DEFAULT_MILVUS_SCORE_FLOOR,
        memory_type_collection_map: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._client = client
        self._config = backend_config
        self._embedding_fn = embedding_fn
        self._output_fields = list(output_fields)
        self._score_floor = float(score_floor)
        self._memory_type_collection_map = dict(
            memory_type_collection_map or DEFAULT_VECTOR_COLLECTION_MEMORY_TYPE
        )

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        """Run vector similarity search in Milvus across selected collections."""
        if not self._config.milvus.enabled:
            raise RuntimeError("milvus searcher cannot run when milvus is disabled")
        query_vector = [float(item) for item in self._embedding_fn(query)]
        if len(query_vector) != self._config.milvus.embedding_dim:
            raise ValueError(
                "query embedding dimension mismatch: "
                f"expected {self._config.milvus.embedding_dim}, got {len(query_vector)}"
            )
        collection_names = self._resolve_collections(memory_types)
        all_rows: List[RetrievedMemory] = []
        for collection_name in collection_names:
            result = await asyncio.to_thread(
                self._client.search,
                collection_name=collection_name,
                data=[query_vector],
                filter=f'group_id == "{group_id}"',
                limit=int(top_k),
                output_fields=list(self._output_fields),
            )
            hits = result[0] if result else []
            for hit in hits:
                row = self._hit_to_memory(hit)
                if row.score < self._score_floor:
                    continue
                all_rows.append(row)
        all_rows.sort(key=lambda item: float(item.score), reverse=True)
        return all_rows[:top_k]

    def _resolve_collections(self, memory_types: Optional[Sequence[str]]) -> List[str]:
        if memory_types is None:
            return list(dict.fromkeys(self._memory_type_collection_map.values()))
        collections: List[str] = []
        for memory_type in memory_types:
            collection_name = self._memory_type_collection_map.get(str(memory_type))
            if collection_name is None:
                raise ValueError(f"unsupported memory_type for vector search: {memory_type!r}")
            collections.append(collection_name)
        return list(dict.fromkeys(collections))

    def _hit_to_memory(self, hit: Mapping[str, Any]) -> RetrievedMemory:
        entity = hit.get("entity")
        if not isinstance(entity, dict):
            raise ValueError("milvus search hit must include entity dict")
        raw_score = hit.get("distance")
        score = float(raw_score if raw_score is not None else 0.0)
        return RetrievedMemory(
            memory_id=str(entity.get("memory_id") or ""),
            memory_type=str(entity.get("memory_type") or ""),
            content=str(entity.get("content") or ""),
            score=score,
            source="vector",
            timestamp_ms=_normalize_optional_int(entity.get("timestamp_ms"), field_name="timestamp_ms"),
            metadata={},
        )


@dataclass(frozen=True)
class EverMemOSExternalBundle:
    """Concrete external adapters required by EverMemOS insertion/retrieval workflows."""

    conversation_status_store: MongoConversationStatusStore
    conversation_buffer_store: MongoConversationBufferStore
    memcell_store: MongoMemCellStore
    memory_artifact_store: EverMemOSArtifactStore
    topic_state_store: MongoTopicStateStore
    profile_store: MongoProfileStore
    keyword_searcher: EverMemOSElasticsearchSearcher
    vector_searcher: EverMemOSMilvusSearcher


@dataclass(frozen=True)
class EverMemOSExternalClients:
    """Live SDK clients for EverMemOS external backends."""

    mongo_client: Any
    mongo_database: Any
    elasticsearch_client: Optional[Any]
    milvus_client: Optional[Any]

    def close(self) -> None:
        """Close all initialized clients."""
        close_method = getattr(self.mongo_client, "close", None)
        if callable(close_method):
            close_method()
        if self.elasticsearch_client is not None:
            close_method = getattr(self.elasticsearch_client, "close", None)
            if callable(close_method):
                close_method()
        if self.milvus_client is not None:
            close_method = getattr(self.milvus_client, "close", None)
            if callable(close_method):
                close_method()


def create_evermemos_external_clients(
    *,
    backend_config: EverMemOSBackendConfig,
) -> EverMemOSExternalClients:
    """Create concrete Mongo/Elasticsearch/Milvus SDK clients from config."""
    try:
        from pymongo import MongoClient  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "pymongo is required to create EverMemOS external Mongo client"
        ) from exc

    mongo_client = MongoClient(backend_config.mongo.uri)
    mongo_database = mongo_client[backend_config.mongo.database]

    elasticsearch_client: Optional[Any] = None
    if backend_config.elasticsearch.enabled:
        try:
            from elasticsearch import Elasticsearch  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "elasticsearch package is required when EVERMEMOS_ES_ENABLED=true"
            ) from exc
        elasticsearch_client = Elasticsearch(
            hosts=list(backend_config.elasticsearch.hosts),
            basic_auth=(
                (
                    backend_config.elasticsearch.username,
                    backend_config.elasticsearch.password,
                )
                if (
                    backend_config.elasticsearch.username is not None
                    and backend_config.elasticsearch.password is not None
                )
                else None
            ),
            verify_certs=backend_config.elasticsearch.verify_certs,
        )

    milvus_client: Optional[Any] = None
    if backend_config.milvus.enabled:
        try:
            from pymilvus import MilvusClient  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "pymilvus is required when EVERMEMOS_MILVUS_ENABLED=true"
            ) from exc
        milvus_client = MilvusClient(
            uri=backend_config.milvus.uri,
            token=backend_config.milvus.token,
            db_name=backend_config.milvus.database,
        )

    return EverMemOSExternalClients(
        mongo_client=mongo_client,
        mongo_database=mongo_database,
        elasticsearch_client=elasticsearch_client,
        milvus_client=milvus_client,
    )


def build_evermemos_external_bundle(
    *,
    mongo_database: Any,
    backend_config: EverMemOSBackendConfig,
    elasticsearch_client: Optional[Any],
    milvus_client: Optional[Any],
    embedding_fn: Callable[[str], Sequence[float]],
) -> EverMemOSExternalBundle:
    """Build concrete EverMemOS backend adapters from external clients.

    Args:
        mongo_database: Mongo database handle implementing ``__getitem__`` for collections.
        backend_config: Parsed backend config.
        elasticsearch_client: Elasticsearch client when ES is enabled.
        milvus_client: Milvus client when Milvus is enabled.
        embedding_fn: Text-to-vector function used for Milvus write/search.
    """
    if mongo_database is None:
        raise ValueError("mongo_database must not be None")
    if backend_config.elasticsearch.enabled and elasticsearch_client is None:
        raise ValueError("elasticsearch_client is required when elasticsearch is enabled")
    if backend_config.milvus.enabled and milvus_client is None:
        raise ValueError("milvus_client is required when milvus is enabled")
    if embedding_fn is None:
        raise ValueError("embedding_fn must not be None")

    mongo_collections = backend_config.mongo.collections
    episodic_collection = mongo_database[mongo_collections.episodic_memories]
    foresight_collection = mongo_database[mongo_collections.foresight_records]
    event_log_collection = mongo_database[mongo_collections.event_log_records]
    memcell_store = MongoMemCellStore(collection=mongo_database[mongo_collections.memcells])
    bundle = EverMemOSExternalBundle(
        conversation_status_store=MongoConversationStatusStore(
            collection=mongo_database[mongo_collections.conversation_status]
        ),
        conversation_buffer_store=MongoConversationBufferStore(
            collection=mongo_database[mongo_collections.conversation_data]
        ),
        memcell_store=memcell_store,
        memory_artifact_store=EverMemOSArtifactStore(
            memcell_store=memcell_store,
            episodic_collection=episodic_collection,
            foresight_collection=foresight_collection,
            event_log_collection=event_log_collection,
            elasticsearch_client=elasticsearch_client,
            milvus_client=milvus_client,
            backend_config=backend_config,
            embedding_fn=embedding_fn,
        ),
        topic_state_store=MongoTopicStateStore(
            collection=mongo_database[mongo_collections.cluster_states]
        ),
        profile_store=MongoProfileStore(
            collection=mongo_database[mongo_collections.user_profiles]
        ),
        keyword_searcher=EverMemOSElasticsearchSearcher(
            client=elasticsearch_client,
            backend_config=backend_config,
            memory_type_index_map={
                "episode": backend_config.elasticsearch.indexes.episodic_memories,
                "foresight": backend_config.elasticsearch.indexes.foresight_records,
                "event_log": backend_config.elasticsearch.indexes.event_log_records,
            },
        ),
        vector_searcher=EverMemOSMilvusSearcher(
            client=milvus_client,
            backend_config=backend_config,
            embedding_fn=embedding_fn,
            memory_type_collection_map={
                "episode": backend_config.milvus.collections.episodic_memories,
                "foresight": backend_config.milvus.collections.foresight_records,
                "event_log": backend_config.milvus.collections.event_log_records,
            },
        ),
    )
    return bundle
