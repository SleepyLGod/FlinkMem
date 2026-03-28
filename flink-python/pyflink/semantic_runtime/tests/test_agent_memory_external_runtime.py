"""Tests for EverMemOS external runtime adapters."""

from __future__ import annotations

import asyncio
import math
import os
import pathlib
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import pyflink as _pf  # noqa: E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    ConversationMessage,
    DecompositionArtifacts,
    EventLogArtifact,
    ForesightArtifact,
    MemCellRecord,
    TopicClusterState,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.external_config import (
    EverMemOSBackendConfig,
    EverMemOSElasticsearchConfig,
    EverMemOSElasticsearchIndexes,
    EverMemOSMilvusCollections,
    EverMemOSMilvusConfig,
    EverMemOSMongoCollections,
    EverMemOSMongoConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.external_runtime import (
    build_evermemos_external_bundle,
)


@dataclass
class _InsertOneResult:
    inserted_id: str


@dataclass
class _UpdateResult:
    matched_count: int


class _FakeCollection:
    def __init__(self) -> None:
        self._docs: List[Dict[str, Any]] = []

    def find_one(self, query: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        for doc in self._docs:
            if _matches(doc, query):
                return dict(doc)
        return None

    def update_one(self, query: Mapping[str, Any], update: Mapping[str, Any], upsert: bool) -> _UpdateResult:
        for index, doc in enumerate(self._docs):
            if _matches(doc, query):
                self._docs[index] = _apply_set_update(doc, update)
                return _UpdateResult(matched_count=1)
        if upsert:
            base = dict(query)
            merged = _apply_set_update(base, update)
            if "_id" not in merged and "_id" in query:
                merged["_id"] = query["_id"]
            self._docs.append(merged)
            return _UpdateResult(matched_count=1)
        return _UpdateResult(matched_count=0)

    def insert_one(self, doc: Mapping[str, Any]) -> _InsertOneResult:
        copied = dict(doc)
        doc_id = str(copied.get("_id", ""))
        if not doc_id:
            raise ValueError("insert_one requires non-empty _id")
        if any(str(item.get("_id", "")) == doc_id for item in self._docs):
            raise ValueError(f"duplicate _id={doc_id}")
        self._docs.append(copied)
        return _InsertOneResult(inserted_id=doc_id)

    def insert_many(self, docs: Iterable[Mapping[str, Any]]) -> None:
        for item in docs:
            self.insert_one(item)

    def find(self, query: Mapping[str, Any]) -> Iterable[Dict[str, Any]]:
        return [dict(doc) for doc in self._docs if _matches(doc, query)]

    def delete_many(self, query: Mapping[str, Any]) -> None:
        kept = [doc for doc in self._docs if not _matches(doc, query)]
        self._docs = kept


class _FakeMongoDatabase:
    def __init__(self) -> None:
        self._collections: Dict[str, _FakeCollection] = {}

    def __getitem__(self, name: str) -> _FakeCollection:
        if name not in self._collections:
            self._collections[name] = _FakeCollection()
        return self._collections[name]


class _FakeElasticsearchClient:
    def __init__(self) -> None:
        self._docs: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def index(self, *, index: str, id: str, document: Mapping[str, Any], refresh: bool) -> None:
        _ = refresh
        if index not in self._docs:
            self._docs[index] = {}
        self._docs[index][id] = dict(document)

    def search(self, *, index: Sequence[str], size: int, query: Mapping[str, Any]) -> Dict[str, Any]:
        indexes = list(index)
        phrase = str(query["bool"]["must"][0]["match"]["content"])
        group_id = str(query["bool"]["filter"][0]["term"]["group_id"])
        terms = [item for item in phrase.lower().split() if item]
        hits: List[Dict[str, Any]] = []
        for index_name in indexes:
            for doc_id, source in self._docs.get(index_name, {}).items():
                if str(source.get("group_id", "")) != group_id:
                    continue
                content = str(source.get("content", "")).lower()
                score = float(sum(1 for term in terms if term in content))
                if score <= 0:
                    continue
                hits.append(
                    {
                        "_id": doc_id,
                        "_index": index_name,
                        "_score": score,
                        "_source": dict(source),
                    }
                )
        hits.sort(key=lambda item: float(item["_score"]), reverse=True)
        return {"hits": {"hits": hits[: int(size)]}}


class _FakeMilvusClient:
    def __init__(self) -> None:
        self._rows: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def upsert(self, *, collection_name: str, data: Sequence[Mapping[str, Any]]) -> None:
        if collection_name not in self._rows:
            self._rows[collection_name] = {}
        for item in data:
            key = str(item["memory_id"])
            self._rows[collection_name][key] = dict(item)

    def search(
        self,
        *,
        collection_name: str,
        data: Sequence[Sequence[float]],
        filter: str,
        limit: int,
        output_fields: Sequence[str],
    ) -> List[List[Dict[str, Any]]]:
        _ = output_fields
        if len(data) != 1:
            raise ValueError("search expects one query vector")
        vector = list(float(item) for item in data[0])
        group_id = _parse_group_filter(filter)
        scored: List[Dict[str, Any]] = []
        for row in self._rows.get(collection_name, {}).values():
            if str(row.get("group_id", "")) != group_id:
                continue
            embedding = [float(item) for item in row.get("embedding", [])]
            score = _cosine(vector, embedding)
            scored.append(
                {
                    "id": row["memory_id"],
                    "distance": score,
                    "entity": {
                        "memory_id": row["memory_id"],
                        "memory_type": row["memory_type"],
                        "content": row["content"],
                        "timestamp_ms": row["timestamp_ms"],
                    },
                }
            )
        scored.sort(key=lambda item: float(item["distance"]), reverse=True)
        return [scored[: int(limit)]]


def _parse_group_filter(expression: str) -> str:
    prefix = 'group_id == "'
    suffix = '"'
    if not expression.startswith(prefix) or not expression.endswith(suffix):
        raise ValueError(f"unsupported filter expression: {expression!r}")
    return expression[len(prefix):-len(suffix)]


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise ValueError("vector dim mismatch in fake milvus cosine")
    left_norm = math.sqrt(sum(item * item for item in left))
    right_norm = math.sqrt(sum(item * item for item in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return float(sum(a * b for a, b in zip(left, right)) / (left_norm * right_norm))


def _matches(doc: Mapping[str, Any], query: Mapping[str, Any]) -> bool:
    for key, expected in query.items():
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$in" in expected:
                values = [str(item) for item in expected["$in"]]
                if str(actual) not in values:
                    return False
                continue
            if "$gte" in expected:
                if int(actual) < int(expected["$gte"]):
                    return False
                continue
            raise ValueError(f"unsupported query operator in test fake: {expected!r}")
        if actual != expected:
            return False
    return True


def _apply_set_update(doc: Mapping[str, Any], update: Mapping[str, Any]) -> Dict[str, Any]:
    if "$set" not in update:
        raise ValueError("only $set update is supported in test fake")
    merged = dict(doc)
    merged.update(dict(update["$set"]))
    return merged


def _embedding_fn(text: str) -> Sequence[float]:
    tokens = [token for token in text.lower().split() if token]
    return [
        float(len(tokens) + 1),
        float(sum(1 for token in tokens if "plan" in token) + 1),
        float(sum(1 for token in tokens if "travel" in token) + 1),
        float(sum(1 for token in tokens if "beijing" in token) + 1),
    ]


def _backend_config() -> EverMemOSBackendConfig:
    return EverMemOSBackendConfig(
        mongo=EverMemOSMongoConfig(
            uri="mongodb://unused",
            database="evermemos_test",
            collections=EverMemOSMongoCollections(),
        ),
        elasticsearch=EverMemOSElasticsearchConfig(
            enabled=True,
            hosts=("http://unused:9200",),
            indexes=EverMemOSElasticsearchIndexes(),
        ),
        milvus=EverMemOSMilvusConfig(
            enabled=True,
            uri="http://unused:19530",
            database="default",
            embedding_dim=4,
            collections=EverMemOSMilvusCollections(),
        ),
    )


def _message(message_id: str, content: str, timestamp_ms: int) -> ConversationMessage:
    return ConversationMessage(
        message_id=message_id,
        group_id="g1",
        sender_id="u1",
        content=content,
        timestamp_ms=timestamp_ms,
    )


def _memcell(memcell_id: str, content: str, timestamp_ms: int) -> MemCellRecord:
    return MemCellRecord(
        memcell_id=memcell_id,
        group_id="g1",
        timestamp_ms=timestamp_ms,
        original_messages=[_message(f"{memcell_id}_m", content, timestamp_ms)],
        participants=["u1"],
        scene="assistant",
        summary="summary",
        subject="subject",
        episode=content,
        topic_id="topic_000",
    )


def test_external_bundle_persists_and_syncs_artifacts() -> None:
    mongo_db = _FakeMongoDatabase()
    es = _FakeElasticsearchClient()
    milvus = _FakeMilvusClient()
    bundle = build_evermemos_external_bundle(
        mongo_database=mongo_db,
        backend_config=_backend_config(),
        elasticsearch_client=es,
        milvus_client=milvus,
        embedding_fn=_embedding_fn,
    )

    cell = _memcell("c1", "plan travel beijing next week", 1000)
    persisted_id = asyncio.run(bundle.memcell_store.create_memcell(cell))
    assert persisted_id == "c1"

    artifacts = DecompositionArtifacts(
        episode="plan travel beijing next week",
        subject="travel",
        foresights=[ForesightArtifact(content="travel beijing next week")],
        event_logs=[EventLogArtifact(atomic_fact="user planned travel")],
    )
    asyncio.run(
        bundle.memory_artifact_store.persist_decomposition(
            group_id="g1",
            memcell_id="c1",
            scene="assistant",
            artifacts=artifacts,
        )
    )
    asyncio.run(bundle.memory_artifact_store.sync_indexes(group_id="g1", memcell_id="c1"))

    assert "episodic_memories" in es._docs
    assert "c1" in es._docs["episodic_memories"]
    assert "episodic_memories" in milvus._rows
    assert "c1" in milvus._rows["episodic_memories"]


def test_external_bundle_keyword_and_vector_search() -> None:
    mongo_db = _FakeMongoDatabase()
    es = _FakeElasticsearchClient()
    milvus = _FakeMilvusClient()
    bundle = build_evermemos_external_bundle(
        mongo_database=mongo_db,
        backend_config=_backend_config(),
        elasticsearch_client=es,
        milvus_client=milvus,
        embedding_fn=_embedding_fn,
    )

    first = _memcell("c1", "plan travel beijing next week", 1000)
    second = _memcell("c2", "shopping groceries", 1200)
    asyncio.run(bundle.memcell_store.create_memcell(first))
    asyncio.run(bundle.memcell_store.create_memcell(second))
    asyncio.run(
        bundle.memory_artifact_store.persist_decomposition(
            group_id="g1",
            memcell_id="c1",
            scene="assistant",
            artifacts=DecompositionArtifacts(
                episode=first.episode or "",
                subject=first.subject or "",
                foresights=[],
                event_logs=[],
            ),
        )
    )
    asyncio.run(
        bundle.memory_artifact_store.persist_decomposition(
            group_id="g1",
            memcell_id="c2",
            scene="assistant",
            artifacts=DecompositionArtifacts(
                episode=second.episode or "",
                subject=second.subject or "",
                foresights=[],
                event_logs=[],
            ),
        )
    )
    asyncio.run(bundle.memory_artifact_store.sync_indexes(group_id="g1", memcell_id="c1"))
    asyncio.run(bundle.memory_artifact_store.sync_indexes(group_id="g1", memcell_id="c2"))

    keyword_rows = asyncio.run(
        bundle.keyword_searcher.search(
            group_id="g1",
            query="travel beijing",
            top_k=5,
            memory_types=["episode"],
        )
    )
    vector_rows = asyncio.run(
        bundle.vector_searcher.search(
            group_id="g1",
            query="plan beijing",
            top_k=5,
            memory_types=["episode"],
        )
    )

    assert keyword_rows[0].memory_id == "c1"
    assert vector_rows[0].memory_id == "c1"


def test_external_bundle_supports_state_and_profile_stores() -> None:
    mongo_db = _FakeMongoDatabase()
    es = _FakeElasticsearchClient()
    milvus = _FakeMilvusClient()
    bundle = build_evermemos_external_bundle(
        mongo_database=mongo_db,
        backend_config=_backend_config(),
        elasticsearch_client=es,
        milvus_client=milvus,
        embedding_fn=_embedding_fn,
    )

    asyncio.run(bundle.conversation_status_store.update_last_memcell_time_ms("g1", 1111))
    status = asyncio.run(bundle.conversation_status_store.get_last_memcell_time_ms("g1"))
    assert status == 1111

    asyncio.run(bundle.conversation_buffer_store.append_messages("g1", [_message("m1", "hi", 1000)]))
    loaded = asyncio.run(bundle.conversation_buffer_store.load_messages_since("g1", None))
    assert len(loaded) == 1
    asyncio.run(bundle.conversation_buffer_store.clear_consumed_messages("g1", ["m1"]))
    loaded_after = asyncio.run(bundle.conversation_buffer_store.load_messages_since("g1", None))
    assert len(loaded_after) == 0

    state = TopicClusterState(
        event_ids=["c1"],
        eventid_to_topic={"c1": "topic_000"},
        topic_centroids={"topic_000": [0.1, 0.2, 0.3, 0.4]},
        topic_representatives={"topic_000": "travel"},
        topic_counts={"topic_000": 1},
        topic_last_ts={"topic_000": 1000},
        next_topic_idx=1,
    )
    asyncio.run(bundle.topic_state_store.save_state("g1", state))
    loaded_state = asyncio.run(bundle.topic_state_store.load_state("g1"))
    assert loaded_state.topic_counts["topic_000"] == 1

    asyncio.run(bundle.profile_store.save_profiles("g1", {"u1": {"trait": "planner"}}))
    profiles = asyncio.run(bundle.profile_store.load_profiles("g1"))
    assert profiles["u1"]["trait"] == "planner"
