"""Unit tests for Mem0 Graph workflow."""

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

from typing import Dict, List, Optional, Sequence, Tuple

from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.config import (
    DEFAULT_MEM0_GRAPH_ENTITY_EXTRACTION_PROMPT,
    DEFAULT_MEM0_GRAPH_ENTITY_IDENTITY_PROMPT,
    DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT,
    DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT,
    Mem0GraphConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphEntityCandidate,
    Mem0GraphEntityResolution,
    Mem0GraphExtractedEntity,
    Mem0GraphExtractedRelation,
    Mem0GraphRelationCandidate,
    Mem0GraphRelationResolution,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.graph import (
    Mem0GraphWorkflow,
)


class _InMemoryGraphStore:
    def __init__(self) -> None:
        self.entities: Dict[str, Dict[str, str]] = {}
        self.relations: Dict[str, Dict[str, str]] = {}
        self._entity_counter = 0
        self._relation_counter = 0

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        entity_type: str,
        existing_entity_id: Optional[str],
    ) -> str:
        if existing_entity_id is not None:
            if existing_entity_id not in self.entities:
                raise KeyError(f"unknown entity_id={existing_entity_id}")
            row = self.entities[existing_entity_id]
            if row["group_id"] != group_id:
                raise ValueError("group_id mismatch during upsert_entity")
            row["entity_name"] = entity_name
            row["entity_type"] = entity_type
            return existing_entity_id

        self._entity_counter += 1
        entity_id = f"e{self._entity_counter}"
        self.entities[entity_id] = {
            "group_id": group_id,
            "entity_name": entity_name,
            "entity_type": entity_type,
        }
        return entity_id

    async def add_relation(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relationship: str,
    ) -> str:
        self._relation_counter += 1
        relation_id = f"r{self._relation_counter}"
        self.relations[relation_id] = {
            "group_id": group_id,
            "source_entity_id": source_entity_id,
            "destination_entity_id": destination_entity_id,
            "relationship": relationship,
        }
        return relation_id

    async def update_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
        relationship: str,
    ) -> None:
        if relation_id not in self.relations:
            raise KeyError(f"unknown relation_id={relation_id}")
        row = self.relations[relation_id]
        if row["group_id"] != group_id:
            raise ValueError("group_id mismatch during update_relation")
        row["relationship"] = relationship

    async def delete_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
    ) -> None:
        if relation_id not in self.relations:
            raise KeyError(f"unknown relation_id={relation_id}")
        row = self.relations[relation_id]
        if row["group_id"] != group_id:
            raise ValueError("group_id mismatch during delete_relation")
        del self.relations[relation_id]


class _ScriptedEntitySearcher:
    def __init__(self, rows: Dict[str, Sequence[Mem0GraphEntityCandidate]]) -> None:
        self._rows = rows
        self.calls = 0

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[Mem0GraphEntityCandidate]:
        _ = (group_id, top_k)
        self.calls += 1
        return list(self._rows.get(query, []))


class _ScriptedRelationSearcher:
    def __init__(self, rows: Dict[str, Sequence[Mem0GraphRelationCandidate]]) -> None:
        self._rows = rows
        self.calls = 0

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mem0GraphRelationCandidate]:
        _ = (group_id, top_k, source_entity_id, destination_entity_id)
        self.calls += 1
        return list(self._rows.get(query, []))


class _ScriptedGraphSemanticRuntime:
    def __init__(
        self,
        *,
        entities: Sequence[Mem0GraphExtractedEntity],
        relations: Sequence[Mem0GraphExtractedRelation],
        entity_resolutions: Dict[str, Mem0GraphEntityResolution],
        relation_resolutions: Dict[Tuple[str, str, str], Mem0GraphRelationResolution],
    ) -> None:
        self._entities = list(entities)
        self._relations = list(relations)
        self._entity_resolutions = dict(entity_resolutions)
        self._relation_resolutions = dict(relation_resolutions)
        self.entity_extract_prompts: List[str] = []
        self.relation_extract_prompts: List[str] = []
        self.entity_identity_prompts: List[str] = []
        self.relation_resolution_prompts: List[str] = []

    async def extract_entities(
        self,
        *,
        messages: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedEntity]:
        _ = messages
        self.entity_extract_prompts.append(prompt)
        return list(self._entities)

    async def extract_relations(
        self,
        *,
        messages: Sequence[str],
        entities: Sequence[Mem0GraphExtractedEntity],
        allowed_entity_names: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedRelation]:
        _ = (messages, entities, allowed_entity_names)
        self.relation_extract_prompts.append(prompt)
        return list(self._relations)

    async def resolve_entity(
        self,
        *,
        entity: Mem0GraphExtractedEntity,
        candidates: Sequence[Mem0GraphEntityCandidate],
        prompt: str,
    ) -> Mem0GraphEntityResolution:
        _ = candidates
        self.entity_identity_prompts.append(prompt)
        if entity.entity_name not in self._entity_resolutions:
            raise KeyError(f"missing entity resolution for {entity.entity_name!r}")
        return self._entity_resolutions[entity.entity_name]

    async def resolve_relation(
        self,
        *,
        relation: Mem0GraphExtractedRelation,
        candidates: Sequence[Mem0GraphRelationCandidate],
        prompt: str,
    ) -> Mem0GraphRelationResolution:
        _ = candidates
        self.relation_resolution_prompts.append(prompt)
        key = (
            relation.source_entity_name,
            relation.relationship,
            relation.destination_entity_name,
        )
        if key not in self._relation_resolutions:
            raise KeyError(f"missing relation resolution for key={key!r}")
        return self._relation_resolutions[key]


def test_mem0_graph_add_executes_new_update_delete() -> None:
    store = _InMemoryGraphStore()
    store.entities["e_existing"] = {
        "group_id": "g1",
        "entity_name": "Alice",
        "entity_type": "person",
    }
    store.relations["r_old_keep"] = {
        "group_id": "g1",
        "source_entity_id": "e_existing",
        "destination_entity_id": "e_existing",
        "relationship": "legacy",
    }
    store.relations["r_old_drop"] = {
        "group_id": "g1",
        "source_entity_id": "e_existing",
        "destination_entity_id": "e_existing",
        "relationship": "obsolete",
    }

    entity_searcher = _ScriptedEntitySearcher(
        {
            "Alice": [
                Mem0GraphEntityCandidate(
                    entity_id="e_existing",
                    entity_name="Alice",
                    entity_type="person",
                    score=0.95,
                    source="vector",
                )
            ],
            "Bob": [],
        }
    )
    relation_searcher = _ScriptedRelationSearcher(
        {
            "knows": [
                Mem0GraphRelationCandidate(
                    relation_id="r_old_keep",
                    source_entity_id="e_existing",
                    destination_entity_id="e_existing",
                    relationship="legacy",
                    score=0.8,
                    source="vector",
                )
            ],
            "collaborates with": [
                Mem0GraphRelationCandidate(
                    relation_id="r_old_keep",
                    source_entity_id="e_existing",
                    destination_entity_id="e_existing",
                    relationship="legacy",
                    score=0.81,
                    source="vector",
                )
            ],
            "dislikes": [
                Mem0GraphRelationCandidate(
                    relation_id="r_old_drop",
                    source_entity_id="e_existing",
                    destination_entity_id="e_existing",
                    relationship="obsolete",
                    score=0.82,
                    source="vector",
                )
            ],
        }
    )

    runtime = _ScriptedGraphSemanticRuntime(
        entities=[
            Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
            Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
        ],
        relations=[
            Mem0GraphExtractedRelation(
                source_entity_name="Alice",
                relationship="knows",
                destination_entity_name="Bob",
            ),
            Mem0GraphExtractedRelation(
                source_entity_name="Alice",
                relationship="collaborates with",
                destination_entity_name="Bob",
            ),
            Mem0GraphExtractedRelation(
                source_entity_name="Alice",
                relationship="dislikes",
                destination_entity_name="Bob",
            ),
        ],
        entity_resolutions={
            "Alice": Mem0GraphEntityResolution(
                decision="SAME",
                entity_name="Alice",
                target_entity_id="e_existing",
                reason="same person",
                confidence=0.9,
            ),
            "Bob": Mem0GraphEntityResolution(
                decision="DIFFERENT",
                entity_name="Bob",
                reason="new person",
                confidence=0.9,
            ),
        },
        relation_resolutions={
            ("Alice", "knows", "Bob"): Mem0GraphRelationResolution(
                action="NEW",
                source_entity_name="Alice",
                destination_entity_name="Bob",
                relationship="knows",
                reason="new relation",
                confidence=0.8,
            ),
            ("Alice", "collaborates with", "Bob"): Mem0GraphRelationResolution(
                action="AUGMENTS",
                source_entity_name="Alice",
                destination_entity_name="Bob",
                relationship="collaborates with",
                target_relation_id="r_old_keep",
                reason="more specific",
                confidence=0.85,
            ),
            ("Alice", "dislikes", "Bob"): Mem0GraphRelationResolution(
                action="CONTRADICTS",
                source_entity_name="Alice",
                destination_entity_name="Bob",
                relationship="dislikes",
                target_relation_id="r_old_drop",
                reason="contradiction",
                confidence=0.82,
            ),
        },
    )

    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(),
        semantic_runtime=runtime,
        graph_store=store,
        entity_searcher=entity_searcher,
        relation_searcher=relation_searcher,
    )

    result = asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["Alice and Bob talked about work"],
        )
    )

    assert result.extracted_entity_count == 2
    assert result.extracted_relation_count == 3
    assert result.upserted_entities == 2
    assert result.added_relations == 1
    assert result.updated_relations == 1
    assert result.deleted_relations == 1
    assert len(result.operations) == 3
    assert entity_searcher.calls == 2
    assert relation_searcher.calls == 3
    assert runtime.entity_extract_prompts == [Mem0GraphConfig().entity_extraction_prompt]
    assert runtime.relation_extract_prompts == [Mem0GraphConfig().relation_extraction_prompt]
    assert runtime.entity_identity_prompts == [Mem0GraphConfig().entity_identity_prompt] * 2
    assert runtime.relation_resolution_prompts == [
        Mem0GraphConfig().relation_resolution_prompt
    ] * 3


def test_mem0_graph_llm_recall_backend_routes_to_llm_searchers() -> None:
    embedding_entity_searcher = _ScriptedEntitySearcher({})
    embedding_relation_searcher = _ScriptedRelationSearcher({})

    llm_entity_searcher = _ScriptedEntitySearcher(
        {
            "Alice": [
                Mem0GraphEntityCandidate(
                    entity_id="e_a",
                    entity_name="Alice",
                    entity_type="person",
                    score=0.9,
                    source="llm",
                )
            ]
        }
    )
    llm_relation_searcher = _ScriptedRelationSearcher(
        {
            "works with": [
                Mem0GraphRelationCandidate(
                    relation_id="r_x",
                    source_entity_id="e_a",
                    destination_entity_id="e_a",
                    relationship="works with",
                    score=0.9,
                    source="llm",
                )
            ]
        }
    )

    store = _InMemoryGraphStore()
    store.entities["e_a"] = {
        "group_id": "g1",
        "entity_name": "Alice",
        "entity_type": "person",
    }

    runtime = _ScriptedGraphSemanticRuntime(
        entities=[Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person")],
        relations=[
            Mem0GraphExtractedRelation(
                source_entity_name="Alice",
                relationship="works with",
                destination_entity_name="Alice",
            )
        ],
        entity_resolutions={
            "Alice": Mem0GraphEntityResolution(
                decision="SAME",
                entity_name="Alice",
                target_entity_id="e_a",
            )
        },
        relation_resolutions={
            ("Alice", "works with", "Alice"): Mem0GraphRelationResolution(
                action="NEW",
                source_entity_name="Alice",
                destination_entity_name="Alice",
                relationship="works with",
            )
        },
    )

    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(
            entity_recall_backend="llm",
            relation_recall_backend="llm",
        ),
        semantic_runtime=runtime,
        graph_store=store,
        entity_searcher=embedding_entity_searcher,
        relation_searcher=embedding_relation_searcher,
        llm_entity_searcher=llm_entity_searcher,
        llm_relation_searcher=llm_relation_searcher,
    )

    asyncio.run(
        workflow.add(
            group_id="g1",
            messages=["Alice works with Alice"],
        )
    )

    assert embedding_entity_searcher.calls == 0
    assert embedding_relation_searcher.calls == 0
    assert llm_entity_searcher.calls == 1
    assert llm_relation_searcher.calls == 1


def test_mem0_graph_llm_entity_backend_requires_llm_searcher() -> None:
    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(entity_recall_backend="llm"),
        semantic_runtime=_ScriptedGraphSemanticRuntime(
            entities=[Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person")],
            relations=[],
            entity_resolutions={
                "Alice": Mem0GraphEntityResolution(
                    decision="DIFFERENT",
                    entity_name="Alice",
                )
            },
            relation_resolutions={},
        ),
        graph_store=_InMemoryGraphStore(),
        entity_searcher=_ScriptedEntitySearcher({}),
        relation_searcher=_ScriptedRelationSearcher({}),
    )

    try:
        asyncio.run(workflow.add(group_id="g1", messages=["hello"]))
    except ValueError as exc:
        assert "llm_entity_searcher is required" in str(exc)
        return
    raise AssertionError("llm entity backend should require llm_entity_searcher")


def test_mem0_graph_search_uses_relation_recall_backend() -> None:
    relation = Mem0GraphRelationCandidate(
        relation_id="r1",
        source_entity_id="e1",
        destination_entity_id="e2",
        relationship="knows",
        score=0.9,
        source="vector",
    )
    relation_searcher = _ScriptedRelationSearcher({"who knows who": [relation]})

    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(search_top_k=3),
        semantic_runtime=_ScriptedGraphSemanticRuntime(
            entities=[],
            relations=[],
            entity_resolutions={},
            relation_resolutions={},
        ),
        graph_store=_InMemoryGraphStore(),
        entity_searcher=_ScriptedEntitySearcher({}),
        relation_searcher=relation_searcher,
    )

    result = asyncio.run(
        workflow.search(
            group_id="g1",
            query="who knows who",
            top_k=1,
        )
    )

    assert result.top_k == 1
    assert len(result.relations) == 1
    assert result.relations[0].relation_id == "r1"
    assert result.metadata["relation_recall_backend"] == "embedding"
    assert relation_searcher.calls == 1


def test_mem0_graph_default_prompts_align_with_mem0_tool_schema() -> None:
    assert "extract_entities" in DEFAULT_MEM0_GRAPH_ENTITY_EXTRACTION_PROMPT
    assert "entity_type" in DEFAULT_MEM0_GRAPH_ENTITY_EXTRACTION_PROMPT
    assert "establish_relationships" in DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT
    assert "source_index" in DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT
    assert "destination_index" in DEFAULT_MEM0_GRAPH_RELATION_EXTRACTION_PROMPT
    assert "SAME|DIFFERENT" in DEFAULT_MEM0_GRAPH_ENTITY_IDENTITY_PROMPT
    assert "add_graph_memory" in DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT
    assert "update_graph_memory" in DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT
    assert "delete_graph_memory" in DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT
    assert "source_type" in DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT
    assert "destination_type" in DEFAULT_MEM0_GRAPH_RELATION_RESOLUTION_PROMPT


def test_mem0_graph_add_rejects_same_target_entity_id_outside_candidates() -> None:
    store = _InMemoryGraphStore()
    runtime = _ScriptedGraphSemanticRuntime(
        entities=[Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person")],
        relations=[],
        entity_resolutions={
            "Alice": Mem0GraphEntityResolution(
                decision="SAME",
                entity_name="Alice",
                target_entity_id="e_missing",
            )
        },
        relation_resolutions={},
    )
    entity_searcher = _ScriptedEntitySearcher(
        {
            "Alice": [
                Mem0GraphEntityCandidate(
                    entity_id="e_existing",
                    entity_name="Alice",
                    entity_type="person",
                    score=0.91,
                    source="vector",
                )
            ]
        }
    )

    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(),
        semantic_runtime=runtime,
        graph_store=store,
        entity_searcher=entity_searcher,
        relation_searcher=_ScriptedRelationSearcher({}),
    )

    try:
        asyncio.run(workflow.add(group_id="g1", messages=["Alice says hello"]))
    except ValueError as exc:
        assert "not present in candidates" in str(exc)
        return
    raise AssertionError("Expected ValueError for SAME target_entity_id outside candidates")


def test_mem0_graph_add_rejects_relation_entity_outside_resolved_set() -> None:
    store = _InMemoryGraphStore()
    runtime = _ScriptedGraphSemanticRuntime(
        entities=[Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person")],
        relations=[
            Mem0GraphExtractedRelation(
                source_entity_name="Alice",
                relationship="mentions",
                destination_entity_name="Bob",
            )
        ],
        entity_resolutions={
            "Alice": Mem0GraphEntityResolution(
                decision="DIFFERENT",
                entity_name="Alice",
            )
        },
        relation_resolutions={},
    )
    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(),
        semantic_runtime=runtime,
        graph_store=store,
        entity_searcher=_ScriptedEntitySearcher({}),
        relation_searcher=_ScriptedRelationSearcher({}),
    )

    try:
        asyncio.run(workflow.add(group_id="g1", messages=["Alice mentions Bob"]))
    except ValueError as exc:
        assert "destination entity outside resolved set" in str(exc)
        return
    raise AssertionError("Expected ValueError for relation entity outside resolved set")


def test_mem0_graph_add_accepts_relation_entity_with_case_whitespace_variation() -> None:
    store = _InMemoryGraphStore()
    runtime = _ScriptedGraphSemanticRuntime(
        entities=[
            Mem0GraphExtractedEntity(entity_name="Alice", entity_type="person"),
            Mem0GraphExtractedEntity(entity_name="Bob", entity_type="person"),
        ],
        relations=[
            Mem0GraphExtractedRelation(
                source_entity_name="  alice  ",
                relationship="mentions",
                destination_entity_name="BOB",
            )
        ],
        entity_resolutions={
            "Alice": Mem0GraphEntityResolution(
                decision="DIFFERENT",
                entity_name="Alice",
            ),
            "Bob": Mem0GraphEntityResolution(
                decision="DIFFERENT",
                entity_name="Bob",
            ),
        },
        relation_resolutions={
            ("  alice  ", "mentions", "BOB"): Mem0GraphRelationResolution(
                action="NEW",
                source_entity_name="  alice  ",
                destination_entity_name="BOB",
                relationship="mentions",
            )
        },
    )
    workflow = Mem0GraphWorkflow(
        config=Mem0GraphConfig(),
        semantic_runtime=runtime,
        graph_store=store,
        entity_searcher=_ScriptedEntitySearcher({}),
        relation_searcher=_ScriptedRelationSearcher({}),
    )

    result = asyncio.run(workflow.add(group_id="g1", messages=["Alice mentions Bob"]))
    assert result.added_relations == 1
