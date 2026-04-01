"""Mem0 Graph workflow implementation."""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.config import (
    Mem0GraphConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphAddResult,
    Mem0GraphEntityCandidate,
    Mem0GraphExtractedEntity,
    Mem0GraphRelationCandidate,
    Mem0GraphRelationOperation,
    Mem0GraphSearchResult,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.interfaces import (
    Mem0GraphEntitySearcher,
    Mem0GraphLLMEntitySearcher,
    Mem0GraphLLMRelationSearcher,
    Mem0GraphRelationSearcher,
    Mem0GraphSemanticRuntime,
    Mem0GraphStore,
)


def _normalize_entity_name_key(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("entity_name must be non-empty")
    return " ".join(text.split()).casefold()


class Mem0GraphWorkflow:
    """Reconstruct Mem0 Graph flow with strict dependency injection."""

    def __init__(
        self,
        *,
        config: Mem0GraphConfig,
        semantic_runtime: Mem0GraphSemanticRuntime,
        graph_store: Mem0GraphStore,
        entity_searcher: Mem0GraphEntitySearcher,
        relation_searcher: Mem0GraphRelationSearcher,
        llm_entity_searcher: Optional[Mem0GraphLLMEntitySearcher] = None,
        llm_relation_searcher: Optional[Mem0GraphLLMRelationSearcher] = None,
    ) -> None:
        self._config = config
        self._semantic_runtime = semantic_runtime
        self._graph_store = graph_store
        self._entity_searcher = entity_searcher
        self._relation_searcher = relation_searcher
        self._llm_entity_searcher = llm_entity_searcher
        self._llm_relation_searcher = llm_relation_searcher

    async def add(
        self,
        *,
        group_id: str,
        messages: Sequence[str],
    ) -> Mem0GraphAddResult:
        """Process one Mem0 Graph ``add(messages)`` request."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not messages:
            raise ValueError("messages must be non-empty")
        for message in messages:
            if not str(message).strip():
                raise ValueError("messages must contain non-empty strings")

        entities = list(
            await self._semantic_runtime.extract_entities(
                messages=list(messages),
                prompt=self._config.entity_extraction_prompt,
            )
        )
        entity_ids = await self._resolve_entities(
            group_id=group_id,
            entities=entities,
        )
        normalized_entity_ids: Dict[str, str] = {}
        for entity_name, entity_id in entity_ids.items():
            normalized_name = _normalize_entity_name_key(entity_name)
            previous_entity_id = normalized_entity_ids.get(normalized_name)
            if previous_entity_id is not None and previous_entity_id != entity_id:
                raise ValueError(
                    "ambiguous normalized entity name maps to multiple entity ids: "
                    f"{entity_name!r}"
                )
            normalized_entity_ids[normalized_name] = entity_id

        relations = list(
            await self._semantic_runtime.extract_relations(
                messages=list(messages),
                entities=entities,
                allowed_entity_names=list(entity_ids.keys()),
                prompt=self._config.relation_extraction_prompt,
            )
        )

        operations: List[Mem0GraphRelationOperation] = []
        added_relations = 0
        updated_relations = 0
        deleted_relations = 0

        for relation in relations:
            normalized_source = _normalize_entity_name_key(relation.source_entity_name)
            normalized_destination = _normalize_entity_name_key(
                relation.destination_entity_name
            )
            if normalized_source not in normalized_entity_ids:
                raise ValueError(
                    "extract_relations returned source entity outside resolved set: "
                    f"{relation.source_entity_name!r}"
                )
            if normalized_destination not in normalized_entity_ids:
                raise ValueError(
                    "extract_relations returned destination entity outside resolved set: "
                    f"{relation.destination_entity_name!r}"
                )
            source_entity_id = normalized_entity_ids[normalized_source]
            destination_entity_id = normalized_entity_ids[normalized_destination]
            candidates = await self._search_relation_candidates(
                group_id=group_id,
                query=relation.relationship,
                top_k=self._config.relation_recall_top_k,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
            )
            resolution = await self._semantic_runtime.resolve_relation(
                relation=relation,
                candidates=candidates,
                prompt=self._config.relation_resolution_prompt,
            )

            if resolution.action == "NEW":
                relation_id = await self._graph_store.add_relation(
                    group_id=group_id,
                    source_entity_id=source_entity_id,
                    destination_entity_id=destination_entity_id,
                    relationship=resolution.relationship,
                )
                added_relations += 1
            elif resolution.action == "AUGMENTS":
                relation_id = str(resolution.target_relation_id)
                await self._graph_store.update_relation(
                    group_id=group_id,
                    relation_id=relation_id,
                    relationship=resolution.relationship,
                )
                updated_relations += 1
            elif resolution.action == "CONTRADICTS":
                relation_id = str(resolution.target_relation_id)
                await self._graph_store.delete_relation(
                    group_id=group_id,
                    relation_id=relation_id,
                )
                deleted_relations += 1
            else:
                raise RuntimeError(
                    f"unsupported relation action={resolution.action!r}"
                )

            operations.append(
                Mem0GraphRelationOperation(
                    action=resolution.action,
                    relation_id=relation_id,
                    source_entity_id=source_entity_id,
                    destination_entity_id=destination_entity_id,
                    relationship=resolution.relationship,
                    reason=str(resolution.reason),
                    confidence=float(resolution.confidence),
                )
            )

        return Mem0GraphAddResult(
            extracted_entity_count=len(entities),
            extracted_relation_count=len(relations),
            upserted_entities=len(entities),
            added_relations=added_relations,
            updated_relations=updated_relations,
            deleted_relations=deleted_relations,
            operations=operations,
        )

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: Optional[int] = None,
    ) -> Mem0GraphSearchResult:
        """Process one Mem0 Graph ``search(query)`` request."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not query:
            raise ValueError("query must be non-empty")
        resolved_top_k = int(top_k) if top_k is not None else self._config.search_top_k
        if resolved_top_k <= 0:
            raise ValueError("top_k must be > 0")

        relations = await self._search_relation_candidates(
            group_id=group_id,
            query=query,
            top_k=resolved_top_k,
            source_entity_id=None,
            destination_entity_id=None,
        )
        return Mem0GraphSearchResult(
            query=query,
            top_k=resolved_top_k,
            relations=list(relations),
            metadata={
                "workflow": "mem0_graph",
                "relation_recall_backend": self._config.relation_recall_backend,
            },
        )

    async def _resolve_entities(
        self,
        *,
        group_id: str,
        entities: Sequence[Mem0GraphExtractedEntity],
    ) -> Dict[str, str]:
        entity_ids: Dict[str, str] = {}
        for entity in entities:
            candidates = await self._search_entity_candidates(
                group_id=group_id,
                query=entity.entity_name,
                top_k=self._config.entity_recall_top_k,
            )
            candidate_entity_ids = {candidate.entity_id for candidate in candidates}
            resolution = await self._semantic_runtime.resolve_entity(
                entity=entity,
                candidates=candidates,
                prompt=self._config.entity_identity_prompt,
            )
            if resolution.decision == "SAME":
                existing_entity_id = str(resolution.target_entity_id)
                if existing_entity_id not in candidate_entity_ids:
                    raise ValueError(
                        "resolve_entity returned SAME target_entity_id "
                        f"not present in candidates: entity_name={entity.entity_name!r} "
                        f"target_entity_id={existing_entity_id!r}"
                    )
            else:
                existing_entity_id = None
            entity_id = await self._graph_store.upsert_entity(
                group_id=group_id,
                entity_name=entity.entity_name,
                entity_type=entity.entity_type,
                existing_entity_id=existing_entity_id,
            )
            entity_ids[entity.entity_name] = entity_id
        return entity_ids

    async def _search_entity_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[Mem0GraphEntityCandidate]:
        if self._config.entity_recall_backend == "embedding":
            return await self._entity_searcher.search(
                group_id=group_id,
                query=query,
                top_k=top_k,
            )
        if self._llm_entity_searcher is None:
            raise ValueError(
                "llm_entity_searcher is required when entity_recall_backend='llm'"
            )
        return await self._llm_entity_searcher.search(
            group_id=group_id,
            query=query,
            top_k=top_k,
        )

    async def _search_relation_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mem0GraphRelationCandidate]:
        if self._config.relation_recall_backend == "embedding":
            return await self._relation_searcher.search(
                group_id=group_id,
                query=query,
                top_k=top_k,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
            )
        if self._llm_relation_searcher is None:
            raise ValueError(
                "llm_relation_searcher is required when relation_recall_backend='llm'"
            )
        return await self._llm_relation_searcher.search(
            group_id=group_id,
            query=query,
            top_k=top_k,
            source_entity_id=source_entity_id,
            destination_entity_id=destination_entity_id,
        )
