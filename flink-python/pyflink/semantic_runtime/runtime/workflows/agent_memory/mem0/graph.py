"""Mem0 Graph workflow implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.concurrency import (
    amap_grouped_serial_bounded,
    amap_ordered_bounded,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.config import (
    Mem0GraphConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphAddResult,
    Mem0GraphEntityCandidate,
    Mem0GraphExtractedEntity,
    Mem0GraphExtractedRelation,
    Mem0GraphRelationCandidate,
    Mem0GraphRelationOperation,
    Mem0GraphRelationResolution,
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


@dataclass(frozen=True)
class _EntityPlan:
    """One extracted entity plus dedup target decided by semantic runtime."""

    entity: Mem0GraphExtractedEntity
    existing_entity_id: Optional[str]


@dataclass(frozen=True)
class _RelationPlan:
    """One extracted relation bound to concrete entity ids and action resolution."""

    relation: Mem0GraphExtractedRelation
    source_entity_id: str
    destination_entity_id: str
    resolution: Mem0GraphRelationResolution


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
        entity_plans = await amap_ordered_bounded(
            items=entities,
            concurrency=self._config.entity_resolve_concurrency,
            worker=lambda index, entity: self._resolve_entity_plan(
                index=index,
                group_id=group_id,
                entity=entity,
            ),
        )
        entity_rows = await amap_grouped_serial_bounded(
            items=entity_plans,
            group_key=self._entity_upsert_group_key,
            concurrency=self._config.entity_upsert_group_concurrency,
            worker=lambda index, plan: self._upsert_entity_plan(
                index=index,
                group_id=group_id,
                plan=plan,
            ),
        )
        entity_ids: Dict[str, str] = {}
        for entity_name, entity_id in entity_rows:
            entity_ids[entity_name] = entity_id
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

        relation_plans = await amap_ordered_bounded(
            items=relations,
            concurrency=self._config.relation_resolve_concurrency,
            worker=lambda index, relation: self._resolve_relation_plan(
                index=index,
                group_id=group_id,
                relation=relation,
                normalized_entity_ids=normalized_entity_ids,
            ),
        )
        operations = await amap_grouped_serial_bounded(
            items=relation_plans,
            group_key=self._relation_write_group_key,
            concurrency=self._config.relation_write_group_concurrency,
            worker=lambda index, plan: self._apply_relation_plan(
                index=index,
                group_id=group_id,
                plan=plan,
            ),
        )

        added_relations = 0
        updated_relations = 0
        deleted_relations = 0
        for operation in operations:
            if operation.action == "NEW":
                added_relations += 1
            elif operation.action == "AUGMENTS":
                updated_relations += 1
            elif operation.action == "CONTRADICTS":
                deleted_relations += 1
            else:
                raise RuntimeError(
                    f"unsupported relation action={operation.action!r}"
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

    async def _resolve_entity_plan(
        self,
        *,
        index: int,
        group_id: str,
        entity: Mem0GraphExtractedEntity,
    ) -> _EntityPlan:
        _ = index
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
        return _EntityPlan(entity=entity, existing_entity_id=existing_entity_id)

    def _entity_upsert_group_key(self, index: int, plan: _EntityPlan) -> str:
        _ = index
        return _normalize_entity_name_key(plan.entity.entity_name)

    async def _upsert_entity_plan(
        self,
        *,
        index: int,
        group_id: str,
        plan: _EntityPlan,
    ) -> Tuple[str, str]:
        _ = index
        entity_id = await self._graph_store.upsert_entity(
            group_id=group_id,
            entity_name=plan.entity.entity_name,
            entity_type=plan.entity.entity_type,
            existing_entity_id=plan.existing_entity_id,
        )
        return (plan.entity.entity_name, entity_id)

    async def _resolve_relation_plan(
        self,
        *,
        index: int,
        group_id: str,
        relation: Mem0GraphExtractedRelation,
        normalized_entity_ids: Dict[str, str],
    ) -> _RelationPlan:
        _ = index
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
        return _RelationPlan(
            relation=relation,
            source_entity_id=source_entity_id,
            destination_entity_id=destination_entity_id,
            resolution=resolution,
        )

    def _relation_write_group_key(self, index: int, plan: _RelationPlan) -> str:
        action = str(plan.resolution.action)
        if action in {"AUGMENTS", "CONTRADICTS"}:
            relation_id = str(plan.resolution.target_relation_id)
            if not relation_id:
                raise ValueError(
                    "target_relation_id is required for AUGMENTS/CONTRADICTS "
                    "write grouping"
                )
            return f"relation:{relation_id}"
        if action == "NEW":
            relation_key = " ".join(str(plan.resolution.relationship).split()).casefold()
            return (
                f"new:{plan.source_entity_id}:"
                f"{plan.destination_entity_id}:{relation_key}"
            )
        raise RuntimeError(f"unsupported relation action={action!r} at index={index}")

    async def _apply_relation_plan(
        self,
        *,
        index: int,
        group_id: str,
        plan: _RelationPlan,
    ) -> Mem0GraphRelationOperation:
        _ = index
        action = str(plan.resolution.action)
        if action == "NEW":
            relation_id = await self._graph_store.add_relation(
                group_id=group_id,
                source_entity_id=plan.source_entity_id,
                destination_entity_id=plan.destination_entity_id,
                relationship=plan.resolution.relationship,
            )
        elif action == "AUGMENTS":
            relation_id = str(plan.resolution.target_relation_id)
            await self._graph_store.update_relation(
                group_id=group_id,
                relation_id=relation_id,
                relationship=plan.resolution.relationship,
            )
        elif action == "CONTRADICTS":
            relation_id = str(plan.resolution.target_relation_id)
            await self._graph_store.delete_relation(
                group_id=group_id,
                relation_id=relation_id,
            )
        else:
            raise RuntimeError(f"unsupported relation action={action!r}")

        return Mem0GraphRelationOperation(
            action=action,
            relation_id=relation_id,
            source_entity_id=plan.source_entity_id,
            destination_entity_id=plan.destination_entity_id,
            relationship=plan.resolution.relationship,
            reason=str(plan.resolution.reason),
            confidence=float(plan.resolution.confidence),
        )

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
