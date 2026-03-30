"""Zep/Graphiti workflow implementation."""

from __future__ import annotations

from typing import Dict, List

from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.config import (
    ZepWorkflowConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepAddEpisodeResult,
    ZepResolvedEntity,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.interfaces import (
    ZepGraphStore,
    ZepSemanticRuntime,
)


class ZepAddEpisodeWorkflow:
    """Reconstruct Zep/Graphiti add_episode flow with strict dependency injection."""

    def __init__(
        self,
        *,
        config: ZepWorkflowConfig,
        semantic_runtime: ZepSemanticRuntime,
        graph_store: ZepGraphStore,
    ) -> None:
        self._config = config
        self._semantic_runtime = semantic_runtime
        self._graph_store = graph_store

    async def add_episode(
        self,
        *,
        group_id: str,
        message: str,
        valid_at_ms: int,
    ) -> ZepAddEpisodeResult:
        """Process one Zep `add_episode(message, group_id, valid_at)` call."""
        if not group_id:
            raise ValueError("group_id must be non-empty")
        if not str(message).strip():
            raise ValueError("message must be non-empty")
        if int(valid_at_ms) < 0:
            raise ValueError("valid_at_ms must be >= 0")

        recent_episodes = list(
            await self._graph_store.get_recent_episodes(
                group_id=group_id,
                limit=self._config.recent_episode_limit,
            )
        )
        episode_id = await self._graph_store.upsert_episode(
            group_id=group_id,
            content=message,
            valid_at_ms=valid_at_ms,
        )

        extracted_entities = list(
            await self._semantic_runtime.extract_entities(
                message=message,
                recent_episodes=recent_episodes,
                prompt=self._config.entity_extraction_prompt,
            )
        )

        resolved_entities: List[ZepResolvedEntity] = []
        entity_name_to_id: Dict[str, str] = {}
        for entity in extracted_entities:
            candidates = await self._graph_store.search_entity_candidates(
                group_id=group_id,
                query=entity.entity_name,
                top_k=self._config.entity_candidate_top_k,
            )
            resolution = await self._semantic_runtime.resolve_entity(
                extracted_entity=entity,
                candidates=candidates,
                message=message,
                recent_episodes=recent_episodes,
                prompt=self._config.entity_dedup_prompt,
            )
            summary = await self._semantic_runtime.summarize_entity(
                extracted_entity=entity,
                message=message,
                recent_episodes=recent_episodes,
                prompt=self._config.entity_summary_prompt,
            )
            existing_entity_id = (
                str(resolution.target_entity_id)
                if resolution.decision == "EXISTING"
                else None
            )
            entity_id = await self._graph_store.upsert_entity(
                group_id=group_id,
                entity_name=entity.entity_name,
                type_id=entity.type_id,
                summary=summary,
                existing_entity_id=existing_entity_id,
            )
            entity_name_to_id[entity.entity_name] = entity_id
            resolved_entities.append(
                ZepResolvedEntity(
                    entity_id=entity_id,
                    entity_name=entity.entity_name,
                    type_id=entity.type_id,
                    summary=summary,
                )
            )

        extracted_edges = list(
            await self._semantic_runtime.extract_edges(
                message=message,
                resolved_entities=resolved_entities,
                recent_episodes=recent_episodes,
                prompt=self._config.edge_extraction_prompt,
            )
        )

        added_edge_count = 0
        duplicate_edge_count = 0
        contradicted_edge_count = 0

        for edge in extracted_edges:
            source_entity_id = entity_name_to_id[edge.source_entity_name]
            destination_entity_id = entity_name_to_id[edge.destination_entity_name]
            candidates = await self._graph_store.search_edge_candidates(
                group_id=group_id,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
                query_fact=edge.fact,
                top_k=self._config.edge_candidate_top_k,
            )
            resolution = await self._semantic_runtime.resolve_edge(
                extracted_edge=edge,
                candidates=candidates,
                message=message,
                recent_episodes=recent_episodes,
                prompt=self._config.edge_resolution_prompt,
            )

            if resolution.action == "DUPLICATE":
                duplicate_edge_count += 1
                continue

            invalidates_edge_id = (
                str(resolution.target_edge_id)
                if resolution.action == "CONTRADICTS"
                else None
            )
            await self._graph_store.upsert_edge(
                group_id=group_id,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
                relation=resolution.relation,
                fact=resolution.fact,
                invalidates_edge_id=invalidates_edge_id,
            )

            if resolution.action == "CONTRADICTS":
                contradicted_edge_count += 1
            elif resolution.action == "ADD":
                added_edge_count += 1
            else:
                raise RuntimeError(f"unsupported edge action={resolution.action!r}")

        return ZepAddEpisodeResult(
            episode_id=episode_id,
            recalled_episode_count=len(recent_episodes),
            extracted_entity_count=len(extracted_entities),
            resolved_entity_count=len(resolved_entities),
            extracted_edge_count=len(extracted_edges),
            added_edge_count=added_edge_count,
            duplicate_edge_count=duplicate_edge_count,
            contradicted_edge_count=contradicted_edge_count,
            resolved_entities=resolved_entities,
        )
