"""Zep/Graphiti workflow implementation."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.concurrency import (
    amap_grouped_serial_bounded,
    amap_ordered_bounded,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.config import (
    ZepWorkflowConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepAddEpisodeResult,
    ZepEdgeResolution,
    ZepEpisodeCandidate,
    ZepExtractedEdge,
    ZepExtractedEntity,
    ZepResolvedEntity,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.interfaces import (
    ZepGraphStore,
    ZepSemanticRuntime,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.entity_reference_mode import (
    DRIFT_POLICY_FAIL_FAST,
    is_upstream_compatible_drift_policy,
    normalize_drift_policy,
)


@dataclass(frozen=True)
class _ResolvedEntityPlan:
    """One extracted entity plus dedup decision."""

    entity: ZepExtractedEntity
    existing_entity_id: str | None


@dataclass(frozen=True)
class _ResolvedEdgePlan:
    """One extracted edge plus resolved action and concrete endpoint ids."""

    edge: ZepExtractedEdge
    resolution: ZepEdgeResolution
    source_entity_id: str
    destination_entity_id: str


@dataclass(frozen=True)
class _SummaryUpdatePlan:
    """One persisted entity scheduled for post-edge summary update."""

    entity: ZepExtractedEntity
    persisted_entity: ZepResolvedEntity


@dataclass(frozen=True)
class _SummaryWritePlan:
    """One summary update ready to write back to graph store."""

    persisted_entity: ZepResolvedEntity
    summary: str


@dataclass(frozen=True)
class _SkippedEdgePlan:
    """Marker for one extracted edge intentionally skipped before resolution."""

    reason: str


def _normalize_entity_name_key(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("entity_name must be non-empty")
    return " ".join(text.split()).casefold()


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
        entity_plans = await amap_ordered_bounded(
            items=extracted_entities,
            concurrency=self._config.entity_resolve_concurrency,
            worker=lambda index, entity: self._resolve_entity_plan(
                index=index,
                group_id=group_id,
                entity=entity,
                message=message,
                recent_episodes=recent_episodes,
            ),
        )
        provisional_entities = await amap_grouped_serial_bounded(
            items=entity_plans,
            group_key=self._entity_upsert_group_key,
            concurrency=self._config.entity_upsert_group_concurrency,
            worker=lambda index, plan: self._upsert_entity_plan(
                index=index,
                group_id=group_id,
                plan=plan,
            ),
        )
        entity_name_to_id: Dict[str, str] = {}
        normalized_entity_name_to_id: Dict[str, str] = {}
        for resolved_entity in provisional_entities:
            entity_name_to_id[resolved_entity.entity_name] = resolved_entity.entity_id
            normalized_name = _normalize_entity_name_key(resolved_entity.entity_name)
            previous = normalized_entity_name_to_id.get(normalized_name)
            if previous is not None and previous != resolved_entity.entity_id:
                raise ValueError(
                    "ambiguous normalized entity name maps to multiple entity ids: "
                    f"{resolved_entity.entity_name!r}"
                )
            normalized_entity_name_to_id[normalized_name] = resolved_entity.entity_id

        extracted_edges = list(
            await self._semantic_runtime.extract_edges(
                message=message,
                resolved_entities=provisional_entities,
                allowed_entity_names=list(entity_name_to_id.keys()),
                recent_episodes=recent_episodes,
                prompt=self._config.edge_extraction_prompt,
            )
        )

        edge_plan_candidates = await amap_ordered_bounded(
            items=extracted_edges,
            concurrency=self._config.edge_resolve_concurrency,
            worker=lambda index, edge: self._resolve_edge_plan(
                index=index,
                group_id=group_id,
                edge=edge,
                message=message,
                recent_episodes=recent_episodes,
                normalized_entity_name_to_id=normalized_entity_name_to_id,
            ),
        )
        edge_plans: list[_ResolvedEdgePlan] = []
        for plan in edge_plan_candidates:
            if isinstance(plan, _SkippedEdgePlan):
                continue
            edge_plans.append(plan)
        edge_actions = await amap_grouped_serial_bounded(
            items=edge_plans,
            group_key=self._edge_write_group_key,
            concurrency=self._config.edge_write_group_concurrency,
            worker=lambda index, plan: self._apply_edge_plan(
                index=index,
                group_id=group_id,
                plan=plan,
            ),
        )
        summary_plans = [
            _SummaryUpdatePlan(entity=plan.entity, persisted_entity=persisted_entity)
            for plan, persisted_entity in zip(
                entity_plans, provisional_entities, strict=True
            )
        ]
        summary_write_plans = await amap_ordered_bounded(
            items=summary_plans,
            concurrency=self._config.entity_summary_concurrency,
            worker=lambda index, plan: self._build_summary_write_plan(
                index=index,
                plan=plan,
                message=message,
                recent_episodes=recent_episodes,
            ),
        )
        resolved_entities = await amap_grouped_serial_bounded(
            items=summary_write_plans,
            group_key=self._summary_update_group_key,
            concurrency=self._config.entity_upsert_group_concurrency,
            worker=lambda index, plan: self._apply_summary_write_plan(
                index=index,
                group_id=group_id,
                plan=plan,
            ),
        )

        added_edge_count = 0
        duplicate_edge_count = 0
        contradicted_edge_count = 0
        for action in edge_actions:
            if action == "ADD":
                added_edge_count += 1
            elif action == "DUPLICATE":
                duplicate_edge_count += 1
            elif action == "CONTRADICTS":
                contradicted_edge_count += 1
            else:
                raise RuntimeError(f"unsupported edge action={action!r}")

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

    async def _resolve_entity_plan(
        self,
        *,
        index: int,
        group_id: str,
        entity: ZepExtractedEntity,
        message: str,
        recent_episodes: List[ZepEpisodeCandidate],
    ) -> _ResolvedEntityPlan:
        _ = index
        candidates = await self._graph_store.search_entity_candidates(
            group_id=group_id,
            query=entity.entity_name,
            top_k=self._config.entity_candidate_top_k,
        )
        candidate_entity_ids = {candidate.entity_id for candidate in candidates}
        resolution = await self._semantic_runtime.resolve_entity(
            extracted_entity=entity,
            candidates=candidates,
            message=message,
            recent_episodes=recent_episodes,
            prompt=self._config.entity_dedup_prompt,
        )
        if resolution.decision == "EXISTING":
            existing_entity_id = str(resolution.target_entity_id)
            if existing_entity_id not in candidate_entity_ids:
                if self._is_upstream_compatible_drift_policy():
                    existing_entity_id = None
                    return _ResolvedEntityPlan(
                        entity=entity,
                        existing_entity_id=existing_entity_id,
                    )
                raise ValueError(
                    "resolve_entity returned EXISTING target_entity_id "
                    f"not present in candidates: entity_name={entity.entity_name!r} "
                    f"target_entity_id={existing_entity_id!r}"
                )
        else:
            existing_entity_id = None
        return _ResolvedEntityPlan(
            entity=entity,
            existing_entity_id=existing_entity_id,
        )

    def _entity_upsert_group_key(self, index: int, plan: _ResolvedEntityPlan) -> str:
        _ = index
        return _normalize_entity_name_key(plan.entity.entity_name)

    async def _upsert_entity_plan(
        self,
        *,
        index: int,
        group_id: str,
        plan: _ResolvedEntityPlan,
    ) -> ZepResolvedEntity:
        _ = index
        provisional_summary = self._build_provisional_summary(plan.entity)
        entity_id = await self._graph_store.upsert_entity(
            group_id=group_id,
            entity_name=plan.entity.entity_name,
            type_id=plan.entity.type_id,
            summary=provisional_summary,
            existing_entity_id=plan.existing_entity_id,
        )
        return ZepResolvedEntity(
            entity_id=entity_id,
            entity_name=plan.entity.entity_name,
            type_id=plan.entity.type_id,
            summary=provisional_summary,
        )

    def _summary_update_group_key(self, index: int, plan: _SummaryWritePlan) -> str:
        _ = index
        return _normalize_entity_name_key(plan.persisted_entity.entity_name)

    async def _build_summary_write_plan(
        self,
        *,
        index: int,
        plan: _SummaryUpdatePlan,
        message: str,
        recent_episodes: List[ZepEpisodeCandidate],
    ) -> _SummaryWritePlan:
        _ = index
        summary = await self._semantic_runtime.summarize_entity(
            extracted_entity=plan.entity,
            message=message,
            recent_episodes=recent_episodes,
            prompt=self._config.entity_summary_prompt,
        )
        return _SummaryWritePlan(
            persisted_entity=plan.persisted_entity,
            summary=summary,
        )

    async def _apply_summary_write_plan(
        self,
        *,
        index: int,
        group_id: str,
        plan: _SummaryWritePlan,
    ) -> ZepResolvedEntity:
        _ = index
        entity_id = await self._graph_store.upsert_entity(
            group_id=group_id,
            entity_name=plan.persisted_entity.entity_name,
            type_id=plan.persisted_entity.type_id,
            summary=plan.summary,
            existing_entity_id=plan.persisted_entity.entity_id,
        )
        if entity_id != plan.persisted_entity.entity_id:
            raise RuntimeError(
                "entity summary update changed entity id unexpectedly: "
                f"before={plan.persisted_entity.entity_id!r} after={entity_id!r}"
            )
        return ZepResolvedEntity(
            entity_id=entity_id,
            entity_name=plan.persisted_entity.entity_name,
            type_id=plan.persisted_entity.type_id,
            summary=plan.summary,
        )

    def _build_provisional_summary(self, entity: ZepExtractedEntity) -> str:
        summary = str(entity.entity_name).strip()
        if not summary:
            raise ValueError("entity_name must be non-empty for provisional summary")
        return summary

    def _drift_policy(self) -> str:
        policy = getattr(
            self._semantic_runtime,
            "drift_policy",
            DRIFT_POLICY_FAIL_FAST,
        )
        return normalize_drift_policy(
            policy,
            field_name="drift_policy",
        )

    def _is_upstream_compatible_drift_policy(self) -> bool:
        return is_upstream_compatible_drift_policy(self._drift_policy())

    async def _resolve_edge_plan(
        self,
        *,
        index: int,
        group_id: str,
        edge: ZepExtractedEdge,
        message: str,
        recent_episodes: List[ZepEpisodeCandidate],
        normalized_entity_name_to_id: Dict[str, str],
    ) -> _ResolvedEdgePlan | _SkippedEdgePlan:
        _ = index
        source_name = _normalize_entity_name_key(edge.source_entity_name)
        destination_name = _normalize_entity_name_key(edge.destination_entity_name)
        if source_name not in normalized_entity_name_to_id:
            if self._is_upstream_compatible_drift_policy():
                return _SkippedEdgePlan(reason="source endpoint outside resolved set")
            raise ValueError(
                "extract_edges returned source entity outside resolved set: "
                f"{edge.source_entity_name!r}"
            )
        if destination_name not in normalized_entity_name_to_id:
            if self._is_upstream_compatible_drift_policy():
                return _SkippedEdgePlan(
                    reason="destination endpoint outside resolved set"
                )
            raise ValueError(
                "extract_edges returned destination entity outside resolved set: "
                f"{edge.destination_entity_name!r}"
            )
        source_entity_id = normalized_entity_name_to_id[source_name]
        destination_entity_id = normalized_entity_name_to_id[destination_name]
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
        return _ResolvedEdgePlan(
            edge=edge,
            resolution=resolution,
            source_entity_id=source_entity_id,
            destination_entity_id=destination_entity_id,
        )

    def _edge_write_group_key(self, index: int, plan: _ResolvedEdgePlan) -> str:
        action = str(plan.resolution.action)
        if action == "DUPLICATE":
            return f"duplicate:{index}"
        if action == "CONTRADICTS":
            target_edge_id = str(plan.resolution.target_edge_id)
            if not target_edge_id:
                raise ValueError(
                    "target_edge_id is required for CONTRADICTS write grouping"
                )
            return f"edge:{target_edge_id}"
        if action == "ADD":
            relation = " ".join(str(plan.resolution.relation).split()).casefold()
            return (
                f"add:{plan.source_entity_id}:"
                f"{plan.destination_entity_id}:{relation}"
            )
        raise RuntimeError(f"unsupported edge action={action!r}")

    async def _apply_edge_plan(
        self,
        *,
        index: int,
        group_id: str,
        plan: _ResolvedEdgePlan,
    ) -> str:
        _ = index
        action = str(plan.resolution.action)
        if action == "DUPLICATE":
            return action
        invalidates_edge_id = (
            str(plan.resolution.target_edge_id)
            if action == "CONTRADICTS"
            else None
        )
        await self._graph_store.upsert_edge(
            group_id=group_id,
            source_entity_id=plan.source_entity_id,
            destination_entity_id=plan.destination_entity_id,
            relation=plan.resolution.relation,
            fact=plan.resolution.fact,
            invalidates_edge_id=invalidates_edge_id,
        )
        if action not in {"ADD", "CONTRADICTS"}:
            raise RuntimeError(f"unsupported edge action={action!r}")
        return action
