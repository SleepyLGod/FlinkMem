"""Unit tests for Zep/Graphiti workflow skeleton."""

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

from typing import Dict, List, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.config import (
    ZepWorkflowConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.contracts import (
    ZepEdgeCandidate,
    ZepEdgeResolution,
    ZepEntityCandidate,
    ZepEntityResolution,
    ZepEpisodeCandidate,
    ZepExtractedEdge,
    ZepExtractedEntity,
    ZepResolvedEntity,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.workflow import (
    ZepAddEpisodeWorkflow,
)


class _ScriptedGraphStore:
    def __init__(self) -> None:
        self.episodes: List[str] = []
        self.entities: Dict[str, str] = {}
        self.edges: List[str] = []
        self._entity_counter = 0
        self._edge_counter = 0

    async def get_recent_episodes(
        self,
        *,
        group_id: str,
        limit: int,
    ) -> Sequence[ZepEpisodeCandidate]:
        _ = (group_id, limit)
        return [
            ZepEpisodeCandidate(
                episode_id="ep-old-1",
                content="Alice likes ML",
                created_at_ms=100,
            )
        ]

    async def upsert_episode(
        self,
        *,
        group_id: str,
        content: str,
        valid_at_ms: int,
    ) -> str:
        _ = (group_id, valid_at_ms)
        self.episodes.append(content)
        return "ep-new-1"

    async def search_entity_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[ZepEntityCandidate]:
        _ = (group_id, top_k)
        if query == "Alice":
            return [
                ZepEntityCandidate(
                    entity_id="e-alice",
                    entity_name="Alice",
                    summary="Alice profile",
                    score=0.93,
                    source="graph",
                )
            ]
        return []

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        type_id: str,
        summary: str,
        existing_entity_id: str | None,
    ) -> str:
        _ = (group_id, type_id, summary)
        if existing_entity_id is not None:
            self.entities[entity_name] = existing_entity_id
            return existing_entity_id
        self._entity_counter += 1
        entity_id = f"e-new-{self._entity_counter}"
        self.entities[entity_name] = entity_id
        return entity_id

    async def search_edge_candidates(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        query_fact: str,
        top_k: int,
    ) -> Sequence[ZepEdgeCandidate]:
        _ = (group_id, source_entity_id, destination_entity_id, top_k)
        if "works" in query_fact:
            return [
                ZepEdgeCandidate(
                    edge_id="edge-old-1",
                    source_entity_id=source_entity_id,
                    destination_entity_id=destination_entity_id,
                    relation="works_with",
                    fact="Alice works with Bob",
                    score=0.88,
                    source="graph",
                )
            ]
        return []

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
        _ = (group_id, source_entity_id, destination_entity_id, invalidates_edge_id)
        self._edge_counter += 1
        edge_id = f"edge-{self._edge_counter}"
        self.edges.append(f"{edge_id}:{relation}:{fact}")
        return edge_id


class _ScriptedSemanticRuntime:
    def __init__(self) -> None:
        self.entity_prompts: List[str] = []
        self.edge_prompts: List[str] = []

    async def extract_entities(
        self,
        *,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEntity]:
        _ = (message, recent_episodes)
        self.entity_prompts.append(prompt)
        return [
            ZepExtractedEntity(entity_name="Alice", type_id="person"),
            ZepExtractedEntity(entity_name="Bob", type_id="person"),
        ]

    async def resolve_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        candidates: Sequence[ZepEntityCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEntityResolution:
        _ = (candidates, message, recent_episodes)
        self.entity_prompts.append(prompt)
        if extracted_entity.entity_name == "Alice":
            return ZepEntityResolution(
                decision="EXISTING",
                entity_name="Alice",
                target_entity_id="e-alice",
            )
        return ZepEntityResolution(
            decision="NEW",
            entity_name=extracted_entity.entity_name,
        )

    async def summarize_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> str:
        _ = (message, recent_episodes)
        self.entity_prompts.append(prompt)
        return f"Summary for {extracted_entity.entity_name}"

    async def extract_edges(
        self,
        *,
        message: str,
        resolved_entities: Sequence[ZepResolvedEntity],
        allowed_entity_names: Sequence[str],
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEdge]:
        _ = (message, resolved_entities, allowed_entity_names, recent_episodes)
        self.edge_prompts.append(prompt)
        return [
            ZepExtractedEdge(
                source_entity_name="Alice",
                destination_entity_name="Bob",
                relation="works_with",
                fact="Alice works with Bob",
            ),
            ZepExtractedEdge(
                source_entity_name="Alice",
                destination_entity_name="Bob",
                relation="met",
                fact="Alice met Bob today",
            ),
        ]

    async def resolve_edge(
        self,
        *,
        extracted_edge: ZepExtractedEdge,
        candidates: Sequence[ZepEdgeCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEdgeResolution:
        _ = (candidates, message, recent_episodes)
        self.edge_prompts.append(prompt)
        if extracted_edge.relation == "works_with":
            return ZepEdgeResolution(
                action="DUPLICATE",
                source_entity_name=extracted_edge.source_entity_name,
                destination_entity_name=extracted_edge.destination_entity_name,
                relation=extracted_edge.relation,
                fact=extracted_edge.fact,
            )
        return ZepEdgeResolution(
            action="ADD",
            source_entity_name=extracted_edge.source_entity_name,
            destination_entity_name=extracted_edge.destination_entity_name,
            relation=extracted_edge.relation,
            fact=extracted_edge.fact,
        )


def test_zep_add_episode_workflow_executes_pipeline() -> None:
    workflow = ZepAddEpisodeWorkflow(
        config=ZepWorkflowConfig(),
        semantic_runtime=_ScriptedSemanticRuntime(),
        graph_store=_ScriptedGraphStore(),
    )

    result = asyncio.run(
        workflow.add_episode(
            group_id="g1",
            message="Alice met Bob and talked about work",
            valid_at_ms=1234,
        )
    )

    assert result.episode_id == "ep-new-1"
    assert result.recalled_episode_count == 1
    assert result.extracted_entity_count == 2
    assert result.resolved_entity_count == 2
    assert result.extracted_edge_count == 2
    assert result.added_edge_count == 1
    assert result.duplicate_edge_count == 1
    assert result.contradicted_edge_count == 0


def test_zep_add_episode_rejects_existing_entity_id_outside_candidates() -> None:
    class _InvalidEntityResolutionRuntime(_ScriptedSemanticRuntime):
        async def resolve_entity(
            self,
            *,
            extracted_entity: ZepExtractedEntity,
            candidates: Sequence[ZepEntityCandidate],
            message: str,
            recent_episodes: Sequence[ZepEpisodeCandidate],
            prompt: str,
        ) -> ZepEntityResolution:
            _ = (candidates, message, recent_episodes, prompt)
            if extracted_entity.entity_name == "Alice":
                return ZepEntityResolution(
                    decision="EXISTING",
                    entity_name="Alice",
                    target_entity_id="e-not-in-candidates",
                )
            return ZepEntityResolution(
                decision="NEW",
                entity_name=extracted_entity.entity_name,
            )

    workflow = ZepAddEpisodeWorkflow(
        config=ZepWorkflowConfig(),
        semantic_runtime=_InvalidEntityResolutionRuntime(),
        graph_store=_ScriptedGraphStore(),
    )

    try:
        asyncio.run(
            workflow.add_episode(
                group_id="g1",
                message="Alice met Bob and talked about work",
                valid_at_ms=1234,
            )
        )
        raise AssertionError("Expected ValueError for invalid EXISTING target_entity_id")
    except ValueError as exc:
        assert "not present in candidates" in str(exc)


def test_zep_add_episode_summarizes_after_edge_writes() -> None:
    ordering_state: Dict[str, bool] = {"edge_written": False}

    class _EdgeAwareStore(_ScriptedGraphStore):
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
            ordering_state["edge_written"] = True
            return await super().upsert_edge(
                group_id=group_id,
                source_entity_id=source_entity_id,
                destination_entity_id=destination_entity_id,
                relation=relation,
                fact=fact,
                invalidates_edge_id=invalidates_edge_id,
            )

    class _OrderRuntime(_ScriptedSemanticRuntime):
        async def summarize_entity(
            self,
            *,
            extracted_entity: ZepExtractedEntity,
            message: str,
            recent_episodes: Sequence[ZepEpisodeCandidate],
            prompt: str,
        ) -> str:
            _ = (extracted_entity, message, recent_episodes, prompt)
            if not ordering_state["edge_written"]:
                raise RuntimeError("summarize_entity called before edge writes")
            return "final summary"

    workflow = ZepAddEpisodeWorkflow(
        config=ZepWorkflowConfig(),
        semantic_runtime=_OrderRuntime(),
        graph_store=_EdgeAwareStore(),
    )
    result = asyncio.run(
        workflow.add_episode(
            group_id="g1",
            message="Alice met Bob and talked about work",
            valid_at_ms=1234,
        )
    )
    assert result.added_edge_count == 1
    assert ordering_state["edge_written"] is True
