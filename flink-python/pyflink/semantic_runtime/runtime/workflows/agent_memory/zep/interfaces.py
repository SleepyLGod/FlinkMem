"""Protocols for Zep/Graphiti workflow dependencies."""

from __future__ import annotations

from typing import Protocol, Sequence

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


class ZepGraphStore(Protocol):
    """Persistence and retrieval interface for Zep graph state."""

    async def get_recent_episodes(
        self,
        *,
        group_id: str,
        limit: int,
    ) -> Sequence[ZepEpisodeCandidate]:
        """Return recent episodic nodes for context retrieval."""

    async def upsert_episode(
        self,
        *,
        group_id: str,
        content: str,
        valid_at_ms: int,
    ) -> str:
        """Persist one episodic node and return its id."""

    async def search_entity_candidates(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[ZepEntityCandidate]:
        """Return candidate entity nodes for one extracted entity."""

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        type_id: str,
        summary: str,
        existing_entity_id: str | None,
    ) -> str:
        """Upsert one entity and return resolved entity id."""

    async def search_edge_candidates(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        query_fact: str,
        top_k: int,
    ) -> Sequence[ZepEdgeCandidate]:
        """Return candidate edges for duplicate/contradiction resolution."""

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
        """Insert/update one relation edge and return edge id."""


class ZepSemanticRuntime(Protocol):
    """Semantic execution interface for Zep add-episode flow."""

    async def extract_entities(
        self,
        *,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEntity]:
        """Extract entities from incoming message and episodic context."""

    async def resolve_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        candidates: Sequence[ZepEntityCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEntityResolution:
        """Resolve one extracted entity against candidate entities."""

    async def summarize_entity(
        self,
        *,
        extracted_entity: ZepExtractedEntity,
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> str:
        """Generate or update summary for one resolved entity."""

    async def extract_edges(
        self,
        *,
        message: str,
        resolved_entities: Sequence[ZepResolvedEntity],
        allowed_entity_names: Sequence[str],
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> Sequence[ZepExtractedEdge]:
        """Extract candidate factual edges from incoming message."""

    async def resolve_edge(
        self,
        *,
        extracted_edge: ZepExtractedEdge,
        candidates: Sequence[ZepEdgeCandidate],
        message: str,
        recent_episodes: Sequence[ZepEpisodeCandidate],
        prompt: str,
    ) -> ZepEdgeResolution:
        """Resolve one extracted edge into ADD/DUPLICATE/CONTRADICTS."""
