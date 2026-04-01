"""Protocols for Mem0 workflow dependencies."""

from __future__ import annotations

from typing import Optional, Protocol, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.contracts import (
    Mem0GraphEntityCandidate,
    Mem0GraphEntityResolution,
    Mem0GraphExtractedEntity,
    Mem0GraphExtractedRelation,
    Mem0GraphRelationCandidate,
    Mem0GraphRelationResolution,
    Mem0FactResolution,
)


class Mem0FactStore(Protocol):
    """Persistence interface for Mem0 fact memory records."""

    async def add_fact(self, *, group_id: str, content: str) -> str:
        """Persist one new fact and return created memory id."""

    async def update_fact(self, *, group_id: str, memory_id: str, content: str) -> None:
        """Update one existing fact memory."""

    async def delete_fact(self, *, group_id: str, memory_id: str) -> None:
        """Delete one existing fact memory."""


class Mem0FactSearcher(Protocol):
    """Recall interface for Mem0 candidate memories (embedding backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        """Return candidate memories for one fact/query."""


class Mem0LLMFactSearcher(Protocol):
    """Recall interface for Mem0 candidate memories (LLM backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        """Return candidate memories for one fact/query."""


class Mem0BasicSemanticRuntime(Protocol):
    """Semantic execution interface for Mem0 Basic flow."""

    async def extract_facts(
        self,
        *,
        messages: Sequence[str],
        prompt: str,
    ) -> Sequence[str]:
        """Extract standalone facts from input messages."""

    async def resolve_facts(
        self,
        *,
        facts: Sequence[str],
        candidates_by_fact: Sequence[Sequence[RetrievedMemory]],
        prompt: str,
    ) -> Sequence[Mem0FactResolution]:
        """Resolve all extracted facts into one operation list in fact order."""


class Mem0GraphStore(Protocol):
    """Persistence interface for Mem0 graph entities and relations."""

    async def upsert_entity(
        self,
        *,
        group_id: str,
        entity_name: str,
        entity_type: str,
        existing_entity_id: Optional[str],
    ) -> str:
        """Upsert one entity and return resolved entity id."""

    async def add_relation(
        self,
        *,
        group_id: str,
        source_entity_id: str,
        destination_entity_id: str,
        relationship: str,
    ) -> str:
        """Create one relation and return relation id."""

    async def update_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
        relationship: str,
    ) -> None:
        """Update one existing relation."""

    async def delete_relation(
        self,
        *,
        group_id: str,
        relation_id: str,
    ) -> None:
        """Delete one existing relation."""


class Mem0GraphEntitySearcher(Protocol):
    """Entity recall interface (embedding backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[Mem0GraphEntityCandidate]:
        """Return recalled entity candidates for one query."""


class Mem0GraphLLMEntitySearcher(Protocol):
    """Entity recall interface (LLM backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
    ) -> Sequence[Mem0GraphEntityCandidate]:
        """Return recalled entity candidates for one query."""


class Mem0GraphRelationSearcher(Protocol):
    """Relation recall interface (embedding backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mem0GraphRelationCandidate]:
        """Return recalled relation candidates for one query."""


class Mem0GraphLLMRelationSearcher(Protocol):
    """Relation recall interface (LLM backend)."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        source_entity_id: Optional[str],
        destination_entity_id: Optional[str],
    ) -> Sequence[Mem0GraphRelationCandidate]:
        """Return recalled relation candidates for one query."""


class Mem0GraphSemanticRuntime(Protocol):
    """Semantic execution interface for Mem0 Graph flow."""

    async def extract_entities(
        self,
        *,
        messages: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedEntity]:
        """Extract entities from input messages."""

    async def extract_relations(
        self,
        *,
        messages: Sequence[str],
        entities: Sequence[Mem0GraphExtractedEntity],
        allowed_entity_names: Sequence[str],
        prompt: str,
    ) -> Sequence[Mem0GraphExtractedRelation]:
        """Extract relations from input messages and extracted entities."""

    async def resolve_entity(
        self,
        *,
        entity: Mem0GraphExtractedEntity,
        candidates: Sequence[Mem0GraphEntityCandidate],
        prompt: str,
    ) -> Mem0GraphEntityResolution:
        """Resolve one extracted entity against recalled candidates."""

    async def resolve_relation(
        self,
        *,
        relation: Mem0GraphExtractedRelation,
        candidates: Sequence[Mem0GraphRelationCandidate],
        prompt: str,
    ) -> Mem0GraphRelationResolution:
        """Resolve one extracted relation into a graph action."""
