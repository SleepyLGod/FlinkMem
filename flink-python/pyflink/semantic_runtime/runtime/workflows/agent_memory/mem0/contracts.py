"""Typed contracts for Mem0 workflow reconstruction."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    RetrievedMemory,
)


VALID_MEM0_ACTIONS = frozenset({"ADD", "UPDATE", "DELETE", "NONE"})
VALID_MEM0_GRAPH_ENTITY_DECISIONS = frozenset({"SAME", "DIFFERENT"})
VALID_MEM0_GRAPH_RELATION_ACTIONS = frozenset({"CONTRADICTS", "AUGMENTS", "NEW"})


@dataclass(frozen=True)
class Mem0FactResolution:
    """One resolved memory action for one extracted fact."""

    action: str
    fact: str
    target_memory_id: Optional[str] = None
    content: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0

    def __post_init__(self) -> None:
        if self.action not in VALID_MEM0_ACTIONS:
            raise ValueError(
                f"Mem0FactResolution.action must be one of {sorted(VALID_MEM0_ACTIONS)!r}"
            )
        if not self.fact:
            raise ValueError("Mem0FactResolution.fact must be non-empty")
        if self.action in {"UPDATE", "DELETE"} and not self.target_memory_id:
            raise ValueError(
                f"Mem0FactResolution.target_memory_id is required for action={self.action}"
            )
        if self.action in {"ADD", "UPDATE"} and not self.content:
            raise ValueError(
                f"Mem0FactResolution.content is required for action={self.action}"
            )
        if not isinstance(self.confidence, (int, float)):
            raise TypeError("Mem0FactResolution.confidence must be numeric")


@dataclass(frozen=True)
class Mem0BasicOperation:
    """Executed operation record for one resolved fact."""

    action: str
    fact: str
    memory_id: Optional[str] = None
    content: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0


@dataclass(frozen=True)
class Mem0BasicAddResult:
    """Result of one Mem0 Basic ``add(messages)`` call."""

    extracted_fact_count: int
    added: int
    updated: int
    deleted: int
    noop: int
    operations: Sequence[Mem0BasicOperation] = field(default_factory=list)


@dataclass(frozen=True)
class Mem0BasicSearchResult:
    """Result of one Mem0 Basic ``search(query)`` call."""

    query: str
    top_k: int
    memories: Sequence[RetrievedMemory]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Mem0GraphExtractedEntity:
    """One entity extracted from messages in Mem0 Graph workflow."""

    entity_name: str
    entity_type: str

    def __post_init__(self) -> None:
        if not self.entity_name:
            raise ValueError("Mem0GraphExtractedEntity.entity_name must be non-empty")
        if not self.entity_type:
            raise ValueError("Mem0GraphExtractedEntity.entity_type must be non-empty")


@dataclass(frozen=True)
class Mem0GraphExtractedRelation:
    """One relation extracted from messages in Mem0 Graph workflow."""

    source_entity_name: str
    relationship: str
    destination_entity_name: str

    def __post_init__(self) -> None:
        if not self.source_entity_name:
            raise ValueError(
                "Mem0GraphExtractedRelation.source_entity_name must be non-empty"
            )
        if not self.relationship:
            raise ValueError("Mem0GraphExtractedRelation.relationship must be non-empty")
        if not self.destination_entity_name:
            raise ValueError(
                "Mem0GraphExtractedRelation.destination_entity_name must be non-empty"
            )


@dataclass(frozen=True)
class Mem0GraphEntityCandidate:
    """One recalled graph entity candidate."""

    entity_id: str
    entity_name: str
    entity_type: str
    score: float
    source: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.entity_id:
            raise ValueError("Mem0GraphEntityCandidate.entity_id must be non-empty")
        if not self.entity_name:
            raise ValueError("Mem0GraphEntityCandidate.entity_name must be non-empty")
        if not self.entity_type:
            raise ValueError("Mem0GraphEntityCandidate.entity_type must be non-empty")
        if not self.source:
            raise ValueError("Mem0GraphEntityCandidate.source must be non-empty")
        if not isinstance(self.score, (int, float)):
            raise TypeError("Mem0GraphEntityCandidate.score must be numeric")


@dataclass(frozen=True)
class Mem0GraphRelationCandidate:
    """One recalled graph relation candidate."""

    relation_id: str
    source_entity_id: str
    destination_entity_id: str
    relationship: str
    score: float
    source: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.relation_id:
            raise ValueError("Mem0GraphRelationCandidate.relation_id must be non-empty")
        if not self.source_entity_id:
            raise ValueError(
                "Mem0GraphRelationCandidate.source_entity_id must be non-empty"
            )
        if not self.destination_entity_id:
            raise ValueError(
                "Mem0GraphRelationCandidate.destination_entity_id must be non-empty"
            )
        if not self.relationship:
            raise ValueError("Mem0GraphRelationCandidate.relationship must be non-empty")
        if not self.source:
            raise ValueError("Mem0GraphRelationCandidate.source must be non-empty")
        if not isinstance(self.score, (int, float)):
            raise TypeError("Mem0GraphRelationCandidate.score must be numeric")


@dataclass(frozen=True)
class Mem0GraphEntityResolution:
    """Entity identity resolution output for one extracted entity."""

    decision: str
    entity_name: str
    target_entity_id: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0

    def __post_init__(self) -> None:
        if self.decision not in VALID_MEM0_GRAPH_ENTITY_DECISIONS:
            raise ValueError(
                "Mem0GraphEntityResolution.decision must be one of "
                f"{sorted(VALID_MEM0_GRAPH_ENTITY_DECISIONS)!r}"
            )
        if not self.entity_name:
            raise ValueError("Mem0GraphEntityResolution.entity_name must be non-empty")
        if self.decision == "SAME" and not self.target_entity_id:
            raise ValueError(
                "Mem0GraphEntityResolution.target_entity_id is required for decision='SAME'"
            )
        if not isinstance(self.confidence, (int, float)):
            raise TypeError("Mem0GraphEntityResolution.confidence must be numeric")


@dataclass(frozen=True)
class Mem0GraphRelationResolution:
    """Relation action resolution output for one extracted relation."""

    action: str
    source_entity_name: str
    destination_entity_name: str
    relationship: str
    target_relation_id: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0

    def __post_init__(self) -> None:
        if self.action not in VALID_MEM0_GRAPH_RELATION_ACTIONS:
            raise ValueError(
                "Mem0GraphRelationResolution.action must be one of "
                f"{sorted(VALID_MEM0_GRAPH_RELATION_ACTIONS)!r}"
            )
        if not self.source_entity_name:
            raise ValueError(
                "Mem0GraphRelationResolution.source_entity_name must be non-empty"
            )
        if not self.destination_entity_name:
            raise ValueError(
                "Mem0GraphRelationResolution.destination_entity_name must be non-empty"
            )
        if not self.relationship:
            raise ValueError(
                "Mem0GraphRelationResolution.relationship must be non-empty"
            )
        if self.action in {"CONTRADICTS", "AUGMENTS"} and not self.target_relation_id:
            raise ValueError(
                "Mem0GraphRelationResolution.target_relation_id is required for "
                f"action={self.action!r}"
            )
        if not isinstance(self.confidence, (int, float)):
            raise TypeError("Mem0GraphRelationResolution.confidence must be numeric")


@dataclass(frozen=True)
class Mem0GraphRelationOperation:
    """Executed operation record for one resolved graph relation."""

    action: str
    relation_id: Optional[str]
    source_entity_id: str
    destination_entity_id: str
    relationship: str
    reason: str = ""
    confidence: float = 0.0


@dataclass(frozen=True)
class Mem0GraphAddResult:
    """Result of one Mem0 Graph ``add(messages)`` call."""

    extracted_entity_count: int
    extracted_relation_count: int
    upserted_entities: int
    added_relations: int
    updated_relations: int
    deleted_relations: int
    operations: Sequence[Mem0GraphRelationOperation] = field(default_factory=list)


@dataclass(frozen=True)
class Mem0GraphSearchResult:
    """Result of one Mem0 Graph ``search(query)`` call."""

    query: str
    top_k: int
    relations: Sequence[Mem0GraphRelationCandidate]
    metadata: Mapping[str, Any] = field(default_factory=dict)
