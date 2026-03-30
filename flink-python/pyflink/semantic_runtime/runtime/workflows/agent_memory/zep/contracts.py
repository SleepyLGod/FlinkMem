"""Typed contracts for Zep/Graphiti workflow reconstruction."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence


VALID_ZEP_ENTITY_DECISIONS = frozenset({"EXISTING", "NEW"})
VALID_ZEP_EDGE_ACTIONS = frozenset({"ADD", "DUPLICATE", "CONTRADICTS"})


@dataclass(frozen=True)
class ZepEpisodeCandidate:
    """One recalled episodic context item."""

    episode_id: str
    content: str
    created_at_ms: int

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("episode_id must be non-empty")
        if not self.content:
            raise ValueError("content must be non-empty")
        if int(self.created_at_ms) < 0:
            raise ValueError("created_at_ms must be >= 0")


@dataclass(frozen=True)
class ZepExtractedEntity:
    """One entity extracted from the incoming episode message."""

    entity_name: str
    type_id: str

    def __post_init__(self) -> None:
        if not self.entity_name:
            raise ValueError("entity_name must be non-empty")
        if not self.type_id:
            raise ValueError("type_id must be non-empty")


@dataclass(frozen=True)
class ZepEntityCandidate:
    """One candidate graph entity for dedup resolution."""

    entity_id: str
    entity_name: str
    summary: str
    score: float
    source: str

    def __post_init__(self) -> None:
        if not self.entity_id:
            raise ValueError("entity_id must be non-empty")
        if not self.entity_name:
            raise ValueError("entity_name must be non-empty")
        if not self.summary:
            raise ValueError("summary must be non-empty")
        if not self.source:
            raise ValueError("source must be non-empty")
        if not isinstance(self.score, (int, float)):
            raise TypeError("score must be numeric")


@dataclass(frozen=True)
class ZepEntityResolution:
    """Resolution decision for one extracted entity."""

    decision: str
    entity_name: str
    target_entity_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.decision not in VALID_ZEP_ENTITY_DECISIONS:
            raise ValueError(
                f"decision must be one of {sorted(VALID_ZEP_ENTITY_DECISIONS)!r}"
            )
        if not self.entity_name:
            raise ValueError("entity_name must be non-empty")
        if self.decision == "EXISTING" and not self.target_entity_id:
            raise ValueError("target_entity_id is required for decision='EXISTING'")


@dataclass(frozen=True)
class ZepResolvedEntity:
    """One entity after dedup resolution and summary update."""

    entity_id: str
    entity_name: str
    type_id: str
    summary: str

    def __post_init__(self) -> None:
        if not self.entity_id:
            raise ValueError("entity_id must be non-empty")
        if not self.entity_name:
            raise ValueError("entity_name must be non-empty")
        if not self.type_id:
            raise ValueError("type_id must be non-empty")
        if not self.summary:
            raise ValueError("summary must be non-empty")


@dataclass(frozen=True)
class ZepExtractedEdge:
    """One factual edge extracted from the incoming episode message."""

    source_entity_name: str
    destination_entity_name: str
    relation: str
    fact: str

    def __post_init__(self) -> None:
        if not self.source_entity_name:
            raise ValueError("source_entity_name must be non-empty")
        if not self.destination_entity_name:
            raise ValueError("destination_entity_name must be non-empty")
        if not self.relation:
            raise ValueError("relation must be non-empty")
        if not self.fact:
            raise ValueError("fact must be non-empty")


@dataclass(frozen=True)
class ZepEdgeCandidate:
    """One candidate edge for duplicate/contradiction resolution."""

    edge_id: str
    source_entity_id: str
    destination_entity_id: str
    relation: str
    fact: str
    score: float
    source: str

    def __post_init__(self) -> None:
        if not self.edge_id:
            raise ValueError("edge_id must be non-empty")
        if not self.source_entity_id:
            raise ValueError("source_entity_id must be non-empty")
        if not self.destination_entity_id:
            raise ValueError("destination_entity_id must be non-empty")
        if not self.relation:
            raise ValueError("relation must be non-empty")
        if not self.fact:
            raise ValueError("fact must be non-empty")
        if not self.source:
            raise ValueError("source must be non-empty")
        if not isinstance(self.score, (int, float)):
            raise TypeError("score must be numeric")


@dataclass(frozen=True)
class ZepEdgeResolution:
    """Resolution for one extracted edge against candidate historical edges."""

    action: str
    source_entity_name: str
    destination_entity_name: str
    relation: str
    fact: str
    target_edge_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.action not in VALID_ZEP_EDGE_ACTIONS:
            raise ValueError(
                f"action must be one of {sorted(VALID_ZEP_EDGE_ACTIONS)!r}"
            )
        if not self.source_entity_name:
            raise ValueError("source_entity_name must be non-empty")
        if not self.destination_entity_name:
            raise ValueError("destination_entity_name must be non-empty")
        if not self.relation:
            raise ValueError("relation must be non-empty")
        if not self.fact:
            raise ValueError("fact must be non-empty")
        if self.action == "CONTRADICTS" and not self.target_edge_id:
            raise ValueError("target_edge_id is required for action='CONTRADICTS'")


@dataclass(frozen=True)
class ZepAddEpisodeResult:
    """Result for one add_episode call."""

    episode_id: str
    recalled_episode_count: int
    extracted_entity_count: int
    resolved_entity_count: int
    extracted_edge_count: int
    added_edge_count: int
    duplicate_edge_count: int
    contradicted_edge_count: int
    resolved_entities: Sequence[ZepResolvedEntity] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("episode_id must be non-empty")
