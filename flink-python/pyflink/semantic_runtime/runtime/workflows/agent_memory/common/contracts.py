"""Typed data contracts for agent-memory workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional


@dataclass(frozen=True)
class ConversationMessage:
    """One append-only message in a conversation stream."""

    message_id: str
    group_id: str
    sender_id: str
    content: str
    timestamp_ms: int
    sender_name: Optional[str] = None
    role: str = "user"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.message_id:
            raise ValueError("ConversationMessage requires non-empty message_id")
        if not self.group_id:
            raise ValueError("ConversationMessage requires non-empty group_id")
        if not self.sender_id:
            raise ValueError("ConversationMessage requires non-empty sender_id")
        if not isinstance(self.timestamp_ms, int):
            raise TypeError("ConversationMessage.timestamp_ms must be int")


@dataclass(frozen=True)
class BoundaryDecision:
    """Boundary detection output for segment sealing."""

    should_end: bool
    should_wait: bool
    reasoning: str
    confidence: float
    forced: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.confidence, (int, float)):
            raise TypeError("BoundaryDecision.confidence must be numeric")


@dataclass(frozen=True)
class ForesightArtifact:
    """One foresight item extracted from a sealed segment."""

    content: str
    evidence: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_days: Optional[int] = None


@dataclass(frozen=True)
class EventLogArtifact:
    """One atomic event-log item extracted from a sealed segment."""

    atomic_fact: str
    timestamp_ms: Optional[int] = None


@dataclass(frozen=True)
class DecompositionArtifacts:
    """Artifacts produced from one sealed MemCell segment."""

    episode: str
    subject: str
    foresights: List[ForesightArtifact] = field(default_factory=list)
    event_logs: List[EventLogArtifact] = field(default_factory=list)


@dataclass
class MemCellRecord:
    """Persistent MemCell record used across insertion stages."""

    memcell_id: str
    group_id: str
    timestamp_ms: int
    original_messages: List[ConversationMessage]
    participants: List[str]
    scene: str
    summary: Optional[str] = None
    subject: Optional[str] = None
    episode: Optional[str] = None
    topic_id: Optional[str] = None


@dataclass
class TopicClusterState:
    """Incremental topic-clustering state per group."""

    event_ids: List[str] = field(default_factory=list)
    eventid_to_topic: Dict[str, str] = field(default_factory=dict)
    topic_centroids: Dict[str, List[float]] = field(default_factory=dict)
    topic_representatives: Dict[str, str] = field(default_factory=dict)
    topic_counts: Dict[str, int] = field(default_factory=dict)
    topic_last_ts: Dict[str, int] = field(default_factory=dict)
    next_topic_idx: int = 0


@dataclass(frozen=True)
class TopicAssignmentResult:
    """Result of assigning one memcell into topic state."""

    topic_id: str
    cluster_size: int
    updated_state: TopicClusterState


@dataclass(frozen=True)
class InsertionResult:
    """Insertion pipeline result for one memorize call."""

    status: str
    extracted_count: int
    memcell_id: Optional[str] = None
    topic_id: Optional[str] = None
    boundary: Optional[BoundaryDecision] = None
    profile_updated: bool = False


@dataclass(frozen=True)
class RetrievedMemory:
    """One retrieved memory candidate for query-time consumption."""

    memory_id: str
    memory_type: str
    content: str
    score: float
    source: str
    timestamp_ms: Optional[int] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.memory_id:
            raise ValueError("RetrievedMemory requires non-empty memory_id")
        if not self.memory_type:
            raise ValueError("RetrievedMemory requires non-empty memory_type")
        if not self.content:
            raise ValueError("RetrievedMemory requires non-empty content")
        if not self.source:
            raise ValueError("RetrievedMemory requires non-empty source")
        if not isinstance(self.score, (int, float)):
            raise TypeError("RetrievedMemory.score must be numeric")


@dataclass(frozen=True)
class RetrievalResult:
    """Workflow retrieval output for one query request."""

    mode: str
    selected_mode: str
    memories: List[RetrievedMemory]
    candidate_count: int
    reranked: bool
