"""Runtime interfaces for agent-memory workflow composition."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Protocol, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    BoundaryDecision,
    ConversationMessage,
    DecompositionArtifacts,
    MemCellRecord,
    RetrievedMemory,
    TopicAssignmentResult,
    TopicClusterState,
)


class ConversationStatusStore(Protocol):
    """Persistence interface for segmentation status."""

    async def get_last_memcell_time_ms(self, group_id: str) -> Optional[int]:
        """Return the last sealed MemCell timestamp for one group."""

    async def update_last_memcell_time_ms(self, group_id: str, timestamp_ms: int) -> None:
        """Persist the new last sealed MemCell timestamp."""


class ConversationBufferStore(Protocol):
    """Persistence interface for accumulated pre-boundary messages."""

    async def load_messages_since(
        self,
        group_id: str,
        start_time_ms: Optional[int],
    ) -> Sequence[ConversationMessage]:
        """Load buffered messages since one timestamp (inclusive)."""

    async def append_messages(
        self,
        group_id: str,
        messages: Sequence[ConversationMessage],
    ) -> None:
        """Append new raw messages into accumulation buffer."""

    async def clear_consumed_messages(
        self,
        group_id: str,
        consumed_message_ids: Sequence[str],
    ) -> None:
        """Remove consumed historical messages after a boundary is sealed."""


class MemCellStore(Protocol):
    """Persistence interface for MemCell entities."""

    async def create_memcell(self, memcell: MemCellRecord) -> str:
        """Persist a new MemCell record and return its id."""

    async def update_memcell_fields(
        self,
        memcell_id: str,
        *,
        summary: Optional[str],
        subject: Optional[str],
        episode: Optional[str],
        topic_id: Optional[str],
    ) -> None:
        """Persist semantic fields for one existing MemCell."""

    async def list_memcells_by_topic(
        self,
        group_id: str,
        topic_id: str,
    ) -> Sequence[MemCellRecord]:
        """Return MemCells belonging to one topic."""


class MemoryArtifactStore(Protocol):
    """Persistence and index sync interface for decomposition artifacts."""

    async def persist_decomposition(
        self,
        *,
        group_id: str,
        memcell_id: str,
        scene: str,
        artifacts: DecompositionArtifacts,
    ) -> None:
        """Persist episode/foresight/event-log outputs."""

    async def sync_indexes(self, *, group_id: str, memcell_id: str) -> None:
        """Synchronize secondary indexes (e.g., ES / vector)."""


class TopicStateStore(Protocol):
    """Persistence interface for topic-clustering state."""

    async def load_state(self, group_id: str) -> TopicClusterState:
        """Load current topic state for one group."""

    async def save_state(self, group_id: str, state: TopicClusterState) -> None:
        """Persist updated topic state for one group."""


class TopicAssigner(Protocol):
    """Semantic topic assignment interface."""

    async def assign_memcell(
        self,
        *,
        memcell: MemCellRecord,
        state: TopicClusterState,
    ) -> TopicAssignmentResult:
        """Assign one MemCell into topic state and return updated state."""


class ProfileStore(Protocol):
    """Persistence interface for user/group profiles."""

    async def load_profiles(self, group_id: str) -> Mapping[str, Any]:
        """Load existing profiles for one group."""

    async def save_profiles(self, group_id: str, profiles: Mapping[str, Any]) -> None:
        """Persist updated profiles for one group."""


class SemanticWorkflowRuntime(Protocol):
    """Semantic execution interface for workflow-level orchestration."""

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: Optional[int],
        scene: str,
    ) -> BoundaryDecision:
        """Run semantic boundary detection for one candidate segment."""

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        """Extract episode/subject/foresight/event-log from one sealed MemCell."""

    async def distill_profiles(
        self,
        *,
        cluster_memcells: Sequence[MemCellRecord],
        old_profiles: Mapping[str, Any],
        scene: str,
    ) -> Mapping[str, Any]:
        """Distill updated profiles from cluster memcells and old profile state."""


class RetrievalSearcher(Protocol):
    """Query-time search abstraction for one retrieval backend."""

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        """Return ranked candidates from one backend."""


class RetrievalReranker(Protocol):
    """Semantic rerank interface (typically backed by sem_topk)."""

    async def rerank(
        self,
        *,
        query: str,
        candidates: Sequence[RetrievedMemory],
        top_k: int,
        scene: str,
    ) -> Sequence[RetrievedMemory]:
        """Return reranked candidates, sorted descending by relevance."""


class RetrievalPlanner(Protocol):
    """Agentic retrieval planner interface."""

    async def choose_mode(
        self,
        *,
        query: str,
        scene: str,
    ) -> str:
        """Choose retrieval mode from {'keyword', 'vector', 'hybrid', 'rrf'}."""
