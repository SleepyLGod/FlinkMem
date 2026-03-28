"""EverMemOS-style incremental topic assignment."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyflink.semantic_runtime.llm_client import (
    LLMClient,
    LLMClientConfig,
    create_llm_client,
)
from pyflink.semantic_runtime.runtime.steps.sem_group_assign import (
    evaluate_sem_group_assignments,
)
from pyflink.semantic_runtime.runtime.simple_text_encoder import HashingTextEncoder
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    MemCellRecord,
    TopicAssignmentResult,
    TopicClusterState,
)


DEFAULT_TOPIC_ASSIGNER_SIMILARITY_THRESHOLD = 0.7
DEFAULT_TOPIC_ASSIGNER_MAX_TIME_GAP_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_TOPIC_ASSIGNER_EMBEDDING_DIM = 128
DEFAULT_TOPIC_ASSIGNMENT_INTENT = (
    "Assign this memory episode to the most suitable existing thematic topic. "
    "If no existing topic matches, create a new topic."
)
TOPIC_ASSIGNMENT_EVENT_SEQ_ID = 0


@dataclass(frozen=True)
class EverMemOSTopicAssignerConfig:
    """Config for EverMemOS incremental centroid-based topic assignment."""

    similarity_threshold: float = DEFAULT_TOPIC_ASSIGNER_SIMILARITY_THRESHOLD
    max_time_gap_ms: int = DEFAULT_TOPIC_ASSIGNER_MAX_TIME_GAP_MS
    embedding_dim: int = DEFAULT_TOPIC_ASSIGNER_EMBEDDING_DIM
    assignment_intent: str = DEFAULT_TOPIC_ASSIGNMENT_INTENT

    def __post_init__(self) -> None:
        if not 0.0 <= float(self.similarity_threshold) <= 1.0:
            raise ValueError("similarity_threshold must be in [0.0, 1.0]")
        if int(self.max_time_gap_ms) <= 0:
            raise ValueError("max_time_gap_ms must be > 0")
        if int(self.embedding_dim) <= 0:
            raise ValueError("embedding_dim must be > 0")
        if not str(self.assignment_intent).strip():
            raise ValueError("assignment_intent must be non-empty")


class EverMemOSTopicAssigner:
    """Incremental topic assigner with semantic assignment and centroid state updates."""

    def __init__(
        self,
        *,
        config: EverMemOSTopicAssignerConfig,
        llm_config: LLMClientConfig,
        client_factory: Optional[Callable[[LLMClientConfig], LLMClient]] = None,
        encoder: Optional[HashingTextEncoder] = None,
    ) -> None:
        self._config = config
        self._encoder = encoder or HashingTextEncoder(dim=config.embedding_dim)
        self._client = (client_factory or create_llm_client)(llm_config)
        if int(self._encoder.dim) != int(config.embedding_dim):
            raise ValueError(
                "encoder.dim must equal config.embedding_dim "
                f"(got encoder.dim={self._encoder.dim}, config.embedding_dim={config.embedding_dim})"
            )

    def close(self) -> None:
        """Release assigner-owned resources."""
        self._client.close()

    async def assign_memcell(
        self,
        *,
        memcell: MemCellRecord,
        state: TopicClusterState,
    ) -> TopicAssignmentResult:
        """Assign one MemCell into topic state and return updated state."""
        content = self._resolve_memcell_text(memcell)
        event_embedding = self._encoder.encode_dense(content)
        if not event_embedding:
            raise ValueError("topic assignment requires non-empty event embedding")

        next_state = self._copy_state(state)
        existing_groups, eligible_topic_ids = self._build_existing_groups_payload(
            event_timestamp_ms=int(memcell.timestamp_ms),
            state=next_state,
        )
        assignment = await self._evaluate_semantic_assignment_async(
            existing_groups,
            content,
            int(memcell.timestamp_ms),
        )
        assigned_topic, assigned_label = self._resolve_semantic_assignment(
            assignment=assignment,
            eligible_topic_ids=eligible_topic_ids,
        )

        if assigned_topic is not None and not self._passes_similarity_gate(
            topic_id=assigned_topic,
            event_embedding=event_embedding,
            state=next_state,
        ):
            assigned_topic = None

        if assigned_topic is None:
            assigned_topic = self._find_best_topic(
                event_embedding=event_embedding,
                event_timestamp_ms=int(memcell.timestamp_ms),
                state=next_state,
            )

        if assigned_topic is None:
            assigned_topic = f"topic_{next_state.next_topic_idx:03d}"
            next_state.next_topic_idx += 1
            next_state.topic_centroids[assigned_topic] = list(event_embedding)
            next_state.topic_counts[assigned_topic] = 0
            next_state.topic_representatives[assigned_topic] = assigned_label or content
        elif assigned_topic not in next_state.topic_representatives:
            next_state.topic_representatives[assigned_topic] = content

        previous_count = int(next_state.topic_counts.get(assigned_topic, 0))
        updated_count = previous_count + 1
        previous_centroid = next_state.topic_centroids.get(assigned_topic)
        next_state.topic_centroids[assigned_topic] = self._updated_centroid(
            previous_centroid=previous_centroid,
            previous_count=previous_count,
            event_embedding=event_embedding,
        )
        next_state.topic_counts[assigned_topic] = updated_count
        next_state.topic_last_ts[assigned_topic] = int(memcell.timestamp_ms)

        next_state.event_ids.append(memcell.memcell_id)
        next_state.eventid_to_topic[memcell.memcell_id] = assigned_topic

        return TopicAssignmentResult(
            topic_id=assigned_topic,
            cluster_size=updated_count,
            updated_state=next_state,
        )

    def _build_existing_groups_payload(
        self,
        *,
        event_timestamp_ms: int,
        state: TopicClusterState,
    ) -> Tuple[List[Dict[str, object]], Sequence[str]]:
        groups: List[Dict[str, object]] = []
        eligible_topic_ids: List[str] = []
        for topic_id in state.topic_centroids.keys():
            topic_last_ts = state.topic_last_ts.get(topic_id)
            if topic_last_ts is None:
                raise ValueError(f"missing topic_last_ts for topic_id={topic_id!r}")
            if abs(event_timestamp_ms - int(topic_last_ts)) > self._config.max_time_gap_ms:
                continue
            representative = str(state.topic_representatives.get(topic_id, "") or "")
            groups.append(
                {
                    "group_id": topic_id,
                    "label": representative,
                    "summary": representative,
                    "event_count": int(state.topic_counts.get(topic_id, 0)),
                    "examples": [representative] if representative else [],
                }
            )
            eligible_topic_ids.append(topic_id)
        return groups, eligible_topic_ids

    async def _evaluate_semantic_assignment_async(
        self,
        existing_groups: List[Dict[str, object]],
        content: str,
        event_timestamp_ms: int,
    ) -> Dict[str, object]:
        assignments = await evaluate_sem_group_assignments(
            client=self._client,
            intent=self._config.assignment_intent,
            existing_groups=list(existing_groups),
            events=[
                {
                    "event_seq_id": TOPIC_ASSIGNMENT_EVENT_SEQ_ID,
                    "payload": content,
                    "timestamp_ms": event_timestamp_ms,
                }
            ],
        )
        if len(assignments) != 1:
            raise ValueError("topic assignment requires exactly one semantic assignment row")
        row = assignments[0]
        if int(row["event_seq_id"]) != TOPIC_ASSIGNMENT_EVENT_SEQ_ID:
            raise ValueError(
                f"topic assignment returned unexpected event_seq_id={row['event_seq_id']!r}"
            )
        return row

    def _resolve_semantic_assignment(
        self,
        *,
        assignment: Dict[str, object],
        eligible_topic_ids: Sequence[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        decision = str(assignment["decision"])
        if decision == "existing":
            topic_id = str(assignment["group_id"])
            if topic_id not in set(eligible_topic_ids):
                raise ValueError(
                    f"topic assignment selected non-eligible topic_id={topic_id!r}"
                )
            return topic_id, None
        if decision == "new":
            label = str(assignment["label"])
            return None, label
        raise ValueError(f"topic assignment decision must be existing/new, got {decision!r}")

    def _passes_similarity_gate(
        self,
        *,
        topic_id: str,
        event_embedding: List[float],
        state: TopicClusterState,
    ) -> bool:
        centroid = state.topic_centroids.get(topic_id)
        if centroid is None:
            raise ValueError(f"missing centroid for topic_id={topic_id!r}")
        if len(centroid) != len(event_embedding):
            raise ValueError(
                f"centroid dim mismatch for topic_id={topic_id!r}: "
                f"expected {len(event_embedding)}, got {len(centroid)}"
            )
        similarity = self._dot(event_embedding, centroid)
        return similarity >= self._config.similarity_threshold

    def _find_best_topic(
        self,
        *,
        event_embedding: List[float],
        event_timestamp_ms: int,
        state: TopicClusterState,
    ) -> Optional[str]:
        best_topic: Optional[str] = None
        best_score = float("-inf")
        for topic_id, centroid in state.topic_centroids.items():
            if len(centroid) != len(event_embedding):
                raise ValueError(
                    f"centroid dim mismatch for topic_id={topic_id!r}: "
                    f"expected {len(event_embedding)}, got {len(centroid)}"
                )
            topic_last_ts = state.topic_last_ts.get(topic_id)
            if topic_last_ts is None:
                raise ValueError(
                    f"missing topic_last_ts for topic_id={topic_id!r}"
                )
            if abs(event_timestamp_ms - int(topic_last_ts)) > self._config.max_time_gap_ms:
                continue
            similarity = self._dot(event_embedding, centroid)
            if similarity > best_score:
                best_score = similarity
                best_topic = topic_id
        if best_topic is None:
            return None
        if best_score < self._config.similarity_threshold:
            return None
        return best_topic

    def _updated_centroid(
        self,
        *,
        previous_centroid: Optional[List[float]],
        previous_count: int,
        event_embedding: List[float],
    ) -> List[float]:
        if previous_centroid is None:
            if previous_count != 0:
                raise ValueError(
                    "topic state invariant violated: previous_centroid is None "
                    "but previous_count is non-zero"
                )
            return list(event_embedding)
        if len(previous_centroid) != len(event_embedding):
            raise ValueError(
                "centroid dim mismatch while updating topic centroid"
            )
        denominator = float(previous_count + 1)
        merged = [
            (float(previous_centroid[index]) * float(previous_count) + float(event_embedding[index]))
            / denominator
            for index in range(len(event_embedding))
        ]
        return self._l2_normalize(merged)

    def _resolve_memcell_text(self, memcell: MemCellRecord) -> str:
        if memcell.episode:
            return str(memcell.episode)
        if memcell.subject:
            return str(memcell.subject)
        if memcell.summary:
            return str(memcell.summary)
        raise ValueError(
            "topic assignment requires memcell.episode or memcell.subject or memcell.summary"
        )

    def _copy_state(self, state: TopicClusterState) -> TopicClusterState:
        return TopicClusterState(
            event_ids=list(state.event_ids),
            eventid_to_topic=dict(state.eventid_to_topic),
            topic_centroids={topic_id: list(vector) for topic_id, vector in state.topic_centroids.items()},
            topic_representatives=dict(state.topic_representatives),
            topic_counts=dict(state.topic_counts),
            topic_last_ts=dict(state.topic_last_ts),
            next_topic_idx=int(state.next_topic_idx),
        )

    def _dot(self, left: List[float], right: List[float]) -> float:
        return float(sum(float(a) * float(b) for a, b in zip(left, right)))

    def _l2_normalize(self, values: List[float]) -> List[float]:
        squared_sum = sum(float(value) * float(value) for value in values)
        if squared_sum <= 0.0:
            raise ValueError("centroid normalization requires non-zero vector")
        norm = math.sqrt(squared_sum)
        return [float(value) / norm for value in values]
