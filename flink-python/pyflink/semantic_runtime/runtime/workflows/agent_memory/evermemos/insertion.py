"""EverMemOS-style insertion workflow engine."""

from __future__ import annotations

import uuid
from typing import Callable, Sequence

from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    BoundaryDecision,
    ConversationMessage,
    DecompositionArtifacts,
    InsertionResult,
    MemCellRecord,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.interfaces import (
    ConversationBufferStore,
    ConversationStatusStore,
    MemCellStore,
    MemoryArtifactStore,
    ProfileStore,
    SemanticWorkflowRuntime,
    TopicAssigner,
    TopicStateStore,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.config import (
    EverMemOSWorkflowConfig,
)


TokenCounter = Callable[[Sequence[ConversationMessage]], int]


class EverMemOSInsertionWorkflow:
    """Reconstruct EverMemOS insertion flow with strict dependency injection."""

    def __init__(
        self,
        *,
        config: EverMemOSWorkflowConfig,
        semantic_runtime: SemanticWorkflowRuntime,
        token_counter: TokenCounter,
        conversation_status_store: ConversationStatusStore,
        conversation_buffer_store: ConversationBufferStore,
        memcell_store: MemCellStore,
        memory_artifact_store: MemoryArtifactStore,
        topic_state_store: TopicStateStore,
        topic_assigner: TopicAssigner,
        profile_store: ProfileStore,
    ) -> None:
        self._config = config
        self._semantic_runtime = semantic_runtime
        self._token_counter = token_counter
        self._conversation_status_store = conversation_status_store
        self._conversation_buffer_store = conversation_buffer_store
        self._memcell_store = memcell_store
        self._memory_artifact_store = memory_artifact_store
        self._topic_state_store = topic_state_store
        self._topic_assigner = topic_assigner
        self._profile_store = profile_store

    async def memorize(
        self,
        *,
        group_id: str,
        scene: str,
        new_messages: Sequence[ConversationMessage],
    ) -> InsertionResult:
        """Process one memorize request and return insertion result."""
        if not group_id:
            raise ValueError("memorize requires non-empty group_id")
        if scene not in self._config.supported_scenes:
            raise ValueError(
                f"memorize received unsupported scene={scene!r}; "
                f"supported={sorted(self._config.supported_scenes)!r}"
            )
        if not new_messages:
            raise ValueError("memorize requires non-empty new_messages")
        for message in new_messages:
            if message.group_id != group_id:
                raise ValueError(
                    "memorize received new_messages with mismatched group_id"
                )

        last_memcell_time_ms = await self._conversation_status_store.get_last_memcell_time_ms(
            group_id
        )
        history_messages = list(
            await self._conversation_buffer_store.load_messages_since(
                group_id,
                last_memcell_time_ms,
            )
        )
        candidate_segment = history_messages + list(new_messages)
        boundary = await self._resolve_boundary(
            history_messages=history_messages,
            new_messages=new_messages,
            candidate_segment=candidate_segment,
            scene=scene,
        )

        if not boundary.should_end:
            await self._conversation_buffer_store.append_messages(group_id, new_messages)
            return InsertionResult(
                status="accumulated",
                extracted_count=0,
                boundary=boundary,
            )

        consumed_ids = [message.message_id for message in history_messages]
        await self._conversation_buffer_store.clear_consumed_messages(
            group_id,
            consumed_ids,
        )
        await self._conversation_buffer_store.append_messages(group_id, new_messages)

        memcell = self._build_memcell(
            group_id=group_id,
            scene=scene,
            candidate_segment=candidate_segment,
            boundary=boundary,
        )
        memcell_id = await self._memcell_store.create_memcell(memcell)
        if memcell_id != memcell.memcell_id:
            memcell.memcell_id = memcell_id

        artifacts = await self._semantic_runtime.decompose_memcell(
            memcell=memcell,
            scene=scene,
        )
        self._validate_scene_artifacts(scene=scene, artifacts=artifacts)
        memcell.episode = artifacts.episode
        memcell.subject = artifacts.subject
        await self._memcell_store.update_memcell_fields(
            memcell.memcell_id,
            summary=memcell.summary,
            subject=memcell.subject,
            episode=memcell.episode,
            topic_id=None,
        )
        await self._memory_artifact_store.persist_decomposition(
            group_id=group_id,
            memcell_id=memcell.memcell_id,
            scene=scene,
            artifacts=artifacts,
        )

        topic_state = await self._topic_state_store.load_state(group_id)
        topic_assignment = await self._topic_assigner.assign_memcell(
            memcell=memcell,
            state=topic_state,
        )
        memcell.topic_id = topic_assignment.topic_id
        await self._topic_state_store.save_state(group_id, topic_assignment.updated_state)
        await self._memcell_store.update_memcell_fields(
            memcell.memcell_id,
            summary=memcell.summary,
            subject=memcell.subject,
            episode=memcell.episode,
            topic_id=memcell.topic_id,
        )

        profile_updated = False
        if topic_assignment.cluster_size >= self._config.profile_min_memcells:
            cluster_memcells = await self._memcell_store.list_memcells_by_topic(
                group_id,
                topic_assignment.topic_id,
            )
            old_profiles = await self._profile_store.load_profiles(group_id)
            updated_profiles = await self._semantic_runtime.distill_profiles(
                cluster_memcells=cluster_memcells,
                old_profiles=old_profiles,
                scene=scene,
            )
            await self._profile_store.save_profiles(group_id, updated_profiles)
            profile_updated = True

        await self._memory_artifact_store.sync_indexes(
            group_id=group_id,
            memcell_id=memcell.memcell_id,
        )
        await self._conversation_status_store.update_last_memcell_time_ms(
            group_id,
            memcell.timestamp_ms,
        )

        extracted_count = self._count_extracted_artifacts(artifacts=artifacts)
        return InsertionResult(
            status="extracted",
            extracted_count=extracted_count,
            memcell_id=memcell.memcell_id,
            topic_id=topic_assignment.topic_id,
            boundary=boundary,
            profile_updated=profile_updated,
        )

    async def _resolve_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        candidate_segment: Sequence[ConversationMessage],
        scene: str,
    ) -> BoundaryDecision:
        if self._is_forced_boundary(candidate_segment):
            return BoundaryDecision(
                should_end=True,
                should_wait=False,
                reasoning="force_split_threshold",
                confidence=1.0,
                forced=True,
            )
        time_gap_ms = self._resolve_time_gap_ms(
            history_messages=history_messages,
            new_messages=new_messages,
        )
        return await self._semantic_runtime.detect_boundary(
            history_messages=history_messages,
            new_messages=new_messages,
            time_gap_ms=time_gap_ms,
            scene=scene,
        )

    def _is_forced_boundary(
        self,
        messages: Sequence[ConversationMessage],
    ) -> bool:
        token_count = int(self._token_counter(messages))
        message_count = len(messages)
        return (
            token_count >= self._config.force_split_token_threshold
            or message_count >= self._config.force_split_message_threshold
        )

    def _resolve_time_gap_ms(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
    ) -> int | None:
        if not history_messages:
            return None
        if not new_messages:
            return None
        history_last_ms = int(history_messages[-1].timestamp_ms)
        new_first_ms = int(new_messages[0].timestamp_ms)
        return new_first_ms - history_last_ms

    def _build_memcell(
        self,
        *,
        group_id: str,
        scene: str,
        candidate_segment: Sequence[ConversationMessage],
        boundary: BoundaryDecision,
    ) -> MemCellRecord:
        latest_timestamp_ms = max(message.timestamp_ms for message in candidate_segment)
        participants = sorted({message.sender_id for message in candidate_segment})
        return MemCellRecord(
            memcell_id=uuid.uuid4().hex,
            group_id=group_id,
            timestamp_ms=int(latest_timestamp_ms),
            original_messages=list(candidate_segment),
            participants=participants,
            scene=scene,
            summary=boundary.reasoning,
        )

    def _validate_scene_artifacts(
        self,
        *,
        scene: str,
        artifacts: DecompositionArtifacts,
    ) -> None:
        if scene == "assistant":
            return
        if artifacts.foresights:
            raise ValueError(
                "group_chat scene does not allow foresight artifacts in EverMemOS flow"
            )
        if artifacts.event_logs:
            raise ValueError(
                "group_chat scene does not allow event_log artifacts in EverMemOS flow"
            )

    def _count_extracted_artifacts(self, *, artifacts: DecompositionArtifacts) -> int:
        return 1 + len(artifacts.foresights) + len(artifacts.event_logs)

