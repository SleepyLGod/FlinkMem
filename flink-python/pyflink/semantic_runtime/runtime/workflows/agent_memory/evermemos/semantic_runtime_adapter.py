"""Semantic runtime adapter for EverMemOS workflow composition."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Mapping, Sequence

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.steps import evaluate_all_history_sem_continuity_sync
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    BoundaryDecision,
    ConversationMessage,
    DecompositionArtifacts,
    MemCellRecord,
)


DecomposeFn = Callable[[MemCellRecord, str], Awaitable[DecompositionArtifacts]]
DistillFn = Callable[
    [Sequence[MemCellRecord], Mapping[str, Any], str],
    Awaitable[Mapping[str, Any]],
]


class EverMemOSAllHistoryRuntime:
    """Workflow runtime that uses sem_window(all_history) for boundary decisions."""

    def __init__(
        self,
        *,
        llm_config: LLMClientConfig,
        decompose_fn: DecomposeFn,
        distill_fn: DistillFn,
    ) -> None:
        self._llm_config = llm_config
        self._client = create_llm_client(llm_config)
        self._decompose_fn = decompose_fn
        self._distill_fn = distill_fn
        self.detect_boundary_calls: int = 0

    def close(self) -> None:
        """Release runtime-owned resources."""
        self._client.close()

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: int | None,
        scene: str,
    ) -> BoundaryDecision:
        """Run all-history continuity and convert to boundary decision."""
        _ = (time_gap_ms, scene)
        self.detect_boundary_calls += 1
        if not new_messages:
            raise ValueError("detect_boundary requires non-empty new_messages")

        active_window_events = [self._to_event(message) for message in history_messages]
        current_event = self._to_event(new_messages[-1])
        result = await asyncio.to_thread(
            evaluate_all_history_sem_continuity_sync,
            client=self._client,
            llm_config=self._llm_config,
            active_window_events=active_window_events,
            current_event=current_event,
        )
        continue_window = bool(result["continue_window"])
        confidence = float(result["confidence"])
        reason = str(result["reason"])
        return BoundaryDecision(
            should_end=not continue_window,
            should_wait=continue_window,
            reasoning=reason,
            confidence=confidence,
            forced=False,
        )

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        """Delegate decomposition to injected workflow function."""
        return await self._decompose_fn(memcell, scene)

    async def distill_profiles(
        self,
        *,
        cluster_memcells: Sequence[MemCellRecord],
        old_profiles: Mapping[str, Any],
        scene: str,
    ) -> Mapping[str, Any]:
        """Delegate profile distillation to injected workflow function."""
        return await self._distill_fn(cluster_memcells, old_profiles, scene)

    def _to_event(self, message: ConversationMessage) -> dict[str, object]:
        return {
            "key": message.group_id,
            "payload": message.content,
            "event_time_ms": int(message.timestamp_ms),
            "event_seq_id": int(message.timestamp_ms),
            "metadata": {
                "message_id": message.message_id,
                "sender_id": message.sender_id,
                "sender_name": message.sender_name or "",
                "role": message.role,
            },
            "boundary_flags": {},
            "source": "agent_memory",
            "group_id": message.group_id,
            "confidence": 1.0,
        }
