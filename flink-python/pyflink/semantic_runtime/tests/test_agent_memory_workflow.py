"""Unit tests for EverMemOS-style workflow composition."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import asyncio
import json
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402
_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from dataclasses import replace
from typing import Any, Dict, Mapping, Optional, Sequence

from pyflink.semantic_runtime.llm_client import LLMCallMetrics, LLMClient, LLMClientConfig
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    BoundaryDecision,
    ConversationMessage,
    DecompositionArtifacts,
    EventLogArtifact,
    ForesightArtifact,
    MemCellRecord,
    RetrievedMemory,
    TopicAssignmentResult,
    TopicClusterState,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.config import (
    EverMemOSRetrievalConfig,
    EverMemOSWorkflowConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.insertion import (
    EverMemOSInsertionWorkflow,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.operator_runtime import (
    EverMemOSOperatorRuntime,
    EverMemOSOperatorRuntimeConfig,
    EverMemOSSemTopKReranker,
    EverMemOSSemTopKRerankerConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.retrieval import (
    EverMemOSRetrievalWorkflow,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.semantic_runtime_adapter import (
    EverMemOSAllHistoryRuntime,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.topic_assignment import (
    EverMemOSTopicAssigner,
    EverMemOSTopicAssignerConfig,
)


class _InMemoryConversationStatusStore:
    def __init__(self) -> None:
        self._values: Dict[str, int] = {}

    async def get_last_memcell_time_ms(self, group_id: str) -> Optional[int]:
        return self._values.get(group_id)

    async def update_last_memcell_time_ms(self, group_id: str, timestamp_ms: int) -> None:
        self._values[group_id] = int(timestamp_ms)


class _InMemoryConversationBufferStore:
    def __init__(self) -> None:
        self._messages: Dict[str, list[ConversationMessage]] = {}

    async def load_messages_since(
        self,
        group_id: str,
        start_time_ms: Optional[int],
    ) -> Sequence[ConversationMessage]:
        items = list(self._messages.get(group_id, []))
        if start_time_ms is None:
            return items
        return [message for message in items if message.timestamp_ms >= start_time_ms]

    async def append_messages(
        self,
        group_id: str,
        messages: Sequence[ConversationMessage],
    ) -> None:
        self._messages.setdefault(group_id, []).extend(messages)

    async def clear_consumed_messages(
        self,
        group_id: str,
        consumed_message_ids: Sequence[str],
    ) -> None:
        consumed = set(consumed_message_ids)
        self._messages[group_id] = [
            message
            for message in self._messages.get(group_id, [])
            if message.message_id not in consumed
        ]


class _InMemoryMemCellStore:
    def __init__(self) -> None:
        self._records: Dict[str, MemCellRecord] = {}

    async def create_memcell(self, memcell: MemCellRecord) -> str:
        self._records[memcell.memcell_id] = memcell
        return memcell.memcell_id

    async def update_memcell_fields(
        self,
        memcell_id: str,
        *,
        summary: Optional[str],
        subject: Optional[str],
        episode: Optional[str],
        topic_id: Optional[str],
    ) -> None:
        if memcell_id not in self._records:
            raise KeyError(f"unknown memcell_id={memcell_id}")
        current = self._records[memcell_id]
        self._records[memcell_id] = replace(
            current,
            summary=summary,
            subject=subject,
            episode=episode,
            topic_id=topic_id,
        )

    async def list_memcells_by_topic(
        self,
        group_id: str,
        topic_id: str,
    ) -> Sequence[MemCellRecord]:
        return [
            memcell
            for memcell in self._records.values()
            if memcell.group_id == group_id and memcell.topic_id == topic_id
        ]


class _InMemoryArtifactStore:
    def __init__(self) -> None:
        self.persist_calls: int = 0
        self.sync_calls: int = 0
        self.persisted: list[dict[str, Any]] = []
        self.synced: list[dict[str, str]] = []

    async def persist_decomposition(
        self,
        *,
        group_id: str,
        memcell_id: str,
        scene: str,
        artifacts: DecompositionArtifacts,
    ) -> None:
        self.persisted.append(
            {
                "group_id": group_id,
                "memcell_id": memcell_id,
                "scene": scene,
                "artifacts": artifacts,
            }
        )
        self.persist_calls += 1

    async def sync_indexes(self, *, group_id: str, memcell_id: str) -> None:
        self.synced.append(
            {
                "group_id": group_id,
                "memcell_id": memcell_id,
            }
        )
        self.sync_calls += 1


class _InMemoryTopicStateStore:
    def __init__(self) -> None:
        self._states: Dict[str, TopicClusterState] = {}

    async def load_state(self, group_id: str) -> TopicClusterState:
        return self._states.get(group_id, TopicClusterState())

    async def save_state(self, group_id: str, state: TopicClusterState) -> None:
        self._states[group_id] = state


class _SimpleTopicAssigner:
    async def assign_memcell(
        self,
        *,
        memcell: MemCellRecord,
        state: TopicClusterState,
    ) -> TopicAssignmentResult:
        topic_id = "topic_000"
        state.event_ids.append(memcell.memcell_id)
        state.eventid_to_topic[memcell.memcell_id] = topic_id
        state.topic_counts[topic_id] = state.topic_counts.get(topic_id, 0) + 1
        state.topic_last_ts[topic_id] = memcell.timestamp_ms
        return TopicAssignmentResult(
            topic_id=topic_id,
            cluster_size=state.topic_counts[topic_id],
            updated_state=state,
        )


class _InMemoryProfileStore:
    def __init__(self) -> None:
        self._profiles: Dict[str, Dict[str, Any]] = {}

    async def load_profiles(self, group_id: str) -> Mapping[str, Any]:
        return dict(self._profiles.get(group_id, {}))

    async def save_profiles(self, group_id: str, profiles: Mapping[str, Any]) -> None:
        self._profiles[group_id] = dict(profiles)


class _PromptRoutingClient(LLMClient):
    def __init__(self, prompt_log: list[str]) -> None:
        self._prompt_log = prompt_log

    async def call(self, prompt: str):
        self._prompt_log.append(prompt)
        if "Has the conversation reached a natural boundary" in prompt:
            return (
                '{"decision": true, "confidence": 0.91, "reason": "natural boundary"}',
                LLMCallMetrics(),
            )
        if "Synthesize this conversation into a concise third-person episodic narrative" in prompt:
            return ('{"episode": "episode_from_sem_map"}', LLMCallMetrics())
        if "What is the central subject of this conversation?" in prompt:
            return ('{"subject": "subject_from_sem_map"}', LLMCallMetrics())
        if "Extract time-bounded future predictions or planned actions" in prompt:
            return (
                '{"foresights": [{"content": "foresight_from_sem_map"}]}',
                LLMCallMetrics(),
            )
        if "Extract discrete atomic factual events" in prompt:
            return (
                '{"event_logs": [{"atomic_fact": "fact_from_sem_map"}]}',
                LLMCallMetrics(),
            )
        if "Update the running compressed memory" in prompt:
            return ('{"summary": "{\\"u1\\": {\\"trait\\": \\"planner\\"}}"}', LLMCallMetrics())
        if "Given old profiles and distilled cluster evidence, return updated profiles." in prompt:
            return ('{"profiles": {"u1": {"trait": "planner"}}}', LLMCallMetrics())
        if "Score how relevant this memory item is to the query in [0,1]." in prompt:
            if "alpha memory" in prompt:
                return (
                    '{"score": 0.9, "confidence": 0.9, "reason": "high relevance"}',
                    LLMCallMetrics(),
                )
            return (
                '{"score": 0.1, "confidence": 0.8, "reason": "low relevance"}',
                LLMCallMetrics(),
            )
        raise RuntimeError(f"unexpected prompt in test routing client: {prompt}")

    def close(self) -> None:
        return None


class _PromptRoutingClientFactory:
    def __init__(self) -> None:
        self.prompts: list[str] = []

    def __call__(self, _config: LLMClientConfig) -> LLMClient:
        return _PromptRoutingClient(self.prompts)


class _TopicAssignClient(LLMClient):
    def __init__(self, prompt_log: list[str]) -> None:
        self._prompt_log = prompt_log

    async def call(self, prompt: str) -> tuple[str, LLMCallMetrics]:
        self._prompt_log.append(prompt)
        existing_groups = self._extract_json_block(
            prompt=prompt,
            begin_marker="Existing groups:\n",
            end_marker="\n\nEvents to assign:\n",
        )
        events = self._extract_json_block(
            prompt=prompt,
            begin_marker="\n\nEvents to assign:\n",
            end_marker="\n\nFor each event, either assign it to one existing group_id or create one new group.\n",
        )
        if not isinstance(existing_groups, list):
            raise ValueError("topic_assign test client expected existing_groups list")
        if not isinstance(events, list) or len(events) != 1:
            raise ValueError("topic_assign test client expected exactly one event")
        event = events[0]
        if not isinstance(event, dict) or "event_seq_id" not in event:
            raise ValueError("topic_assign test client expected event_seq_id")
        event_seq_id = int(event["event_seq_id"])

        if existing_groups:
            first = existing_groups[0]
            if not isinstance(first, dict) or "group_id" not in first:
                raise ValueError("topic_assign test client expected group_id in existing_groups")
            group_id = str(first["group_id"])
            return (
                json.dumps(
                    {
                        "assignments": [
                            {
                                "event_seq_id": event_seq_id,
                                "decision": "existing",
                                "group_id": group_id,
                                "label": "",
                                "confidence": 0.9,
                                "reason": "match_existing",
                            }
                        ]
                    }
                ),
                LLMCallMetrics(),
            )
        return (
            json.dumps(
                {
                    "assignments": [
                        {
                            "event_seq_id": event_seq_id,
                            "decision": "new",
                            "group_id": "",
                            "label": "new_topic",
                            "confidence": 0.9,
                            "reason": "new_topic",
                        }
                    ]
                }
            ),
            LLMCallMetrics(),
        )

    def close(self) -> None:
        return None

    def _extract_json_block(
        self,
        *,
        prompt: str,
        begin_marker: str,
        end_marker: str,
    ) -> Any:
        if begin_marker not in prompt:
            raise ValueError(f"missing marker in prompt: {begin_marker!r}")
        begin = prompt.split(begin_marker, 1)[1]
        if end_marker not in begin:
            raise ValueError(f"missing end marker in prompt: {end_marker!r}")
        payload = begin.split(end_marker, 1)[0].strip()
        return json.loads(payload)


class _TopicAssignClientFactory:
    def __init__(self) -> None:
        self.prompts: list[str] = []

    def __call__(self, _config: LLMClientConfig) -> LLMClient:
        return _TopicAssignClient(self.prompts)


class _SemanticRuntimeStub:
    def __init__(self, *, should_end: bool) -> None:
        self._should_end = should_end
        self.detect_boundary_calls = 0
        self.distill_calls = 0

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: Optional[int],
        scene: str,
    ) -> BoundaryDecision:
        _ = (history_messages, new_messages, time_gap_ms, scene)
        self.detect_boundary_calls += 1
        return BoundaryDecision(
            should_end=self._should_end,
            should_wait=not self._should_end,
            reasoning="stub_boundary",
            confidence=0.9,
            forced=False,
        )

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        _ = memcell
        if scene == "assistant":
            return DecompositionArtifacts(
                episode="episode_a",
                subject="subject_a",
                foresights=[ForesightArtifact(content="foresight_a")],
                event_logs=[EventLogArtifact(atomic_fact="fact_a")],
            )
        return DecompositionArtifacts(
            episode="episode_g",
            subject="subject_g",
            foresights=[],
            event_logs=[],
        )

    async def distill_profiles(
        self,
        *,
        cluster_memcells: Sequence[MemCellRecord],
        old_profiles: Mapping[str, Any],
        scene: str,
    ) -> Mapping[str, Any]:
        _ = (cluster_memcells, old_profiles, scene)
        self.distill_calls += 1
        return {"user_1": {"trait": "updated"}}


class _BoundaryAlwaysRuntime:
    def __init__(self) -> None:
        self.detect_boundary_calls = 0

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: Optional[int],
        scene: str,
    ) -> BoundaryDecision:
        _ = (history_messages, new_messages, time_gap_ms, scene)
        self.detect_boundary_calls += 1
        return BoundaryDecision(
            should_end=True,
            should_wait=False,
            reasoning="boundary_true",
            confidence=1.0,
            forced=False,
        )

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        _ = scene
        return DecompositionArtifacts(
            episode=memcell.original_messages[-1].content,
            subject="subject",
            foresights=[],
            event_logs=[],
        )

    async def distill_profiles(
        self,
        *,
        cluster_memcells: Sequence[MemCellRecord],
        old_profiles: Mapping[str, Any],
        scene: str,
    ) -> Mapping[str, Any]:
        _ = (cluster_memcells, old_profiles, scene)
        return {}


class _ScriptedBoundaryRuntime:
    def __init__(self, decisions: Sequence[bool]) -> None:
        self._decisions = list(decisions)
        self.detect_boundary_calls = 0

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: Optional[int],
        scene: str,
    ) -> BoundaryDecision:
        _ = (history_messages, new_messages, time_gap_ms, scene)
        self.detect_boundary_calls += 1
        if not self._decisions:
            raise ValueError("scripted boundary runtime has no remaining decision")
        should_end = bool(self._decisions.pop(0))
        return BoundaryDecision(
            should_end=should_end,
            should_wait=not should_end,
            reasoning="scripted_boundary",
            confidence=0.9,
            forced=False,
        )

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        _ = scene
        return DecompositionArtifacts(
            episode=f"episode_{len(memcell.original_messages)}",
            subject="subject_scripted",
            foresights=[],
            event_logs=[],
        )

    async def distill_profiles(
        self,
        *,
        cluster_memcells: Sequence[MemCellRecord],
        old_profiles: Mapping[str, Any],
        scene: str,
    ) -> Mapping[str, Any]:
        _ = (cluster_memcells, old_profiles, scene)
        return {}


class _InMemorySearcher:
    def __init__(self, rows: Sequence[RetrievedMemory]) -> None:
        self._rows = list(rows)
        self.calls: int = 0

    async def search(
        self,
        *,
        group_id: str,
        query: str,
        top_k: int,
        memory_types: Optional[Sequence[str]],
    ) -> Sequence[RetrievedMemory]:
        _ = (group_id, query)
        self.calls += 1
        rows = self._rows
        if memory_types is not None:
            allowed = set(memory_types)
            rows = [row for row in rows if row.memory_type in allowed]
        return rows[:top_k]


class _IdentityReranker:
    def __init__(self) -> None:
        self.calls: int = 0

    async def rerank(
        self,
        *,
        query: str,
        candidates: Sequence[RetrievedMemory],
        top_k: int,
        scene: str,
    ) -> Sequence[RetrievedMemory]:
        _ = (query, scene)
        self.calls += 1
        return list(candidates)[:top_k]


class _PlannerStub:
    def __init__(self, mode: str) -> None:
        self._mode = mode
        self.calls: int = 0

    async def choose_mode(
        self,
        *,
        query: str,
        scene: str,
    ) -> str:
        _ = (query, scene)
        self.calls += 1
        return self._mode


def _message(message_id: str, ts: int, group_id: str = "g1") -> ConversationMessage:
    return ConversationMessage(
        message_id=message_id,
        group_id=group_id,
        sender_id="u1",
        content=f"content_{message_id}",
        timestamp_ms=ts,
    )


def _memcell(memcell_id: str, episode: str, ts: int) -> MemCellRecord:
    return MemCellRecord(
        memcell_id=memcell_id,
        group_id="g1",
        timestamp_ms=ts,
        original_messages=[],
        participants=["u1"],
        scene="assistant",
        episode=episode,
        subject="subject",
        summary="summary",
    )


async def _adapter_decompose(
    memcell: MemCellRecord,
    scene: str,
) -> DecompositionArtifacts:
    _ = scene
    return DecompositionArtifacts(
        episode=memcell.original_messages[-1].content,
        subject="adapter_subject",
        foresights=[],
        event_logs=[],
    )


async def _adapter_distill(
    cluster_memcells: Sequence[MemCellRecord],
    old_profiles: Mapping[str, Any],
    scene: str,
) -> Mapping[str, Any]:
    _ = (cluster_memcells, old_profiles, scene)
    return {}


def test_evermemos_workflow_accumulates_when_no_boundary() -> None:
    config = EverMemOSWorkflowConfig(
        force_split_token_threshold=10_000,
        force_split_message_threshold=100,
        profile_min_memcells=1,
    )
    semantic_runtime = _SemanticRuntimeStub(should_end=False)
    workflow = EverMemOSInsertionWorkflow(
        config=config,
        semantic_runtime=semantic_runtime,
        token_counter=lambda messages: 1,
        conversation_status_store=_InMemoryConversationStatusStore(),
        conversation_buffer_store=_InMemoryConversationBufferStore(),
        memcell_store=_InMemoryMemCellStore(),
        memory_artifact_store=_InMemoryArtifactStore(),
        topic_state_store=_InMemoryTopicStateStore(),
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=_InMemoryProfileStore(),
    )

    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[_message("m1", 1000)],
        )
    )
    assert result.status == "accumulated"
    assert result.extracted_count == 0
    assert semantic_runtime.detect_boundary_calls == 1


def test_evermemos_workflow_force_split_extracts_and_updates_profile() -> None:
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()
    semantic_runtime = _SemanticRuntimeStub(should_end=False)

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=1,
            force_split_message_threshold=1000,
            profile_min_memcells=1,
        ),
        semantic_runtime=semantic_runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )

    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[_message("m2", 2000)],
        )
    )

    assert result.status == "extracted"
    assert result.extracted_count == 3
    assert result.profile_updated is True
    assert result.topic_id == "topic_000"
    assert result.boundary is not None
    assert result.boundary.forced is True
    assert semantic_runtime.detect_boundary_calls == 0
    assert semantic_runtime.distill_calls == 1
    assert artifact_store.persist_calls == 1
    assert artifact_store.sync_calls == 1
    assert asyncio.run(status_store.get_last_memcell_time_ms("g1")) == 2000


def test_evermemos_retrieval_hybrid_and_rrf_modes() -> None:
    keyword = _InMemorySearcher(
        [
            RetrievedMemory(
                memory_id="a",
                memory_type="episode",
                content="kw_a",
                score=0.9,
                source="keyword",
            ),
            RetrievedMemory(
                memory_id="b",
                memory_type="episode",
                content="kw_b",
                score=0.8,
                source="keyword",
            ),
        ]
    )
    vector = _InMemorySearcher(
        [
            RetrievedMemory(
                memory_id="b",
                memory_type="episode",
                content="vec_b",
                score=0.95,
                source="vector",
            ),
            RetrievedMemory(
                memory_id="c",
                memory_type="episode",
                content="vec_c",
                score=0.9,
                source="vector",
            ),
        ]
    )
    workflow = EverMemOSRetrievalWorkflow(
        config=EverMemOSRetrievalConfig(enable_rerank=False),
        keyword_searcher=keyword,
        vector_searcher=vector,
    )

    hybrid = asyncio.run(
        workflow.retrieve(
            group_id="g1",
            query="beijing trip",
            scene="assistant",
            mode="hybrid",
            top_k=3,
        )
    )
    assert hybrid.selected_mode == "hybrid"
    assert [item.memory_id for item in hybrid.memories] == ["b", "a", "c"]
    assert hybrid.reranked is False

    rrf = asyncio.run(
        workflow.retrieve(
            group_id="g1",
            query="beijing trip",
            scene="assistant",
            mode="rrf",
            top_k=3,
        )
    )
    assert rrf.selected_mode == "rrf"
    assert [item.memory_id for item in rrf.memories] == ["b", "a", "c"]
    assert rrf.reranked is False


def test_evermemos_retrieval_agentic_mode_uses_planner_and_reranker() -> None:
    keyword = _InMemorySearcher(
        [
            RetrievedMemory(
                memory_id="m1",
                memory_type="event_log",
                content="keyword_result",
                score=0.7,
                source="keyword",
            )
        ]
    )
    vector = _InMemorySearcher(
        [
            RetrievedMemory(
                memory_id="m2",
                memory_type="event_log",
                content="vector_result",
                score=0.95,
                source="vector",
            )
        ]
    )
    planner = _PlannerStub(mode="vector")
    reranker = _IdentityReranker()
    workflow = EverMemOSRetrievalWorkflow(
        config=EverMemOSRetrievalConfig(enable_rerank=True),
        keyword_searcher=keyword,
        vector_searcher=vector,
        planner=planner,
        reranker=reranker,
    )

    result = asyncio.run(
        workflow.retrieve(
            group_id="g1",
            query="next actions",
            scene="assistant",
            mode="agentic",
            top_k=1,
            memory_types=["event_log"],
        )
    )
    assert result.mode == "agentic"
    assert result.selected_mode == "vector"
    assert result.reranked is True
    assert len(result.memories) == 1
    assert result.memories[0].memory_id == "m2"
    assert planner.calls == 1
    assert reranker.calls == 1
    assert keyword.calls == 0
    assert vector.calls == 1


def test_topic_assigner_creates_and_merges_topics_with_time_gate() -> None:
    topic_assign_factory = _TopicAssignClientFactory()
    assigner = EverMemOSTopicAssigner(
        config=EverMemOSTopicAssignerConfig(
            similarity_threshold=0.2,
            max_time_gap_ms=1000,
            embedding_dim=64,
        ),
        llm_config=LLMClientConfig(backend="mock"),
        client_factory=topic_assign_factory,
    )
    state = TopicClusterState()

    first = asyncio.run(
        assigner.assign_memcell(
            memcell=_memcell("m1", "buy apples tomorrow", 1000),
            state=state,
        )
    )
    assert first.topic_id == "topic_000"
    assert first.cluster_size == 1

    second = asyncio.run(
        assigner.assign_memcell(
            memcell=_memcell("m2", "apples shopping list", 1500),
            state=first.updated_state,
        )
    )
    assert second.topic_id == "topic_000"
    assert second.cluster_size == 2

    third = asyncio.run(
        assigner.assign_memcell(
            memcell=_memcell("m3", "apples shopping list", 10_000),
            state=second.updated_state,
        )
    )
    assert third.topic_id == "topic_001"
    assert third.cluster_size == 1
    assigner.close()
    assert len(topic_assign_factory.prompts) == 3
    assert all(
        "For each event, either assign it to one existing group_id or create one new group."
        in item
        for item in topic_assign_factory.prompts
    )


def test_insertion_workflow_with_real_topic_assigner_merges_then_splits_by_time_gap() -> None:
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()
    semantic_runtime = _BoundaryAlwaysRuntime()
    topic_assign_factory = _TopicAssignClientFactory()
    topic_assigner = EverMemOSTopicAssigner(
        config=EverMemOSTopicAssignerConfig(
            similarity_threshold=0.1,
            max_time_gap_ms=1000,
            embedding_dim=64,
        ),
        llm_config=LLMClientConfig(backend="mock"),
        client_factory=topic_assign_factory,
    )

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=10_000,
            force_split_message_threshold=10_000,
            profile_min_memcells=99,
        ),
        semantic_runtime=semantic_runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=topic_assigner,
        profile_store=profile_store,
    )

    first_message = ConversationMessage(
        message_id="w1",
        group_id="g1",
        sender_id="u1",
        content="plan trip to beijing",
        timestamp_ms=1000,
    )
    second_message = ConversationMessage(
        message_id="w2",
        group_id="g1",
        sender_id="u1",
        content="plan trip to beijing",
        timestamp_ms=1500,
    )
    third_message = ConversationMessage(
        message_id="w3",
        group_id="g1",
        sender_id="u1",
        content="plan trip to beijing",
        timestamp_ms=5000,
    )

    first = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[first_message],
        )
    )
    second = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[second_message],
        )
    )
    third = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[third_message],
        )
    )

    assert first.topic_id == "topic_000"
    assert second.topic_id == "topic_000"
    assert third.topic_id == "topic_001"

    state = asyncio.run(topic_state_store.load_state("g1"))
    assert state.topic_counts["topic_000"] == 2
    assert state.topic_counts["topic_001"] == 1
    assert semantic_runtime.detect_boundary_calls == 3
    topic_assigner.close()
    assert len(topic_assign_factory.prompts) == 3


def test_insertion_workflow_parity_replay_accumulate_then_extract() -> None:
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()
    semantic_runtime = _ScriptedBoundaryRuntime(decisions=[False, True])

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=10_000,
            force_split_message_threshold=10_000,
            profile_min_memcells=99,
        ),
        semantic_runtime=semantic_runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )

    first = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[
                ConversationMessage(
                    message_id="p1",
                    group_id="g1",
                    sender_id="u1",
                    content="first message",
                    timestamp_ms=1000,
                )
            ],
        )
    )
    assert first.status == "accumulated"
    assert first.extracted_count == 0

    second = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[
                ConversationMessage(
                    message_id="p2",
                    group_id="g1",
                    sender_id="u1",
                    content="second message",
                    timestamp_ms=1500,
                )
            ],
        )
    )
    assert second.status == "extracted"
    assert second.extracted_count == 1
    assert second.topic_id == "topic_000"
    assert semantic_runtime.detect_boundary_calls == 2
    assert artifact_store.persist_calls == 1
    assert artifact_store.sync_calls == 1
    assert asyncio.run(status_store.get_last_memcell_time_ms("g1")) == 1500


def test_all_history_runtime_adapter_drives_boundary_in_workflow() -> None:
    runtime = EverMemOSAllHistoryRuntime(
        llm_config=LLMClientConfig(
            backend="mock",
            mock_delay_s=0.0,
            mock_response=(
                '{"continue_window": false, "confidence": 0.88, "reason": "topic shift"}'
            ),
        ),
        decompose_fn=_adapter_decompose,
        distill_fn=_adapter_distill,
    )
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=10_000,
            force_split_message_threshold=10_000,
            profile_min_memcells=99,
        ),
        semantic_runtime=runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )

    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[_message("a1", 1000)],
        )
    )
    runtime.close()

    assert result.status == "extracted"
    assert result.boundary is not None
    assert result.boundary.reasoning == "topic shift"
    assert result.boundary.forced is False
    assert runtime.detect_boundary_calls == 1


def test_force_split_skips_all_history_runtime_call() -> None:
    runtime = EverMemOSAllHistoryRuntime(
        llm_config=LLMClientConfig(
            backend="mock",
            mock_delay_s=0.0,
            mock_response=(
                '{"continue_window": true, "confidence": 0.5, "reason": "should not be used"}'
            ),
        ),
        decompose_fn=_adapter_decompose,
        distill_fn=_adapter_distill,
    )
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=1,
            force_split_message_threshold=10_000,
            profile_min_memcells=99,
        ),
        semantic_runtime=runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )

    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[_message("a2", 2000)],
        )
    )
    runtime.close()

    assert result.status == "extracted"
    assert result.boundary is not None
    assert result.boundary.forced is True
    assert runtime.detect_boundary_calls == 0


def test_insertion_workflow_field_level_parity_contract() -> None:
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()
    semantic_runtime = _SemanticRuntimeStub(should_end=True)

    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=10_000,
            force_split_message_threshold=10_000,
            profile_min_memcells=99,
        ),
        semantic_runtime=semantic_runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )

    message = ConversationMessage(
        message_id="parity_1",
        group_id="g1",
        sender_id="u1",
        content="user plans travel next week",
        timestamp_ms=1234,
    )
    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[message],
        )
    )

    assert result.status == "extracted"
    assert result.extracted_count == 3
    assert result.topic_id == "topic_000"
    assert result.memcell_id is not None
    assert result.boundary is not None
    assert result.boundary.forced is False

    assert result.memcell_id in memcell_store._records
    stored_memcell = memcell_store._records[result.memcell_id]
    assert stored_memcell.group_id == "g1"
    assert stored_memcell.timestamp_ms == 1234
    assert stored_memcell.scene == "assistant"
    assert stored_memcell.episode == "episode_a"
    assert stored_memcell.subject == "subject_a"
    assert stored_memcell.topic_id == "topic_000"
    assert stored_memcell.participants == ["u1"]
    assert len(stored_memcell.original_messages) == 1
    assert stored_memcell.original_messages[0].message_id == "parity_1"

    assert artifact_store.persist_calls == 1
    assert artifact_store.sync_calls == 1
    persisted = artifact_store.persisted[0]
    assert persisted["group_id"] == "g1"
    assert persisted["memcell_id"] == result.memcell_id
    assert persisted["scene"] == "assistant"
    persisted_artifacts = persisted["artifacts"]
    assert isinstance(persisted_artifacts, DecompositionArtifacts)
    assert len(persisted_artifacts.foresights) == 1
    assert len(persisted_artifacts.event_logs) == 1

    state = asyncio.run(topic_state_store.load_state("g1"))
    assert state.eventid_to_topic[result.memcell_id] == "topic_000"
    assert state.topic_counts["topic_000"] == 1
    assert asyncio.run(status_store.get_last_memcell_time_ms("g1")) == 1234


def test_operator_runtime_executes_evermemos_predicates_via_sem_ops() -> None:
    status_store = _InMemoryConversationStatusStore()
    buffer_store = _InMemoryConversationBufferStore()
    memcell_store = _InMemoryMemCellStore()
    artifact_store = _InMemoryArtifactStore()
    topic_state_store = _InMemoryTopicStateStore()
    profile_store = _InMemoryProfileStore()
    factory = _PromptRoutingClientFactory()
    runtime = EverMemOSOperatorRuntime(
        llm_config=LLMClientConfig(backend="mock"),
        config=EverMemOSOperatorRuntimeConfig(
            boundary_strategy="sem_filter",
            profile_agg_mode="compressive",
        ),
        client_factory=factory,
    )
    workflow = EverMemOSInsertionWorkflow(
        config=EverMemOSWorkflowConfig(
            force_split_token_threshold=10_000,
            force_split_message_threshold=10_000,
            profile_min_memcells=1,
        ),
        semantic_runtime=runtime,
        token_counter=lambda messages: len(messages),
        conversation_status_store=status_store,
        conversation_buffer_store=buffer_store,
        memcell_store=memcell_store,
        memory_artifact_store=artifact_store,
        topic_state_store=topic_state_store,
        topic_assigner=_SimpleTopicAssigner(),
        profile_store=profile_store,
    )
    result = asyncio.run(
        workflow.memorize(
            group_id="g1",
            scene="assistant",
            new_messages=[_message("op_1", 3000)],
        )
    )
    runtime.close()

    assert result.status == "extracted"
    assert result.extracted_count == 3
    assert result.profile_updated is True
    assert runtime.detect_boundary_calls == 1
    assert runtime.decompose_calls == 1
    assert runtime.distill_calls == 1

    stored = memcell_store._records[result.memcell_id]
    assert stored.episode == "episode_from_sem_map"
    assert stored.subject == "subject_from_sem_map"

    assert any("Has the conversation reached a natural boundary" in item for item in factory.prompts)
    assert any(
        "Synthesize this conversation into a concise third-person episodic narrative"
        in item
        for item in factory.prompts
    )
    assert any("What is the central subject of this conversation?" in item for item in factory.prompts)
    assert any(
        "Extract time-bounded future predictions or planned actions" in item
        for item in factory.prompts
    )
    assert any("Extract discrete atomic factual events" in item for item in factory.prompts)
    assert any("Update the running compressed memory" in item for item in factory.prompts)
    assert any("Given old profiles and distilled cluster evidence, return updated profiles." in item for item in factory.prompts)


def test_sem_topk_reranker_uses_sem_score_path() -> None:
    factory = _PromptRoutingClientFactory()
    reranker = EverMemOSSemTopKReranker(
        llm_config=LLMClientConfig(backend="mock"),
        config=EverMemOSSemTopKRerankerConfig(
            intent="Score how relevant this memory item is to the query in [0,1]."
        ),
        client_factory=factory,
    )
    candidates = [
        RetrievedMemory(
            memory_id="m1",
            memory_type="episode",
            content="alpha memory",
            score=0.0,
            source="vector",
        ),
        RetrievedMemory(
            memory_id="m2",
            memory_type="episode",
            content="beta memory",
            score=0.0,
            source="vector",
        ),
    ]
    reranked = asyncio.run(
        reranker.rerank(
            query="alpha",
            candidates=candidates,
            top_k=2,
            scene="assistant",
        )
    )
    reranker.close()

    assert [item.memory_id for item in reranked] == ["m1", "m2"]
    assert any("Item to score:" in item for item in factory.prompts)


def test_operator_runtime_config_from_env_boundary_strategy() -> None:
    previous = dict(os.environ)
    try:
        os.environ["EVERMEMOS_BOUNDARY_STRATEGY"] = "all_history"
        config = EverMemOSOperatorRuntimeConfig.from_env()
        assert config.boundary_strategy == "all_history"
    finally:
        os.environ.clear()
        os.environ.update(previous)
