"""Operator-backed semantic runtime for EverMemOS workflow."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from pyflink.semantic_runtime.llm_client import (
    LLMClient,
    LLMClientConfig,
    create_llm_client,
)
from pyflink.semantic_runtime.operators.row.sem_filter import SemFilterFunction
from pyflink.semantic_runtime.operators.row.sem_map import SemMapFunction
from pyflink.semantic_runtime.runtime.prompt_templates import (
    build_sem_filter_prompt,
    build_sem_map_prompt,
)
from pyflink.semantic_runtime.runtime.steps import (
    evaluate_all_history_sem_continuity,
    evaluate_sem_agg_summary_update,
    evaluate_sem_score,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    BoundaryDecision,
    ConversationMessage,
    DecompositionArtifacts,
    EventLogArtifact,
    ForesightArtifact,
    MemCellRecord,
    RetrievedMemory,
)


DEFAULT_EVERMEMOS_BOUNDARY_INTENT = (
    "Has the conversation reached a natural boundary "
    "(topic shift, long time gap, or logical conclusion)?"
)
DEFAULT_EVERMEMOS_EPISODE_INTENT = (
    "Synthesize this conversation into a concise third-person episodic narrative "
    "capturing the key events and context"
)
DEFAULT_EVERMEMOS_SUBJECT_INTENT = "What is the central subject of this conversation?"
DEFAULT_EVERMEMOS_FORESIGHT_INTENT = (
    "Extract time-bounded future predictions or planned actions and return exactly one "
    "JSON object with field foresights as a list. Each foresights item must include a "
    "non-empty string field content, and may include optional fields evidence (string), "
    "start_time (string), end_time (string), duration_days (integer). "
    "Do not use alternative keys such as action/timeframe."
)
DEFAULT_EVERMEMOS_EVENT_LOG_INTENT = (
    "Extract discrete atomic factual events and return exactly one JSON object with "
    "field event_logs as a list of objects. Each event_logs item must include "
    "a non-empty string field atomic_fact, and may include optional integer "
    "field timestamp_ms. Do not use alternative keys such as who/did_what/when/"
    "specific_details."
)
DEFAULT_EVERMEMOS_PROFILE_DISTILL_INTENT = (
    "Given old profiles and distilled cluster evidence, return updated profiles."
)
DEFAULT_EVERMEMOS_RERANK_INTENT = (
    "Score how relevant this memory item is to the query in [0,1]."
)
DEFAULT_EVERMEMOS_RERANK_BLOCK_SIZE = 1

VALID_BOUNDARY_STRATEGIES = {"sem_filter", "all_history"}
VALID_PROFILE_AGG_MODES = {"summarize", "compressive"}
EVERMEMOS_BOUNDARY_STRATEGY_ENV = "EVERMEMOS_BOUNDARY_STRATEGY"


class _NoopRuntimeContext:
    """No-op runtime context used when invoking row operators outside Flink."""


@dataclass(frozen=True)
class EverMemOSOperatorRuntimeConfig:
    """Config for operator-backed EverMemOS semantic runtime."""

    boundary_strategy: str = "sem_filter"
    boundary_intent: str = DEFAULT_EVERMEMOS_BOUNDARY_INTENT
    episode_intent: str = DEFAULT_EVERMEMOS_EPISODE_INTENT
    subject_intent: str = DEFAULT_EVERMEMOS_SUBJECT_INTENT
    foresight_intent: str = DEFAULT_EVERMEMOS_FORESIGHT_INTENT
    event_log_intent: str = DEFAULT_EVERMEMOS_EVENT_LOG_INTENT
    profile_agg_mode: str = "compressive"
    profile_distill_intent: str = DEFAULT_EVERMEMOS_PROFILE_DISTILL_INTENT

    def __post_init__(self) -> None:
        if self.boundary_strategy not in VALID_BOUNDARY_STRATEGIES:
            raise ValueError(
                f"Invalid boundary_strategy={self.boundary_strategy!r}; "
                f"must be one of {VALID_BOUNDARY_STRATEGIES!r}"
            )
        if self.profile_agg_mode not in VALID_PROFILE_AGG_MODES:
            raise ValueError(
                f"Invalid profile_agg_mode={self.profile_agg_mode!r}; "
                f"must be one of {VALID_PROFILE_AGG_MODES!r}"
            )

    @classmethod
    def from_env(cls) -> "EverMemOSOperatorRuntimeConfig":
        """Build runtime config from environment variables."""
        boundary_strategy = os.getenv(
            EVERMEMOS_BOUNDARY_STRATEGY_ENV,
            "sem_filter",
        ).strip()
        return cls(boundary_strategy=boundary_strategy)


class EverMemOSOperatorRuntime:
    """Semantic runtime backed by existing semantic operators."""

    def __init__(
        self,
        *,
        llm_config: LLMClientConfig,
        config: Optional[EverMemOSOperatorRuntimeConfig] = None,
        client_factory: Optional[Callable[[LLMClientConfig], LLMClient]] = None,
    ) -> None:
        self._llm_config = llm_config
        self._config = config or EverMemOSOperatorRuntimeConfig()
        self._client_factory = client_factory or create_llm_client
        self.detect_boundary_calls: int = 0
        self.decompose_calls: int = 0
        self.distill_calls: int = 0

        self._boundary_filter: Optional[SemFilterFunction] = None
        self._all_history_client: Optional[LLMClient] = None
        self._episode_map: SemMapFunction
        self._subject_map: SemMapFunction
        self._foresight_map: SemMapFunction
        self._event_log_map: SemMapFunction
        self._profile_map: SemMapFunction
        self._profile_agg_client: LLMClient = self._client_factory(self._llm_config)

        self._init_boundary_runtime()
        self._episode_map = self._build_structured_map(
            intent=self._config.episode_intent,
            output_schema={"episode": str},
        )
        self._subject_map = self._build_structured_map(
            intent=self._config.subject_intent,
            output_schema={"subject": str},
        )
        self._foresight_map = self._build_structured_map(
            intent=self._config.foresight_intent,
            output_schema={"foresights": list},
        )
        self._event_log_map = self._build_structured_map(
            intent=self._config.event_log_intent,
            output_schema={"event_logs": list},
        )
        self._profile_map = self._build_structured_map(
            intent=self._config.profile_distill_intent,
            output_schema={"profiles": dict},
        )

    def close(self) -> None:
        """Release runtime-owned resources."""
        if self._boundary_filter is not None:
            self._boundary_filter.close()
        if self._all_history_client is not None:
            self._all_history_client.close()
        self._episode_map.close()
        self._subject_map.close()
        self._foresight_map.close()
        self._event_log_map.close()
        self._profile_map.close()
        self._profile_agg_client.close()

    async def aclose(self) -> None:
        """Asynchronously release runtime-owned resources when supported."""
        await self._close_optional_operator(self._boundary_filter)
        self._boundary_filter = None
        await self._close_optional_client(self._all_history_client)
        self._all_history_client = None
        await self._close_optional_operator(self._episode_map)
        await self._close_optional_operator(self._subject_map)
        await self._close_optional_operator(self._foresight_map)
        await self._close_optional_operator(self._event_log_map)
        await self._close_optional_operator(self._profile_map)
        await self._close_optional_client(self._profile_agg_client)

    async def detect_boundary(
        self,
        *,
        history_messages: Sequence[ConversationMessage],
        new_messages: Sequence[ConversationMessage],
        time_gap_ms: Optional[int],
        scene: str,
    ) -> BoundaryDecision:
        """Detect conversation boundary using configured semantic strategy."""
        self.detect_boundary_calls += 1
        if not new_messages:
            raise ValueError("detect_boundary requires non-empty new_messages")

        if self._config.boundary_strategy == "all_history":
            assert self._all_history_client is not None
            result = await evaluate_all_history_sem_continuity(
                client=self._all_history_client,
                llm_config=self._llm_config,
                active_window_events=[self._to_sem_window_event(item) for item in history_messages],
                current_event=self._to_sem_window_event(new_messages[-1]),
            )
            continue_window = bool(result["continue_window"])
            return BoundaryDecision(
                should_end=not continue_window,
                should_wait=continue_window,
                reasoning=str(result["reason"]),
                confidence=float(result["confidence"]),
                forced=False,
            )

        assert self._boundary_filter is not None
        payload = {
            "history_messages": [self._to_message_payload(item) for item in history_messages],
            "new_messages": [self._to_message_payload(item) for item in new_messages],
            "time_gap_ms": time_gap_ms,
            "scene": scene,
        }
        parsed = await self._invoke_json(self._boundary_filter, payload)
        decision = bool(parsed["decision"])
        return BoundaryDecision(
            should_end=decision,
            should_wait=not decision,
            reasoning=str(parsed["reason"]),
            confidence=float(parsed["confidence"]),
            forced=False,
        )

    async def decompose_memcell(
        self,
        *,
        memcell: MemCellRecord,
        scene: str,
    ) -> DecompositionArtifacts:
        """Decompose MemCell with sem_map predicates aligned to EverMemOS flow."""
        self.decompose_calls += 1
        payload = {
            "group_id": memcell.group_id,
            "scene": scene,
            "participants": list(memcell.participants),
            "messages": [self._to_message_payload(item) for item in memcell.original_messages],
        }
        episode_task = self._invoke_json(self._episode_map, payload)
        subject_task = self._invoke_json(self._subject_map, payload)

        if scene == "assistant":
            foresight_task = self._invoke_json(self._foresight_map, payload)
            event_log_task = self._invoke_json(self._event_log_map, payload)
            episode_row, subject_row, foresight_row, event_log_row = await asyncio.gather(
                episode_task,
                subject_task,
                foresight_task,
                event_log_task,
            )
            foresights = self._normalize_foresights(foresight_row["foresights"])
            event_logs = self._normalize_event_logs(event_log_row["event_logs"])
            return DecompositionArtifacts(
                episode=str(episode_row["episode"]),
                subject=str(subject_row["subject"]),
                foresights=foresights,
                event_logs=event_logs,
            )

        episode_row, subject_row = await asyncio.gather(episode_task, subject_task)
        return DecompositionArtifacts(
            episode=str(episode_row["episode"]),
            subject=str(subject_row["subject"]),
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
        """Distill profile updates using sem_agg summarization + sem_map structuring."""
        self.distill_calls += 1
        added_events = [self._to_cluster_event(item) for item in cluster_memcells]
        agg_row = await evaluate_sem_agg_summary_update(
            client=self._profile_agg_client,
            mode=self._config.profile_agg_mode,
            current_summary=json.dumps(old_profiles, ensure_ascii=False),
            added_events=added_events,
        )
        profile_row = await self._invoke_json(
            self._profile_map,
            {
                "old_profiles": dict(old_profiles),
                "distilled_summary": str(agg_row["summary"]),
                "scene": scene,
            },
        )
        profiles = profile_row.get("profiles")
        if not isinstance(profiles, dict):
            raise ValueError("profile distill must return dict profiles")
        return profiles

    def _init_boundary_runtime(self) -> None:
        if self._config.boundary_strategy == "all_history":
            self._all_history_client = self._client_factory(self._llm_config)
            self._boundary_filter = None
            return
        self._all_history_client = None
        prompt = build_sem_filter_prompt(self._config.boundary_intent)
        self._boundary_filter = SemFilterFunction(prompt, self._llm_config)
        self._boundary_filter.open(_NoopRuntimeContext())
        # Override row-operator client with injected factory for deterministic routing/tests.
        self._boundary_filter.close()
        self._boundary_filter._client = self._client_factory(self._llm_config)  # type: ignore[attr-defined]

    def _build_structured_map(
        self,
        *,
        intent: str,
        output_schema: Dict[str, type],
    ) -> SemMapFunction:
        prompt_template = build_sem_map_prompt(
            intent,
            output_schema=output_schema,
            output_mode="json",
        )
        fn = SemMapFunction(
            prompt_template=prompt_template,
            output_schema=output_schema,
            llm_config=self._llm_config,
            return_mode="json",
        )
        fn.open(_NoopRuntimeContext())
        # Override row-operator client with injected factory for deterministic routing/tests.
        fn.close()
        fn._client = self._client_factory(self._llm_config)  # type: ignore[attr-defined]
        return fn

    async def _invoke_json(
        self,
        fn: SemMapFunction | SemFilterFunction,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        rows = await fn.async_invoke(payload)
        if len(rows) != 1:
            raise RuntimeError("semantic operator invocation must return exactly one row")
        parsed = json.loads(rows[0])
        if not isinstance(parsed, dict):
            raise ValueError("semantic operator invocation must return one JSON object")
        return parsed

    def _normalize_foresights(self, values: Any) -> List[ForesightArtifact]:
        if not isinstance(values, list):
            raise ValueError("foresights must be a list")
        items: List[ForesightArtifact] = []
        for value in values:
            if isinstance(value, str):
                items.append(ForesightArtifact(content=value))
                continue
            if not isinstance(value, dict):
                raise ValueError("foresight entries must be str or dict")
            content = value.get("content")
            if not isinstance(content, str) or not content.strip():
                raise ValueError("foresight dict entries must include non-empty content")
            items.append(
                ForesightArtifact(
                    content=content,
                    evidence=self._optional_str(value.get("evidence")),
                    start_time=self._optional_str(value.get("start_time")),
                    end_time=self._optional_str(value.get("end_time")),
                    duration_days=self._optional_int(value.get("duration_days")),
                )
            )
        return items

    def _normalize_event_logs(self, values: Any) -> List[EventLogArtifact]:
        if not isinstance(values, list):
            raise ValueError("event_logs must be a list")
        items: List[EventLogArtifact] = []
        for value in values:
            if isinstance(value, str):
                items.append(EventLogArtifact(atomic_fact=value))
                continue
            if not isinstance(value, dict):
                raise ValueError("event_log entries must be str or dict")
            atomic_fact = value.get("atomic_fact")
            if not isinstance(atomic_fact, str) or not atomic_fact.strip():
                raise ValueError("event_log dict entries must include non-empty atomic_fact")
            items.append(
                EventLogArtifact(
                    atomic_fact=atomic_fact,
                    timestamp_ms=self._optional_int(value.get("timestamp_ms")),
                )
            )
        return items

    def _to_message_payload(self, message: ConversationMessage) -> Dict[str, Any]:
        return {
            "message_id": message.message_id,
            "group_id": message.group_id,
            "sender_id": message.sender_id,
            "sender_name": message.sender_name,
            "role": message.role,
            "content": message.content,
            "timestamp_ms": int(message.timestamp_ms),
            "metadata": dict(message.metadata),
        }

    def _to_sem_window_event(self, message: ConversationMessage) -> Dict[str, Any]:
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

    def _to_cluster_event(self, memcell: MemCellRecord) -> Dict[str, Any]:
        return {
            "memcell_id": memcell.memcell_id,
            "timestamp_ms": int(memcell.timestamp_ms),
            "episode": memcell.episode or "",
            "subject": memcell.subject or "",
            "summary": memcell.summary or "",
            "topic_id": memcell.topic_id or "",
            "scene": memcell.scene,
        }

    def _optional_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("expected optional string field")
        return value

    def _optional_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if not isinstance(value, int):
            raise ValueError("expected optional int field")
        return value

    async def _close_optional_operator(
        self,
        operator: Optional[SemMapFunction | SemFilterFunction],
    ) -> None:
        if operator is None:
            return
        client = getattr(operator, "_client", None)
        if client is not None:
            await self._close_optional_client(client)
            operator._client = None  # type: ignore[attr-defined]

    async def _close_optional_client(self, client: Optional[LLMClient]) -> None:
        if client is None:
            return
        async_close = getattr(client, "aclose", None)
        if callable(async_close):
            await async_close()
            return
        client.close()


@dataclass(frozen=True)
class EverMemOSSemTopKRerankerConfig:
    """Config for semantic reranker used in EverMemOS retrieval."""

    intent: str = DEFAULT_EVERMEMOS_RERANK_INTENT
    block_size: int = DEFAULT_EVERMEMOS_RERANK_BLOCK_SIZE

    def __post_init__(self) -> None:
        if self.block_size <= 0:
            raise ValueError("block_size must be > 0")


class EverMemOSSemTopKReranker:
    """Point-wise semantic reranker built on sem_score execution step."""

    def __init__(
        self,
        *,
        llm_config: LLMClientConfig,
        config: Optional[EverMemOSSemTopKRerankerConfig] = None,
        client_factory: Optional[Callable[[LLMClientConfig], LLMClient]] = None,
    ) -> None:
        self._llm_config = llm_config
        self._config = config or EverMemOSSemTopKRerankerConfig()
        factory = client_factory or create_llm_client
        self._client = factory(llm_config)

    def close(self) -> None:
        """Release runtime-owned resources."""
        self._client.close()

    async def rerank(
        self,
        *,
        query: str,
        candidates: Sequence[RetrievedMemory],
        top_k: int,
        scene: str,
    ) -> Sequence[RetrievedMemory]:
        """Rerank candidates via semantic scoring and return sorted top_k."""
        _ = scene
        if top_k <= 0:
            raise ValueError("rerank requires top_k > 0")
        if not candidates:
            return []

        scored = await asyncio.gather(
            *[
                evaluate_sem_score(
                    client=self._client,
                    llm_config=self._llm_config,
                    intent=self._config.intent,
                    item={
                        "query": query,
                        "memory_id": item.memory_id,
                        "memory_type": item.memory_type,
                        "content": item.content,
                        "source": item.source,
                        "timestamp_ms": item.timestamp_ms,
                        "metadata": dict(item.metadata),
                    },
                )
                for item in candidates
            ]
        )
        reranked = [
            RetrievedMemory(
                memory_id=item.memory_id,
                memory_type=item.memory_type,
                content=item.content,
                score=float(score_row["score"]),
                source=item.source,
                timestamp_ms=item.timestamp_ms,
                metadata=item.metadata,
            )
            for item, score_row in zip(candidates, scored)
        ]
        reranked.sort(key=lambda item: float(item.score), reverse=True)
        return reranked[:top_k]
