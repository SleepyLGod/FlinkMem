"""Run real agent-memory workflows with dataset-driven inputs."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Sequence

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (
    ConversationMessage,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos import (
    EverMemOSBackendConfig,
    EverMemOSInsertionWorkflow,
    EverMemOSOperatorRuntime,
    EverMemOSOperatorRuntimeConfig,
    EverMemOSTopicAssigner,
    EverMemOSTopicAssignerConfig,
    EverMemOSWorkflowConfig,
    build_evermemos_external_bundle,
    create_evermemos_external_clients,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0 import (
    Mem0BackendConfig,
    Mem0BasicConfig,
    Mem0BasicWorkflow,
    Mem0GraphConfig,
    Mem0GraphWorkflow,
    build_mem0_external_bundle,
    create_mem0_external_clients,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.dataset_loader import (
    DatasetConversation,
    load_dataset_conversation,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.runtime.llm_semantic_runtimes import (
    Mem0BasicLLMSemanticRuntime,
    Mem0GraphLLMSemanticRuntime,
    ZepLLMSemanticRuntime,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep import (
    ZepAddEpisodeWorkflow,
    ZepBackendConfig,
    ZepWorkflowConfig,
    build_zep_external_bundle,
    create_zep_external_clients,
)


DEFAULT_LLM_BACKEND = "openai"
DEFAULT_LLM_MODEL = "deepseek-chat"
DEFAULT_LLM_API_BASE = "https://api.deepseek.com/v1"
DEFAULT_LLM_API_KEY_ENV = "DEEPSEEK_API_KEY"
DEFAULT_LLM_TIMEOUT_S = 60.0
DEFAULT_LLM_MAX_RETRIES = 2
DEFAULT_LLM_RETRY_BASE_DELAY_S = 0.5

DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OLLAMA_EMBED_MODEL = "all-minilm"
DEFAULT_OLLAMA_TIMEOUT_S = 30
DEFAULT_OLLAMA_MAX_RETRIES = 3
DEFAULT_OLLAMA_RETRY_BASE_DELAY_S = 0.5
DEFAULT_OLLAMA_EMBED_MAX_INPUT_CHARS = 512

DEFAULT_DATASET_SOURCE = "longmemeval"
DEFAULT_DATASET_SAMPLE_INDEX = 0
DEFAULT_DATASET_MAX_MESSAGES = 24
DEFAULT_ZEP_PROMPT_MESSAGE_MAX_CHARS = 1_200
DEFAULT_ZEP_PROMPT_MAX_RECENT_EPISODES = 3
DEFAULT_ZEP_PROMPT_RECENT_EPISODE_MAX_CHARS = 400
DEFAULT_ZEP_PROMPT_MAX_EDGE_ENTITIES = 16
DEFAULT_ZEP_EDGE_ENTITY_REFERENCE_MODE = "name"
DEFAULT_MEM0_RELATION_ENTITY_REFERENCE_MODE = "name"
DEFAULT_INPUT_SCOPE_POLICY = "none"
VALID_INPUT_SCOPE_POLICIES = frozenset({"none", "sliding", "session"})
DEFAULT_INPUT_SCOPE_SLIDING_SIZE = 8
DEFAULT_INPUT_SCOPE_SESSION_GAP_MS = 30 * 60 * 1_000


def _resolve_repo_root(*, start: pathlib.Path) -> pathlib.Path:
    current = start.resolve()
    markers = (
        "tools/agent_memory/run_local_agent_memory_stack.sh",
        ".git",
    )
    for parent in [current, *current.parents]:
        if all((parent / marker).exists() for marker in markers):
            return parent
        if (parent / markers[0]).exists():
            return parent
    raise RuntimeError(f"unable to locate repo root from {start}")


REPO_ROOT = _resolve_repo_root(start=pathlib.Path(__file__))


@dataclass(frozen=True)
class ReplayEvent:
    """One replay event with optional input-scope context."""

    message: ConversationMessage
    scope_messages: Sequence[ConversationMessage]


def _load_repo_env(*, repo_root: pathlib.Path) -> None:
    env_candidates = [
        repo_root / ".env",
        repo_root / "tools" / "agent_memory" / ".env",
    ]
    for env_path in env_candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            key = key.strip()
            if not key:
                continue
            normalized_value = value.strip().strip("'").strip('"')
            if key not in os.environ:
                os.environ[key] = normalized_value


def _create_ollama_embedding_fn(
    *,
    base_url: str,
    model: str,
    timeout_s: int,
    max_retries: int,
    retry_base_delay_s: float,
    max_input_chars: int | None,
):
    normalized_base_url = str(base_url).rstrip("/")
    normalized_model = str(model).strip()
    if not normalized_model:
        raise ValueError("ollama embedding model must be non-empty")
    if int(timeout_s) <= 0:
        raise ValueError("ollama timeout_s must be > 0")
    if int(max_retries) <= 0:
        raise ValueError("ollama max_retries must be > 0")
    if float(retry_base_delay_s) <= 0.0:
        raise ValueError("ollama retry_base_delay_s must be > 0")
    if max_input_chars is not None and int(max_input_chars) <= 0:
        raise ValueError("ollama max_input_chars must be > 0 when provided")

    def _embed(text: str) -> Sequence[float]:
        normalized_text = str(text).strip()
        if not normalized_text:
            raise ValueError("embedding text must be non-empty")
        if max_input_chars is not None and len(normalized_text) > max_input_chars:
            normalized_text = normalized_text[:max_input_chars]
        request_data = json.dumps(
            {"model": normalized_model, "prompt": normalized_text},
            ensure_ascii=False,
        ).encode("utf-8")
        request = urllib.request.Request(
            url=f"{normalized_base_url}/api/embeddings",
            data=request_data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        response_text = ""
        last_error: Exception | None = None
        retryable_http_codes = {429, 500, 502, 503, 504}
        for attempt in range(1, int(max_retries) + 1):
            try:
                with urllib.request.urlopen(request, timeout=int(timeout_s)) as response:
                    response_text = response.read().decode("utf-8")
                last_error = None
                break
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(
                    f"ollama embedding HTTP {exc.code}: {error_body}"
                )
                if exc.code not in retryable_http_codes or attempt == int(max_retries):
                    raise last_error from exc
                delay = float(retry_base_delay_s) * (2 ** (attempt - 1))
                time.sleep(delay)
            except urllib.error.URLError as exc:
                last_error = RuntimeError(f"ollama embedding request failed: {exc}")
                if attempt == int(max_retries):
                    raise last_error from exc
                delay = float(retry_base_delay_s) * (2 ** (attempt - 1))
                time.sleep(delay)
        if last_error is not None:
            raise last_error
        payload = json.loads(response_text)
        if not isinstance(payload, Mapping):
            raise ValueError("ollama embedding response must be object")
        embedding = payload.get("embedding")
        if not isinstance(embedding, list):
            raise ValueError("ollama embedding response.embedding must be list")
        if not embedding:
            raise ValueError("ollama embedding vector must be non-empty")
        output: list[float] = []
        for index, value in enumerate(embedding):
            if not isinstance(value, (int, float)):
                raise TypeError(f"ollama embedding item[{index}] must be numeric")
            output.append(float(value))
        return output

    return _embed


async def _close_component(component: Any) -> None:
    """Close a workflow component, preferring async close when available."""
    async_close = getattr(component, "aclose", None)
    if callable(async_close):
        await async_close()
        return
    close_method = getattr(component, "close", None)
    if not callable(close_method):
        raise TypeError(f"component {type(component)!r} does not expose close/aclose")
    close_method()


class _OllamaTextEncoder:
    """Adapter with HashingTextEncoder-compatible interface."""

    def __init__(self, *, embedding_fn, embedding_dim: int) -> None:
        self._embedding_fn = embedding_fn
        self.dim = int(embedding_dim)

    def encode_dense(self, text: str) -> Sequence[float]:
        vector = list(self._embedding_fn(text))
        if len(vector) != self.dim:
            raise ValueError(
                f"embedding dim mismatch: expected {self.dim}, got {len(vector)}"
            )
        return vector


def _token_counter(messages: Sequence[ConversationMessage]) -> int:
    return int(sum(len(message.content) for message in messages))


def _build_dataset_messages(
    *,
    conversation: DatasetConversation,
    group_id_prefix: str,
) -> Sequence[ConversationMessage]:
    messages: list[ConversationMessage] = []
    group_id = f"{group_id_prefix}:{conversation.sample_id}"
    for row in conversation.messages:
        message_id = str(row["message_id"])
        sender_id = str(row["sender_id"])
        role = str(row["role"])
        content = str(row["content"])
        timestamp_ms = int(row["timestamp_ms"])
        messages.append(
            ConversationMessage(
                message_id=message_id,
                group_id=group_id,
                sender_id=sender_id,
                content=content,
                timestamp_ms=timestamp_ms,
                role=role,
            )
        )
    return messages


def _build_llm_config() -> LLMClientConfig:
    model = os.getenv("SEM_RUNTIME_MODEL", DEFAULT_LLM_MODEL).strip()
    api_base = os.getenv("SEM_RUNTIME_API_BASE", DEFAULT_LLM_API_BASE).strip()
    api_key_env = os.getenv("SEM_RUNTIME_API_KEY_ENV", DEFAULT_LLM_API_KEY_ENV).strip()
    timeout_s = float(os.getenv("SEM_RUNTIME_TIMEOUT_S", str(DEFAULT_LLM_TIMEOUT_S)))
    max_retries = int(os.getenv("SEM_RUNTIME_MAX_RETRIES", str(DEFAULT_LLM_MAX_RETRIES)))
    retry_base_delay_s = float(
        os.getenv("SEM_RUNTIME_RETRY_BASE_DELAY_S", str(DEFAULT_LLM_RETRY_BASE_DELAY_S))
    )
    if not model:
        raise ValueError("SEM_RUNTIME_MODEL must be non-empty")
    if not api_base:
        raise ValueError("SEM_RUNTIME_API_BASE must be non-empty")
    if not api_key_env:
        raise ValueError("SEM_RUNTIME_API_KEY_ENV must be non-empty")
    return LLMClientConfig(
        backend=DEFAULT_LLM_BACKEND,
        model=model,
        api_base=api_base,
        api_key_env=api_key_env,
        timeout_s=timeout_s,
        max_retries=max_retries,
        retry_base_delay_s=retry_base_delay_s,
    )


def _build_workflow_llm_config(
    *,
    workflow_name: str,
    fallback: LLMClientConfig,
) -> LLMClientConfig:
    """Build workflow-specific LLM config with fallback to SEM_RUNTIME_* defaults."""
    if workflow_name == "mem0":
        prefix = "MEM0_LLM_"
    elif workflow_name == "zep":
        prefix = "ZEP_LLM_"
    else:
        return fallback

    return _build_prefixed_llm_config(prefix=prefix, fallback=fallback)


def _build_prefixed_llm_config(
    *,
    prefix: str,
    fallback: LLMClientConfig,
) -> LLMClientConfig:
    """Build LLM config from an env prefix, with fallback defaults."""

    backend = os.getenv(f"{prefix}PROVIDER", fallback.backend).strip()
    model = os.getenv(f"{prefix}MODEL", fallback.model).strip()
    api_base_raw = os.getenv(f"{prefix}BASE_URL")
    api_base = (
        api_base_raw.strip()
        if api_base_raw is not None and api_base_raw.strip()
        else fallback.api_base
    )
    timeout_s = float(os.getenv(f"{prefix}TIMEOUT_S", str(fallback.timeout_s)))
    max_retries = int(os.getenv(f"{prefix}MAX_RETRIES", str(fallback.max_retries)))
    retry_base_delay_s = float(
        os.getenv(
            f"{prefix}RETRY_BASE_DELAY_S",
            str(fallback.retry_base_delay_s),
        )
    )

    api_key_env = os.getenv(f"{prefix}API_KEY_ENV", "").strip()
    api_key_raw = os.getenv(f"{prefix}API_KEY")
    if api_key_env:
        resolved_api_key_env = api_key_env
    elif api_key_raw is not None and api_key_raw.strip():
        runtime_key_env = f"{prefix}API_KEY_RUNTIME"
        os.environ[runtime_key_env] = api_key_raw.strip()
        resolved_api_key_env = runtime_key_env
    else:
        resolved_api_key_env = fallback.api_key_env

    if not backend:
        raise ValueError(f"{prefix}PROVIDER must be non-empty when provided")
    if not model:
        raise ValueError(f"{prefix}MODEL must be non-empty when provided")
    if api_base is None or not str(api_base).strip():
        raise ValueError(f"{prefix}BASE_URL must be non-empty when provided")
    if timeout_s <= 0:
        raise ValueError(f"{prefix}TIMEOUT_S must be > 0")
    if max_retries <= 0:
        raise ValueError(f"{prefix}MAX_RETRIES must be > 0")
    if retry_base_delay_s <= 0:
        raise ValueError(f"{prefix}RETRY_BASE_DELAY_S must be > 0")
    if not resolved_api_key_env:
        raise ValueError(f"{prefix}API_KEY_ENV must be non-empty")

    return LLMClientConfig(
        backend=backend,
        model=model,
        api_base=api_base,
        api_key_env=resolved_api_key_env,
        timeout_s=timeout_s,
        max_retries=max_retries,
        retry_base_delay_s=retry_base_delay_s,
    )


def _iter_ordered_messages(
    messages: Sequence[ConversationMessage],
) -> Sequence[ConversationMessage]:
    if not messages:
        raise ValueError("messages must be non-empty")
    return sorted(
        messages,
        key=lambda row: (int(row.timestamp_ms), str(row.message_id)),
    )


def _resolve_input_scope_policy() -> str:
    policy = os.getenv(
        "MEMORY_INPUT_SCOPE_POLICY",
        DEFAULT_INPUT_SCOPE_POLICY,
    ).strip()
    if policy not in VALID_INPUT_SCOPE_POLICIES:
        raise ValueError(
            f"MEMORY_INPUT_SCOPE_POLICY must be one of "
            f"{sorted(VALID_INPUT_SCOPE_POLICIES)!r}"
        )
    return policy


def _build_replay_events(
    *,
    ordered_messages: Sequence[ConversationMessage],
    scope_policy: str,
) -> Sequence[ReplayEvent]:
    if scope_policy == "none":
        return [
            ReplayEvent(message=message, scope_messages=[message])
            for message in ordered_messages
        ]
    if scope_policy == "sliding":
        sliding_size = int(
            os.getenv(
                "MEMORY_INPUT_SCOPE_SLIDING_SIZE",
                str(DEFAULT_INPUT_SCOPE_SLIDING_SIZE),
            )
        )
        if sliding_size <= 0:
            raise ValueError("MEMORY_INPUT_SCOPE_SLIDING_SIZE must be > 0")
        replay_events: list[ReplayEvent] = []
        for index, message in enumerate(ordered_messages):
            start_index = max(0, index - sliding_size + 1)
            replay_events.append(
                ReplayEvent(
                    message=message,
                    scope_messages=ordered_messages[start_index : index + 1],
                )
            )
        return replay_events
    if scope_policy == "session":
        session_gap_ms = int(
            os.getenv(
                "MEMORY_INPUT_SCOPE_SESSION_GAP_MS",
                str(DEFAULT_INPUT_SCOPE_SESSION_GAP_MS),
            )
        )
        if session_gap_ms <= 0:
            raise ValueError("MEMORY_INPUT_SCOPE_SESSION_GAP_MS must be > 0")
        replay_events: list[ReplayEvent] = []
        session_start_index = 0
        previous_time_ms: int | None = None
        for index, message in enumerate(ordered_messages):
            current_time_ms = int(message.timestamp_ms)
            if (
                previous_time_ms is not None
                and (current_time_ms - previous_time_ms) > session_gap_ms
            ):
                session_start_index = index
            replay_events.append(
                ReplayEvent(
                    message=message,
                    scope_messages=ordered_messages[session_start_index : index + 1],
                )
            )
            previous_time_ms = current_time_ms
        return replay_events
    raise RuntimeError(f"unsupported scope_policy={scope_policy!r}")


async def _run_evermemos_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    llm_config: LLMClientConfig,
    embedding_fn,
) -> Mapping[str, Any]:
    ordered_messages = _iter_ordered_messages(dataset_messages)
    backend_config = EverMemOSBackendConfig.from_env()
    clients = create_evermemos_external_clients(backend_config=backend_config)
    operator_runtime = EverMemOSOperatorRuntime(
        llm_config=llm_config,
        config=EverMemOSOperatorRuntimeConfig.from_env(),
    )
    embedding_dim = len(list(embedding_fn("embedding dimension probe")))
    topic_assigner = EverMemOSTopicAssigner(
        config=EverMemOSTopicAssignerConfig(embedding_dim=embedding_dim),
        llm_config=llm_config,
        encoder=_OllamaTextEncoder(embedding_fn=embedding_fn, embedding_dim=embedding_dim),
    )
    try:
        bundle = build_evermemos_external_bundle(
            mongo_database=clients.mongo_database,
            backend_config=backend_config,
            elasticsearch_client=clients.elasticsearch_client,
            milvus_client=clients.milvus_client,
            embedding_fn=embedding_fn,
        )
        workflow = EverMemOSInsertionWorkflow(
            config=EverMemOSWorkflowConfig(profile_min_memcells=1),
            semantic_runtime=operator_runtime,
            token_counter=_token_counter,
            conversation_status_store=bundle.conversation_status_store,
            conversation_buffer_store=bundle.conversation_buffer_store,
            memcell_store=bundle.memcell_store,
            memory_artifact_store=bundle.memory_artifact_store,
            topic_state_store=bundle.topic_state_store,
            topic_assigner=topic_assigner,
            profile_store=bundle.profile_store,
        )
        first_group_id = ordered_messages[0].group_id
        results = []
        for message in ordered_messages:
            result = await workflow.memorize(
                group_id=first_group_id,
                scene="assistant",
                new_messages=[message],
            )
            results.append(result)
        if not results:
            raise RuntimeError("evermemos replay produced no result")
        result = results[-1]
        extracted_events = sum(
            1 for item in results if str(item.status) == "extracted"
        )
        accumulated_events = sum(
            1 for item in results if str(item.status) == "accumulated"
        )
        total_extracted_count = sum(int(item.extracted_count) for item in results)
        latest_extracted = next(
            (item for item in reversed(results) if str(item.status) == "extracted"),
            None,
        )
        return {
            "status": result.status,
            "extracted_count": total_extracted_count,
            "extracted_events": extracted_events,
            "accumulated_events": accumulated_events,
            "memcell_id": (
                latest_extracted.memcell_id if latest_extracted is not None else None
            ),
            "topic_id": (
                latest_extracted.topic_id if latest_extracted is not None else None
            ),
            "profile_updated": bool(
                any(bool(item.profile_updated) for item in results)
            ),
            "boundary": {
                "should_end": result.boundary.should_end if result.boundary else None,
                "reasoning": result.boundary.reasoning if result.boundary else None,
                "confidence": result.boundary.confidence if result.boundary else None,
            },
            "detect_boundary_calls": operator_runtime.detect_boundary_calls,
            "decompose_calls": operator_runtime.decompose_calls,
            "distill_calls": operator_runtime.distill_calls,
        }
    finally:
        await _close_component(topic_assigner)
        await _close_component(operator_runtime)
        clients.close()


async def _run_mem0_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    benchmark_query: str,
    llm_config: LLMClientConfig,
    embedding_fn,
    input_scope_policy: str,
) -> Mapping[str, Any]:
    ordered_messages = _iter_ordered_messages(dataset_messages)
    replay_events = _build_replay_events(
        ordered_messages=ordered_messages,
        scope_policy=input_scope_policy,
    )
    backend_config = Mem0BackendConfig.from_env()
    clients = create_mem0_external_clients(backend_config=backend_config)
    mem0_llm_config = _build_workflow_llm_config(
        workflow_name="mem0",
        fallback=llm_config,
    )
    llm_client = create_llm_client(mem0_llm_config)
    mem0_basic_fact_resolve_concurrency = int(
        os.getenv(
            "MEM0_BASIC_FACT_RESOLVE_CONCURRENCY",
            str(Mem0BasicConfig().fact_resolve_concurrency),
        )
    )
    mem0_basic_fact_write_group_concurrency = int(
        os.getenv(
            "MEM0_BASIC_FACT_WRITE_GROUP_CONCURRENCY",
            str(Mem0BasicConfig().fact_write_group_concurrency),
        )
    )
    mem0_graph_entity_resolve_concurrency = int(
        os.getenv(
            "MEM0_GRAPH_ENTITY_RESOLVE_CONCURRENCY",
            str(Mem0GraphConfig().entity_resolve_concurrency),
        )
    )
    mem0_graph_entity_upsert_group_concurrency = int(
        os.getenv(
            "MEM0_GRAPH_ENTITY_UPSERT_GROUP_CONCURRENCY",
            str(Mem0GraphConfig().entity_upsert_group_concurrency),
        )
    )
    mem0_graph_relation_resolve_concurrency = int(
        os.getenv(
            "MEM0_GRAPH_RELATION_RESOLVE_CONCURRENCY",
            str(Mem0GraphConfig().relation_resolve_concurrency),
        )
    )
    mem0_graph_relation_write_group_concurrency = int(
        os.getenv(
            "MEM0_GRAPH_RELATION_WRITE_GROUP_CONCURRENCY",
            str(Mem0GraphConfig().relation_write_group_concurrency),
        )
    )
    mem0_relation_entity_reference_mode = os.getenv(
        "MEM0_RELATION_ENTITY_REFERENCE_MODE",
        DEFAULT_MEM0_RELATION_ENTITY_REFERENCE_MODE,
    ).strip()
    try:
        bundle = build_mem0_external_bundle(
            backend_config=backend_config,
            embedding_fn=embedding_fn,
            neo4j_driver=clients.neo4j_driver,
        )
        group_id = f"mem0:{ordered_messages[0].group_id}"

        basic_runtime = Mem0BasicLLMSemanticRuntime(client=llm_client)
        basic_workflow = Mem0BasicWorkflow(
            config=Mem0BasicConfig(
                recall_backend="embedding",
                fact_resolve_concurrency=mem0_basic_fact_resolve_concurrency,
                fact_write_group_concurrency=mem0_basic_fact_write_group_concurrency,
            ),
            semantic_runtime=basic_runtime,
            fact_store=bundle.fact_store,
            fact_searcher=bundle.fact_searcher,
        )
        basic_extracted_fact_count = 0
        basic_added = 0
        basic_updated = 0
        basic_deleted = 0
        basic_noop = 0
        for event in replay_events:
            basic_add_result = await basic_workflow.add(
                group_id=group_id,
                messages=[str(event.message.content)],
            )
            basic_extracted_fact_count += int(basic_add_result.extracted_fact_count)
            basic_added += int(basic_add_result.added)
            basic_updated += int(basic_add_result.updated)
            basic_deleted += int(basic_add_result.deleted)
            basic_noop += int(basic_add_result.noop)
        basic_search_result = await basic_workflow.search(
            group_id=group_id,
            query=benchmark_query,
            top_k=3,
        )

        if bundle.graph_store is None:
            raise ValueError("mem0 graph_store must be enabled for graph workflow run")
        if bundle.graph_entity_searcher is None:
            raise ValueError("mem0 graph_entity_searcher must be enabled")
        if bundle.graph_relation_searcher is None:
            raise ValueError("mem0 graph_relation_searcher must be enabled")

        graph_runtime = Mem0GraphLLMSemanticRuntime(
            client=llm_client,
            relation_entity_reference_mode=mem0_relation_entity_reference_mode,
        )
        graph_workflow = Mem0GraphWorkflow(
            config=Mem0GraphConfig(
                entity_recall_backend="embedding",
                relation_recall_backend="embedding",
                entity_resolve_concurrency=mem0_graph_entity_resolve_concurrency,
                entity_upsert_group_concurrency=(
                    mem0_graph_entity_upsert_group_concurrency
                ),
                relation_resolve_concurrency=mem0_graph_relation_resolve_concurrency,
                relation_write_group_concurrency=(
                    mem0_graph_relation_write_group_concurrency
                ),
            ),
            semantic_runtime=graph_runtime,
            graph_store=bundle.graph_store,
            entity_searcher=bundle.graph_entity_searcher,
            relation_searcher=bundle.graph_relation_searcher,
        )
        graph_extracted_entity_count = 0
        graph_extracted_relation_count = 0
        graph_upserted_entities = 0
        graph_added_relations = 0
        graph_updated_relations = 0
        graph_deleted_relations = 0
        for event in replay_events:
            graph_add_result = await graph_workflow.add(
                group_id=group_id,
                messages=[str(event.message.content)],
            )
            graph_extracted_entity_count += int(graph_add_result.extracted_entity_count)
            graph_extracted_relation_count += int(
                graph_add_result.extracted_relation_count
            )
            graph_upserted_entities += int(graph_add_result.upserted_entities)
            graph_added_relations += int(graph_add_result.added_relations)
            graph_updated_relations += int(graph_add_result.updated_relations)
            graph_deleted_relations += int(graph_add_result.deleted_relations)
        graph_search_result = await graph_workflow.search(
            group_id=group_id,
            query=benchmark_query,
            top_k=3,
        )

        return {
            "basic": {
                "extracted_fact_count": basic_extracted_fact_count,
                "added": basic_added,
                "updated": basic_updated,
                "deleted": basic_deleted,
                "noop": basic_noop,
                "processed_messages": len(replay_events),
                "search_count": len(basic_search_result.memories),
            },
            "graph": {
                "extracted_entity_count": graph_extracted_entity_count,
                "extracted_relation_count": graph_extracted_relation_count,
                "upserted_entities": graph_upserted_entities,
                "added_relations": graph_added_relations,
                "updated_relations": graph_updated_relations,
                "deleted_relations": graph_deleted_relations,
                "processed_messages": len(replay_events),
                "search_count": len(graph_search_result.relations),
            },
            "input_scope_policy": input_scope_policy,
            "max_scope_size": max(len(event.scope_messages) for event in replay_events),
        }
    finally:
        await _close_component(llm_client)
        clients.close()


async def _run_zep_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    llm_config: LLMClientConfig,
    input_scope_policy: str,
) -> Mapping[str, Any]:
    ordered_messages = _iter_ordered_messages(dataset_messages)
    replay_events = _build_replay_events(
        ordered_messages=ordered_messages,
        scope_policy=input_scope_policy,
    )
    backend_config = ZepBackendConfig.from_env()
    clients = create_zep_external_clients(backend_config=backend_config)
    zep_llm_config = _build_workflow_llm_config(
        workflow_name="zep",
        fallback=llm_config,
    )
    zep_summary_llm_config = _build_prefixed_llm_config(
        prefix="ZEP_SUMMARY_LLM_",
        fallback=zep_llm_config,
    )
    llm_client = create_llm_client(zep_llm_config)
    if zep_summary_llm_config == zep_llm_config:
        summary_llm_client = llm_client
    else:
        summary_llm_client = create_llm_client(zep_summary_llm_config)
    zep_prompt_message_max_chars = int(
        os.getenv(
            "ZEP_PROMPT_MESSAGE_MAX_CHARS",
            str(DEFAULT_ZEP_PROMPT_MESSAGE_MAX_CHARS),
        )
    )
    zep_prompt_max_recent_episodes = int(
        os.getenv(
            "ZEP_PROMPT_MAX_RECENT_EPISODES",
            str(DEFAULT_ZEP_PROMPT_MAX_RECENT_EPISODES),
        )
    )
    zep_prompt_recent_episode_max_chars = int(
        os.getenv(
            "ZEP_PROMPT_RECENT_EPISODE_MAX_CHARS",
            str(DEFAULT_ZEP_PROMPT_RECENT_EPISODE_MAX_CHARS),
        )
    )
    zep_prompt_max_edge_entities = int(
        os.getenv(
            "ZEP_PROMPT_MAX_EDGE_ENTITIES",
            str(DEFAULT_ZEP_PROMPT_MAX_EDGE_ENTITIES),
        )
    )
    zep_edge_entity_reference_mode = os.getenv(
        "ZEP_EDGE_ENTITY_REFERENCE_MODE",
        DEFAULT_ZEP_EDGE_ENTITY_REFERENCE_MODE,
    ).strip()
    zep_entity_resolve_concurrency = int(
        os.getenv(
            "ZEP_ENTITY_RESOLVE_CONCURRENCY",
            str(ZepWorkflowConfig().entity_resolve_concurrency),
        )
    )
    zep_entity_upsert_group_concurrency = int(
        os.getenv(
            "ZEP_ENTITY_UPSERT_GROUP_CONCURRENCY",
            str(ZepWorkflowConfig().entity_upsert_group_concurrency),
        )
    )
    zep_entity_summary_concurrency = int(
        os.getenv(
            "ZEP_ENTITY_SUMMARY_CONCURRENCY",
            str(ZepWorkflowConfig().entity_summary_concurrency),
        )
    )
    zep_edge_resolve_concurrency = int(
        os.getenv(
            "ZEP_EDGE_RESOLVE_CONCURRENCY",
            str(ZepWorkflowConfig().edge_resolve_concurrency),
        )
    )
    zep_edge_write_group_concurrency = int(
        os.getenv(
            "ZEP_EDGE_WRITE_GROUP_CONCURRENCY",
            str(ZepWorkflowConfig().edge_write_group_concurrency),
        )
    )
    try:
        bundle = build_zep_external_bundle(
            backend_config=backend_config,
            neo4j_driver=clients.neo4j_driver,
        )
        runtime = ZepLLMSemanticRuntime(
            client=llm_client,
            summary_client=summary_llm_client,
            max_message_chars=zep_prompt_message_max_chars,
            max_recent_episodes=zep_prompt_max_recent_episodes,
            max_recent_episode_chars=zep_prompt_recent_episode_max_chars,
            max_edge_entities=zep_prompt_max_edge_entities,
            edge_entity_reference_mode=zep_edge_entity_reference_mode,
        )
        workflow = ZepAddEpisodeWorkflow(
            config=ZepWorkflowConfig(
                entity_resolve_concurrency=zep_entity_resolve_concurrency,
                entity_upsert_group_concurrency=zep_entity_upsert_group_concurrency,
                entity_summary_concurrency=zep_entity_summary_concurrency,
                edge_resolve_concurrency=zep_edge_resolve_concurrency,
                edge_write_group_concurrency=zep_edge_write_group_concurrency,
            ),
            semantic_runtime=runtime,
            graph_store=bundle.graph_store,
        )
        group_id = f"zep:{ordered_messages[0].group_id}"
        results = []
        for event in replay_events:
            result = await workflow.add_episode(
                group_id=group_id,
                message=str(event.message.content),
                valid_at_ms=int(event.message.timestamp_ms),
            )
            results.append(result)
        if not results:
            raise RuntimeError("zep replay produced no result")
        result = results[-1]
        return {
            "episode_id": result.episode_id,
            "recalled_episode_count": result.recalled_episode_count,
            "processed_messages": len(results),
            "extracted_entity_count": sum(
                int(item.extracted_entity_count) for item in results
            ),
            "resolved_entity_count": sum(
                int(item.resolved_entity_count) for item in results
            ),
            "extracted_edge_count": sum(int(item.extracted_edge_count) for item in results),
            "added_edge_count": sum(int(item.added_edge_count) for item in results),
            "duplicate_edge_count": sum(
                int(item.duplicate_edge_count) for item in results
            ),
            "contradicted_edge_count": sum(
                int(item.contradicted_edge_count) for item in results
            ),
            "input_scope_policy": input_scope_policy,
            "max_scope_size": max(len(event.scope_messages) for event in replay_events),
        }
    finally:
        if summary_llm_client is not llm_client:
            await _close_component(summary_llm_client)
        await _close_component(llm_client)
        clients.close()


def _resolve_dataset_path(*, dataset_source: str, dataset_path: str | None) -> str:
    if dataset_path is not None and str(dataset_path).strip():
        return str(dataset_path).strip()
    if dataset_source == "longmemeval":
        env_value = os.getenv("LONGMEMEVAL_DATASET_PATH", "").strip()
        if env_value:
            return env_value
    if dataset_source == "locomo":
        env_value = os.getenv("LOCOMO_DATASET_PATH", "").strip()
        if env_value:
            return env_value
    raise ValueError(
        "dataset_path is required; set --dataset-path or "
        "LONGMEMEVAL_DATASET_PATH/LOCOMO_DATASET_PATH"
    )


async def _main_async(args: argparse.Namespace) -> Mapping[str, Any]:
    _load_repo_env(repo_root=REPO_ROOT)
    dataset_path = _resolve_dataset_path(
        dataset_source=args.dataset_source,
        dataset_path=args.dataset_path,
    )
    conversation = load_dataset_conversation(
        dataset_name=args.dataset_source,
        dataset_path=dataset_path,
        sample_index=args.sample_index,
        max_messages=args.max_messages,
    )
    dataset_messages = _build_dataset_messages(
        conversation=conversation,
        group_id_prefix=f"{conversation.dataset_name}",
    )
    benchmark_query = str(conversation.metadata.get("question", "")).strip()
    input_scope_policy = _resolve_input_scope_policy()
    llm_config = _build_llm_config()
    ollama_base = os.getenv("MEM0_EMBEDDER_OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL).strip()
    ollama_model = os.getenv("MEM0_EMBEDDER_MODEL", DEFAULT_OLLAMA_EMBED_MODEL).strip()
    ollama_timeout_s = int(os.getenv("OLLAMA_TIMEOUT_S", str(DEFAULT_OLLAMA_TIMEOUT_S)))
    ollama_max_retries = int(
        os.getenv("OLLAMA_MAX_RETRIES", str(DEFAULT_OLLAMA_MAX_RETRIES))
    )
    ollama_retry_base_delay_s = float(
        os.getenv(
            "OLLAMA_RETRY_BASE_DELAY_S",
            str(DEFAULT_OLLAMA_RETRY_BASE_DELAY_S),
        )
    )
    ollama_embed_max_input_chars = int(
        os.getenv(
            "OLLAMA_EMBED_MAX_INPUT_CHARS",
            str(DEFAULT_OLLAMA_EMBED_MAX_INPUT_CHARS),
        )
    )
    embedding_fn = _create_ollama_embedding_fn(
        base_url=ollama_base,
        model=ollama_model,
        timeout_s=ollama_timeout_s,
        max_retries=ollama_max_retries,
        retry_base_delay_s=ollama_retry_base_delay_s,
        max_input_chars=ollama_embed_max_input_chars,
    )
    output: Dict[str, Any] = {
        "dataset": {
            "source": conversation.dataset_name,
            "path": dataset_path,
            "sample_id": conversation.sample_id,
            "sample_index": args.sample_index,
            "max_messages": args.max_messages,
            "message_count": len(dataset_messages),
            "metadata": dict(conversation.metadata),
        },
        "llm": {
            "backend": llm_config.backend,
            "model": llm_config.model,
            "api_base": llm_config.api_base,
            "api_key_env": llm_config.api_key_env,
        },
        "embedding": {
            "provider": "ollama",
            "base_url": ollama_base,
            "model": ollama_model,
        },
        "replay": {
            "event_unit": "message",
            "input_scope_policy": input_scope_policy,
        },
    }
    selected = {item.strip() for item in args.workflows.split(",") if item.strip()}
    valid = {"evermemos", "mem0", "zep"}
    unknown = selected - valid
    if unknown:
        raise ValueError(f"unknown workflows: {sorted(unknown)!r}")
    if "evermemos" in selected:
        output["evermemos"] = await _run_evermemos_workflow(
            dataset_messages=dataset_messages,
            llm_config=llm_config,
            embedding_fn=embedding_fn,
        )
    if "mem0" in selected:
        if not benchmark_query:
            raise ValueError(
                "mem0 workflow requires non-empty dataset metadata.question as retrieval query"
            )
        output["mem0"] = await _run_mem0_workflow(
            dataset_messages=dataset_messages,
            benchmark_query=benchmark_query,
            llm_config=llm_config,
            embedding_fn=embedding_fn,
            input_scope_policy=input_scope_policy,
        )
    if "zep" in selected:
        output["zep"] = await _run_zep_workflow(
            dataset_messages=dataset_messages,
            llm_config=llm_config,
            input_scope_policy=input_scope_policy,
        )
    return output


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run real agent-memory workflows")
    parser.add_argument(
        "--workflows",
        default="evermemos,mem0,zep",
        help="comma separated workflows: evermemos,mem0,zep",
    )
    parser.add_argument(
        "--dataset-source",
        default=DEFAULT_DATASET_SOURCE,
        choices=["longmemeval", "locomo"],
        help="dataset source",
    )
    parser.add_argument(
        "--dataset-path",
        default=None,
        help="dataset file path; if absent, read LONGMEMEVAL_DATASET_PATH/LOCOMO_DATASET_PATH",
    )
    parser.add_argument(
        "--sample-index",
        type=int,
        default=DEFAULT_DATASET_SAMPLE_INDEX,
        help="dataset sample index",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=DEFAULT_DATASET_MAX_MESSAGES,
        help="max number of messages extracted from one sample",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="optional artifact output directory",
    )
    return parser.parse_args()


def _write_artifact(*, artifact_dir: str | None, output: Mapping[str, Any]) -> None:
    if artifact_dir is None:
        return
    path = pathlib.Path(artifact_dir)
    path.mkdir(parents=True, exist_ok=True)
    target = path / "workflow_result.json"
    target.write_text(
        json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> None:
    args = _parse_args()
    output = asyncio.run(_main_async(args))
    _write_artifact(artifact_dir=args.artifact_dir, output=output)
    print(json.dumps(output, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
