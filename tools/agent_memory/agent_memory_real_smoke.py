#!/usr/bin/env python3
"""Run real agent-memory workflows with dataset-driven inputs."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, Mapping, Sequence

import pyflink as _pf


def _bootstrap_semantic_runtime_path() -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    sem_runtime_src = repo_root / "flink-python" / "pyflink" / "semantic_runtime"
    sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
    if not sem_runtime_dst.exists():
        os.symlink(sem_runtime_src, sem_runtime_dst)
    return repo_root


REPO_ROOT = _bootstrap_semantic_runtime_path()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyflink.semantic_runtime.llm_client import LLMClientConfig, create_llm_client  # noqa: E402
from pyflink.semantic_runtime.runtime.workflows.agent_memory.common.contracts import (  # noqa: E402
    ConversationMessage,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos import (  # noqa: E402
    EverMemOSInsertionWorkflow,
    EverMemOSOperatorRuntime,
    EverMemOSOperatorRuntimeConfig,
    EverMemOSTopicAssigner,
    EverMemOSTopicAssignerConfig,
    EverMemOSWorkflowConfig,
    build_evermemos_external_bundle,
    create_evermemos_external_clients,
    EverMemOSBackendConfig,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0 import (  # noqa: E402
    Mem0BackendConfig,
    Mem0BasicConfig,
    Mem0BasicWorkflow,
    Mem0GraphConfig,
    Mem0GraphWorkflow,
    build_mem0_external_bundle,
    create_mem0_external_clients,
)
from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep import (  # noqa: E402
    ZepAddEpisodeWorkflow,
    ZepBackendConfig,
    ZepWorkflowConfig,
    build_zep_external_bundle,
    create_zep_external_clients,
)
from tools.agent_memory.dataset_loader import (  # noqa: E402
    DatasetConversation,
    load_dataset_conversation,
)
from tools.agent_memory.llm_semantic_runtimes import (  # noqa: E402
    Mem0BasicLLMSemanticRuntime,
    Mem0GraphLLMSemanticRuntime,
    ZepLLMSemanticRuntime,
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

DEFAULT_DATASET_SOURCE = "longmemeval"
DEFAULT_DATASET_SAMPLE_INDEX = 0
DEFAULT_DATASET_MAX_MESSAGES = 24


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
):
    normalized_base_url = str(base_url).rstrip("/")
    normalized_model = str(model).strip()
    if not normalized_model:
        raise ValueError("ollama embedding model must be non-empty")
    if int(timeout_s) <= 0:
        raise ValueError("ollama timeout_s must be > 0")

    def _embed(text: str) -> Sequence[float]:
        normalized_text = str(text).strip()
        if not normalized_text:
            raise ValueError("embedding text must be non-empty")
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
        try:
            with urllib.request.urlopen(request, timeout=int(timeout_s)) as response:
                response_text = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise RuntimeError(f"ollama embedding request failed: {exc}") from exc
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


async def _run_evermemos_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    llm_config: LLMClientConfig,
    embedding_fn,
) -> Mapping[str, Any]:
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
        first_group_id = dataset_messages[0].group_id
        result = await workflow.memorize(
            group_id=first_group_id,
            scene="assistant",
            new_messages=list(dataset_messages),
        )
        return {
            "status": result.status,
            "extracted_count": result.extracted_count,
            "memcell_id": result.memcell_id,
            "topic_id": result.topic_id,
            "profile_updated": result.profile_updated,
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
        topic_assigner.close()
        operator_runtime.close()
        clients.close()


async def _run_mem0_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    llm_config: LLMClientConfig,
    embedding_fn,
) -> Mapping[str, Any]:
    backend_config = Mem0BackendConfig.from_env()
    clients = create_mem0_external_clients(backend_config=backend_config)
    llm_client = create_llm_client(llm_config)
    try:
        bundle = build_mem0_external_bundle(
            backend_config=backend_config,
            embedding_fn=embedding_fn,
            neo4j_driver=clients.neo4j_driver,
        )
        message_texts = [message.content for message in dataset_messages]
        if not message_texts:
            raise ValueError("dataset_messages must be non-empty")
        group_id = f"mem0:{dataset_messages[0].group_id}"

        basic_runtime = Mem0BasicLLMSemanticRuntime(client=llm_client)
        basic_workflow = Mem0BasicWorkflow(
            config=Mem0BasicConfig(recall_backend="embedding"),
            semantic_runtime=basic_runtime,
            fact_store=bundle.fact_store,
            fact_searcher=bundle.fact_searcher,
        )
        basic_add_result = await basic_workflow.add(
            group_id=group_id,
            messages=message_texts,
        )
        basic_search_result = await basic_workflow.search(
            group_id=group_id,
            query=message_texts[-1],
            top_k=3,
        )

        if bundle.graph_store is None:
            raise ValueError("mem0 graph_store must be enabled for graph workflow run")
        if bundle.graph_entity_searcher is None:
            raise ValueError("mem0 graph_entity_searcher must be enabled")
        if bundle.graph_relation_searcher is None:
            raise ValueError("mem0 graph_relation_searcher must be enabled")

        graph_runtime = Mem0GraphLLMSemanticRuntime(client=llm_client)
        graph_workflow = Mem0GraphWorkflow(
            config=Mem0GraphConfig(
                entity_recall_backend="embedding",
                relation_recall_backend="embedding",
            ),
            semantic_runtime=graph_runtime,
            graph_store=bundle.graph_store,
            entity_searcher=bundle.graph_entity_searcher,
            relation_searcher=bundle.graph_relation_searcher,
        )
        graph_add_result = await graph_workflow.add(
            group_id=group_id,
            messages=message_texts,
        )
        graph_search_result = await graph_workflow.search(
            group_id=group_id,
            query=message_texts[-1],
            top_k=3,
        )

        return {
            "basic": {
                "extracted_fact_count": basic_add_result.extracted_fact_count,
                "added": basic_add_result.added,
                "updated": basic_add_result.updated,
                "deleted": basic_add_result.deleted,
                "noop": basic_add_result.noop,
                "search_count": len(basic_search_result.memories),
            },
            "graph": {
                "extracted_entity_count": graph_add_result.extracted_entity_count,
                "extracted_relation_count": graph_add_result.extracted_relation_count,
                "upserted_entities": graph_add_result.upserted_entities,
                "added_relations": graph_add_result.added_relations,
                "updated_relations": graph_add_result.updated_relations,
                "deleted_relations": graph_add_result.deleted_relations,
                "search_count": len(graph_search_result.relations),
            },
        }
    finally:
        llm_client.close()
        clients.close()


async def _run_zep_workflow(
    *,
    dataset_messages: Sequence[ConversationMessage],
    llm_config: LLMClientConfig,
) -> Mapping[str, Any]:
    backend_config = ZepBackendConfig.from_env()
    clients = create_zep_external_clients(backend_config=backend_config)
    llm_client = create_llm_client(llm_config)
    try:
        bundle = build_zep_external_bundle(
            backend_config=backend_config,
            neo4j_driver=clients.neo4j_driver,
        )
        runtime = ZepLLMSemanticRuntime(client=llm_client)
        workflow = ZepAddEpisodeWorkflow(
            config=ZepWorkflowConfig(),
            semantic_runtime=runtime,
            graph_store=bundle.graph_store,
        )
        group_id = f"zep:{dataset_messages[0].group_id}"
        episode_message = dataset_messages[-1].content
        valid_at_ms = int(dataset_messages[-1].timestamp_ms)
        result = await workflow.add_episode(
            group_id=group_id,
            message=episode_message,
            valid_at_ms=valid_at_ms,
        )
        return {
            "episode_id": result.episode_id,
            "recalled_episode_count": result.recalled_episode_count,
            "extracted_entity_count": result.extracted_entity_count,
            "resolved_entity_count": result.resolved_entity_count,
            "extracted_edge_count": result.extracted_edge_count,
            "added_edge_count": result.added_edge_count,
            "duplicate_edge_count": result.duplicate_edge_count,
            "contradicted_edge_count": result.contradicted_edge_count,
        }
    finally:
        llm_client.close()
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
    llm_config = _build_llm_config()
    ollama_base = os.getenv("MEM0_EMBEDDER_OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL).strip()
    ollama_model = os.getenv("MEM0_EMBEDDER_MODEL", DEFAULT_OLLAMA_EMBED_MODEL).strip()
    ollama_timeout_s = int(os.getenv("OLLAMA_TIMEOUT_S", str(DEFAULT_OLLAMA_TIMEOUT_S)))
    embedding_fn = _create_ollama_embedding_fn(
        base_url=ollama_base,
        model=ollama_model,
        timeout_s=ollama_timeout_s,
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
        output["mem0"] = await _run_mem0_workflow(
            dataset_messages=dataset_messages,
            llm_config=llm_config,
            embedding_fn=embedding_fn,
        )
    if "zep" in selected:
        output["zep"] = await _run_zep_workflow(
            dataset_messages=dataset_messages,
            llm_config=llm_config,
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
