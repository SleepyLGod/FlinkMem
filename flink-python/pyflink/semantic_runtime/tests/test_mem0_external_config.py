"""Unit tests for Mem0 external backend config parsing."""

from __future__ import annotations

import os

from pyflink.semantic_runtime.runtime.workflows.agent_memory.mem0.external_config import (
    Mem0BackendConfig,
    Mem0GraphStoreConfig,
    Mem0VectorStoreConfig,
)


def test_mem0_vector_config_from_env_parses_provider_and_dims() -> None:
    previous = dict(os.environ)
    try:
        os.environ["MEM0_VECTOR_PROVIDER"] = "qdrant"
        os.environ["MEM0_VECTOR_COLLECTION_NAME"] = "memories"
        os.environ["MEM0_VECTOR_EMBEDDING_DIM"] = "1024"
        os.environ["MEM0_VECTOR_ENDPOINT"] = "http://localhost:6333"

        config = Mem0VectorStoreConfig.from_env()

        assert config.provider == "qdrant"
        assert config.collection_name == "memories"
        assert config.embedding_dim == 1024
        assert config.endpoint == "http://localhost:6333"
    finally:
        os.environ.clear()
        os.environ.update(previous)


def test_mem0_vector_config_rejects_unsupported_provider() -> None:
    try:
        Mem0VectorStoreConfig(provider="unknown")
    except ValueError as exc:
        assert "unsupported" in str(exc)
        return
    raise AssertionError("Mem0VectorStoreConfig should reject unknown provider")


def test_mem0_graph_config_requires_credentials_when_enabled() -> None:
    previous = dict(os.environ)
    try:
        os.environ["MEM0_GRAPH_ENABLED"] = "true"
        os.environ["MEM0_GRAPH_PROVIDER"] = "neo4j"

        try:
            Mem0GraphStoreConfig.from_env()
        except ValueError as exc:
            assert "requires url, username, and password" in str(exc)
            return
        raise AssertionError("Mem0GraphStoreConfig should require neo4j credentials")
    finally:
        os.environ.clear()
        os.environ.update(previous)


def test_mem0_backend_config_from_env_builds_full_config() -> None:
    previous = dict(os.environ)
    try:
        os.environ["MEM0_LLM_PROVIDER"] = "openai"
        os.environ["MEM0_LLM_MODEL"] = "gpt-4.1-mini"
        os.environ["MEM0_EMBEDDER_PROVIDER"] = "openai"
        os.environ["MEM0_EMBEDDER_MODEL"] = "text-embedding-3-small"
        os.environ["MEM0_VECTOR_PROVIDER"] = "faiss"
        os.environ["MEM0_VECTOR_PATH"] = "/tmp/mem0-faiss"
        os.environ["MEM0_GRAPH_ENABLED"] = "false"

        config = Mem0BackendConfig.from_env()

        assert config.llm.model == "gpt-4.1-mini"
        assert config.vector_store.provider == "faiss"
        assert config.vector_store.path == "/tmp/mem0-faiss"
        assert config.graph_store.enabled is False
    finally:
        os.environ.clear()
        os.environ.update(previous)
