"""Tests for Zep external backend config parsing."""

from __future__ import annotations

# -- bootstrap pyflink.semantic_runtime import path --------------------------
import os  # noqa: E401,E402
import pathlib  # noqa: E401,E402

import pyflink as _pf  # noqa: E401,E402

_sem_runtime_src = pathlib.Path(__file__).resolve().parents[1]
_sem_runtime_dst = pathlib.Path(_pf.__file__).parent / "semantic_runtime"
if not _sem_runtime_dst.exists():
    os.symlink(_sem_runtime_src, _sem_runtime_dst)
# ---------------------------------------------------------------------------

from pyflink.semantic_runtime.runtime.workflows.agent_memory.zep.external_config import (
    ZepBackendConfig,
)


def test_zep_backend_config_from_env_parses_all_sections(monkeypatch) -> None:
    monkeypatch.setenv("ZEP_LLM_PROVIDER", "openai")
    monkeypatch.setenv("ZEP_LLM_MODEL", "gpt-4.1-mini")
    monkeypatch.setenv("ZEP_LLM_TEMPERATURE", "0.2")
    monkeypatch.setenv("ZEP_LLM_MAX_TOKENS", "1024")

    monkeypatch.setenv("ZEP_EMBEDDER_PROVIDER", "openai")
    monkeypatch.setenv("ZEP_EMBEDDER_MODEL", "text-embedding-3-small")
    monkeypatch.setenv("ZEP_EMBEDDER_EMBEDDING_DIM", "1536")

    monkeypatch.setenv("ZEP_GRAPH_URI", "bolt://localhost:7687")
    monkeypatch.setenv("ZEP_GRAPH_USERNAME", "neo4j")
    monkeypatch.setenv("ZEP_GRAPH_PASSWORD", "secret")
    monkeypatch.setenv("ZEP_GRAPH_DATABASE", "neo4j")

    cfg = ZepBackendConfig.from_env()

    assert cfg.llm.model == "gpt-4.1-mini"
    assert cfg.llm.max_tokens == 1024
    assert cfg.embedder.embedding_dim == 1536
    assert cfg.graph.uri == "bolt://localhost:7687"


def test_zep_backend_config_requires_graph_connection(monkeypatch) -> None:
    monkeypatch.delenv("ZEP_GRAPH_URI", raising=False)
    monkeypatch.delenv("ZEP_GRAPH_USERNAME", raising=False)
    monkeypatch.delenv("ZEP_GRAPH_PASSWORD", raising=False)

    try:
        ZepBackendConfig.from_env()
    except ValueError as exc:
        assert "ZEP_GRAPH_URI is required" in str(exc)
        return
    raise AssertionError("ZepBackendConfig.from_env must require graph URI")
