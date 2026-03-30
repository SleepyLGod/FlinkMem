"""Source-aligned external backend config for Zep/Graphiti workflow."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


DEFAULT_ZEP_LLM_MODEL = "gpt-4.1-nano-2025-04-14"
DEFAULT_ZEP_LLM_TEMPERATURE = 0.1
DEFAULT_ZEP_LLM_MAX_TOKENS = 2000
DEFAULT_ZEP_EMBEDDER_MODEL = "text-embedding-3-small"
DEFAULT_ZEP_EMBEDDING_DIM = 1536
DEFAULT_ZEP_NEO4J_DATABASE = "neo4j"

DEFAULT_ZEP_LLM_ENV_PREFIX = "ZEP_LLM_"
DEFAULT_ZEP_EMBEDDER_ENV_PREFIX = "ZEP_EMBEDDER_"
DEFAULT_ZEP_GRAPH_ENV_PREFIX = "ZEP_GRAPH_"


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _parse_float_env(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be float") from exc


def _parse_int_env(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be int") from exc


@dataclass(frozen=True)
class ZepLLMBackendConfig:
    """LLM backend config for Zep semantic extraction/resolution prompts."""

    provider: str = "openai"
    model: str = DEFAULT_ZEP_LLM_MODEL
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: float = DEFAULT_ZEP_LLM_TEMPERATURE
    max_tokens: int = DEFAULT_ZEP_LLM_MAX_TOKENS

    def __post_init__(self) -> None:
        _require_non_empty(self.provider, field_name="llm.provider")
        _require_non_empty(self.model, field_name="llm.model")
        if self.api_key is not None and not str(self.api_key).strip():
            raise ValueError("llm.api_key must be non-empty when provided")
        if self.base_url is not None and not str(self.base_url).strip():
            raise ValueError("llm.base_url must be non-empty when provided")
        if float(self.temperature) < 0.0:
            raise ValueError("llm.temperature must be >= 0")
        if int(self.max_tokens) <= 0:
            raise ValueError("llm.max_tokens must be > 0")

    @classmethod
    def from_env(cls, *, prefix: str = DEFAULT_ZEP_LLM_ENV_PREFIX) -> "ZepLLMBackendConfig":
        """Build LLM backend config from environment variables."""
        provider = os.getenv(f"{prefix}PROVIDER", "openai").strip()
        model = os.getenv(f"{prefix}MODEL", DEFAULT_ZEP_LLM_MODEL).strip()
        api_key = os.getenv(f"{prefix}API_KEY")
        base_url = os.getenv(f"{prefix}BASE_URL")
        temperature = _parse_float_env(f"{prefix}TEMPERATURE", default=DEFAULT_ZEP_LLM_TEMPERATURE)
        max_tokens = _parse_int_env(f"{prefix}MAX_TOKENS", default=DEFAULT_ZEP_LLM_MAX_TOKENS)
        return cls(
            provider=provider,
            model=model,
            api_key=api_key.strip() if api_key is not None else None,
            base_url=base_url.strip() if base_url is not None else None,
            temperature=temperature,
            max_tokens=max_tokens,
        )


@dataclass(frozen=True)
class ZepEmbedderBackendConfig:
    """Embedding backend config for Zep vector/hybrid retrieval."""

    provider: str = "openai"
    model: str = DEFAULT_ZEP_EMBEDDER_MODEL
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    ollama_base_url: Optional[str] = None
    embedding_dim: int = DEFAULT_ZEP_EMBEDDING_DIM

    def __post_init__(self) -> None:
        _require_non_empty(self.provider, field_name="embedder.provider")
        _require_non_empty(self.model, field_name="embedder.model")
        if self.api_key is not None and not str(self.api_key).strip():
            raise ValueError("embedder.api_key must be non-empty when provided")
        if self.base_url is not None and not str(self.base_url).strip():
            raise ValueError("embedder.base_url must be non-empty when provided")
        if self.ollama_base_url is not None and not str(self.ollama_base_url).strip():
            raise ValueError("embedder.ollama_base_url must be non-empty when provided")
        if int(self.embedding_dim) <= 0:
            raise ValueError("embedder.embedding_dim must be > 0")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = DEFAULT_ZEP_EMBEDDER_ENV_PREFIX,
    ) -> "ZepEmbedderBackendConfig":
        """Build embedding backend config from environment variables."""
        provider = os.getenv(f"{prefix}PROVIDER", "openai").strip()
        model = os.getenv(f"{prefix}MODEL", DEFAULT_ZEP_EMBEDDER_MODEL).strip()
        api_key = os.getenv(f"{prefix}API_KEY")
        base_url = os.getenv(f"{prefix}BASE_URL")
        ollama_base_url = os.getenv(f"{prefix}OLLAMA_BASE_URL")
        embedding_dim = _parse_int_env(f"{prefix}EMBEDDING_DIM", default=DEFAULT_ZEP_EMBEDDING_DIM)
        return cls(
            provider=provider,
            model=model,
            api_key=api_key.strip() if api_key is not None else None,
            base_url=base_url.strip() if base_url is not None else None,
            ollama_base_url=ollama_base_url.strip() if ollama_base_url is not None else None,
            embedding_dim=embedding_dim,
        )


@dataclass(frozen=True)
class ZepNeo4jConfig:
    """Neo4j connectivity config for Zep/Graphiti state store."""

    uri: str
    username: str
    password: str
    database: str = DEFAULT_ZEP_NEO4J_DATABASE

    entity_name_index: str = "node_name_and_summary"
    edge_fact_index: str = "edge_name_and_fact"

    def __post_init__(self) -> None:
        _require_non_empty(self.uri, field_name="graph.uri")
        _require_non_empty(self.username, field_name="graph.username")
        _require_non_empty(self.password, field_name="graph.password")
        _require_non_empty(self.database, field_name="graph.database")
        _require_non_empty(self.entity_name_index, field_name="graph.entity_name_index")
        _require_non_empty(self.edge_fact_index, field_name="graph.edge_fact_index")

    @classmethod
    def from_env(cls, *, prefix: str = DEFAULT_ZEP_GRAPH_ENV_PREFIX) -> "ZepNeo4jConfig":
        """Build Neo4j config from environment variables."""
        uri = os.getenv(f"{prefix}URI", "").strip()
        if not uri:
            raise ValueError(f"{prefix}URI is required")
        username = os.getenv(f"{prefix}USERNAME", "").strip()
        if not username:
            raise ValueError(f"{prefix}USERNAME is required")
        password = os.getenv(f"{prefix}PASSWORD", "").strip()
        if not password:
            raise ValueError(f"{prefix}PASSWORD is required")
        database = os.getenv(f"{prefix}DATABASE", DEFAULT_ZEP_NEO4J_DATABASE).strip()
        entity_name_index = os.getenv(f"{prefix}ENTITY_NAME_INDEX", "node_name_and_summary").strip()
        edge_fact_index = os.getenv(f"{prefix}EDGE_FACT_INDEX", "edge_name_and_fact").strip()
        return cls(
            uri=uri,
            username=username,
            password=password,
            database=database,
            entity_name_index=entity_name_index,
            edge_fact_index=edge_fact_index,
        )


@dataclass(frozen=True)
class ZepBackendConfig:
    """Top-level backend config for Zep/Graphiti workflow."""

    llm: ZepLLMBackendConfig
    embedder: ZepEmbedderBackendConfig
    graph: ZepNeo4jConfig

    @classmethod
    def from_env(cls) -> "ZepBackendConfig":
        """Build complete backend config from environment variables."""
        return cls(
            llm=ZepLLMBackendConfig.from_env(),
            embedder=ZepEmbedderBackendConfig.from_env(),
            graph=ZepNeo4jConfig.from_env(),
        )
