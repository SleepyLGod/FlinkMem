"""Source-aligned external backend config for Mem0 workflows.

This module keeps configuration parsing/validation independent from concrete SDK
imports. It mirrors Mem0's provider/config model while staying workflow-runtime
agnostic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


DEFAULT_MEM0_VERSION = "v1.1"
DEFAULT_MEM0_GRAPH_THRESHOLD = 0.7
DEFAULT_MEM0_LLM_MODEL = "gpt-4.1-nano-2025-04-14"
DEFAULT_MEM0_LLM_TEMPERATURE = 0.1
DEFAULT_MEM0_LLM_MAX_TOKENS = 2000
DEFAULT_MEM0_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_MEM0_EMBEDDING_DIM = 1536
DEFAULT_MEM0_VECTOR_COLLECTION = "mem0"

DEFAULT_MEM0_LLM_ENV_PREFIX = "MEM0_LLM_"
DEFAULT_MEM0_EMBEDDER_ENV_PREFIX = "MEM0_EMBEDDER_"
DEFAULT_MEM0_VECTOR_ENV_PREFIX = "MEM0_VECTOR_"
DEFAULT_MEM0_GRAPH_ENV_PREFIX = "MEM0_GRAPH_"
DEFAULT_MEM0_RUNTIME_ENV_PREFIX = "MEM0_RUNTIME_"

SUPPORTED_MEM0_VECTOR_PROVIDERS = frozenset(
    {
        "qdrant",
        "chroma",
        "pgvector",
        "pinecone",
        "mongodb",
        "milvus",
        "baidu",
        "cassandra",
        "neptune",
        "upstash_vector",
        "azure_ai_search",
        "azure_mysql",
        "redis",
        "valkey",
        "databricks",
        "elasticsearch",
        "vertex_ai_vector_search",
        "opensearch",
        "supabase",
        "weaviate",
        "faiss",
        "langchain",
        "s3_vectors",
        "turbopuffer",
    }
)

SUPPORTED_MEM0_GRAPH_PROVIDERS = frozenset(
    {
        "neo4j",
        "memgraph",
        "neptune",
        "neptunedb",
        "kuzu",
        "apache_age",
    }
)


def _require_non_empty(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _parse_bool_env(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false/1/0/yes/no/on/off")


def _parse_int_env(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be int") from exc


def _parse_float_env(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be float") from exc


@dataclass(frozen=True)
class Mem0LLMBackendConfig:
    """LLM backend config aligned with Mem0 `llm` section semantics."""

    provider: str = "openai"
    model: str = DEFAULT_MEM0_LLM_MODEL
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: float = DEFAULT_MEM0_LLM_TEMPERATURE
    max_tokens: int = DEFAULT_MEM0_LLM_MAX_TOKENS

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
    def from_env(cls, *, prefix: str = DEFAULT_MEM0_LLM_ENV_PREFIX) -> "Mem0LLMBackendConfig":
        """Build LLM config from environment variables."""
        provider = os.getenv(f"{prefix}PROVIDER", "openai").strip()
        model = os.getenv(f"{prefix}MODEL", DEFAULT_MEM0_LLM_MODEL).strip()
        api_key = os.getenv(f"{prefix}API_KEY")
        base_url = os.getenv(f"{prefix}BASE_URL")
        temperature = _parse_float_env(f"{prefix}TEMPERATURE", default=DEFAULT_MEM0_LLM_TEMPERATURE)
        max_tokens = _parse_int_env(f"{prefix}MAX_TOKENS", default=DEFAULT_MEM0_LLM_MAX_TOKENS)
        return cls(
            provider=provider,
            model=model,
            api_key=api_key.strip() if api_key is not None else None,
            base_url=base_url.strip() if base_url is not None else None,
            temperature=temperature,
            max_tokens=max_tokens,
        )


@dataclass(frozen=True)
class Mem0EmbedderBackendConfig:
    """Embedding backend config aligned with Mem0 `embedder` section semantics."""

    provider: str = "openai"
    model: str = DEFAULT_MEM0_EMBEDDING_MODEL
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    ollama_base_url: Optional[str] = None
    embedding_dim: int = DEFAULT_MEM0_EMBEDDING_DIM

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
        prefix: str = DEFAULT_MEM0_EMBEDDER_ENV_PREFIX,
    ) -> "Mem0EmbedderBackendConfig":
        """Build embedder config from environment variables."""
        provider = os.getenv(f"{prefix}PROVIDER", "openai").strip()
        model = os.getenv(f"{prefix}MODEL", DEFAULT_MEM0_EMBEDDING_MODEL).strip()
        api_key = os.getenv(f"{prefix}API_KEY")
        base_url = os.getenv(f"{prefix}BASE_URL")
        ollama_base_url = os.getenv(f"{prefix}OLLAMA_BASE_URL")
        embedding_dim = _parse_int_env(f"{prefix}EMBEDDING_DIM", default=DEFAULT_MEM0_EMBEDDING_DIM)
        return cls(
            provider=provider,
            model=model,
            api_key=api_key.strip() if api_key is not None else None,
            base_url=base_url.strip() if base_url is not None else None,
            ollama_base_url=ollama_base_url.strip() if ollama_base_url is not None else None,
            embedding_dim=embedding_dim,
        )


@dataclass(frozen=True)
class Mem0VectorStoreConfig:
    """Vector store config aligned with Mem0 vector provider registry."""

    enabled: bool = True
    provider: str = "qdrant"
    collection_name: str = DEFAULT_MEM0_VECTOR_COLLECTION
    embedding_dim: int = DEFAULT_MEM0_EMBEDDING_DIM
    endpoint: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[str] = None
    api_key: Optional[str] = None
    index_name: Optional[str] = None
    metric: Optional[str] = None
    namespace: Optional[str] = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.provider not in SUPPORTED_MEM0_VECTOR_PROVIDERS:
            raise ValueError(
                f"vector.provider={self.provider!r} is unsupported; "
                f"must be one of {sorted(SUPPORTED_MEM0_VECTOR_PROVIDERS)!r}"
            )
        _require_non_empty(self.collection_name, field_name="vector.collection_name")
        if int(self.embedding_dim) <= 0:
            raise ValueError("vector.embedding_dim must be > 0")
        if self.endpoint is not None and not str(self.endpoint).strip():
            raise ValueError("vector.endpoint must be non-empty when provided")
        if self.host is not None and not str(self.host).strip():
            raise ValueError("vector.host must be non-empty when provided")
        if self.path is not None and not str(self.path).strip():
            raise ValueError("vector.path must be non-empty when provided")
        if self.api_key is not None and not str(self.api_key).strip():
            raise ValueError("vector.api_key must be non-empty when provided")
        if self.index_name is not None and not str(self.index_name).strip():
            raise ValueError("vector.index_name must be non-empty when provided")
        if self.metric is not None and not str(self.metric).strip():
            raise ValueError("vector.metric must be non-empty when provided")
        if self.namespace is not None and not str(self.namespace).strip():
            raise ValueError("vector.namespace must be non-empty when provided")
        if self.port is not None and int(self.port) <= 0:
            raise ValueError("vector.port must be > 0 when provided")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = DEFAULT_MEM0_VECTOR_ENV_PREFIX,
    ) -> "Mem0VectorStoreConfig":
        """Build vector store config from environment variables."""
        enabled = _parse_bool_env(f"{prefix}ENABLED", default=True)
        provider = os.getenv(f"{prefix}PROVIDER", "qdrant").strip()
        collection_name = os.getenv(
            f"{prefix}COLLECTION_NAME",
            DEFAULT_MEM0_VECTOR_COLLECTION,
        ).strip()
        embedding_dim = _parse_int_env(f"{prefix}EMBEDDING_DIM", default=DEFAULT_MEM0_EMBEDDING_DIM)
        endpoint = os.getenv(f"{prefix}ENDPOINT")
        host = os.getenv(f"{prefix}HOST")
        port_raw = os.getenv(f"{prefix}PORT")
        port = int(port_raw) if port_raw is not None else None
        path = os.getenv(f"{prefix}PATH")
        api_key = os.getenv(f"{prefix}API_KEY")
        index_name = os.getenv(f"{prefix}INDEX_NAME")
        metric = os.getenv(f"{prefix}METRIC")
        namespace = os.getenv(f"{prefix}NAMESPACE")
        return cls(
            enabled=enabled,
            provider=provider,
            collection_name=collection_name,
            embedding_dim=embedding_dim,
            endpoint=endpoint.strip() if endpoint is not None else None,
            host=host.strip() if host is not None else None,
            port=port,
            path=path.strip() if path is not None else None,
            api_key=api_key.strip() if api_key is not None else None,
            index_name=index_name.strip() if index_name is not None else None,
            metric=metric.strip() if metric is not None else None,
            namespace=namespace.strip() if namespace is not None else None,
        )


@dataclass(frozen=True)
class Mem0GraphStoreConfig:
    """Graph store config aligned with Mem0 graph provider registry."""

    enabled: bool = False
    provider: str = "neo4j"
    threshold: float = DEFAULT_MEM0_GRAPH_THRESHOLD

    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    base_label: Optional[bool] = None

    endpoint: Optional[str] = None
    app_id: str = "Mem0"
    collection_name: Optional[str] = None

    db: Optional[str] = None

    host: Optional[str] = None
    port: Optional[int] = None
    graph_name: Optional[str] = None

    custom_prompt: Optional[str] = None

    def __post_init__(self) -> None:
        if self.provider not in SUPPORTED_MEM0_GRAPH_PROVIDERS:
            raise ValueError(
                f"graph.provider={self.provider!r} is unsupported; "
                f"must be one of {sorted(SUPPORTED_MEM0_GRAPH_PROVIDERS)!r}"
            )
        if not (0.0 <= float(self.threshold) <= 1.0):
            raise ValueError("graph.threshold must be within [0.0, 1.0]")
        _require_non_empty(self.app_id, field_name="graph.app_id")

        if self.custom_prompt is not None and not str(self.custom_prompt).strip():
            raise ValueError("graph.custom_prompt must be non-empty when provided")

        if not self.enabled:
            return

        if self.provider in {"neo4j", "memgraph"}:
            if not self.url or not self.username or not self.password:
                raise ValueError(
                    "graph provider neo4j/memgraph requires url, username, and password"
                )
        elif self.provider in {"neptune", "neptunedb"}:
            if not self.endpoint:
                raise ValueError("graph provider neptune/neptunedb requires endpoint")
            endpoint = str(self.endpoint)
            if not endpoint.startswith("neptune-db://") and not endpoint.startswith(
                "neptune-graph://"
            ):
                raise ValueError(
                    "graph.endpoint must start with neptune-db:// or neptune-graph://"
                )
        elif self.provider == "apache_age":
            if not self.database or not self.username or not self.password:
                raise ValueError(
                    "graph provider apache_age requires database, username, and password"
                )
        elif self.provider == "kuzu":
            if self.db is not None and not str(self.db).strip():
                raise ValueError("graph.db must be non-empty when provided")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = DEFAULT_MEM0_GRAPH_ENV_PREFIX,
    ) -> "Mem0GraphStoreConfig":
        """Build graph store config from environment variables."""
        enabled = _parse_bool_env(f"{prefix}ENABLED", default=False)
        provider = os.getenv(f"{prefix}PROVIDER", "neo4j").strip()
        threshold = _parse_float_env(f"{prefix}THRESHOLD", default=DEFAULT_MEM0_GRAPH_THRESHOLD)

        url = os.getenv(f"{prefix}URL")
        username = os.getenv(f"{prefix}USERNAME")
        password = os.getenv(f"{prefix}PASSWORD")
        database = os.getenv(f"{prefix}DATABASE")
        base_label_raw = os.getenv(f"{prefix}BASE_LABEL")
        base_label = None
        if base_label_raw is not None:
            base_label = _parse_bool_env(f"{prefix}BASE_LABEL", default=False)

        endpoint = os.getenv(f"{prefix}ENDPOINT")
        app_id = os.getenv(f"{prefix}APP_ID", "Mem0").strip()
        collection_name = os.getenv(f"{prefix}COLLECTION_NAME")

        db = os.getenv(f"{prefix}DB")

        host = os.getenv(f"{prefix}HOST")
        port_raw = os.getenv(f"{prefix}PORT")
        port = int(port_raw) if port_raw is not None else None
        graph_name = os.getenv(f"{prefix}GRAPH_NAME")

        custom_prompt = os.getenv(f"{prefix}CUSTOM_PROMPT")

        return cls(
            enabled=enabled,
            provider=provider,
            threshold=threshold,
            url=url.strip() if url is not None else None,
            username=username.strip() if username is not None else None,
            password=password.strip() if password is not None else None,
            database=database.strip() if database is not None else None,
            base_label=base_label,
            endpoint=endpoint.strip() if endpoint is not None else None,
            app_id=app_id,
            collection_name=collection_name.strip() if collection_name is not None else None,
            db=db.strip() if db is not None else None,
            host=host.strip() if host is not None else None,
            port=port,
            graph_name=graph_name.strip() if graph_name is not None else None,
            custom_prompt=custom_prompt.strip() if custom_prompt is not None else None,
        )


@dataclass(frozen=True)
class Mem0RuntimeConfig:
    """Runtime-scoped options mirroring Mem0 memory runtime config fields."""

    version: str = DEFAULT_MEM0_VERSION
    custom_fact_extraction_prompt: Optional[str] = None
    custom_update_memory_prompt: Optional[str] = None

    def __post_init__(self) -> None:
        _require_non_empty(self.version, field_name="runtime.version")
        if self.custom_fact_extraction_prompt is not None and not str(
            self.custom_fact_extraction_prompt
        ).strip():
            raise ValueError("runtime.custom_fact_extraction_prompt must be non-empty")
        if self.custom_update_memory_prompt is not None and not str(
            self.custom_update_memory_prompt
        ).strip():
            raise ValueError("runtime.custom_update_memory_prompt must be non-empty")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = DEFAULT_MEM0_RUNTIME_ENV_PREFIX,
    ) -> "Mem0RuntimeConfig":
        """Build runtime options from environment variables."""
        version = os.getenv(f"{prefix}VERSION", DEFAULT_MEM0_VERSION).strip()
        custom_fact_extraction_prompt = os.getenv(f"{prefix}CUSTOM_FACT_EXTRACTION_PROMPT")
        custom_update_memory_prompt = os.getenv(f"{prefix}CUSTOM_UPDATE_MEMORY_PROMPT")
        return cls(
            version=version,
            custom_fact_extraction_prompt=(
                custom_fact_extraction_prompt.strip()
                if custom_fact_extraction_prompt is not None
                else None
            ),
            custom_update_memory_prompt=(
                custom_update_memory_prompt.strip()
                if custom_update_memory_prompt is not None
                else None
            ),
        )


@dataclass(frozen=True)
class Mem0BackendConfig:
    """Top-level external config for Mem0 Basic/Graph workflow reconstruction."""

    llm: Mem0LLMBackendConfig = field(default_factory=Mem0LLMBackendConfig)
    embedder: Mem0EmbedderBackendConfig = field(default_factory=Mem0EmbedderBackendConfig)
    vector_store: Mem0VectorStoreConfig = field(default_factory=Mem0VectorStoreConfig)
    graph_store: Mem0GraphStoreConfig = field(default_factory=Mem0GraphStoreConfig)
    runtime: Mem0RuntimeConfig = field(default_factory=Mem0RuntimeConfig)

    @classmethod
    def from_env(cls) -> "Mem0BackendConfig":
        """Build full backend config from environment variables."""
        return cls(
            llm=Mem0LLMBackendConfig.from_env(),
            embedder=Mem0EmbedderBackendConfig.from_env(),
            vector_store=Mem0VectorStoreConfig.from_env(),
            graph_store=Mem0GraphStoreConfig.from_env(),
            runtime=Mem0RuntimeConfig.from_env(),
        )
