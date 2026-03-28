"""Source-aligned external backend config for EverMemOS workflow.

This module intentionally contains only pure configuration parsing and validation.
It does not import third-party database SDKs, so it can be imported in any
environment. Runtime wiring of concrete clients is handled outside this module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Sequence, Tuple


DEFAULT_MONGO_COLLECTION_CONVERSATION_STATUS = "conversation_status"
DEFAULT_MONGO_COLLECTION_CONVERSATION_DATA = "conversation_data"
DEFAULT_MONGO_COLLECTION_MEMCELLS = "memcells"
DEFAULT_MONGO_COLLECTION_EPISODIC_MEMORIES = "episodic_memories"
DEFAULT_MONGO_COLLECTION_FORESIGHT_RECORDS = "foresight_records"
DEFAULT_MONGO_COLLECTION_EVENT_LOG_RECORDS = "event_log_records"
DEFAULT_MONGO_COLLECTION_CLUSTER_STATES = "cluster_states"
DEFAULT_MONGO_COLLECTION_USER_PROFILES = "user_profiles"

DEFAULT_ES_INDEX_EPISODIC_MEMORIES = "episodic_memories"
DEFAULT_ES_INDEX_FORESIGHT_RECORDS = "foresight_records"
DEFAULT_ES_INDEX_EVENT_LOG_RECORDS = "event_log_records"

DEFAULT_MILVUS_COLLECTION_EPISODIC_MEMORIES = "episodic_memories"
DEFAULT_MILVUS_COLLECTION_FORESIGHT_RECORDS = "foresight_records"
DEFAULT_MILVUS_COLLECTION_EVENT_LOG_RECORDS = "event_log_records"

DEFAULT_MONGO_DATABASE = "evermemos"
DEFAULT_MILVUS_DATABASE = "default"
DEFAULT_MILVUS_EMBEDDING_DIM = 1536

DEFAULT_MONGO_ENV_PREFIX = "EVERMEMOS_MONGO_"
DEFAULT_ES_ENV_PREFIX = "EVERMEMOS_ES_"
DEFAULT_MILVUS_ENV_PREFIX = "EVERMEMOS_MILVUS_"


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


def _parse_csv_env(name: str, *, default: Sequence[str]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None:
        values = tuple(str(item).strip() for item in default if str(item).strip())
    else:
        values = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not values:
        raise ValueError(f"{name} must contain at least one value")
    return values


@dataclass(frozen=True)
class EverMemOSMongoCollections:
    """MongoDB collection names used by EverMemOS insertion/retrieval workflow."""

    conversation_status: str = DEFAULT_MONGO_COLLECTION_CONVERSATION_STATUS
    conversation_data: str = DEFAULT_MONGO_COLLECTION_CONVERSATION_DATA
    memcells: str = DEFAULT_MONGO_COLLECTION_MEMCELLS
    episodic_memories: str = DEFAULT_MONGO_COLLECTION_EPISODIC_MEMORIES
    foresight_records: str = DEFAULT_MONGO_COLLECTION_FORESIGHT_RECORDS
    event_log_records: str = DEFAULT_MONGO_COLLECTION_EVENT_LOG_RECORDS
    cluster_states: str = DEFAULT_MONGO_COLLECTION_CLUSTER_STATES
    user_profiles: str = DEFAULT_MONGO_COLLECTION_USER_PROFILES

    def __post_init__(self) -> None:
        for field_name in (
            "conversation_status",
            "conversation_data",
            "memcells",
            "episodic_memories",
            "foresight_records",
            "event_log_records",
            "cluster_states",
            "user_profiles",
        ):
            value = getattr(self, field_name)
            _require_non_empty(value, field_name=field_name)


@dataclass(frozen=True)
class EverMemOSElasticsearchIndexes:
    """Elasticsearch index names for searchable memory artifacts."""

    episodic_memories: str = DEFAULT_ES_INDEX_EPISODIC_MEMORIES
    foresight_records: str = DEFAULT_ES_INDEX_FORESIGHT_RECORDS
    event_log_records: str = DEFAULT_ES_INDEX_EVENT_LOG_RECORDS

    def __post_init__(self) -> None:
        for field_name in ("episodic_memories", "foresight_records", "event_log_records"):
            value = getattr(self, field_name)
            _require_non_empty(value, field_name=field_name)


@dataclass(frozen=True)
class EverMemOSMilvusCollections:
    """Milvus collection names for vector memory retrieval."""

    episodic_memories: str = DEFAULT_MILVUS_COLLECTION_EPISODIC_MEMORIES
    foresight_records: str = DEFAULT_MILVUS_COLLECTION_FORESIGHT_RECORDS
    event_log_records: str = DEFAULT_MILVUS_COLLECTION_EVENT_LOG_RECORDS

    def __post_init__(self) -> None:
        for field_name in ("episodic_memories", "foresight_records", "event_log_records"):
            value = getattr(self, field_name)
            _require_non_empty(value, field_name=field_name)


@dataclass(frozen=True)
class EverMemOSMongoConfig:
    """MongoDB connectivity config for EverMemOS workflow."""

    uri: str
    database: str = DEFAULT_MONGO_DATABASE
    collections: EverMemOSMongoCollections = field(default_factory=EverMemOSMongoCollections)

    def __post_init__(self) -> None:
        _require_non_empty(self.uri, field_name="mongo.uri")
        _require_non_empty(self.database, field_name="mongo.database")

    @classmethod
    def from_env(cls, *, prefix: str = DEFAULT_MONGO_ENV_PREFIX) -> "EverMemOSMongoConfig":
        """Build Mongo config from environment variables."""
        uri = os.getenv(f"{prefix}URI", "").strip()
        if not uri:
            raise ValueError(f"{prefix}URI is required")
        database = os.getenv(f"{prefix}DATABASE", DEFAULT_MONGO_DATABASE).strip()
        collections = EverMemOSMongoCollections(
            conversation_status=os.getenv(
                f"{prefix}COLLECTION_CONVERSATION_STATUS",
                DEFAULT_MONGO_COLLECTION_CONVERSATION_STATUS,
            ).strip(),
            conversation_data=os.getenv(
                f"{prefix}COLLECTION_CONVERSATION_DATA",
                DEFAULT_MONGO_COLLECTION_CONVERSATION_DATA,
            ).strip(),
            memcells=os.getenv(
                f"{prefix}COLLECTION_MEMCELLS",
                DEFAULT_MONGO_COLLECTION_MEMCELLS,
            ).strip(),
            episodic_memories=os.getenv(
                f"{prefix}COLLECTION_EPISODIC_MEMORIES",
                DEFAULT_MONGO_COLLECTION_EPISODIC_MEMORIES,
            ).strip(),
            foresight_records=os.getenv(
                f"{prefix}COLLECTION_FORESIGHT_RECORDS",
                DEFAULT_MONGO_COLLECTION_FORESIGHT_RECORDS,
            ).strip(),
            event_log_records=os.getenv(
                f"{prefix}COLLECTION_EVENT_LOG_RECORDS",
                DEFAULT_MONGO_COLLECTION_EVENT_LOG_RECORDS,
            ).strip(),
            cluster_states=os.getenv(
                f"{prefix}COLLECTION_CLUSTER_STATES",
                DEFAULT_MONGO_COLLECTION_CLUSTER_STATES,
            ).strip(),
            user_profiles=os.getenv(
                f"{prefix}COLLECTION_USER_PROFILES",
                DEFAULT_MONGO_COLLECTION_USER_PROFILES,
            ).strip(),
        )
        return cls(uri=uri, database=database, collections=collections)


@dataclass(frozen=True)
class EverMemOSElasticsearchConfig:
    """Elasticsearch connectivity config for EverMemOS workflow."""

    enabled: bool = True
    hosts: Tuple[str, ...] = field(default_factory=lambda: ("http://localhost:9200",))
    username: Optional[str] = None
    password: Optional[str] = None
    verify_certs: bool = True
    indexes: EverMemOSElasticsearchIndexes = field(default_factory=EverMemOSElasticsearchIndexes)

    def __post_init__(self) -> None:
        if not self.hosts:
            raise ValueError("elasticsearch.hosts must not be empty")
        for host in self.hosts:
            _require_non_empty(host, field_name="elasticsearch.host")
        if self.username is not None and not str(self.username).strip():
            raise ValueError("elasticsearch.username must be non-empty when provided")
        if self.password is not None and not str(self.password).strip():
            raise ValueError("elasticsearch.password must be non-empty when provided")

    @classmethod
    def from_env(cls, *, prefix: str = DEFAULT_ES_ENV_PREFIX) -> "EverMemOSElasticsearchConfig":
        """Build Elasticsearch config from environment variables."""
        enabled = _parse_bool_env(f"{prefix}ENABLED", default=True)
        hosts = _parse_csv_env(f"{prefix}HOSTS", default=("http://localhost:9200",))
        username = os.getenv(f"{prefix}USERNAME")
        password = os.getenv(f"{prefix}PASSWORD")
        verify_certs = _parse_bool_env(f"{prefix}VERIFY_CERTS", default=True)
        indexes = EverMemOSElasticsearchIndexes(
            episodic_memories=os.getenv(
                f"{prefix}INDEX_EPISODIC_MEMORIES",
                DEFAULT_ES_INDEX_EPISODIC_MEMORIES,
            ).strip(),
            foresight_records=os.getenv(
                f"{prefix}INDEX_FORESIGHT_RECORDS",
                DEFAULT_ES_INDEX_FORESIGHT_RECORDS,
            ).strip(),
            event_log_records=os.getenv(
                f"{prefix}INDEX_EVENT_LOG_RECORDS",
                DEFAULT_ES_INDEX_EVENT_LOG_RECORDS,
            ).strip(),
        )
        return cls(
            enabled=enabled,
            hosts=hosts,
            username=username.strip() if username is not None else None,
            password=password.strip() if password is not None else None,
            verify_certs=verify_certs,
            indexes=indexes,
        )


@dataclass(frozen=True)
class EverMemOSMilvusConfig:
    """Milvus connectivity config for EverMemOS workflow."""

    enabled: bool = True
    uri: str = "http://localhost:19530"
    token: Optional[str] = None
    database: str = DEFAULT_MILVUS_DATABASE
    embedding_dim: int = DEFAULT_MILVUS_EMBEDDING_DIM
    collections: EverMemOSMilvusCollections = field(default_factory=EverMemOSMilvusCollections)
    consistency_level: str = "Bounded"

    def __post_init__(self) -> None:
        _require_non_empty(self.uri, field_name="milvus.uri")
        _require_non_empty(self.database, field_name="milvus.database")
        _require_non_empty(self.consistency_level, field_name="milvus.consistency_level")
        if self.token is not None and not str(self.token).strip():
            raise ValueError("milvus.token must be non-empty when provided")
        if int(self.embedding_dim) <= 0:
            raise ValueError("milvus.embedding_dim must be > 0")

    @classmethod
    def from_env(cls, *, prefix: str = DEFAULT_MILVUS_ENV_PREFIX) -> "EverMemOSMilvusConfig":
        """Build Milvus config from environment variables."""
        enabled = _parse_bool_env(f"{prefix}ENABLED", default=True)
        uri = os.getenv(f"{prefix}URI", "http://localhost:19530").strip()
        token = os.getenv(f"{prefix}TOKEN")
        database = os.getenv(f"{prefix}DATABASE", DEFAULT_MILVUS_DATABASE).strip()
        embedding_dim_raw = os.getenv(f"{prefix}EMBEDDING_DIM", str(DEFAULT_MILVUS_EMBEDDING_DIM))
        try:
            embedding_dim = int(embedding_dim_raw)
        except ValueError as exc:
            raise ValueError(f"{prefix}EMBEDDING_DIM must be int") from exc
        consistency_level = os.getenv(f"{prefix}CONSISTENCY_LEVEL", "Bounded").strip()
        collections = EverMemOSMilvusCollections(
            episodic_memories=os.getenv(
                f"{prefix}COLLECTION_EPISODIC_MEMORIES",
                DEFAULT_MILVUS_COLLECTION_EPISODIC_MEMORIES,
            ).strip(),
            foresight_records=os.getenv(
                f"{prefix}COLLECTION_FORESIGHT_RECORDS",
                DEFAULT_MILVUS_COLLECTION_FORESIGHT_RECORDS,
            ).strip(),
            event_log_records=os.getenv(
                f"{prefix}COLLECTION_EVENT_LOG_RECORDS",
                DEFAULT_MILVUS_COLLECTION_EVENT_LOG_RECORDS,
            ).strip(),
        )
        return cls(
            enabled=enabled,
            uri=uri,
            token=token.strip() if token is not None else None,
            database=database,
            embedding_dim=embedding_dim,
            collections=collections,
            consistency_level=consistency_level,
        )


@dataclass(frozen=True)
class EverMemOSBackendConfig:
    """Top-level external backend config for source-aligned EverMemOS wiring."""

    mongo: EverMemOSMongoConfig
    elasticsearch: EverMemOSElasticsearchConfig = field(
        default_factory=EverMemOSElasticsearchConfig
    )
    milvus: EverMemOSMilvusConfig = field(default_factory=EverMemOSMilvusConfig)
    active_scenes: FrozenSet[str] = field(default_factory=lambda: frozenset({"assistant", "group_chat"}))

    def __post_init__(self) -> None:
        if not self.active_scenes:
            raise ValueError("active_scenes must not be empty")

    @classmethod
    def from_env(cls) -> "EverMemOSBackendConfig":
        """Build full backend config from environment variables."""
        mongo = EverMemOSMongoConfig.from_env()
        elasticsearch = EverMemOSElasticsearchConfig.from_env()
        milvus = EverMemOSMilvusConfig.from_env()
        scenes_raw = os.getenv("EVERMEMOS_ACTIVE_SCENES", "assistant,group_chat")
        active_scenes = frozenset(item.strip() for item in scenes_raw.split(",") if item.strip())
        if not active_scenes:
            raise ValueError("EVERMEMOS_ACTIVE_SCENES must contain at least one scene")
        return cls(
            mongo=mongo,
            elasticsearch=elasticsearch,
            milvus=milvus,
            active_scenes=active_scenes,
        )
