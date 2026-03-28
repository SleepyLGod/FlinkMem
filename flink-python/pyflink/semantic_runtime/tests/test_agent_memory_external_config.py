"""Unit tests for EverMemOS external backend config parsing."""

from __future__ import annotations

import os

from pyflink.semantic_runtime.runtime.workflows.agent_memory.evermemos.external_config import (
    EverMemOSBackendConfig,
    EverMemOSElasticsearchConfig,
    EverMemOSMilvusConfig,
    EverMemOSMongoConfig,
)


def test_mongo_config_from_env_reads_required_uri() -> None:
    previous = dict(os.environ)
    try:
        os.environ["EVERMEMOS_MONGO_URI"] = "mongodb://localhost:27017"
        os.environ["EVERMEMOS_MONGO_DATABASE"] = "evermemos_test"
        cfg = EverMemOSMongoConfig.from_env()
        assert cfg.uri == "mongodb://localhost:27017"
        assert cfg.database == "evermemos_test"
        assert cfg.collections.memcells == "memcells"
    finally:
        os.environ.clear()
        os.environ.update(previous)


def test_elasticsearch_and_milvus_parse_flags_and_hosts() -> None:
    previous = dict(os.environ)
    try:
        os.environ["EVERMEMOS_ES_ENABLED"] = "false"
        os.environ["EVERMEMOS_ES_HOSTS"] = "http://es-1:9200,http://es-2:9200"
        os.environ["EVERMEMOS_MILVUS_ENABLED"] = "true"
        os.environ["EVERMEMOS_MILVUS_URI"] = "http://milvus:19530"
        os.environ["EVERMEMOS_MILVUS_EMBEDDING_DIM"] = "1024"

        es_cfg = EverMemOSElasticsearchConfig.from_env()
        milvus_cfg = EverMemOSMilvusConfig.from_env()

        assert es_cfg.enabled is False
        assert es_cfg.hosts == ("http://es-1:9200", "http://es-2:9200")
        assert milvus_cfg.enabled is True
        assert milvus_cfg.uri == "http://milvus:19530"
        assert milvus_cfg.embedding_dim == 1024
    finally:
        os.environ.clear()
        os.environ.update(previous)


def test_backend_config_from_env_requires_mongo_uri() -> None:
    previous = dict(os.environ)
    try:
        os.environ["EVERMEMOS_MONGO_URI"] = "mongodb://localhost:27017"
        cfg = EverMemOSBackendConfig.from_env()
        assert cfg.mongo.uri == "mongodb://localhost:27017"
        assert "assistant" in cfg.active_scenes
    finally:
        os.environ.clear()
        os.environ.update(previous)
